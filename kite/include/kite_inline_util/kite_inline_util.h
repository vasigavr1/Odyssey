#ifndef KITE_INLINE_UTIL_H
#define KITE_INLINE_UTIL_H

//#include "kvs.h"
#include "hrd.h"


#include "generic_util.h"
#include "kvs_util.h"
#include "kite_debug_util.h"
#include "kite_config_util.h"
#include "inline_util.h"
#include "paxos_util.h"
#include "reserve_stations_util.h"
#include "communication_utility.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <config.h>




/* ---------------------------------------------------------------------------
//------------------------------ PULL NEW REQUESTS ----------------------------------
//---------------------------------------------------------------------------*/

static inline uint32_t batch_requests_to_KVS(uint16_t t_id,
                                             uint32_t trace_iter, trace_t *trace,
                                             trace_op_t *ops,
                                             p_ops_t *p_ops, kv_resp_t *resp,
                                             latency_info_t *latency_info,
                                             struct session_dbg *ses_dbg, uint16_t *last_session_,
                                             uint32_t *sizes_dbg_cntr)
{
  uint16_t writes_num = 0, reads_num = 0, op_i = 0, last_session = *last_session_;
  int working_session = -1;
  // if there are clients the "all_sessions_stalled" flag is not used,
  // so we need not bother checking it
  if (!ENABLE_CLIENTS && p_ops->all_sessions_stalled) {
    debug_all_sessions(ses_dbg, p_ops, t_id);
    return trace_iter;
  }
  for (uint16_t i = 0; i < SESSIONS_PER_THREAD; i++) {
    uint16_t sess_i = (uint16_t)((last_session + i) % SESSIONS_PER_THREAD);
    if (pull_request_from_this_session(p_ops->sess_info[sess_i].stalled, sess_i, t_id)) {
      working_session = sess_i;
      break;
    }
    else debug_sessions(ses_dbg, p_ops, sess_i, t_id);
  }
  //printf("working session = %d\n", working_session);
  if (ENABLE_CLIENTS) {
    if (working_session == -1) return trace_iter;
  }
  else if (ENABLE_ASSERTIONS ) assert(working_session != -1);

  bool passed_over_all_sessions = false;
  while (op_i < MAX_OP_BATCH && !passed_over_all_sessions) {
    if (fill_trace_op(p_ops, &ops[op_i], &trace[trace_iter], op_i, working_session, &writes_num,
                      &reads_num, ses_dbg, latency_info, sizes_dbg_cntr, t_id))
      break;
    // Find out next session to work on
    while (!pull_request_from_this_session(p_ops->sess_info[working_session].stalled,
                                           (uint16_t) working_session, t_id)) {
      debug_sessions(ses_dbg, p_ops, (uint32_t) working_session, t_id);
      MOD_INCR(working_session, SESSIONS_PER_THREAD);
      if (working_session == last_session) {
        passed_over_all_sessions = true;
        // If clients are used the condition does not guarantee that sessions are stalled
        if (!ENABLE_CLIENTS) p_ops->all_sessions_stalled = true;
        break;
      }
    }
    resp[op_i].type = EMPTY;
    op_i++;
    if (!ENABLE_CLIENTS) {
      trace_iter++;
      if (trace[trace_iter].opcode == NOP) trace_iter = 0;
    }
  }


  *last_session_ = (uint16_t) working_session;

  t_stats[t_id].cache_hits_per_thread += op_i;
  KVS_batch_op_trace(op_i, t_id, ops, resp, p_ops);
  //my_printf(cyan, "thread %d  adds %d/%d ops\n", t_id, op_i, MAX_OP_BATCH);
  for (uint16_t i = 0; i < op_i; i++) {
    // my_printf(green, "After: OP_i %u -> session %u \n", i, *(uint32_t *) &ops[i]);
    if (resp[i].type == KVS_MISS)  {
      my_printf(green, "KVS miss %u: bkt %u, server %u, tag %u \n", i,
                   ops[i].key.bkt, ops[i].key.server, ops[i].key.tag);
      assert(false);
      clean_up_on_KVS_miss(&ops[i], p_ops, latency_info, t_id);
      continue;
    }
    // check_version_after_batching_trace_to_cache(&ops[i], &resp[i], t_id);
    // Local reads
    if (resp[i].type == KVS_LOCAL_GET_SUCCESS) {
      //check_state_with_allowed_flags(2, interface[t_id].req_array[ops[i].session_id][ops[i].index_to_req_array].state, IN_PROGRESS_REQ);
      //assert(interface[t_id].req_array[ops[i].session_id][ops[i].index_to_req_array].state == IN_PROGRESS_REQ);
      signal_completion_to_client(ops[i].session_id, ops[i].index_to_req_array, t_id);
    }
    // Writes
    else if (resp[i].type == KVS_PUT_SUCCESS) {
      insert_write(p_ops, &ops[i], FROM_TRACE, 0, t_id);
      signal_completion_to_client(ops[i].session_id, ops[i].index_to_req_array, t_id);
    }
    // RMWS
    else if (ENABLE_RMWS && opcode_is_rmw(ops[i].opcode)) {
       insert_rmw(p_ops, &ops[i], t_id);
    }
    // KVS_GET_SUCCESS: Acquires, out-of-epoch reads, KVS_GET_TS_SUCCESS: Releases, out-of-epoch Writes
    else {
      check_state_with_allowed_flags(3, resp[i].type, KVS_GET_SUCCESS, KVS_GET_TS_SUCCESS);
      insert_read(p_ops, &ops[i], FROM_TRACE, t_id);
      if (ENABLE_ASSERTIONS && ops[i].opcode == KVS_OP_PUT) {
        p_ops->sess_info[ops[i].session_id].writes_not_yet_inserted++;
      }
    }
  }
  return trace_iter;
}

/* ---------------------------------------------------------------------------
//------------------------------ RMW FSM ----------------------------------
//---------------------------------------------------------------------------*/

// Worker inspects its local RMW entries
static inline void inspect_rmws(p_ops_t *p_ops, uint16_t t_id)
{
  for (uint16_t sess_i = 0; sess_i < SESSIONS_PER_THREAD; sess_i++) {
    loc_entry_t* loc_entry = &p_ops->prop_info->entry[sess_i];
    uint8_t state = loc_entry->state;
    if (state == INVALID_RMW) continue;
    if (ENABLE_ASSERTIONS) {
      assert(loc_entry->sess_id == sess_i);
      assert(p_ops->sess_info[sess_i].stalled);
    }

    /* =============== ACCEPTED ======================== */
    if (state == ACCEPTED) {
      check_sum_of_reps(loc_entry);
      //printf("reps %u \n", loc_entry->rmw_reps.tot_replies);
      if (loc_entry->rmw_reps.ready_to_inspect) {
        loc_entry->rmw_reps.inspected = true;
        inspect_accepts(p_ops, loc_entry, t_id);
        check_state_with_allowed_flags(7, (int) loc_entry->state, ACCEPTED, INVALID_RMW, RETRY_WITH_BIGGER_TS,
                                       NEEDS_KV_PTR, MUST_BCAST_COMMITS, MUST_BCAST_COMMITS_FROM_HELP);
        if (ENABLE_ASSERTIONS && loc_entry->rmw_reps.ready_to_inspect)
            assert(loc_entry->state == ACCEPTED && loc_entry->all_aboard);
      }
    }

    /* =============== PROPOSED ======================== */
    if (state == PROPOSED) {
      if (cannot_accept_if_unsatisfied_release(loc_entry, &p_ops->sess_info[sess_i])) {
        continue;
      }

      if (loc_entry->rmw_reps.ready_to_inspect) {
        loc_entry->stalled_reason = NO_REASON;
        // further responses for that broadcast of Propose must be disregarded;
        // in addition we do this before inspecting, so that if we broadcast accepts, they have a fresh l_id
        loc_entry->rmw_reps.inspected = true;
        advance_loc_entry_l_id(loc_entry, t_id);
        inspect_proposes(p_ops, loc_entry, t_id);
        check_state_with_allowed_flags(7, (int) loc_entry->state, INVALID_RMW, RETRY_WITH_BIGGER_TS,
                                       NEEDS_KV_PTR, ACCEPTED, MUST_BCAST_COMMITS, MUST_BCAST_COMMITS_FROM_HELP);
        if (ENABLE_ASSERTIONS) assert(!loc_entry->rmw_reps.ready_to_inspect);
        if (loc_entry->state != ACCEPTED) assert(loc_entry->rmw_reps.tot_replies == 0);
        else assert(loc_entry->rmw_reps.tot_replies == 1);
      }
      else {
        assert(loc_entry->rmw_reps.tot_replies < QUORUM_NUM);
        loc_entry->stalled_reason = STALLED_BECAUSE_NOT_ENOUGH_REPS;
      }
    }

    /* =============== BROADCAST COMMITS ======================== */
    if (state == MUST_BCAST_COMMITS || state == MUST_BCAST_COMMITS_FROM_HELP) {
      loc_entry_t *entry_to_commit =
        state == MUST_BCAST_COMMITS ? loc_entry : loc_entry->help_loc_entry;
      //bool is_commit_helping = loc_entry->helping_flag != NOT_HELPING;
      if (p_ops->virt_w_size < MAX_ALLOWED_W_SIZE) {
        if (state == MUST_BCAST_COMMITS_FROM_HELP && loc_entry->helping_flag == PROPOSE_NOT_LOCALLY_ACKED) {
          my_printf(green, "Wrkr %u sess %u will bcast commits for the latest committed RMW,"
                      " after learning its proposed RMW has already been committed \n",
                    t_id, loc_entry->sess_id);
        }
        insert_write(p_ops, (trace_op_t*) entry_to_commit, FROM_COMMIT, state, t_id);
        loc_entry->state = COMMITTED;
        continue;
      }
    }

    /* =============== RETRY ======================== */
    if (state == RETRY_WITH_BIGGER_TS) {
      take_kv_ptr_with_higher_TS(p_ops, loc_entry, false, t_id);
      check_state_with_allowed_flags(5, (int) loc_entry->state, INVALID_RMW, PROPOSED,
                                     NEEDS_KV_PTR, MUST_BCAST_COMMITS);
      if (loc_entry->state == PROPOSED) {
        insert_prop_to_read_fifo(p_ops, loc_entry, t_id);
      }
    }

    /* =============== NEEDS_KV_PTR ======================== */
    if (state == NEEDS_KV_PTR) {
      handle_needs_kv_ptr_state(p_ops, loc_entry, sess_i, t_id);
      check_state_with_allowed_flags(6, (int) loc_entry->state, INVALID_RMW, PROPOSED, NEEDS_KV_PTR,
                                     ACCEPTED, MUST_BCAST_COMMITS);
    }

  }
}


/* ---------------------------------------------------------------------------
//------------------------------ BROADCASTS ----------------------------------
//---------------------------------------------------------------------------*/
// Broadcast Writes
static inline void broadcast_writes(p_ops_t *p_ops,
                                    uint16_t credits[][MACHINE_NUM], struct hrd_ctrl_blk *cb,
                                    uint32_t *release_rdy_dbg_cnt, uint32_t *time_out_cnt,
                                    struct ibv_sge *w_send_sgl, struct ibv_send_wr *r_send_wr,
                                    struct ibv_send_wr *w_send_wr,
                                    uint64_t *w_br_tx, recv_info_t *ack_recv_info,
                                    recv_info_t *r_rep_recv_info,
                                    uint16_t t_id, uint32_t *outstanding_writes, uint64_t *expected_next_l_id)
{
  //printf("Worker %d bcasting writes \n", t_id);
  uint8_t vc = W_VC;
  uint16_t br_i = 0, mes_sent = 0, available_credits = 0;
  uint32_t bcast_pull_ptr = p_ops->w_fifo->bcast_pull_ptr;
  if (p_ops->w_fifo->bcast_size == 0) return;
  if (release_not_ready(p_ops, &p_ops->w_fifo->info[bcast_pull_ptr], (struct w_message *)
    &p_ops->w_fifo->w_message[bcast_pull_ptr], release_rdy_dbg_cnt, t_id))
    return;
  if (!check_bcast_credits(credits[vc], p_ops->q_info, &time_out_cnt[vc],
                           &available_credits, 1, t_id))
    return;
  if (ENABLE_ASSERTIONS) assert(available_credits <= W_CREDITS);

  while (p_ops->w_fifo->bcast_size > 0 && mes_sent < available_credits) {
    if (mes_sent >  0 &&
      release_not_ready(p_ops, &p_ops->w_fifo->info[bcast_pull_ptr], (struct w_message *)
        &p_ops->w_fifo->w_message[bcast_pull_ptr], release_rdy_dbg_cnt, t_id)) {
      break;
    }
    if (DEBUG_WRITES)
      printf("Wrkr %d has %u write bcasts to send credits %d\n",t_id, p_ops->w_fifo->bcast_size, available_credits);
    // Create the broadcast messages
    forge_w_wr(bcast_pull_ptr, p_ops, cb,  w_send_sgl, w_send_wr, w_br_tx, br_i, credits, vc, t_id);
    br_i++;
    struct w_message *w_mes = (struct w_message *) &p_ops->w_fifo->w_message[bcast_pull_ptr];
      uint8_t coalesce_num = w_mes->coalesce_num;
    debug_and_count_stats_when_broadcasting_writes(p_ops, bcast_pull_ptr, coalesce_num,
                                                   t_id, expected_next_l_id, br_i, outstanding_writes);
    p_ops->w_fifo->bcast_size -= coalesce_num;
    // This message has been sent, do not add other writes to it!
    if (p_ops->w_fifo->bcast_size == 0) reset_write_message(p_ops);
    mes_sent++;
    MOD_INCR(bcast_pull_ptr, W_FIFO_SIZE);
    if (br_i == MAX_BCAST_BATCH) {
      post_receives_for_r_reps_for_accepts(r_rep_recv_info, t_id);
      post_quorum_broadasts_and_recvs(ack_recv_info, MAX_RECV_ACK_WRS - ack_recv_info->posted_recvs,
                                      p_ops->q_info, br_i, *w_br_tx, w_send_wr, cb->dgram_qp[W_QP_ID],
                                      W_ENABLE_INLINING);
      br_i = 0;
    }
  }
  if (br_i > 0) {
    if (ENABLE_ASSERTIONS) assert(MAX_BCAST_BATCH > 1);
    post_receives_for_r_reps_for_accepts(r_rep_recv_info, t_id);
    post_quorum_broadasts_and_recvs(ack_recv_info, MAX_RECV_ACK_WRS - ack_recv_info->posted_recvs,
                                    p_ops->q_info, br_i, *w_br_tx, w_send_wr, cb->dgram_qp[W_QP_ID],
                                    W_ENABLE_INLINING);
  }

  p_ops->w_fifo->bcast_pull_ptr = bcast_pull_ptr;
  if (ENABLE_ASSERTIONS) assert(mes_sent <= available_credits && mes_sent <= W_CREDITS);
  if (mes_sent > 0) decrease_credits(credits, p_ops->q_info, mes_sent, vc);
}


// Broadcast Reads
static inline void broadcast_reads(p_ops_t *p_ops,
                                   uint16_t credits[][MACHINE_NUM], struct hrd_ctrl_blk *cb,
                                   uint32_t *credit_debug_cnt,
                                   uint32_t *time_out_cnt,
                                   struct ibv_sge *r_send_sgl, struct ibv_send_wr *r_send_wr,
                                   struct ibv_send_wr *w_send_wr,
                                   uint64_t *r_br_tx, recv_info_t *r_rep_recv_info,
                                   uint16_t t_id, uint32_t *outstanding_reads)
{
  uint8_t vc = R_VC;
  uint16_t br_i = 0, mes_sent = 0, available_credits = 0;
  uint32_t bcast_pull_ptr = p_ops->r_fifo->bcast_pull_ptr;

  if (p_ops->r_fifo->bcast_size == 0)  return;
  else if (!check_bcast_credits(credits[vc], p_ops->q_info, &time_out_cnt[vc],
                                &available_credits, 1,  t_id)) return;

  if (ENABLE_ASSERTIONS) assert(available_credits <= R_CREDITS);

  while (p_ops->r_fifo->bcast_size > 0 &&  mes_sent < available_credits) {
    if (DEBUG_READS)
      printf("Wrkr %d has %u read bcasts to send credits %d\n",t_id, p_ops->r_fifo->bcast_size, credits[R_VC][0]);
    // Create the broadcast messages
    forge_r_wr(bcast_pull_ptr, p_ops, p_ops->q_info, cb, r_send_sgl, r_send_wr, r_br_tx, br_i, credits, vc, t_id);
    br_i++;
    struct r_message * r_mes = (struct r_message *) &p_ops->r_fifo->r_message[bcast_pull_ptr];
      uint8_t coalesce_num = r_mes->coalesce_num;
    if (ENABLE_ASSERTIONS) {
      assert( p_ops->r_fifo->bcast_size >= coalesce_num);
      (*outstanding_reads) += coalesce_num;
    }
    p_ops->r_fifo->bcast_size -= coalesce_num;
    if (p_ops->r_fifo->bcast_size == 0) reset_read_message(p_ops);
    mes_sent++;
    MOD_INCR(bcast_pull_ptr, R_FIFO_SIZE);
    if (br_i == MAX_BCAST_BATCH) {
      post_quorum_broadasts_and_recvs(r_rep_recv_info, MAX_RECV_R_REP_WRS - r_rep_recv_info->posted_recvs,
                                      p_ops->q_info, br_i, *r_br_tx, r_send_wr, cb->dgram_qp[R_QP_ID],
                                      R_ENABLE_INLINING);
      br_i = 0;
    }
  }
  if (br_i > 0)
    post_quorum_broadasts_and_recvs(r_rep_recv_info, MAX_RECV_R_REP_WRS - r_rep_recv_info->posted_recvs,
                                    p_ops->q_info, br_i, *r_br_tx, r_send_wr, cb->dgram_qp[R_QP_ID],
                                    R_ENABLE_INLINING);
  p_ops->r_fifo->bcast_pull_ptr = bcast_pull_ptr;
  if (mes_sent > 0) decrease_credits(credits, p_ops->q_info, mes_sent, vc);
}


/* ---------------------------------------------------------------------------
//------------------------------ UNICASTS-------------------------------------
//---------------------------------------------------------------------------*/

// Send Read Replies
static inline void send_r_reps(p_ops_t *p_ops, struct hrd_ctrl_blk *cb,
                               struct ibv_send_wr *r_rep_send_wr, struct ibv_sge *r_rep_send_sgl,
                               recv_info_t *r_recv_info, recv_info_t *w_recv_info,
                               uint64_t *r_rep_tx,  uint16_t t_id)
{
  uint16_t mes_i = 0, accept_recvs_to_post = 0, read_recvs_to_post = 0;
  uint32_t pull_ptr = p_ops->r_rep_fifo->pull_ptr;
  struct ibv_send_wr *bad_send_wr;

  struct r_rep_fifo *r_rep_fifo = p_ops->r_rep_fifo;
  while (r_rep_fifo->total_size > 0) {
    struct r_rep_message *r_rep_mes = (struct r_rep_message *) &r_rep_fifo->r_rep_message[pull_ptr];
    // Create the r_rep messages
    forge_r_rep_wr(pull_ptr, mes_i, p_ops, cb, r_rep_send_sgl, r_rep_send_wr, r_rep_tx, t_id);
    uint8_t coalesce_num = r_rep_mes->coalesce_num;
    print_check_count_stats_when_sending_r_rep(r_rep_fifo, coalesce_num, mes_i, t_id);
    r_rep_fifo->total_size -= coalesce_num;
    r_rep_fifo->mes_size--;
    //r_reps_sent += coalesce_num;
    if (r_rep_mes->opcode == ACCEPT_REPLY)
      accept_recvs_to_post++;
    else if (r_rep_mes->opcode != ACCEPT_REPLY_NO_CREDITS)
      read_recvs_to_post++;
    MOD_INCR(pull_ptr, R_REP_FIFO_SIZE);
    mes_i++;
  }
  if (mes_i > 0) {
    if (read_recvs_to_post > 0) {
      if (DEBUG_READ_REPS) printf("Wrkr %d posting %d read recvs\n", t_id,  read_recvs_to_post);
      post_recvs_with_recv_info(r_recv_info, read_recvs_to_post);
    }
    if (accept_recvs_to_post > 0) {
      if (DEBUG_RMW) printf("Wrkr %d posting %d accept recvs\n", t_id,  accept_recvs_to_post);
      post_recvs_with_recv_info(w_recv_info, accept_recvs_to_post);
    }
    r_rep_send_wr[mes_i - 1].next = NULL;
    int ret = ibv_post_send(cb->dgram_qp[R_REP_QP_ID], &r_rep_send_wr[0], &bad_send_wr);
    if (ENABLE_ASSERTIONS) CPE(ret, "R_REP ibv_post_send error", ret);
  }
  r_rep_fifo->pull_ptr = pull_ptr;

}


// Send a batched ack that denotes the first local write id and the number of subsequent lids that are being acked
static inline void send_acks(struct ibv_send_wr *ack_send_wr,
                             uint64_t *sent_ack_tx,
                             struct hrd_ctrl_blk *cb, recv_info_t *w_recv_info,
                             ack_mes_t *acks, uint16_t t_id)
{
  uint8_t ack_i = 0, prev_ack_i = 0, first_wr = 0;
  struct ibv_send_wr *bad_send_wr;
  uint32_t recvs_to_post_num = 0;

  for (uint8_t m_i = 0; m_i < MACHINE_NUM; m_i++) {
    if (acks[m_i].opcode == OP_ACK) continue;
    checks_stats_prints_when_sending_acks(acks, m_i, t_id);
    acks[m_i].opcode = OP_ACK;

    selective_signaling_for_unicast(sent_ack_tx, ACK_SS_BATCH, ack_send_wr,
                                    m_i, cb->dgram_send_cq[ACK_QP_ID], true, "sending acks", t_id);
    if (ack_i > 0) {
      if (DEBUG_ACKS) my_printf(yellow, "Wrkr %u, ack %u points to ack %u \n", t_id, prev_ack_i, m_i);
      ack_send_wr[prev_ack_i].next = &ack_send_wr[m_i];
    }
    else first_wr = m_i;

    recvs_to_post_num += acks[m_i].credits;
    ack_i++;
    prev_ack_i = m_i;
  }
  // RECEIVES for writes
  if (recvs_to_post_num > 0) {
    post_recvs_with_recv_info(w_recv_info, recvs_to_post_num);
    checks_when_posting_write_receives(w_recv_info, recvs_to_post_num, ack_i);
  }
  // SEND the acks
  if (ack_i > 0) {
    if (DEBUG_ACKS) printf("send %u acks, last recipient %u, first recipient %u \n", ack_i, prev_ack_i, first_wr);
    ack_send_wr[prev_ack_i].next = NULL;
    int ret = ibv_post_send(cb->dgram_qp[ACK_QP_ID], &ack_send_wr[first_wr], &bad_send_wr);
    if (ENABLE_ASSERTIONS) CPE(ret, "ACK ibv_post_send error", ret);
  }
}



/* ---------------------------------------------------------------------------
//------------------------------ POLLING-------------------------------------
//---------------------------------------------------------------------------*/


// Poll for the write broadcasts
static inline void poll_for_writes(volatile w_mes_ud_t *incoming_ws,
                                   uint32_t *pull_ptr, p_ops_t *p_ops,
                                   struct ibv_cq *w_recv_cq, struct ibv_wc *w_recv_wc,
                                   recv_info_t *w_recv_info, ack_mes_t *acks,
                                   uint32_t *completed_but_not_polled_writes,
                                   uint16_t t_id)
{

  uint32_t polled_messages = 0, writes_for_kvs = 0;
  int completed_messages =
    find_how_many_write_messages_can_be_polled(w_recv_cq, w_recv_wc, w_recv_info,
                                               acks, completed_but_not_polled_writes, t_id);
  if (completed_messages <= 0) return;
  uint32_t buf_ptr = *pull_ptr;
  // Start polling
  while (polled_messages < completed_messages) {
    struct w_message *w_mes = (struct w_message*) &incoming_ws[buf_ptr].w_mes;
    check_the_polled_write_message(w_mes, buf_ptr, writes_for_kvs, t_id);
    print_polled_write_message_info(w_mes, buf_ptr, t_id);
    uint8_t w_num = w_mes->coalesce_num;
    check_state_with_allowed_flags(4, w_mes->opcode, ONLY_WRITES, ONLY_ACCEPTS, WRITES_AND_ACCEPTS);
    bool is_only_accepts = w_mes->opcode == ONLY_ACCEPTS;


    uint8_t writes_to_be_acked = 0, accepts = 0;
    uint32_t running_writes_for_kvs = writes_for_kvs;
    uint16_t byte_ptr = W_MES_HEADER;
    for (uint16_t i = 0; i < w_num; i++) {
      write_t *write = (write_t *)(((void *)w_mes) + byte_ptr);
      byte_ptr += get_write_size_from_opcode(write->opcode);
      check_a_polled_write(write, i, w_num, w_mes->opcode, t_id);
      handle_configuration_on_receiving_rel(write, t_id);
      if (ENABLE_ASSERTIONS) assert(write->opcode != ACCEPT_OP_BIT_VECTOR);

      if (write->opcode != NO_OP_RELEASE) {
        p_ops->ptrs_to_mes_ops[running_writes_for_kvs] = (void *) write; //(((void *) write) - 3); // align with trace_op
        if (write->opcode == ACCEPT_OP) {
          p_ops->ptrs_to_mes_headers[running_writes_for_kvs] = (struct r_message *) w_mes;
          p_ops->coalesce_r_rep[running_writes_for_kvs] = accepts > 0;
          raise_conf_bit_if_accept_signals_it((struct accept *) write, w_mes->m_id, t_id);
        }
        if (PRINT_LOGS && write->opcode == COMMIT_OP) {
          p_ops->ptrs_to_mes_headers[running_writes_for_kvs] = (struct r_message *) w_mes;
        }
        running_writes_for_kvs++;
      }
      if (write->opcode != ACCEPT_OP) writes_to_be_acked++;
      else accepts++;
    }

    if (ENABLE_ASSERTIONS) assert(accepts + writes_to_be_acked == w_num);
    // Make sure the writes of the message can be processed
    if (!is_only_accepts) {
      if (ENABLE_ASSERTIONS) assert(writes_to_be_acked > 0);
      if (!ack_bookkeeping(&acks[w_mes->m_id], writes_to_be_acked, w_mes->l_id, w_mes->m_id, t_id)) {
        (*completed_but_not_polled_writes) = completed_messages - polled_messages;
        //if (DEBUG_QUORUM)
          my_printf(yellow, "Wrkr %u leaves %u messages for the next polling round \n",
                        t_id, *completed_but_not_polled_writes);
        break;
      }
    }
    else if (ENABLE_ASSERTIONS) assert(w_mes->l_id == 0);

    writes_for_kvs = running_writes_for_kvs;
    count_stats_on_receiving_w_mes_reset_w_num(w_mes, w_num, t_id);
    MOD_INCR(buf_ptr, W_BUF_SLOTS);
    polled_messages++;
  }
  (*pull_ptr) = buf_ptr;

  if (writes_for_kvs > 0) {
    if (DEBUG_WRITES) my_printf(yellow, "Worker %u is going with %u writes to the kvs \n", t_id, writes_for_kvs);
    KVS_batch_op_updates((uint16_t) writes_for_kvs, t_id, (write_t **) p_ops->ptrs_to_mes_ops,
                         p_ops, 0, (uint32_t) MAX_INCOMING_W);
    if (DEBUG_WRITES) my_printf(yellow, "Worker %u propagated %u writes to the kvs \n", t_id, writes_for_kvs);
  }
}


// Poll for the r_rep broadcasts
static inline void poll_for_reads(volatile r_mes_ud_t *incoming_rs,
                                  uint32_t *pull_ptr, p_ops_t *p_ops,
                                  struct ibv_cq *r_recv_cq, struct ibv_wc *r_recv_wc,
                                  uint16_t t_id, uint32_t *dbg_counter)
{
  if (p_ops->r_rep_fifo->mes_size == R_REP_FIFO_SIZE) return;
  int completed_messages =  ibv_poll_cq(r_recv_cq, R_BUF_SLOTS, r_recv_wc);
  if (completed_messages <= 0) return;
  uint32_t index = *pull_ptr;
  uint32_t polled_messages = 0, polled_reads = 0;
  // Start polling
  while (polled_messages < completed_messages) {
    struct r_message *r_mes = (struct r_message*) &incoming_rs[index].r_mes;
    check_when_polling_for_reads(r_mes, index, polled_reads, t_id);
    uint8_t r_num = r_mes->coalesce_num;
    uint16_t byte_ptr = R_MES_HEADER;
    for (uint16_t i = 0; i < r_num; i++) {
      struct read *read = (struct read*)(((void *) r_mes) + byte_ptr);
      //printf("Receiving read opcode %u \n", read->opcode);
      bool is_propose = read->opcode == PROPOSE_OP;
      if (is_propose) {
        struct propose *prop = (struct propose *) read;
        check_state_with_allowed_flags(2, prop->opcode, PROPOSE_OP);
        p_ops->ptrs_to_mes_ops[polled_reads] = (void *) prop; //(((void *) prop) -3); //align with the kvs op
      }
      else {
        check_read_opcode_when_polling_for_reads(read, i, r_num, t_id);
        if (read->opcode == OP_ACQUIRE) {
          read->opcode =
            take_ownership_of_a_conf_bit(r_mes->l_id + i, (uint16_t) r_mes->m_id, false, t_id) ?
            (uint8_t) OP_ACQUIRE_FP : (uint8_t) OP_ACQUIRE;
        }
        if (read->opcode == OP_ACQUIRE_FLIP_BIT)
          raise_conf_bit_iff_owned(*(uint64_t *) &read->key, (uint16_t) r_mes->m_id, false, t_id);

        p_ops->ptrs_to_mes_ops[polled_reads] = (void *) read; //(((void *) read) - 3); //align with the kvs op

      }
      p_ops->ptrs_to_mes_headers[polled_reads] = r_mes;
      p_ops->coalesce_r_rep[polled_reads] = i > 0;
      polled_reads++;
      byte_ptr += get_read_size_from_opcode(read->opcode);
    }
    if (ENABLE_ASSERTIONS) r_mes->coalesce_num = 0;
    MOD_INCR(index, R_BUF_SLOTS);
    polled_messages++;
    if (ENABLE_ASSERTIONS)
      assert(polled_messages + p_ops->r_rep_fifo->mes_size < R_REP_FIFO_SIZE);
  }
  (*pull_ptr) = index;
  // Poll for the completion of the receives
  if (polled_messages > 0) {
    KVS_batch_op_reads(polled_reads, t_id, p_ops, 0, MAX_INCOMING_R);
    if (ENABLE_ASSERTIONS) dbg_counter[R_QP_ID] = 0;
  }
  else if (ENABLE_ASSERTIONS && p_ops->r_rep_fifo->mes_size == 0) dbg_counter[R_QP_ID]++;
}



// Apply the acks that refer to stored writes
static inline void apply_acks(p_ops_t *p_ops, uint16_t ack_num, uint32_t ack_ptr,
                              uint8_t ack_m_id, uint32_t *outstanding_writes,
                              uint64_t l_id, uint64_t pull_lid,
                              quorum_info_t *q_info,
                              latency_info_t *latency_info, uint16_t t_id)
{
  for (uint16_t ack_i = 0; ack_i < ack_num; ack_i++) {
    //printf("Checking my acks \n");
    check_ack_and_print(p_ops, ack_i, ack_ptr, ack_num, l_id, pull_lid, t_id);
    per_write_meta_t *w_meta = &p_ops->w_meta[ack_ptr];
    w_meta->acks_seen++;
    bool ack_m_id_found = false;
    if (ENABLE_ASSERTIONS) assert(w_meta->acks_expected >= REMOTE_QUORUM);

    for (uint8_t i = 0; i < w_meta->acks_expected; i++) {
      if (ack_m_id == w_meta->expected_ids[i]) {
        ack_m_id_found = true;
        w_meta->seen_expected[i] = true;
        break;
      }
    }
    if (w_meta->w_state == SENT_PUT || w_meta->w_state == SENT_COMMIT ||
        w_meta->w_state == SENT_RELEASE) {
      if (!ack_m_id_found) {
        my_printf(red, "Wrkr %u, ack_ptr %u/%u received ack from m_i %u, state %u, received/expected %u/%u "
                    "active-machines/acks-seen: \n",
                  t_id, ack_ptr, PENDING_WRITES, ack_m_id,
                  w_meta->w_state, w_meta->acks_seen, w_meta->acks_expected);
        for (uint8_t i = 0; i < w_meta->acks_expected; i++) {
          my_printf(red, "%u/%u \n", w_meta->expected_ids[i], w_meta->seen_expected[i]);
        }
        assert(ack_m_id_found);
      }
    }


    uint8_t w_state = w_meta->w_state;
//    printf("Wrkr %d valid ack %u/%u, from %u write at ptr %d is %u/%u \n",
//           t_id, ack_i, ack_num, ack_m_id, ack_ptr,
//           w_meta->acks_seen, w_meta->acks_expected);

    // If it's a quorum, the request has been completed -- but releases/writes/commits will
    // still hold a slot in the write FIFO until they see expected acks (or timeout)
    if (w_meta->acks_seen == REMOTE_QUORUM) {
      if (ENABLE_ASSERTIONS) (*outstanding_writes)--;
//      printf("Wrkr %d valid ack %u/%u, write at ptr %d is ready \n",
//         t_id, ack_i, ack_num,  ack_ptr);
      switch(w_state) {
        case SENT_PUT : break;
        case SENT_RELEASE:
          clear_after_release_quorum(p_ops, ack_ptr, t_id);
          if (MEASURE_LATENCY && t_id == LATENCY_THREAD && machine_id == LATENCY_MACHINE &&
              latency_info->measured_req_flag != NO_REQ &&
              p_ops->w_meta[ack_ptr].sess_id == latency_info->measured_sess_id)
            report_latency(latency_info);
          break;
        case SENT_COMMIT:
          act_on_quorum_of_commit_acks(p_ops, ack_ptr, t_id);
          break;
          // THE FOLLOWING ARE WAITING FOR A QUORUM
        case SENT_ACQUIRE:
          if (MEASURE_LATENCY && t_id == LATENCY_THREAD && machine_id == LATENCY_MACHINE &&
              latency_info->measured_req_flag != NO_REQ &&
              p_ops->w_meta[ack_ptr].sess_id == latency_info->measured_sess_id)
            report_latency(latency_info);
        case SENT_RMW_ACQ_COMMIT:
        case SENT_BIT_VECTOR:
        case SENT_NO_OP_RELEASE:
          p_ops->w_meta[ack_ptr].w_state += W_STATE_OFFSET;
          break;
        default:
          if (w_state >= READY_PUT && w_state <= READY_NO_OP_RELEASE)
            break;
          my_printf(red, "Wrkr %u state %u, ptr %u \n", t_id, w_state, ack_ptr);
          assert(false);
      }
    }

    // Free writes/releases/commits
    if (w_meta->acks_seen == w_meta->acks_expected) {
      //assert(w_meta->acks_seen == REM_MACH_NUM);
      if (complete_requests_that_wait_all_acks(&w_meta->w_state, ack_ptr, t_id))
        update_sess_info_with_fully_acked_write(p_ops, ack_ptr, t_id);
    }
    MOD_INCR(ack_ptr, PENDING_WRITES);
  }
}

// Worker polls for acks
static inline void poll_acks(volatile ack_mes_ud_t *incoming_acks, uint32_t *pull_ptr,
                             p_ops_t *p_ops,
                             uint16_t credits[][MACHINE_NUM],
                             struct ibv_cq * ack_recv_cq, struct ibv_wc *ack_recv_wc,
                             recv_info_t *ack_recv_info,
                             latency_info_t *latency_info,
                             uint16_t t_id, uint32_t *dbg_counter,
                             uint32_t *outstanding_writes)
{
  uint32_t index = *pull_ptr;
  uint32_t polled_messages = 0;
  int completed_messages =  ibv_poll_cq(ack_recv_cq, ACK_BUF_SLOTS, ack_recv_wc);
  //printf("Wrkr %u first time %d\n", t_id, completed_messages);
  if (completed_messages <= 0) return;
  while (polled_messages < completed_messages) {
    ack_mes_t *ack = (ack_mes_t *) &incoming_acks[index].ack;
    uint16_t ack_num = ack->ack_num;
    check_ack_message_count_stats(p_ops, ack, index, ack_num, t_id);

    MOD_INCR(index, ACK_BUF_SLOTS);
    polled_messages++;
    uint64_t l_id = ack->l_id;
    uint64_t pull_lid = p_ops->local_w_id; // l_id at the pull pointer
    uint32_t ack_ptr; // a pointer in the FIFO, from where ack should be added
    credits[W_VC][ack->m_id] += ack->credits;
    //if (t_id == 1) printf("Credits %u, %u, \n", credits[W_VC][ack->m_id], ack->credits);
    assert(credits[W_VC][ack->m_id] <= W_CREDITS);
    // if the pending write FIFO is empty it means the acks are for committed messages.
    if (p_ops->w_size == 0 ) {
      ack->opcode = INVALID_OPCODE;
      ack->ack_num = 0; continue;
    }
    if (pull_lid >= l_id) {
      if ((pull_lid - l_id) >= ack_num) {ack->opcode = 5;
        ack->ack_num = 0; continue;}
      ack_num -= (pull_lid - l_id);
      ack_ptr = p_ops->w_pull_ptr;
    }
    else { // l_id > pull_lid
      ack_ptr = (uint32_t) (p_ops->w_pull_ptr + (l_id - pull_lid)) % PENDING_WRITES;
    }
    // Apply the acks that refer to stored writes
    apply_acks(p_ops, ack_num, ack_ptr, ack->m_id,  outstanding_writes, l_id,
               pull_lid, p_ops->q_info,  latency_info, t_id);
    if (ENABLE_ASSERTIONS) assert(credits[W_VC][ack->m_id] <= W_CREDITS);
    ack->opcode = INVALID_OPCODE;
    ack->ack_num = 0;
  } // while

  *pull_ptr = index;
  if (polled_messages > 0) {
    if (ENABLE_ASSERTIONS) dbg_counter[ACK_QP_ID] = 0;
  }
  else {
    if (ENABLE_ASSERTIONS && (*outstanding_writes) > 0) dbg_counter[ACK_QP_ID]++;
    if (ENABLE_STAT_COUNTING && (*outstanding_writes) > 0) t_stats[t_id].stalled_ack++;
  }
  if (ENABLE_ASSERTIONS) assert(ack_recv_info->posted_recvs >= polled_messages);
  ack_recv_info->posted_recvs -= polled_messages;
}




//Poll for read replies
static inline void poll_for_read_replies(volatile r_rep_mes_ud_t *incoming_r_reps,
                                         uint32_t *pull_ptr, p_ops_t *p_ops,
                                         uint16_t credits[][MACHINE_NUM],
                                         struct ibv_cq *r_rep_recv_cq, struct ibv_wc *r_rep_recv_wc,
                                         recv_info_t *r_rep_recv_info, uint16_t t_id,
                                         uint32_t *outstanding_reads, uint32_t *debug_cntr)
{
  if (p_ops->r_rep_fifo->mes_size == R_REP_FIFO_SIZE) return;
  int completed_messages =  ibv_poll_cq(r_rep_recv_cq, R_REP_BUF_SLOTS, r_rep_recv_wc);
  if (completed_messages <= 0) return;
  uint32_t index = *pull_ptr;
  uint32_t polled_messages = 0;
  // Start polling
  while (polled_messages < completed_messages) {
    struct r_rep_message *r_rep_mes = (struct r_rep_message*) &incoming_r_reps[index].r_rep_mes;
    print_and_check_mes_when_polling_r_reps(r_rep_mes, index, t_id);
    bool is_propose = r_rep_mes->opcode == PROP_REPLY;
    bool is_accept = r_rep_mes->opcode == ACCEPT_REPLY ||
                     r_rep_mes->opcode == ACCEPT_REPLY_NO_CREDITS;
    if (r_rep_mes->opcode != ACCEPT_REPLY_NO_CREDITS)
      increase_credits_when_polling_r_reps(credits, is_accept, r_rep_mes->m_id, t_id);

    polled_messages++;
    MOD_INCR(index, R_REP_BUF_SLOTS);
    // If it is a reply to a propose/accept only call a different handler
    if (is_propose || is_accept) {
      handle_rmw_rep_replies(p_ops, r_rep_mes, is_accept, t_id);
      continue;
    }
    check_state_with_allowed_flags(3, r_rep_mes->opcode, READ_REPLY, READ_PROP_REPLY);
    r_rep_mes->opcode = INVALID_OPCODE; // a random meaningless opcode
    uint8_t r_rep_num = r_rep_mes->coalesce_num;
    // Find the request that the reply is referring to
    uint64_t l_id = r_rep_mes->l_id;
    uint64_t pull_lid = p_ops->local_r_id; // l_id at the pull pointer
    uint32_t r_ptr; // a pointer in the FIFO, from where r_rep refers to

    // if the pending read FIFO is empty it means the r_reps are for committed messages.
    if (find_the_r_ptr_rep_refers_to(&r_ptr, l_id, pull_lid, p_ops,
                                     r_rep_mes->opcode, r_rep_num,  t_id)) {
      if (ENABLE_ASSERTIONS) assert(r_rep_mes->opcode == READ_REPLY); // there are no rmw reps
      continue;
    }

    uint16_t byte_ptr = R_REP_MES_HEADER;
    int read_i = -1; // count non-rmw read replies
    for (uint16_t i = 0; i < r_rep_num; i++) {
      struct r_rep_big *r_rep = (struct r_rep_big *)(((void *) r_rep_mes) + byte_ptr);
      //if (r_rep->opcode > CARTS_EQUAL) printf("big opcode comes \n");
      check_a_polled_r_rep(r_rep, r_rep_mes, i, r_rep_num, t_id);
      byte_ptr += get_size_from_opcode(r_rep->opcode);
      bool is_rmw_rep = opcode_is_rmw_rep(r_rep->opcode);
      //printf("Wrkr %u, polling read %u/%u opcode %u irs_rmw %u\n",
      //       t_id, i, r_rep_num, r_rep->opcode, is_rmw_rep);
      if (!is_rmw_rep) {
        read_i++;
        if (handle_single_r_rep(r_rep, &r_ptr, l_id, pull_lid, p_ops, read_i, i, outstanding_reads, t_id))
          continue;
      }
      else handle_single_rmw_rep(p_ops, (struct rmw_rep_last_committed *) r_rep,
        (struct rmw_rep_message *) r_rep_mes, byte_ptr, is_accept, i, t_id);
    }
    if (ENABLE_STAT_COUNTING) {
      if (ENABLE_ASSERTIONS) t_stats[t_id].per_worker_r_reps_received[r_rep_mes->m_id] += r_rep_num;
      t_stats[t_id].received_r_reps += r_rep_num;
      t_stats[t_id].received_r_reps_mes_num++;
    }
  }
  (*pull_ptr) = index;
  // Poll for the completion of the receives
  if (polled_messages > 0) {
    if (ENABLE_ASSERTIONS) assert(r_rep_recv_info->posted_recvs >= polled_messages);
    r_rep_recv_info->posted_recvs -= polled_messages;
    if (ENABLE_ASSERTIONS) debug_cntr[R_REP_QP_ID] = 0;
  }
  else {
    if (ENABLE_ASSERTIONS && (*outstanding_reads) > 0) debug_cntr[R_REP_QP_ID]++;
    if (ENABLE_STAT_COUNTING && (*outstanding_reads) > 0) t_stats[t_id].stalled_r_rep++;
  }
}



/* ---------------------------------------------------------------------------
//------------------------------ COMMITTING-------------------------------------
//---------------------------------------------------------------------------*/


// Handle acked reads: trigger second round if needed, update the KVS if needed
// Handle the first round of Lin Writes
// Increment the epoch_id after an acquire that learnt the node has missed messages
static inline void commit_reads(p_ops_t *p_ops,
                                latency_info_t * latency_info, uint16_t t_id)
{
  uint32_t pull_ptr = p_ops->r_pull_ptr;
  uint16_t writes_for_cache = 0;
  // Acquire needs to have a second READ round irrespective of the
  // timestamps if it is charged with flipping a bit
  bool acq_second_round_to_flip_bit;
  // A relaxed read need not do a second round ever, it does not need to quoromize a value before reading it
  // An acquire must have a second round trip if the timestamp has not been seen by a quorum
  // i.e. if it has been seen locally but not from a REMOTE_QUORUM
  // or has not been seen locally and has not been seen by a full QUORUM
  // or if it is the first round of a release or an out-of-epoch write
  bool insert_write_flag;

  // write_local_kvs : Write the local KVS if the base_ts has not been seen locally
  // or if it is an out-of-epoch write (but NOT a Release!!)
  bool write_local_kvs;

  // Signal completion before going to the KVS on an Acquire that needs not go to the KVS
  bool signal_completion;
  // Signal completion after going to the KVS on an Acquire that needs to go to the KVS but does not need to be sent out (!!),
  // on any out-of epoch write, and an out-of-epoch read that needs to go to the KVS
  bool signal_completion_after_kvs_write;


  /* Because it's possible for a read to insert another read i.e OP_ACQUIRE->OP_ACQUIRE_FLIP_BIT
   * we need to make sure that even if all requests do that, the fifo will have enough space to:
   * 1) not deadlock and 2) not overwrite a read_info that will later get taken to the kvs
   * That means that the fifo must have free slots equal to SESSION_PER_THREADS because
   * this many acquires can possibly exist in the fifo*/
  if (ENABLE_ASSERTIONS) assert(p_ops->virt_r_size < PENDING_READS);
  while(p_ops->r_state[pull_ptr] == READY) {
    r_info_t *read_info = &p_ops->read_info[pull_ptr];
    //set the flags for each read
    set_flags_before_committing_a_read(read_info, &acq_second_round_to_flip_bit, &insert_write_flag,
                                       &write_local_kvs,
                                       &signal_completion, &signal_completion_after_kvs_write, t_id);
    checks_when_committing_a_read(p_ops, pull_ptr, acq_second_round_to_flip_bit, insert_write_flag,
                                  write_local_kvs, signal_completion, signal_completion_after_kvs_write, t_id);

    // Break condition: this read cannot be processed, and thus no subsequent read will be processed
    if (is_there_buffer_space_to_commmit_the_read(p_ops, insert_write_flag,
                                                  write_local_kvs, writes_for_cache))
      break;

    //CACHE: Reads that need to go to kvs
    if (write_local_kvs)
      read_commit_bookkeeping_to_write_local_kvs(p_ops, read_info,
                                                 pull_ptr, &writes_for_cache);

    //INSERT WRITE: Reads that need to be converted to writes: second round of read/acquire or
    // Writes whose first round is a read: out-of-epoch writes/releases
    if (insert_write_flag)
      read_commit_bookkeeping_to_insert_write(p_ops, read_info,
                                              pull_ptr, t_id);

    // FAULT_TOLERANCE: In the off chance that the acquire needs a second round for fault tolerance
    if (unlikely(acq_second_round_to_flip_bit))
      read_commit_spawn_flip_bit_message(p_ops, read_info,
                                         pull_ptr, t_id);

    // SESSION: Acquires that wont have a second round and thus must free the session
    if (!insert_write_flag && (read_info->opcode == OP_ACQUIRE))
      read_commit_acquires_free_sess(p_ops, pull_ptr, latency_info, t_id);

    // COMPLETION: Signal completion for reads/acquires that need not write the local KVS or
    // have a second write round (applicable only for acquires)
    read_commit_complete_and_empty_read_info(p_ops, read_info, signal_completion,
                                             signal_completion_after_kvs_write,
                                             pull_ptr, t_id);
    MOD_INCR(pull_ptr, PENDING_READS);
  }
  p_ops->r_pull_ptr = pull_ptr;
  if (writes_for_cache > 0)
    KVS_batch_op_first_read_round(writes_for_cache, t_id, (r_info_t **) p_ops->ptrs_to_mes_ops,
                                  p_ops, 0, (uint32_t) MAX_INCOMING_R);
}



// Remove writes that have seen all acks
static inline void remove_writes(p_ops_t *p_ops, latency_info_t *latency_info,
                                 uint16_t t_id)
{
  while(p_ops->w_meta[p_ops->w_pull_ptr].w_state >= READY_PUT) {
    p_ops->full_w_q_fifo = 0;
    uint32_t w_pull_ptr = p_ops->w_pull_ptr;
    per_write_meta_t *w_meta = &p_ops->w_meta[p_ops->w_pull_ptr];
    uint8_t w_state = w_meta->w_state;
    if (ENABLE_ASSERTIONS && EMULATE_ABD)
      assert(w_state == READY_RELEASE || w_state == READY_ACQUIRE);
    //if (DEBUG_ACKS)
    //  my_printf(green, "Wkrk %u freeing write at pull_ptr %u, w_size %u, w_state %d, session %u, local_w_id %lu, acks seen %u \n",
    //               g_id, p_ops->w_pull_ptr, p_ops->w_size, p_ops->w_state[p_ops->w_pull_ptr],
    //               p_ops->w_session_id[p_ops->w_pull_ptr], p_ops->local_w_id, p_ops->acks_seen[p_ops->w_pull_ptr]);
    //if (t_id == 1) printf("Wrkr %u Clearing state %u ptr %u \n", t_id, w_state, p_ops->w_pull_ptr);
    uint32_t sess_id = w_meta->sess_id;
    if (ENABLE_ASSERTIONS) assert(sess_id < SESSIONS_PER_THREAD);
    sess_info_t *sess_info = &p_ops->sess_info[sess_id];

    if(w_state == READY_RMW_ACQ_COMMIT || w_state == READY_ACQUIRE) {
      if (!sess_info->stalled)
        printf("state %u ptr %u \n", w_state, p_ops->w_pull_ptr);
      // Releases, and Acquires/RMW-Acquires that needed a "write" round complete here
      signal_completion_to_client(sess_id, p_ops->w_index_to_req_array[w_pull_ptr], t_id);
      sess_info->stalled = false;
      p_ops->all_sessions_stalled = false;
    }

    // This case is tricky because in order to remove the release we must add another release
    // but if the queue was full that would deadlock, therefore we must remove the write before inserting
    // the second round of the release
    if (unlikely((w_state == READY_BIT_VECTOR || w_state == READY_NO_OP_RELEASE))) {
      commit_first_round_of_release_and_spawn_the_second (p_ops, t_id);
    }

    //  Free the write fifo entry
    if (w_state == READY_RELEASE) {
      if (ENABLE_ASSERTIONS) assert(p_ops->virt_w_size >= 2);
      p_ops->virt_w_size -= 2;
    }
    else {
      p_ops->virt_w_size--;
      //my_printf(yellow, "Decreasing virt_w_size %u at %u, state %u \n",
       //             p_ops->virt_w_size, w_pull_ptr, w_state);
    }
    p_ops->w_size--;
    w_meta->w_state = INVALID;
    w_meta->acks_seen = 0;
    p_ops->local_w_id++;
    memset(w_meta->seen_expected, 0, REM_MACH_NUM);
    MOD_INCR(p_ops->w_pull_ptr, PENDING_WRITES);
  } // while loop

  attempt_to_free_partially_acked_write(p_ops, t_id);
  // check_after_removing_writes(p_ops, t_id);
}



#endif /* KITE_INLINE_UTIL_H */

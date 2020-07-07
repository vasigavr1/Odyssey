//
// Created by vasilis on 01/07/20.
//

#ifndef KITE_ZK_RESERVATION_STATIONS_UTIL_H_H
#define KITE_ZK_RESERVATION_STATIONS_UTIL_H_H

#include <inline_util.h>
#include "latency_util.h"
#include "generic_inline_util.h"
#include "zk_debug_util.h"

/*---------------------------------------------------------------------
 * -----------------------POLL COMMITS------------------------------
 * ---------------------------------------------------------------------*/

static inline void flr_increases_write_credits(p_writes_t *p_writes,
                                               uint16_t com_ptr,
                                               struct fifo *remote_w_buf,
                                               uint16_t *credits,
                                               uint8_t flr_id,
                                               uint16_t t_id)
{
  if (p_writes->flr_id[com_ptr] == flr_id) {
    (*credits) += remove_from_the_mirrored_buffer(remote_w_buf,
                                                  1, t_id, 0,
                                                  LEADER_W_BUF_SLOTS);
    if (DEBUG_WRITES)
      my_printf(yellow, "Received a credit, credits: %u \n", *credits);
  }
  if (ENABLE_ASSERTIONS) assert(*credits <= W_CREDITS);
}

static inline bool zk_write_not_ready(zk_com_mes_t *com,
                                      uint16_t com_ptr,
                                      uint16_t com_i,
                                      uint16_t com_num,
                                      p_writes_t *p_writes,
                                      uint16_t t_id)
{
  if (DEBUG_COMMITS)
    printf("Flr %d valid com %u/%u write at ptr %d with g_id %lu is ready \n",
           t_id, com_i, com_num,  com_ptr, p_writes->g_id[com_ptr]);
  // it may be that a commit refers to a subset of writes that
  // we have seen and acked, and a subset not yet seen or acked,
  // We need to commit the seen subset to avoid a deadlock
  bool wrap_around = com->l_id + com_i - p_writes->local_w_id >= FLR_PENDING_WRITES;
  if (wrap_around) assert(com_ptr == p_writes->pull_ptr);


  if (wrap_around || p_writes->w_state[com_ptr] != SENT) {
    //printf("got here \n");
    com->com_num -= com_i;
    com->l_id += com_i;
    if (ENABLE_STAT_COUNTING)  t_stats[t_id].received_coms += com_i;
    uint16_t imaginary_com_ptr = (uint16_t) ((p_writes->pull_ptr +
                                 (com->l_id - p_writes->local_w_id)) % FLR_PENDING_WRITES);
    if(com_ptr != imaginary_com_ptr) {
      printf("com ptr %u/%u, com->l_id %lu, pull_lid %lu, pull_ptr %u, flr pending writes %d \n",
             com_ptr, imaginary_com_ptr, com->l_id, p_writes->local_w_id, p_writes->pull_ptr, FLR_PENDING_WRITES);
      assert(false);
    }
    return true;
  }
  return false;
}

/*---------------------------------------------------------------------
 * -----------------------POLL PREPARES------------------------------
 * ---------------------------------------------------------------------*/

static inline void fill_p_writes_entry(p_writes_t *p_writes,
                                       zk_prepare_t *prepare, uint8_t flr_id,
                                       uint16_t t_id)
{
  uint32_t push_ptr = p_writes->push_ptr;
  p_writes->ptrs_to_ops[push_ptr] = prepare;
  p_writes->g_id[push_ptr] = prepare->g_id;
  p_writes->flr_id[push_ptr] = prepare->flr_id;
  p_writes->is_local[push_ptr] = prepare->flr_id == flr_id;
  p_writes->session_id[push_ptr] = prepare->sess_id; //not useful if not local
  p_writes->w_state[push_ptr] = VALID;
}

/*---------------------------------------------------------------------
 * -----------------------PROPAGATING UPDATES------------------------------
 * ---------------------------------------------------------------------*/



static inline void
flr_increase_counter_if_waiting_for_commit(p_writes_t *p_writes,
                                           uint64_t committed_g_id,
                                           uint16_t t_id)
{
  if (ENABLE_STAT_COUNTING) {
    if ((p_writes->g_id[p_writes->pull_ptr] == committed_g_id + 1) &&
        (p_writes->w_state[p_writes->pull_ptr] == SENT))
    t_stats[t_id].stalled_com_credit++;
  }
}


static inline bool is_expected_g_id_ready(p_writes_t *p_writes,
                                          uint64_t *committed_g_id,
                                          uint16_t *update_op_i,
                                          uint32_t *dbg_counter,
                                          int pending_writes,
                                          protocol_t protocol,
                                          uint16_t t_id)
{
  if (!DISABLE_GID_ORDERING) {
    if (p_writes->g_id[p_writes->pull_ptr] != (*committed_g_id) + 1) {
      if (ENABLE_ASSERTIONS) {
        if((*committed_g_id) >= p_writes->g_id[p_writes->pull_ptr]) {
          my_printf(red, "Committed g_id/expected %lu/%lu \n",
                    (*committed_g_id), p_writes->g_id[p_writes->pull_ptr]);
          assert(false);
        }
        (*dbg_counter)++;

        //if (*dbg_counter % MILLION == 0)
        //  my_printf(yellow, "%s %u expecting/reading %u/%u \n",
        //            prot_to_str(protocol),
        //            t_id, p_writes->g_id[p_writes->pull_ptr], committed_g_id);
      }
      if (ENABLE_STAT_COUNTING) t_stats[t_id].stalled_gid++;
      return false;
    }
  }
  p_writes->w_state[p_writes->pull_ptr] = INVALID;
  if (ENABLE_ASSERTIONS) (*dbg_counter) = 0;
  MOD_INCR(p_writes->pull_ptr, pending_writes);
  (*update_op_i)++;
  (*committed_g_id)++;

  return true;
}


static inline void zk_take_latency_measurement_for_writes(latency_info_t *latency_info,
                                                          uint16_t t_id)
{
  if (MEASURE_LATENCY && latency_info->measured_req_flag == WRITE_REQ &&
      machine_id == LATENCY_MACHINE && t_id == LATENCY_THREAD )
    report_latency(latency_info);
}


static inline void flr_change_latency_measurement_flag(p_writes_t *p_writes,
                                                       latency_info_t *latency_info,
                                                       uint16_t t_id)
{
  if (MEASURE_LATENCY) change_latency_tag(latency_info, p_writes->session_id[p_writes->pull_ptr], t_id);
  if (MEASURE_LATENCY && latency_info->measured_req_flag == WRITE_REQ_BEFORE_CACHE &&
      machine_id == LATENCY_MACHINE && t_id == LATENCY_THREAD &&
      latency_info->measured_sess_id == p_writes->session_id[p_writes->pull_ptr])
    latency_info->measured_req_flag = WRITE_REQ;
}




static inline void zk_signal_completion_and_bookkeepfor_writes(p_writes_t *p_writes,
                                                               uint16_t update_op_i,
                                                               uint32_t pull_ptr,
                                                               uint32_t max_pending_writes,
                                                               protocol_t protocol,
                                                               latency_info_t *latency_info,
                                                               uint16_t t_id)
{
  for (int w_i = 0; w_i < update_op_i; ++w_i) {

    if (protocol == LEADER) p_writes->acks_seen[pull_ptr] = 0;

    if (p_writes->is_local[pull_ptr]) {
      uint32_t sess_id = p_writes->session_id[pull_ptr];
      signal_completion_to_client(p_writes->session_id[pull_ptr],
                                  p_writes->w_index_to_req_array[sess_id], t_id);
      if (DEBUG_WRITES)
        my_printf(cyan, "Found a local req freeing session %d \n", p_writes->session_id[pull_ptr]);
      p_writes->stalled[sess_id] = false;
      p_writes->all_sessions_stalled = false;
      p_writes->is_local[pull_ptr] = false;
      if (protocol == FOLLOWER)
        flr_change_latency_measurement_flag(p_writes, latency_info, t_id);

    }
    MOD_INCR(pull_ptr, max_pending_writes);

  }
  assert(pull_ptr == p_writes->pull_ptr);
}


/* ---------------------------------------------------------------------------
//------------------------------ BROADCASTS -----------------------------
//---------------------------------------------------------------------------*/

static inline void zk_reset_prep_message(p_writes_t *p_writes,
                                         uint8_t coalesce_num,
                                         uint16_t t_id)
{
  // This message has been sent do not add other prepares to it!
  if (coalesce_num < MAX_PREP_COALESCE) {
//      my_printf(yellow, "Broadcasting prep with coalesce num %u \n", coalesce_num);
    MOD_INCR(p_writes->prep_fifo->push_ptr, PREP_FIFO_SIZE);
    p_writes->prep_fifo->prep_message[p_writes->prep_fifo->push_ptr].coalesce_num = 0;
  }
}

// Poll for credits and increment the credits according to the protocol
static inline void ldr_poll_credits(struct ibv_cq* credit_recv_cq, struct ibv_wc* credit_wc,
                                    uint16_t *credits, recv_info_t *cred_recv_info,
                                    quorum_info_t *q_info, uint16_t t_id)
{

  bool poll_for_credits = false;
  for (uint8_t j = 0; j < q_info->active_num; j++) {
    if (credits[q_info->active_ids[j]] == 0) {
      poll_for_credits = true;
      break;
    }
  }
  if (!poll_for_credits) return;

  int credits_found = 0;
  credits_found = ibv_poll_cq(credit_recv_cq, LDR_MAX_CREDIT_RECV, credit_wc);
  if(credits_found > 0) {
    if(unlikely(credit_wc[credits_found - 1].status != 0)) {
      fprintf(stderr, "Bad wc status when polling for credits to send a broadcast %d\n", credit_wc[credits_found -1].status);
      assert(false);
    }
    cred_recv_info->posted_recvs -= credits_found;
    for (uint32_t j = 0; j < credits_found; j++) {
      credits[credit_wc[j].imm_data]+= FLR_CREDITS_IN_MESSAGE;
    }

  }
  else if(unlikely(credits_found < 0)) {
    printf("ERROR In the credit CQ\n"); exit(0);
  }
}


// Form Broadcast work requests for the leader
static inline void forge_commit_wrs(zk_com_mes_t *com_mes, quorum_info_t *q_info, uint16_t t_id,
                                    uint16_t br_i, struct hrd_ctrl_blk *cb, struct ibv_sge *com_send_sgl,
                                    struct ibv_send_wr *send_wr,  uint64_t *commit_br_tx,
                                    uint16_t credits[][MACHINE_NUM])
{
  com_send_sgl[br_i].addr = (uint64_t) (uintptr_t) com_mes;
  com_send_sgl[br_i].length = LDR_COM_SEND_SIZE;
  if (ENABLE_ASSERTIONS) {
    assert(com_send_sgl[br_i].length <= LDR_COM_SEND_SIZE);
    if (!USE_QUORUM)
      assert(com_mes->com_num <= LEADER_PENDING_WRITES);
  }
  //my_printf(green, "Leader %d : I BROADCAST a message with %d commits, opcode %d credits: %d, l_id %lu \n",
  //             t_id, com_mes->com_num, com_mes->opcode, credits[COMM_VC][1], com_mes->l_id);
  form_bcast_links(commit_br_tx, COM_BCAST_SS_BATCH, q_info, br_i,
                   send_wr, cb->dgram_send_cq[COMMIT_W_QP_ID], "forging commits", t_id);

}


// Form the Broadcast work request for the prepare
static inline void forge_prep_wr(uint16_t prep_i, p_writes_t *p_writes,
                                 struct hrd_ctrl_blk *cb, struct ibv_sge *send_sgl,
                                 struct ibv_send_wr *send_wr, uint64_t *prep_br_tx,
                                 uint16_t br_i, uint16_t credits[][MACHINE_NUM],
                                 uint8_t vc, uint16_t t_id) {
  uint16_t i;
  zk_prep_mes_t *prep = &p_writes->prep_fifo->prep_message[prep_i];
  uint32_t backward_ptr = p_writes->prep_fifo->backward_ptrs[prep_i];
  uint16_t coalesce_num = prep->coalesce_num;
  send_sgl[br_i].length = PREP_MES_HEADER + coalesce_num * sizeof(zk_prepare_t);
  send_sgl[br_i].addr = (uint64_t) (uintptr_t) prep;
  for (i = 0; i < coalesce_num; i++) {
    p_writes->w_state[(backward_ptr + i) % LEADER_PENDING_WRITES] = SENT;
    if (DEBUG_PREPARES)
      printf("Prepare %d, val-len %u, message size %d\n", i, prep->prepare[i].val_len,
             send_sgl[br_i].length);
    if (ENABLE_ASSERTIONS) {
      assert(prep->prepare[i].val_len == VALUE_SIZE >> SHIFT_BITS);
      assert(prep->prepare[i].opcode == KVS_OP_PUT);
    }

  }
  if (DEBUG_PREPARES)
    my_printf(green, "Leader %d : I BROADCAST a prepare message %d of %u prepares with size %u,  with  credits: %d, lid: %u  \n",
              t_id, prep->opcode, coalesce_num, send_sgl[br_i].length, credits[vc][0], prep->l_id);
  form_bcast_links(prep_br_tx, PREP_BCAST_SS_BATCH, p_writes->q_info, br_i,
                   send_wr, cb->dgram_send_cq[PREP_ACK_QP_ID], "forging prepares", t_id);
}


/* ---------------------------------------------------------------------------
//------------------------------ UNICASTS --------------------------------
//---------------------------------------------------------------------------*/


// Form the Write work request for the write
static inline void forge_w_wr(p_writes_t *p_writes,
                              struct hrd_ctrl_blk *cb, struct ibv_sge *w_send_sgl,
                              struct ibv_send_wr *w_send_wr, uint64_t *w_tx,
                              uint16_t w_i, uint16_t credits,
                              uint16_t t_id)
{
  zk_w_mes_t *w_mes_fifo = (zk_w_mes_t *) p_writes->w_fifo->fifo;
  uint32_t w_ptr = p_writes->w_fifo->pull_ptr;
  zk_w_mes_t *w_mes = &w_mes_fifo[w_ptr];
  uint16_t coalesce_num = w_mes->write[0].w_num;
  if (ENABLE_ASSERTIONS) assert(coalesce_num > 0);
  w_send_sgl[w_i].length = coalesce_num * sizeof(zk_write_t);
  w_send_sgl[w_i].addr = (uint64_t) (uintptr_t) w_mes;

  checks_and_print_when_forging_write(w_mes, coalesce_num, w_send_sgl[w_i].length,
                                      credits, t_id);

  selective_signaling_for_unicast(w_tx, WRITE_SS_BATCH, w_send_wr,
                                  w_i, cb->dgram_send_cq[COMMIT_W_QP_ID], FLR_W_ENABLE_INLINING,
                                  "sending credits", t_id);
  // Have the last message point to the current message
  if (w_i > 0) w_send_wr[w_i - 1].next = &w_send_wr[w_i];

}

static inline void reset_write_mes(p_writes_t *p_writes,
                                   zk_w_mes_t *w_mes_fifo,
                                   uint16_t coalesce_num,
                                   uint16_t t_id)
{
  // This message has been sent do not add other writes to it!
  if (coalesce_num < MAX_W_COALESCE) {
    MOD_INCR(p_writes->w_fifo->push_ptr, W_FIFO_SIZE);
    w_mes_fifo[p_writes->w_fifo->push_ptr].write[0].w_num = 0;
    //my_printf(yellow, "Zeroing when sending at pointer %u \n", p_writes->prep_fifo->push_ptr);
  }
}


// Add the acked gid to the appropriate commit message
static inline void zk_create_commit_message(zk_com_fifo_t *com_fifo, uint64_t l_id, uint16_t update_op_i)
{
  //l_id refers the oldest write to commit (writes go from l_id to l_id + update_op_i)
  uint16_t com_mes_i = com_fifo->push_ptr;
  assert(com_mes_i < COMMIT_FIFO_SIZE);
  uint16_t last_com_mes_i;
  zk_com_mes_t *commit_messages = com_fifo->commits;
  assert(com_fifo->size <= COMMIT_FIFO_SIZE);
  // see if the new batch can fit with the previous one -- no reason why it should not
  if (com_fifo->size > 0) {
    last_com_mes_i = (COMMIT_FIFO_SIZE + com_mes_i - 1) % COMMIT_FIFO_SIZE;
    zk_com_mes_t *last_commit = &commit_messages[last_com_mes_i];
    uint64_t last_l_id = last_commit->l_id;
    last_l_id += last_commit->com_num;
    if (last_l_id == l_id) {
      if (last_commit->com_num + update_op_i <= MAX_LIDS_IN_A_COMMIT) {
        last_commit->com_num += update_op_i;
        return;
      }
    }
  }
  //otherwise push a new commit
  zk_com_mes_t *new_commit = &commit_messages[com_mes_i];
  new_commit->l_id = l_id;
  new_commit->com_num = update_op_i;
  com_fifo->size++;
  MOD_INCR(com_fifo->push_ptr, COMMIT_FIFO_SIZE);
  if (ENABLE_ASSERTIONS) {
    assert(com_fifo->size <= COMMIT_FIFO_SIZE );
  }
}

/* ---------------------------------------------------------------------------
//------------------------------TRACE --------------------------------
//---------------------------------------------------------------------------*/

static inline void fill_prep(zk_prepare_t *prep, mica_key_t key, uint8_t opcode, uint8_t val_len,
                             uint8_t *value, uint8_t flr_id, uint16_t session_id)
{
  prep->key = key;
  prep->opcode = opcode;
  prep->val_len = val_len;
  memcpy(prep->value, value, (size_t) VALUE_SIZE);
  prep->flr_id = flr_id; // means it's a leader message
  prep->sess_id = session_id;
}

static inline void fill_write(zk_write_t *write, mica_key_t key, uint8_t opcode, uint8_t val_len,
                              uint8_t *value, uint8_t flr_id, uint32_t session_id)
{
  write->key = key;
  write->opcode = opcode;
  write->val_len = val_len;
  memcpy(write->value, value, (size_t) VALUE_SIZE);
  write->flr_id = flr_id; // means it's a leader message
  write->sess_id = session_id;
}

// Insert a new local or remote write to the leader pending writes
static inline void ldr_insert_write(p_writes_t *p_writes, void *source, uint32_t session_id,
                                    bool local, uint16_t t_id)
{
  zk_prep_mes_t *preps = p_writes->prep_fifo->prep_message;
  uint32_t prep_ptr = p_writes->prep_fifo->push_ptr;
  uint32_t inside_prep_ptr = preps[prep_ptr].coalesce_num;
  uint32_t w_ptr = p_writes->push_ptr;
  zk_prepare_t *prep = &preps[prep_ptr].prepare[inside_prep_ptr];

  if (local) {
    zk_trace_op_t *op = (zk_trace_op_t *) source;
    fill_prep(prep, op->key, op->opcode, op->val_len, op->value,
              MACHINE_NUM, (uint16_t) session_id);
    p_writes->w_index_to_req_array[session_id] = op->index_to_req_array;
  }
  else {
    zk_write_t *rem_write = (zk_write_t *) source;
    fill_prep(prep, rem_write->key, rem_write->opcode, rem_write->val_len, rem_write->value,
              rem_write->flr_id, (uint16_t) rem_write->sess_id);
  }

  // link it with p_writes to lead it later it to the KVS
  p_writes->ptrs_to_ops[w_ptr] = prep;

  // If it's the first message give it an lid
  if (inside_prep_ptr == 0) {
    p_writes->prep_fifo->backward_ptrs[prep_ptr] = w_ptr;
    uint32_t message_l_id = (uint32_t) (p_writes->local_w_id + p_writes->size);
    preps[prep_ptr].l_id = (uint32_t) message_l_id;
  }
  checks_when_leader_creates_write(preps, prep_ptr, inside_prep_ptr, p_writes, w_ptr, t_id);

  // Bookkeeping
  p_writes->w_state[w_ptr] = VALID;

  p_writes->is_local[w_ptr] = local;
  p_writes->session_id[w_ptr] = (uint32_t) session_id;
  MOD_INCR(p_writes->push_ptr, LEADER_PENDING_WRITES);
  p_writes->size++;
  p_writes->prep_fifo->bcast_size++;
  preps[prep_ptr].coalesce_num++;
  if (preps[prep_ptr].coalesce_num == MAX_PREP_COALESCE) {
    MOD_INCR(p_writes->prep_fifo->push_ptr, PREP_FIFO_SIZE);
    preps[p_writes->prep_fifo->push_ptr].coalesce_num = 0;
  }
}

// Follower inserts a new local write to the write fifo it maintains (for the writes that it propagates to the leader)
static inline void flr_insert_write(p_writes_t *p_writes, zk_trace_op_t *op, uint32_t session_id,
                                    uint8_t flr_id, uint16_t t_id)
{
  zk_w_mes_t *w_mes = (zk_w_mes_t *) p_writes->w_fifo->fifo;
  uint32_t w_ptr = p_writes->w_fifo->push_ptr;
  uint32_t inside_w_ptr = w_mes[w_ptr].write[0].w_num;
  zk_write_t *write = &w_mes[w_ptr].write[inside_w_ptr];

  fill_write(write, op->key, op->opcode, op->val_len, op->value,
             flr_id, session_id);
  //    printf("Passed session id %u to the op in message %u, with inside ptr %u\n",
  //           *(uint32_t*)w_mes[w_ptr].write[inside_w_ptr].session_id, w_ptr, inside_w_ptr);
  p_writes->w_fifo->size++;
  w_mes[w_ptr].write[0].w_num++;
  p_writes->w_index_to_req_array[session_id] = op->index_to_req_array;

  if (w_mes[w_ptr].write[0].w_num == MAX_W_COALESCE) {
    MOD_INCR(p_writes->w_fifo->push_ptr, W_FIFO_SIZE);
    w_mes[p_writes->w_fifo->push_ptr].write[0].w_num = 0;
    //my_printf(yellow, "Zeroing when in cache at pointer %u \n", p_writes->w_fifo->push_ptr);
  }
}

static inline void zk_fill_trace_op(trace_t *trace, zk_trace_op_t *op, uint16_t op_i,
                                    int working_session, protocol_t protocol,
                                    p_writes_t *p_writes, uint8_t flr_id,
                                    latency_info_t *latency_info, uint16_t t_id)
{
  create_inputs_of_op(&op->value_to_write, &op->value_to_read, &op->real_val_len,
                      &op->opcode, &op->index_to_req_array,
                      &op->key, op->value, trace, working_session, t_id);

  zk_check_op(op);

  if (ENABLE_ASSERTIONS) assert(op->opcode != NOP);
  bool is_update = op->opcode == KVS_OP_PUT;
  if (WRITE_RATIO >= 1000) assert(is_update);
   op->val_len = is_update ? (uint8_t) (VALUE_SIZE >> SHIFT_BITS) : (uint8_t) 0;
  if (MEASURE_LATENCY) start_measurement(latency_info, (uint32_t) working_session, t_id, op->opcode);
  op->session_id = (uint16_t) working_session;

  if (is_update) {
    switch (protocol) {
      case FOLLOWER:
        flr_insert_write(p_writes, op, (uint32_t) working_session, flr_id, t_id);
        break;
      case LEADER:
        ldr_insert_write(p_writes, (void *) op, (uint32_t) working_session, true, t_id);
        break;
      default:
        if (ENABLE_ASSERTIONS) assert(false);
    }
    p_writes->stalled[working_session] = true;

  }

  if (ENABLE_CLIENTS) {
    signal_in_progress_to_client(op->session_id, op->index_to_req_array, t_id);
    if (ENABLE_ASSERTIONS) assert(interface[t_id].wrkr_pull_ptr[working_session] == op->index_to_req_array);
    MOD_INCR(interface[t_id].wrkr_pull_ptr[working_session], PER_SESSION_REQ_NUM);
  }

  if (ENABLE_ASSERTIONS == 1) {
    assert(WRITE_RATIO > 0 || is_update == 0);
    if (is_update) assert(op->val_len > 0);
  }
}


#endif //KITE_ZK_RESERVATION_STATIONS_UTIL_H_H


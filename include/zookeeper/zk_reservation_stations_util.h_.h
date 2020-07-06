//
// Created by vasilis on 01/07/20.
//

#ifndef KITE_ZK_RESERVATION_STATIONS_UTIL_H_H
#define KITE_ZK_RESERVATION_STATIONS_UTIL_H_H

#include "../general_util/latency_util.h"
#include "../general_util/generic_inline_util.h"
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
                                          uint64_t committed_g_id,
                                          uint32_t *dbg_counter,
                                          protocol_t protocol,
                                          uint16_t t_id)
{
  if (DISABLE_GID_ORDERING) return true;
  if (p_writes->g_id[p_writes->pull_ptr] != committed_g_id + 1) {
    if (ENABLE_ASSERTIONS) {
      assert(committed_g_id < p_writes->g_id[p_writes->pull_ptr]);
      (*dbg_counter)++;
      //if (*dbg_counter % MILLION == 0)
      //  my_printf(yellow, "%s %u expecting/reading %u/%u \n",
      //            prot_to_str(protocol),
      //            t_id, p_writes->g_id[p_writes->pull_ptr], committed_g_id);
    }
    if (ENABLE_STAT_COUNTING) t_stats[t_id].stalled_gid++;
    return false;
  }
  else return true;
}


static inline void zk_take_latency_measurement_for_writes(p_writes_t *p_writes,
                                                          latency_info_t *latency_info,
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

static inline void bookkeep_after_finding_expected_gid(p_writes_t *p_writes,
                                                       latency_info_t *latency_info,
                                                       protocol_t protocol,
                                                       uint32_t *dbg_counter,
                                                       uint16_t t_id)
{
  if (ENABLE_ASSERTIONS)(*dbg_counter) = 0;
  p_writes->w_state[p_writes->pull_ptr] = INVALID;
  if (protocol == LEADER) p_writes->acks_seen[p_writes->pull_ptr] = 0;
  if (p_writes->is_local[p_writes->pull_ptr]) {
    if (DEBUG_WRITES)
      my_printf(cyan, "Found a local req freeing session %d \n", p_writes->session_id[p_writes->pull_ptr]);
    p_writes->stalled[p_writes->session_id[p_writes->pull_ptr]] = false;
    p_writes->all_sessions_stalled = false;
    p_writes->is_local[p_writes->pull_ptr] = false;
    if (protocol == FOLLOWER)
      flr_change_latency_measurement_flag(p_writes, latency_info, t_id);
  }

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
  //my_printf(green, "Leader %d : I BROADCAST a message with %d commits with %d credits: %d \n",
  //             t_id, com_mes->com_num, com_mes->opcode, credits[COMM_VC][0]);
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



#endif //KITE_ZK_RESERVATION_STATIONS_UTIL_H_H


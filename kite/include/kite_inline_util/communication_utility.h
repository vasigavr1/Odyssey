//
// Created by vasilis on 22/05/20.
//

#ifndef KITE_COMMUNICATION_UTILITY_H
#define KITE_COMMUNICATION_UTILITY_H

#include "main.h"
#include "latency_util.h"
#include "kite_debug_util.h"
#include "kite_config_util.h"
#include "rdma_gen_util.h"
#include "reserve_stations_util.h"


static inline int find_how_many_write_messages_can_be_polled(struct ibv_cq *w_recv_cq, struct ibv_wc *w_recv_wc,
                                                             recv_info_t *w_recv_info, ack_mes_t *acks,
                                                             uint32_t *completed_but_not_polled_writes,
                                                             uint16_t t_id)
{

  // There is a chance that you wont be able to poll all completed writes,
  // because you wont be able to create acks for them, in which case you
  // pass the number of completed (i.e. from the completion queue) but not polled messages to the next round
  int completed_messages = find_how_many_messages_can_be_polled(w_recv_cq, w_recv_wc, completed_but_not_polled_writes,
                                                                W_BUF_SLOTS, t_id);

  int newly_completed_messages = completed_messages - (*completed_but_not_polled_writes);
  if (DEBUG_RECEIVES) {
    w_recv_info->posted_recvs -= newly_completed_messages;
    if (w_recv_info->posted_recvs < RECV_WR_SAFETY_MARGIN)
      my_printf(red, "Wrkr %u some remote machine has created credits out of thin air \n", t_id);
  }
  if (unlikely(*completed_but_not_polled_writes > 0)) {
    if (DEBUG_QUORUM)
      my_printf(yellow, "Wrkr %u adds %u messages to the %u completed messages \n",
                t_id, *completed_but_not_polled_writes, newly_completed_messages);
  }
  if (ENABLE_ASSERTIONS && completed_messages > 0) {
    for (int i = 0; i < MACHINE_NUM; i++)
      assert(acks[i].opcode == OP_ACK);
  }
  return completed_messages;
}


// Form the  work request for the read reply
static inline void forge_r_rep_wr(uint32_t r_rep_pull_ptr, uint16_t mes_i, p_ops_t *p_ops,
                                  struct hrd_ctrl_blk *cb, struct ibv_sge *send_sgl,
                                  struct ibv_send_wr *send_wr, uint64_t *r_rep_tx,
                                  uint16_t t_id) {

  struct r_rep_fifo *r_rep_fifo = p_ops->r_rep_fifo;
  struct r_rep_message *r_rep_mes = (struct r_rep_message *) &r_rep_fifo->r_rep_message[r_rep_pull_ptr];
  uint8_t coalesce_num = r_rep_mes->coalesce_num;
  send_sgl[mes_i].length = r_rep_fifo->message_sizes[r_rep_pull_ptr];
  if (ENABLE_ASSERTIONS) assert(send_sgl[mes_i].length <= R_REP_SEND_SIZE);
  //printf("Forging a r_resp with size %u \n", send_sgl[mes_i].length);
  send_sgl[mes_i].addr = (uint64_t) (uintptr_t) r_rep_mes;

  checks_and_prints_when_forging_r_rep_wr(coalesce_num, mes_i, send_sgl, r_rep_pull_ptr,
                                          r_rep_mes, r_rep_fifo, t_id);
  uint8_t rm_id = r_rep_fifo->rem_m_id[r_rep_pull_ptr];
  send_wr[mes_i].wr.ud.ah = rem_qp[rm_id][t_id][R_REP_QP_ID].ah;
  send_wr[mes_i].wr.ud.remote_qpn = (uint32) rem_qp[rm_id][t_id][R_REP_QP_ID].qpn;
  selective_signaling_for_unicast(r_rep_tx, R_REP_SS_BATCH, send_wr,
                                  mes_i, cb->dgram_send_cq[R_REP_QP_ID], R_REP_ENABLE_INLINING, "sending r_reps", t_id);
  if (mes_i > 0) send_wr[mes_i - 1].next = &send_wr[mes_i];
}



// Form the Broadcast work request for the red
static inline void forge_r_wr(uint32_t r_mes_i, p_ops_t *p_ops,
                              quorum_info_t *q_info,
                              struct hrd_ctrl_blk *cb, struct ibv_sge *send_sgl,
                              struct ibv_send_wr *send_wr, uint64_t *r_br_tx,
                              uint16_t br_i, uint16_t credits[][MACHINE_NUM],
                              uint8_t vc, uint16_t t_id) {
  uint16_t i;
  struct r_message *r_mes = (struct r_message *) &p_ops->r_fifo->r_message[r_mes_i];
  r_mes_info_t *info = &p_ops->r_fifo->info[r_mes_i];
  uint16_t coalesce_num = r_mes->coalesce_num;
  bool has_reads = info->reads_num > 0;
  bool all_reads = info->reads_num == r_mes->coalesce_num;
  send_sgl[br_i].length = info->message_size;
  send_sgl[br_i].addr = (uint64_t) (uintptr_t) r_mes;
  if (ENABLE_ADAPTIVE_INLINING)
    adaptive_inlining(send_sgl[br_i].length, &send_wr[br_i * MESSAGES_IN_BCAST], MESSAGES_IN_BCAST);
  if (ENABLE_ASSERTIONS) {
    assert(coalesce_num > 0);
    assert(send_sgl[br_i].length <= R_SEND_SIZE);
  }

  if (DEBUG_READS && all_reads)
    my_printf(green, "Wrkr %d : I BROADCAST a read message %d of %u reads with mes_size %u, with credits: %d, lid: %u  \n",
              t_id, r_mes->read[coalesce_num - 1].opcode, coalesce_num, send_sgl[br_i].length,
              credits[vc][(machine_id + 1) % MACHINE_NUM], r_mes->l_id);
  else if (DEBUG_RMW) {
    //struct prop_message *prop_mes = (struct prop_message *) r_mes;
    struct propose *prop = (struct propose *) &r_mes->read[0];
    my_printf(green, "Wrkr %d : I BROADCAST a propose message %d of %u props with mes_size %u, with credits: %d, lid: %u, "
                "rmw_id %u, glob_sess id %u, log_no %u, version %u \n",
              t_id, prop->opcode, coalesce_num, send_sgl[br_i].length,
              credits[vc][(machine_id + 1) % MACHINE_NUM], r_mes->l_id,
              prop->t_rmw_id, prop->t_rmw_id % GLOBAL_SESSION_NUM,
              prop->log_no, prop->ts.version);
  }
  if (has_reads) {
    for (i = 0; i < info->reads_num; i++) {
      p_ops->r_state[(info->backward_ptr + i) % PENDING_READS] = SENT;
      if (DEBUG_READS)
        my_printf(yellow, "Read %d/%u, message mes_size %d, version %u \n", i, coalesce_num,
                  send_sgl[br_i].length, r_mes->read[i].ts.version);
      if (ENABLE_ASSERTIONS && all_reads) {
        check_state_with_allowed_flags(5, r_mes->read[i].opcode, KVS_OP_GET, OP_GET_TS,
                                       OP_ACQUIRE, OP_ACQUIRE_FLIP_BIT);
      }
    }
  }

  form_bcast_links(r_br_tx, R_BCAST_SS_BATCH, p_ops->q_info, br_i,
                   send_wr, cb->dgram_send_cq[R_QP_ID], "forging reads", t_id);
}


// Form the Broadcast work request for the write
static inline void forge_w_wr(uint32_t w_mes_i, p_ops_t *p_ops,
                              struct hrd_ctrl_blk *cb, struct ibv_sge *send_sgl,
                              struct ibv_send_wr *send_wr, uint64_t *w_br_tx,
                              uint16_t br_i, uint16_t credits[][MACHINE_NUM],
                              uint8_t vc, uint16_t t_id) {
  struct w_message *w_mes = (struct w_message *) &p_ops->w_fifo->w_message[w_mes_i];
  w_mes_info_t *info = &p_ops->w_fifo->info[w_mes_i];
  uint8_t coalesce_num = w_mes->coalesce_num;
  uint32_t backward_ptr = info->backward_ptr;
  send_sgl[br_i].length = info->message_size;
  send_sgl[br_i].addr = (uint64_t) (uintptr_t) w_mes;
  if (ENABLE_ADAPTIVE_INLINING)
    adaptive_inlining(send_sgl[br_i].length, &send_wr[br_i * MESSAGES_IN_BCAST], MESSAGES_IN_BCAST);
  // Set the w_state for each write and perform checks

  set_w_state_for_each_write(p_ops, info, w_mes, backward_ptr, coalesce_num,
                             send_sgl, br_i, p_ops->q_info, t_id);

  if (DEBUG_WRITES)
    my_printf(green, "Wrkr %d : I BROADCAST a write message %d of %u writes with mes_size %u,"
                " with credits: %d, lid: %u  \n",
              t_id, w_mes->write[0].opcode, coalesce_num, send_sgl[br_i].length,
              credits[vc][(machine_id + 1) % MACHINE_NUM], w_mes->l_id);

  if (DEBUG_RMW) {
    struct accept *acc = (struct accept *) &w_mes->write[0];
    my_printf(green, "Wrkr %d : I BROADCAST a message %d of %u accepts with mes_size %u, "
                "with credits: %d, lid: %u , "
                "rmw_id %u, glob_sess id %u, log_no %u, version %u  \n",
              t_id, acc->opcode, coalesce_num,
              send_sgl[br_i].length,  credits[vc][(machine_id + 1) % MACHINE_NUM], acc->l_id,
              acc->t_rmw_id, (uint32_t) acc->t_rmw_id % GLOBAL_SESSION_NUM,
              acc->log_no, acc->ts.version);
  }

  form_bcast_links(w_br_tx, W_BCAST_SS_BATCH, p_ops->q_info, br_i,
                   send_wr, cb->dgram_send_cq[W_QP_ID], "forging writes", t_id);

}



// Whe broadcasting writes, some of them may be accepts which trigger read replies instead of write acks
// For that reason we need to potentially also post receives for r_reps when broadcasting writes
static inline void post_receives_for_r_reps_for_accepts(recv_info_t *r_rep_recv_info,
                                                        uint16_t t_id)
{
  uint32_t recvs_to_post_num = MAX_RECV_R_REP_WRS - r_rep_recv_info->posted_recvs;
  if (recvs_to_post_num > 0) {
    // printf("Wrkr %d posting %d recvs\n", t_id,  recvs_to_post_num);
    if (recvs_to_post_num) post_recvs_with_recv_info(r_rep_recv_info, recvs_to_post_num);
  }
}

// Keep track of the write messages to send the appropriate acks
static inline bool ack_bookkeeping(ack_mes_t *ack, uint8_t w_num, uint64_t l_id,
                                   const uint8_t m_id, const uint16_t t_id)
{
  if (ENABLE_ASSERTIONS && ack->opcode != OP_ACK) {
    if(unlikely(ack->l_id) + ack->ack_num != l_id) {
      my_printf(red, "Wrkr %u: Adding to existing ack for machine %u  with l_id %lu, "
                  "ack_num %u with new l_id %lu, coalesce_num %u, opcode %u\n", t_id, m_id,
                ack->l_id, ack->ack_num, l_id, w_num, ack->opcode);
      //assert(false);
      return false;
    }
  }
  if (ack->opcode == OP_ACK) {// new ack
    //if (ENABLE_ASSERTIONS) assert((ack->l_id) + ack->ack_num == l_id);
    memcpy(&ack->l_id, &l_id, sizeof(uint64_t));
    ack->credits = 1;
    ack->ack_num = w_num;
    ack->opcode = ACK_NOT_YET_SENT;
    if (DEBUG_ACKS) my_printf(yellow, "Create an ack with l_id  %lu \n", ack->l_id);
  }
  else {
    if (ENABLE_ASSERTIONS) {
      assert(ack->l_id + ((uint64_t) ack->ack_num) == l_id);
      assert(ack->ack_num < 63000);
      assert(W_CREDITS > 1);
      assert(ack->credits < W_CREDITS);
    }
    ack->credits++;
    ack->ack_num += w_num;
  }
  return true;
}


#endif //KITE_COMMUNICATION_UTILITY_H

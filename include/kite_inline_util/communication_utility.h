//
// Created by vasilis on 22/05/20.
//

#ifndef KITE_COMMUNICATION_UTILITY_H
#define KITE_COMMUNICATION_UTILITY_H

#include "main.h"
#include "../general_util/latency_util.h"
#include "kite_debug_util.h"
#include "config_util.h"
#include "../general_util/rdma_gen_util.h"
#include "reserve_stations_util.h"



static inline void decrease_credits(uint16_t credits[][MACHINE_NUM], struct quorum_info *q_info,
                                    uint16_t mes_sent, uint8_t vc)
{
  for (uint8_t i = 0; i < q_info->active_num; i++) {
    if (ENABLE_ASSERTIONS) {
      assert(credits[vc][q_info->active_ids[i]] >= mes_sent);
      assert(q_info->active_ids[i] != machine_id && q_info->active_ids[i] < MACHINE_NUM);
      //assert(q_info->active_num == REM_MACH_NUM); // debug no-failure case
    }
    credits[vc][q_info->active_ids[i]] -= mes_sent;
  }
}

// Check credits, first see if there are credits from all active nodes, then if not and enough time has passed,
// transition to write_quorum broadcasts
static inline bool check_bcast_credits(uint16_t credits[][MACHINE_NUM], struct quorum_info *q_info,
                                       uint32_t *time_out_cnt, uint8_t vc,
                                       uint16_t *available_credits,
                                       struct ibv_send_wr *r_send_wr, struct ibv_send_wr *w_send_wr,
                                       uint16_t min_credits, uint16_t t_id)
{
  uint16_t i;
  // First check the active ids, to have a fast path when there are not enough credits
  for (i = 0; i < q_info->active_num; i++) {
    if (credits[vc][q_info->active_ids[i]] < min_credits) {
      time_out_cnt[vc]++;
      //if (DEBUG_BIT_VECS && t_id >= 0 && time_out_cnt[vc] % M_1 == 0)
      //my_printf(red, "WKR %u: the timeout cnt is %u for vc %u machine %u, credits %u\n",
      //          t_id, time_out_cnt[vc], vc, q_info->active_ids[i], credits[vc][q_info->active_ids[i]]);
      if (time_out_cnt[vc] == CREDIT_TIMEOUT) {
        if (DEBUG_QUORUM)
          my_printf(red, "Worker %u timed_out on machine %u  for vc % u, writes  done %lu \n",
                    t_id, q_info->active_ids[i], vc, t_stats[t_id].writes_sent);
        // assert(false);
        update_q_info(q_info, credits, min_credits, vc, t_id);
        update_bcast_wr_links(q_info, r_send_wr, t_id);
        update_bcast_wr_links(q_info, w_send_wr, t_id);
        time_out_cnt[vc] = 0;
      }
      return false;
    }
  }

  time_out_cnt[vc] = 0;

  // then check the missing credits to see if we need to change the configuration
  if (q_info->missing_num > 0) {
    for (i = 0; i < q_info->missing_num; i++) {
      if (credits[W_VC][q_info->missing_ids[i]] == W_CREDITS &&
          credits[R_VC][q_info->missing_ids[i]] == R_CREDITS ) {
        if (DEBUG_QUORUM)
          my_printf(red, "Worker %u revives machine %u \n", t_id, q_info->missing_ids[i]);
        revive_machine(q_info, q_info->missing_ids[i]);
        // printf("Wrkr %u, after reviving, active num %u, active_id %u, %u, %u, %u \n",
        //t_id, q_info->active_num, q_info->active_ids[0], q_info->active_ids[1],
        //       q_info->active_ids[2],q_info->active_ids[3]);
        update_bcast_wr_links(q_info, r_send_wr, t_id);
        update_bcast_wr_links(q_info, w_send_wr, t_id);
      }
    }
  }
  if (unlikely(q_info->active_num < REMOTE_QUORUM)) return false;
  //finally count credits
  uint16_t avail_cred = K_64_;
  //printf("avail cred %u\n", avail_cred);
  for (i = 0; i < q_info->active_num; i++) {
    if (ENABLE_ASSERTIONS) assert(q_info->active_ids[i] < MACHINE_NUM &&
                                  q_info->active_ids[i] != machine_id);
    if (credits[vc][q_info->active_ids[i]] < avail_cred)
      avail_cred = credits[vc][q_info->active_ids[i]];
  }
  *available_credits = avail_cred;
  return true;
}




static inline int find_how_many_write_messages_can_be_polled(struct ibv_cq *w_recv_cq, struct ibv_wc *w_recv_wc,
                                                             struct recv_info *w_recv_info, ack_mes_t *acks,
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

  struct ibv_wc signal_send_wc;
  struct r_rep_fifo *r_rep_fifo = p_ops->r_rep_fifo;
  struct r_rep_message *r_rep_mes = (struct r_rep_message *) &r_rep_fifo->r_rep_message[r_rep_pull_ptr];
  uint8_t coalesce_num = r_rep_mes->coalesce_num;
  //struct rmw_rep_message *rmw_rep_mes = (struct rmw_rep_message *)r_rep_mes;
  //printf("%u\n", rmw_rep_mes->rmw_rep[0].opcode);

  send_sgl[mes_i].length = r_rep_fifo->message_sizes[r_rep_pull_ptr];
  if (ENABLE_ASSERTIONS) assert(send_sgl[mes_i].length <= R_REP_SEND_SIZE);
  //printf("Forging a r_resp with size %u \n", send_sgl[mes_i].length);
  send_sgl[mes_i].addr = (uint64_t) (uintptr_t) r_rep_mes;

  checks_and_prints_when_forging_r_rep_wr(coalesce_num, mes_i, send_sgl, r_rep_pull_ptr,
                                          r_rep_mes, r_rep_fifo, t_id);

  uint8_t rm_id = r_rep_fifo->rem_m_id[r_rep_pull_ptr];
  if (ENABLE_ADAPTIVE_INLINING)
    adaptive_inlining(send_sgl[mes_i].length, &send_wr[mes_i], 1);
  else send_wr[mes_i].send_flags = R_REP_ENABLE_INLINING ? IBV_SEND_INLINE : 0;
  send_wr[mes_i].wr.ud.ah = rem_qp[rm_id][t_id][R_REP_QP_ID].ah;
  send_wr[mes_i].wr.ud.remote_qpn = (uint32) rem_qp[rm_id][t_id][R_REP_QP_ID].qpn;
  // Do a Signaled Send every R_SS_BATCH messages
  if ((*r_rep_tx) % R_REP_SS_BATCH == 0) send_wr[mes_i].send_flags |= IBV_SEND_SIGNALED;
  (*r_rep_tx)++;
  if ((*r_rep_tx) % R_REP_SS_BATCH == R_REP_SS_BATCH - 1) {
    //printf("Wrkr %u POLLING for a send completion in read replies \n", m_id);
    poll_cq(cb->dgram_send_cq[R_REP_QP_ID], 1, &signal_send_wc, "POLL_CQ_R_REP");
  }
  if (mes_i > 0) send_wr[mes_i - 1].next = &send_wr[mes_i];

}



// Form the Broadcast work request for the red
static inline void forge_r_wr(uint32_t r_mes_i, p_ops_t *p_ops,
                              struct quorum_info *q_info,
                              struct hrd_ctrl_blk *cb, struct ibv_sge *send_sgl,
                              struct ibv_send_wr *send_wr, uint64_t *r_br_tx,
                              uint16_t br_i, uint16_t credits[][MACHINE_NUM],
                              uint8_t vc, uint16_t t_id) {
  uint16_t i;
  struct ibv_wc signal_send_wc;
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
  //send_wr[0].send_flags = R_ENABLE_INLINING == 1 ? IBV_SEND_INLINE : 0;
  // Do a Signaled Send every R_BCAST_SS_BATCH broadcasts (R_BCAST_SS_BATCH * (MACHINE_NUM - 1) messages)
  if ((*r_br_tx) % R_BCAST_SS_BATCH == 0)
    send_wr[q_info->first_active_rm_id].send_flags |= IBV_SEND_SIGNALED;
  (*r_br_tx)++;
  if ((*r_br_tx) % R_BCAST_SS_BATCH == R_BCAST_SS_BATCH - 1) {
    //printf("Wrkr %u POLLING for a send completion in reads \n", m_id);
    poll_cq(cb->dgram_send_cq[R_QP_ID], 1, &signal_send_wc, "POLL_CQ_R");
  }
  // Have the last message of each broadcast pointing to the first message of the next bcast
  if (br_i > 0)
    send_wr[((br_i - 1) * MESSAGES_IN_BCAST) + q_info->last_active_rm_id].next =
      &send_wr[(br_i * MESSAGES_IN_BCAST) + q_info->first_active_rm_id];

}


// Form the Broadcast work request for the write
static inline void forge_w_wr(uint32_t w_mes_i, p_ops_t *p_ops,
                              struct hrd_ctrl_blk *cb, struct ibv_sge *send_sgl,
                              struct ibv_send_wr *send_wr, uint64_t *w_br_tx,
                              uint16_t br_i, uint16_t credits[][MACHINE_NUM],
                              uint8_t vc, uint16_t t_id) {
  struct ibv_wc signal_send_wc;
  struct w_message *w_mes = (struct w_message *) &p_ops->w_fifo->w_message[w_mes_i];
  w_mes_info_t *info = &p_ops->w_fifo->info[w_mes_i];
  uint8_t coalesce_num = w_mes->coalesce_num;
  bool has_writes = info->writes_num > 0;
  bool all_writes = info->writes_num == w_mes->coalesce_num;
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

  // Do a Signaled Send every W_BCAST_SS_BATCH broadcasts (W_BCAST_SS_BATCH * (MACHINE_NUM - 1) messages)
  if ((*w_br_tx) % W_BCAST_SS_BATCH == 0) {
    if (DEBUG_SS_BATCH)
      printf("Wrkr %u Sending signaled the first message, total %lu, br_i %u \n", t_id, *w_br_tx, br_i);
    send_wr[p_ops->q_info->first_active_rm_id].send_flags |= IBV_SEND_SIGNALED;
  }
  (*w_br_tx)++;
  if ((*w_br_tx) % W_BCAST_SS_BATCH == W_BCAST_SS_BATCH - 1) {
    if (DEBUG_SS_BATCH)
      printf("Wrkr %u POLLING for a send completion in writes, total %lu \n", t_id, *w_br_tx);
    poll_cq(cb->dgram_send_cq[W_QP_ID], 1, &signal_send_wc, "POLL_CQ_W");
  }
  // Have the last message of each broadcast pointing to the first message of the next bcast
  if (br_i > 0) {
    send_wr[((br_i - 1) * MESSAGES_IN_BCAST) + p_ops->q_info->last_active_rm_id].next =
      &send_wr[(br_i * MESSAGES_IN_BCAST) + p_ops->q_info->first_active_rm_id];
  }
}



// Whe broadcasting writes, some of them may be accepts which trigger read replies instead of write acks
// For that reason we need to potentially also post receives for r_reps when broadcasting writes
static inline void post_receives_for_r_reps_for_accepts(struct recv_info *r_rep_recv_info,
                                                        uint16_t t_id)
{
  uint32_t recvs_to_post_num = MAX_RECV_R_REP_WRS - r_rep_recv_info->posted_recvs;
  if (recvs_to_post_num > 0) {
    // printf("Wrkr %d posting %d recvs\n", t_id,  recvs_to_post_num);
    if (recvs_to_post_num) post_recvs_with_recv_info(r_rep_recv_info, recvs_to_post_num);
    r_rep_recv_info->posted_recvs += recvs_to_post_num;
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

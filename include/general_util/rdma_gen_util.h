//
// Created by vasilis on 24/06/2020.
//

#ifndef KITE_RDMA_GEN_UTIL_H
#define KITE_RDMA_GEN_UTIL_H

#include "top.h"
#include "hrd.h"

// Set up the receive info
static recv_info_t* init_recv_info(struct hrd_ctrl_blk *cb,uint32_t push_ptr, uint32_t buf_slots,
                                   uint32_t slot_size, uint32_t posted_recvs,
                                   struct ibv_qp *recv_qp, int max_recv_wrs,
                                   struct ibv_recv_wr *recv_wr,
                                   struct ibv_sge *recv_sgl,
                                   void* buf)
{
  recv_info_t* recv = (recv_info_t*) malloc(sizeof(struct recv_info));
  recv->push_ptr = push_ptr;
  recv->buf_slots = buf_slots;
  recv->slot_size = slot_size;
  recv->posted_recvs = posted_recvs;
  recv->recv_qp = recv_qp;
  recv->buf = buf;
  recv->recv_wr = recv_wr; //(struct ibv_recv_wr *) malloc(max_recv_wrs * sizeof(struct ibv_recv_wr));
  recv->recv_sgl = recv_sgl;//(struct ibv_sge *) malloc(max_recv_wrs * sizeof(struct ibv_sge));

  for (int i = 0; i < max_recv_wrs; i++) {
    recv->recv_sgl[i].length = slot_size;
    recv->recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
    recv->recv_wr[i].sg_list = &(recv->recv_sgl[i]);
    recv->recv_wr[i].num_sge = 1;
  }
  return recv;
}


// Post Receives
static inline void post_recvs_with_recv_info(struct recv_info *recv, uint32_t recv_num)
{
  if (recv_num == 0) return;
  uint16_t j;
  struct ibv_recv_wr *bad_recv_wr;
  for (j = 0; j < recv_num; j++) {
    recv->recv_sgl[j].addr = (uintptr_t) recv->buf + (recv->push_ptr * recv->slot_size);
    //printf("Posting a receive at push ptr %u at address %lu \n", recv->w_push_ptr, recv->recv_sgl[j].addr);
    MOD_INCR(recv->push_ptr, recv->buf_slots);
    recv->recv_wr[j].next = (j == recv_num - 1) ?
                            NULL : &recv->recv_wr[j + 1];
  }
  int ret = ibv_post_recv(recv->recv_qp, &recv->recv_wr[0], &bad_recv_wr);
  if (ENABLE_ASSERTIONS) {
    if (ret != 0 ) {
      my_printf(red, "ibv_post_recv error %d \n", ret);
      assert(false);
    }
  }
}

// polling completion queue --blocking
static inline uint32_t poll_cq(struct ibv_cq *cq, int num_comps, struct ibv_wc *wc, const char* caller_flag)
{
  int comps = 0;
  uint32_t debug_cnt = 0;
  while(comps < num_comps) {
    if (ENABLE_ASSERTIONS && debug_cnt > M_256) {
      printf("Someone is stuck waiting for a completion %d / %d , type %s  \n", comps, num_comps, caller_flag );
      debug_cnt = 0;
    }
    int new_comps = ibv_poll_cq(cq, num_comps - comps, &wc[comps]);
    if(new_comps != 0) {
//			 printf("I see completions %d\n", new_comps);
      /* Ideally, we should check from comps -> new_comps - 1 */
      if(ENABLE_ASSERTIONS && wc[comps].status != 0) {
        fprintf(stderr, "Bad wc status %d: %s\n", wc[comps].status, caller_flag);
        assert(false);
      }
      comps += new_comps;
    }
    if (ENABLE_ASSERTIONS) debug_cnt++;
  }
  return debug_cnt;
}

static inline void adaptive_inlining (uint32_t mes_size, struct ibv_send_wr *send_wr, uint16_t wr_num)
{
  int flag = mes_size < MAXIMUM_INLINE_SIZE ? IBV_SEND_INLINE : 0;
  for (uint16_t i = 0; i < wr_num; i++)
    send_wr[i].send_flags = flag;
}

// Post a quorum broadcast and post the appropriate receives for it
static inline void post_quorum_broadasts_and_recvs(struct recv_info *recv_info, uint32_t recvs_to_post_num,
                                                   struct quorum_info *q_info, uint16_t br_i, uint64_t br_tx,
                                                   struct ibv_send_wr *send_wr, struct ibv_qp *send_qp,
                                                   int enable_inlining)
{
  struct ibv_send_wr *bad_send_wr;
  if (recvs_to_post_num > 0) {
    // printf("Wrkr %d posting %d recvs\n", g_id,  recvs_to_post_num);
    if (recvs_to_post_num) post_recvs_with_recv_info(recv_info, recvs_to_post_num);
    recv_info->posted_recvs += recvs_to_post_num;
  }
  if (DEBUG_SS_BATCH)
    my_printf(green, "Sending %u bcasts, total %lu \n", br_i, br_tx);

  send_wr[((br_i - 1) * MESSAGES_IN_BCAST) + q_info->last_active_rm_id].next = NULL;
  int ret = ibv_post_send(send_qp, &send_wr[q_info->first_active_rm_id], &bad_send_wr);
  if (ENABLE_ASSERTIONS) CPE(ret, "Broadcast ibv_post_send error", ret);
  if (!ENABLE_ADAPTIVE_INLINING)
    send_wr[q_info->first_active_rm_id].send_flags = enable_inlining == 1 ? IBV_SEND_INLINE : 0;
}


static inline int find_how_many_messages_can_be_polled(struct ibv_cq *recv_cq, struct ibv_wc *recv_wc,
                                                       uint32_t *completed_but_not_polled,
                                                       uint32_t buf_slots,
                                                       uint16_t t_id)
{
  int completed_messages = ibv_poll_cq(recv_cq, buf_slots, recv_wc);
  if (ENABLE_ASSERTIONS) assert(completed_messages >= 0);

  // The caller knows that all complted messages get polled
  if (completed_but_not_polled == NULL) return completed_messages;

  // There is a chance that you wont be able to poll all completed messages,
  // because of downstream back-pressure, in which case you
  // pass the number of completed (i.e. from the completion queue) but not polled messages to the next round
  if (unlikely(*completed_but_not_polled > 0)) {
    completed_messages += (*completed_but_not_polled);
    (*completed_but_not_polled) = 0;
  }

  return completed_messages;
}


#endif //KITE_RDMA_GEN_UTIL_H

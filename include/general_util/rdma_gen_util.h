//
// Created by vasilis on 24/06/2020.
//

#ifndef KITE_RDMA_GEN_UTIL_H
#define KITE_RDMA_GEN_UTIL_H

#include "top.h"
#include "hrd.h"
#include "config_util.h"

// Set up the receive info
static recv_info_t* init_recv_info(struct hrd_ctrl_blk *cb, uint32_t push_ptr, uint32_t buf_slots,
                                   uint32_t slot_size, uint32_t posted_recvs,
                                   struct ibv_qp *recv_qp, int max_recv_wrs,
                                   struct ibv_recv_wr *recv_wr,
                                   struct ibv_sge *recv_sgl,
                                   void* buf)
{
  recv_info_t* recv = (recv_info_t*) malloc(sizeof(recv_info_t));
  recv->push_ptr = push_ptr;
  recv->buf_slots = buf_slots;
  recv->slot_size = slot_size;
  recv->posted_recvs = posted_recvs;
  recv->recv_qp = recv_qp;
  recv->buf = buf;
  recv->recv_wr = recv_wr;
  recv->recv_sgl = recv_sgl;

  for (int i = 0; i < max_recv_wrs; i++) {
    // It can be that incoming messages have no payload,
    // and thus sgls (which store buffer pointers) are not useful
    if (recv->buf_slots == 0) {
      recv->recv_wr[i].sg_list = recv->recv_sgl;
      recv->recv_sgl->addr = (uintptr_t) buf;
      recv->recv_sgl->length = slot_size;
      recv->recv_sgl->lkey = cb->dgram_buf_mr->lkey;
    }
    else {
      recv->recv_wr[i].sg_list = &(recv->recv_sgl[i]);
      recv->recv_sgl[i].length = slot_size;
      recv->recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
    }
    recv->recv_wr[i].num_sge = 1;
  }
  return recv;
}


// Post Receives
static inline void post_recvs_with_recv_info(recv_info_t *recv, uint32_t recv_num)
{
  if (ENABLE_ASSERTIONS) assert(recv_num < BILLION); // asserting the number is positive
  if (recv_num == 0) return;
  uint16_t j;
  struct ibv_recv_wr *bad_recv_wr;
  for (j = 0; j < recv_num; j++) {
    if (recv->buf_slots > 0)
      recv->recv_sgl[j].addr = (uintptr_t) recv->buf + (recv->push_ptr * recv->slot_size);
    //printf("Posting a receive at push ptr %u at address %lu \n", recv->w_push_ptr, recv->recv_sgl[j].addr);
    MOD_INCR(recv->push_ptr, recv->buf_slots);
    recv->recv_wr[j].next = (j == recv_num - 1) ?
                            NULL : &recv->recv_wr[j + 1];
  }
  int ret = ibv_post_recv(recv->recv_qp, &recv->recv_wr[0], &bad_recv_wr);
  recv->posted_recvs += recv_num;
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
static inline void post_quorum_broadasts_and_recvs(recv_info_t *recv_info, uint32_t recvs_to_post_num,
                                                   quorum_info_t *q_info, uint16_t br_i, uint64_t br_tx,
                                                   struct ibv_send_wr *send_wr, struct ibv_qp *send_qp,
                                                   int enable_inlining)
{
  struct ibv_send_wr *bad_send_wr;
  if (recvs_to_post_num > 0) {
    // printf("Wrkr %d posting %d recvs\n", g_id,  recvs_to_post_num);
    if (recvs_to_post_num) post_recvs_with_recv_info(recv_info, recvs_to_post_num);
  }
  if (DEBUG_SS_BATCH)
    my_printf(green, "Sending %u bcasts, total %lu \n", br_i, br_tx);

  //send_wr[((br_i - 1) * MESSAGES_IN_BCAST) + q_info->last_active_rm_id].next = NULL;
  send_wr[get_last_message_of_bcast(br_i, q_info)].next = NULL;
  int ret = ibv_post_send(send_qp, &send_wr[get_first_mes_of_bcast(q_info)], &bad_send_wr);
  if (ENABLE_ASSERTIONS) CPE(ret, "Broadcast ibv_post_send error", ret);
  if (!ENABLE_ADAPTIVE_INLINING)
    send_wr[get_first_mes_of_bcast(q_info)].send_flags = enable_inlining ? IBV_SEND_INLINE : 0;
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


static inline void form_bcast_links(uint64_t *br_tx, int bcast_ss_batch, quorum_info_t *q_info,
                                    uint16_t br_i, struct ibv_send_wr *send_wr,
                                    struct ibv_cq *dgram_send_cq, const char *mes, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) assert (bcast_ss_batch > 0);
  struct ibv_wc signal_send_wc;

  // Do a Signaled Send every PREP_BCAST_SS_BATCH broadcasts (PREP_BCAST_SS_BATCH * (MACHINE_NUM - 1) messages)
  if ((*br_tx) % bcast_ss_batch == 0)
    flag_the_first_bcast_message_signaled(q_info, send_wr);

  (*br_tx)++;
  if ((*br_tx) % bcast_ss_batch == bcast_ss_batch - 1) {
    poll_cq(dgram_send_cq, 1, &signal_send_wc, mes);
  }
  // Have the last message of each broadcast pointing to the first message of the next bcast
  if (br_i > 0)
    last_mes_of_bcast_point_to_frst_mes_of_next_bcast(br_i, q_info, send_wr);
}


static inline void selective_signaling_for_unicast(uint64_t *tx, int ss_batch,
                                                   struct ibv_send_wr *send_wr,
                                                   uint16_t mes_i,
                                                   struct ibv_cq *dgram_send_cq,
                                                   bool allow_inline,
                                                   const char *mes,
                                                   uint16_t t_id)
{
  if (!allow_inline && ENABLE_ADAPTIVE_INLINING)
    adaptive_inlining(send_wr->sg_list->length, &send_wr[mes_i], 1);
  else send_wr[mes_i].send_flags = allow_inline ? IBV_SEND_INLINE : 0;


  struct ibv_wc signal_send_wc;
  if ((*tx) % ss_batch == 0) {
    send_wr[mes_i].send_flags |= IBV_SEND_SIGNALED;
  }

  if ((*tx) % ss_batch == ss_batch - 1) {
    poll_cq(dgram_send_cq, 1, &signal_send_wc, mes);
  }
  (*tx)++;
}


#endif //KITE_RDMA_GEN_UTIL_H

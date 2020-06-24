//
// Created by vasilis on 24/06/2020.
//

#ifndef KITE_RDMA_GEN_UTIL_H
#define KITE_RDMA_GEN_UTIL_H

#include "top.h"
#include "hrd.h"

// Set up the receive info
static recv_info_t* init_recv_info(struct hrd_ctrl_blk *cb,
                            uint32_t push_ptr, uint32_t buf_slots,
                            uint32_t slot_size, uint32_t posted_recvs,
                            struct ibv_qp *recv_qp, int max_recv_wrs, void* buf)
{
  recv_info_t* recv = (recv_info_t*) malloc(sizeof(struct recv_info));
  recv->push_ptr = push_ptr;
  recv->buf_slots = buf_slots;
  recv->slot_size = slot_size;
  recv->posted_recvs = posted_recvs;
  recv->recv_qp = recv_qp;
  recv->buf = buf;
  recv->recv_wr = (struct ibv_recv_wr *) malloc(max_recv_wrs * sizeof(struct ibv_recv_wr));
  recv->recv_sgl = (struct ibv_sge *) malloc(max_recv_wrs * sizeof(struct ibv_sge));

  for (int i = 0; i < max_recv_wrs; i++) {
    recv->recv_sgl[i].length = slot_size;
    recv->recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
    recv->recv_wr[i].sg_list = &(recv->recv_sgl[i]);
    recv->recv_wr[i].num_sge = 1;
  }
  return recv;
}

#endif //KITE_RDMA_GEN_UTIL_H

//
// Created by vasilis on 14/07/20.
//

#ifndef ODYSSEY_NETWORK_CONTEXT_H
#define ODYSSEY_NETWORK_CONTEXT_H

#include <multicast.h>
#include <hrd.h>
#include "top.h"
#include "fifo.h"
#include "generic_inline_util.h"

typedef struct context context_t;

typedef void (*insert_helper_t) (context_t *, void*, void *, uint32_t);
typedef bool (*recv_handler_t)(context_t *);
typedef void (*send_helper_t)(context_t *);
typedef void (*recv_kvs_t)(context_t *);

typedef enum{
  SEND_CREDITS_LDR_RECV_NONE,
  RECV_CREDITS,

  SEND_UNI_REQ_RECV_REP,
  SEND_UNI_REP_RECV_UNI_REQ,

  SEND_UNI_REQ_RECV_LDR_REP,
  SEND_UNI_REP_LDR_RECV_UNI_REQ,

  SEND_BCAST_LDR_RECV_UNI,
  SEND_UNI_REP_RECV_LDR_BCAST,

  SEND_UNI_REP_TO_BCAST,
  SEND_BCAST_RECV_UNI

} flow_type_t;

typedef enum {
  RECV_NOTHING,
  RECV_REPLY,
  RECV_REQ,
  RECV_SEC_ROUND

} recv_type_t;

typedef struct qp_meta_mfs {
  recv_handler_t recv_handler;
  send_helper_t send_helper;
  recv_kvs_t recv_kvs;
  insert_helper_t insert_helper;
} mf_t;


typedef struct per_qp_meta {
  struct ibv_send_wr *send_wr;
  struct ibv_sge *send_sgl;
  struct ibv_sge *recv_sgl;
  struct ibv_wc *recv_wc;
  struct ibv_recv_wr *recv_wr;
  struct ibv_mr *send_mr;

  uint32_t send_wr_num;
  uint32_t recv_wr_num;

  uint32_t send_q_depth;
  uint32_t recv_q_depth;
  flow_type_t flow_type;
  recv_type_t recv_type;

  uint32_t receipient_num;
  uint32_t remote_senders_num;
  uint8_t leader_m_id; // if there exist

  uint32_t ss_batch;

  // flow control
  uint16_t *credits;
  bool needs_credits;
  fifo_t *mirror_remote_recv_fifo;

  // send-recv fifos
  fifo_t *recv_fifo;
  bool has_recv_fifo;
  fifo_t *send_fifo;
  bool has_send_fifo;
  uint32_t recv_buf_size;
  uint32_t recv_buf_slot_num;

  uint32_t pull_ptr;
  uint32_t push_ptr;

  struct ibv_qp *send_qp;
  struct ibv_qp *recv_qp; //send and recv qps are the same except if we are using multicast
  struct ibv_cq *recv_cq;
  struct ibv_cq *send_cq;
  recv_info_t *recv_info;
  uint32_t recv_size;
  uint32_t send_size;
  bool mcast_send;
  bool mcast_recv;
  uint16_t mcast_qp_id;
  bool enable_inlining;

  uint64_t sent_tx; //how many messages have been sent

  uint32_t completed_but_not_polled;

  // debug info
  uint32_t outstanding_messages;
  uint32_t wait_for_reps_ctr;

  uint32_t time_out_cnt;

  char *send_string;
  char *recv_string;
  mf_t *mfs;

} per_qp_meta_t;

typedef struct rdma_context {
  struct ibv_mr *recv_mr;
  struct ibv_pd *pd;
  uint16_t *local_id;
} rdma_context_t;


typedef struct context {
  hrd_ctrl_blk_t *cb;

  mcast_cb_t *mcast_cb;
  per_qp_meta_t *qp_meta;
  uint16_t qp_num;
  uint8_t m_id;
  uint16_t t_id;
  uint32_t total_recv_buf_size;
  void *recv_buffer;
  rdma_context_t *rdma_ctx;
  char* local_ip;
  void* appl_ctx;

} context_t;

static void check_ctx(context_t *ctx)
{
  for (int qp_i = 0; qp_i < ctx->qp_num; ++qp_i) {
    per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_i];
    assert(qp_meta->send_wr != NULL);
    assert(qp_meta->send_qp != NULL);
    assert(qp_meta->recv_qp != NULL);
  }

}

static void allocate_work_requests(per_qp_meta_t* qp_meta)
{
  qp_meta->send_wr = malloc(qp_meta->send_wr_num * sizeof(struct ibv_send_wr));
  switch (qp_meta->flow_type){
    case SEND_CREDITS_LDR_RECV_NONE:
      qp_meta->send_sgl = malloc(sizeof(struct ibv_sge));
      break;
    case SEND_BCAST_LDR_RECV_UNI:
    case SEND_BCAST_RECV_UNI:
      qp_meta->send_sgl = malloc(MAX_BCAST_BATCH * sizeof(struct ibv_sge));
      break;
    case RECV_CREDITS:
      break;
    case SEND_UNI_REQ_RECV_REP:
    case SEND_UNI_REQ_RECV_LDR_REP:
    case SEND_UNI_REP_RECV_LDR_BCAST:
    case SEND_UNI_REP_TO_BCAST:
    case SEND_UNI_REP_LDR_RECV_UNI_REQ:
      qp_meta->send_sgl = malloc(qp_meta->send_wr_num * sizeof(struct ibv_sge));
      break;
    default: assert(false);
  }


  if (qp_meta->recv_wr_num > 0) {
    qp_meta->recv_wr = malloc(qp_meta->recv_wr_num * sizeof(struct ibv_recv_wr));
    qp_meta->recv_sgl = malloc(qp_meta->recv_wr_num * sizeof(struct ibv_sge));
    qp_meta->recv_wc = malloc(qp_meta->recv_wr_num * sizeof(struct ibv_wc));
  }
}

static void create_per_qp_meta(per_qp_meta_t* qp_meta,
                               uint32_t send_wr_num,
                               uint32_t recv_wr_num,
                               flow_type_t flow_type,
                               recv_type_t recv_type,

                               uint32_t receipient_num,
                               uint32_t remote_senders_num,
                               uint32_t recv_fifo_slot_num,

                               uint32_t recv_size,
                               uint32_t send_size,
                               bool mcast_send,
                               bool mcast_recv,
                               uint16_t mcast_qp_id,
                               uint8_t leader_m_id,
                               uint32_t send_fifo_slot_num,
                               uint16_t credits,
                               uint16_t mes_header,
                               const char *send_string,
                               const char *recv_string)
{
  qp_meta->send_wr_num = send_wr_num;
  qp_meta->recv_wr_num = recv_wr_num;
  qp_meta->flow_type = flow_type;
  qp_meta->recv_type = recv_type;
  qp_meta->receipient_num = receipient_num;
  qp_meta->remote_senders_num = remote_senders_num;
  qp_meta->pull_ptr = 0;
  qp_meta->push_ptr = 0;
  qp_meta->leader_m_id = leader_m_id;

  qp_meta->recv_buf_slot_num = recv_fifo_slot_num;
  qp_meta->recv_buf_size = recv_fifo_slot_num * recv_size;
  qp_meta->recv_size = recv_size;
  qp_meta->send_size = send_size;
  qp_meta->mcast_send = mcast_send;
  qp_meta->mcast_recv = mcast_recv;
  qp_meta->mcast_qp_id = mcast_qp_id;
  qp_meta->completed_but_not_polled = 0;




  if (send_string != NULL) {
    qp_meta->send_string = malloc(strlen(send_string) + 1);
    strcpy(qp_meta->send_string, send_string);
  }

  if (recv_string != NULL) {
    qp_meta->recv_string = malloc(strlen(recv_string) + 1);
    strcpy(qp_meta->recv_string, recv_string);
  }




  switch (qp_meta->flow_type){
    case SEND_BCAST_LDR_RECV_UNI:
    case SEND_BCAST_RECV_UNI:
      qp_meta->ss_batch = (uint32_t) MAX((MIN_SS_BATCH / (receipient_num)), (MAX_BCAST_BATCH + 2));
      qp_meta->send_q_depth = ((qp_meta->ss_batch * receipient_num) + 10);
      break;
    case RECV_CREDITS:
      break;
    case SEND_CREDITS_LDR_RECV_NONE:
    case SEND_UNI_REQ_RECV_REP:
    case SEND_UNI_REQ_RECV_LDR_REP:
    case SEND_UNI_REP_RECV_LDR_BCAST:
    case SEND_UNI_REP_TO_BCAST:
    case SEND_UNI_REP_RECV_UNI_REQ:
    case SEND_UNI_REP_LDR_RECV_UNI_REQ:
      qp_meta->ss_batch = (uint32_t) MAX(MIN_SS_BATCH, (send_wr_num + 2));
      qp_meta->send_q_depth = qp_meta->ss_batch + 3;
      break;
    default: assert(false);
  }
  qp_meta->recv_q_depth = recv_wr_num + 3;


  if (credits > 0) {
    qp_meta->needs_credits = true;
    int credit_rows =
      receipient_num == 1 ? 1 : MACHINE_NUM;
    qp_meta->credits = malloc(credit_rows * sizeof(uint16_t));
    for (int cr_i = 0; cr_i < credit_rows; ++cr_i) {
      qp_meta->credits[cr_i] = credits;
    }
  }


  qp_meta->enable_inlining = send_size <= MAXIMUM_INLINE_SIZE;
  qp_meta->has_recv_fifo = true;
  qp_meta->recv_fifo = calloc(1, sizeof(fifo_t));
  qp_meta->recv_fifo->fifo = NULL; // will be filled after initializing the hrd_cb
  qp_meta->recv_fifo->max_size = recv_fifo_slot_num;
  qp_meta->recv_fifo->max_byte_size = recv_fifo_slot_num * recv_size;
  qp_meta->recv_fifo->slot_size = recv_size;

  if (send_fifo_slot_num > 0) {
    qp_meta->has_send_fifo = true;
    qp_meta->send_fifo = fifo_constructor(send_fifo_slot_num,
                                          send_size, true, mes_header, 1);
  }
  else qp_meta->has_send_fifo = false;


  allocate_work_requests(qp_meta);
}


// Set up the receive info
static recv_info_t* cust_init_recv_info(uint32_t lkey, per_qp_meta_t *qp_meta,
                                        struct ibv_qp *recv_qp)
{
  recv_info_t* recv = (recv_info_t*) malloc(sizeof(recv_info_t));
  recv->push_ptr = qp_meta->push_ptr;
  recv->buf_slots = qp_meta->recv_buf_slot_num;
  recv->slot_size = qp_meta->recv_size;
  recv->posted_recvs = 0;
  recv->recv_qp = recv_qp;
  recv->buf = qp_meta->recv_fifo->fifo;
  assert(recv->buf != NULL);
  recv->recv_wr = qp_meta->recv_wr;
  recv->recv_sgl = qp_meta->recv_sgl;

  for (int i = 0; i < qp_meta->recv_wr_num; i++) {
    // It can be that incoming messages have no payload,
    // and thus sgls (which store buffer pointers) are not useful
    if (recv->buf_slots == 0) {
      recv->recv_wr[i].sg_list = recv->recv_sgl;
      recv->recv_sgl->addr = (uintptr_t) recv->buf;
      recv->recv_sgl->length = qp_meta->recv_size;
      recv->recv_sgl->lkey = lkey;
    }
    else {
      recv->recv_wr[i].sg_list = &(recv->recv_sgl[i]);
      recv->recv_sgl[i].length = qp_meta->recv_size;
      recv->recv_sgl[i].lkey = lkey;
    }
    recv->recv_wr[i].num_sge = 1;
  }
  return recv;
}

static void init_ctx_recv_infos(context_t *ctx)
{
  for (int qp_i = 0; qp_i < ctx->qp_num; ++qp_i) {
    per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_i];
    if (qp_meta->recv_wr_num == 0) continue;

    uint32_t lkey = ctx->qp_meta[qp_i].mcast_recv ?
                    ctx->mcast_cb->recv_mr->lkey :
                    ctx->rdma_ctx->recv_mr->lkey;


    qp_meta->recv_info =
      cust_init_recv_info(lkey, qp_meta, qp_meta->recv_qp);
  }
}

static void init_ctx_send_mrs(context_t *ctx)
{
  for (int qp_i = 0; qp_i < ctx->qp_num; ++qp_i) {
    per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_i];
    if (!qp_meta->has_send_fifo) continue;
    if (ENABLE_ASSERTIONS) printf("Registering %p through %p \n", qp_meta->send_fifo->fifo,
           qp_meta->send_fifo->fifo +
           qp_meta->send_fifo->max_byte_size);
    qp_meta->send_mr = register_buffer(ctx->rdma_ctx->pd,
                                  qp_meta->send_fifo->fifo,
                                  qp_meta->send_fifo->max_byte_size);

  }
}

static void set_up_ctx_qps(context_t *ctx)
{
  for (int qp_i = 0; qp_i < ctx->qp_num; ++qp_i) {
    per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_i];
    qp_meta->send_qp = ctx->cb->dgram_qp[qp_i];
    qp_meta->send_cq = ctx->cb->dgram_send_cq[qp_i];
    if (qp_meta->mcast_recv) {
      qp_meta->recv_qp = ctx->mcast_cb->recv_qp[qp_meta->mcast_qp_id];
      qp_meta->recv_cq = ctx->mcast_cb->recv_cq[qp_meta->mcast_qp_id];
    }
    else {
      qp_meta->recv_qp = ctx->cb->dgram_qp[qp_i];
      qp_meta->recv_cq = ctx->cb->dgram_recv_cq[qp_i];
    }

    //ctx
  }
}

static void const_set_up_wr(context_t *ctx, uint16_t qp_i,
                            uint16_t wr_i, uint16_t sgl_i,
                            bool last, uint16_t rm_id)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_i];
  struct ibv_send_wr* send_wr = &qp_meta->send_wr[wr_i];
  struct ibv_sge *send_sgl = &qp_meta->send_sgl[sgl_i];

  if (qp_meta->mcast_send) {
    mcast_cb_t *mcast_cb = ctx->mcast_cb;
    send_wr->wr.ud.ah = mcast_cb->send_ah[qp_meta->mcast_qp_id];
    send_wr->wr.ud.remote_qpn = mcast_cb->qpn[qp_meta->mcast_qp_id];
    send_wr->wr.ud.remote_qkey = mcast_cb->qkey[qp_meta->mcast_qp_id];
  }
  else {
    send_wr->wr.ud.ah = rem_qp[rm_id][ctx->t_id][qp_i].ah;
    send_wr->wr.ud.remote_qpn = (uint32_t) rem_qp[rm_id][ctx->t_id][qp_i].qpn;
    send_wr->wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
  }

  if (qp_meta->flow_type == SEND_CREDITS_LDR_RECV_NONE) {
    send_wr->opcode = IBV_WR_SEND_WITH_IMM;
    send_wr->num_sge = 0;
    send_wr->imm_data = ctx->m_id;
  }
  else {
    send_wr->opcode = IBV_WR_SEND;
    send_wr->num_sge = 1;
  }
  send_wr->sg_list = send_sgl;
  send_wr->sg_list->length = qp_meta->send_size;
  if (qp_meta->flow_type == SEND_CREDITS_LDR_RECV_NONE)
    assert(send_wr->sg_list->length == 0);
  if (qp_meta->enable_inlining) send_wr->send_flags = IBV_SEND_INLINE;
  else {
    send_sgl->lkey = qp_meta->send_mr->lkey;
    send_wr->send_flags = 0;
  }

  send_wr->next = last ? NULL : &send_wr[1];
}

static void init_ctx_send_wrs(context_t *ctx)
{
  for (uint16_t qp_i = 0; qp_i < ctx->qp_num; ++qp_i) {
    per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_i];
    switch (qp_meta->flow_type){
      case SEND_BCAST_LDR_RECV_UNI:
      case SEND_BCAST_RECV_UNI:
        for (uint16_t br_i = 0; br_i < MAX_BCAST_BATCH; br_i++) {
          for (uint16_t i = 0; i < MESSAGES_IN_BCAST; i++) {
            uint16_t rm_id = (uint16_t) (i < ctx->m_id ? i : i + 1);
            uint16_t wr_i = (uint16_t) ((br_i * MESSAGES_IN_BCAST) + i);
            bool last = (i == MESSAGES_IN_BCAST - 1);
            const_set_up_wr(ctx, qp_i, wr_i, br_i, last, rm_id);
          }
        }
        break;
      case RECV_CREDITS:
        break;
      case SEND_CREDITS_LDR_RECV_NONE:
        for (uint16_t wr_i = 0; wr_i < qp_meta->send_wr_num; ++wr_i) {
          const_set_up_wr(ctx, qp_i, wr_i, 0, true, qp_meta->leader_m_id);
        }
        break;
      case SEND_UNI_REQ_RECV_REP:
      case SEND_UNI_REQ_RECV_LDR_REP:
      case SEND_UNI_REP_RECV_LDR_BCAST:
      case SEND_UNI_REP_TO_BCAST:
      case SEND_UNI_REP_LDR_RECV_UNI_REQ:
        for (uint16_t wr_i = 0; wr_i < qp_meta->send_wr_num; ++wr_i) {
          const_set_up_wr(ctx, qp_i, wr_i, wr_i, wr_i == qp_meta->send_wr_num - 1,
                          qp_meta->leader_m_id);
        }
        break;
      default: assert(false);
    }
  }
}


static void set_per_qp_meta_recv_fifos(context_t *ctx)
{

  for (int qp_i = 0; qp_i < ctx->qp_num; ++qp_i) {
    per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_i];

    if (qp_i == 0) {
      qp_meta->recv_fifo->fifo = ctx->recv_buffer;
      assert(qp_meta->recv_fifo->fifo != NULL);
    }
    else {
      per_qp_meta_t *prev_qp_meta = &ctx->qp_meta[qp_i - 1];
      assert(prev_qp_meta->recv_fifo->fifo != NULL);
      qp_meta->recv_fifo->fifo = prev_qp_meta->recv_fifo->fifo +
        prev_qp_meta->recv_fifo->max_byte_size;
    }
    if (ENABLE_ASSERTIONS) {
      printf("Recv fifo for qp %u starts at %p ends at %p, total w_size %u, slot number %u \n",
             qp_i, qp_meta->recv_fifo->fifo, qp_meta->recv_fifo->fifo + qp_meta->recv_fifo->max_byte_size,
             qp_meta->recv_fifo->max_byte_size, qp_meta->recv_buf_slot_num);
    }

  }
}


static int *get_recv_q_depths(per_qp_meta_t* qp_meta, uint16_t qp_num)
{
  int *recv_q_depth = (int *) malloc(qp_num * sizeof(int));
  for (int i = 0; i < qp_num; ++i) {
    recv_q_depth[i] = qp_meta[i].recv_q_depth;
    if (recv_q_depth[i] == 0) recv_q_depth[i] = 1;
  }
  return recv_q_depth;
}

static int *get_send_q_depths(per_qp_meta_t* qp_meta, uint16_t qp_num)
{
  int *send_q_depth = (int *) malloc(qp_num * sizeof(int));
  for (int i = 0; i < qp_num; ++i) {
    send_q_depth[i] = qp_meta[i].send_q_depth;
    if (send_q_depth[i] == 0) send_q_depth[i] = 1;
    if (ENABLE_ASSERTIONS) printf("Send q depth %u --> %u \n ", i, send_q_depth[i]);
  }
  return send_q_depth;
}

static void set_up_ctx_mcast(context_t *ctx)
{
  uint32_t *recv_q_depth = NULL;
  uint16_t *group_to_send_to = NULL;
  bool *recvs_from_flow =  NULL;
  uint16_t *groups_per_flow = NULL;
  uint16_t recv_qp_num = 0, send_num = 0, flow_num = 0;
  for (int qp_i = 0; qp_i < ctx->qp_num; ++qp_i) {
    per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_i];
    if (qp_meta->mcast_recv || qp_meta->mcast_send)
      flow_num++;
    if (qp_meta->mcast_recv) recv_qp_num++;
    if (qp_meta->mcast_send) send_num++;      
  }

  if (flow_num == 0) return;
  recvs_from_flow = (bool *) calloc(flow_num, sizeof(bool));
  recv_q_depth = (uint32_t *) calloc(recv_qp_num, sizeof(int));
  group_to_send_to = (uint16_t *) calloc(flow_num, (sizeof(uint16_t)));
  groups_per_flow = (uint16_t *) calloc(flow_num, (sizeof(uint16_t)));
  
  int flow_i = -1;
  for (int qp_i = 0; qp_i < ctx->qp_num; ++qp_i) {
    per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_i];
    if (qp_meta->mcast_recv || qp_meta->mcast_send) {
      flow_i++;
      assert(flow_i == qp_meta->mcast_qp_id);
      switch (qp_meta->flow_type){
        case SEND_BCAST_LDR_RECV_UNI:
        case SEND_UNI_REP_RECV_LDR_BCAST:
          groups_per_flow[flow_i] = 1;
          if (qp_meta->mcast_send)
            group_to_send_to[flow_i] = 0;
          break;
        case SEND_BCAST_RECV_UNI:
        case SEND_UNI_REP_TO_BCAST:
          groups_per_flow[flow_i] = MACHINE_NUM;
          if (qp_meta->mcast_send)
            group_to_send_to[flow_i] = ctx->m_id;
          break;
        case RECV_CREDITS:
        case SEND_CREDITS_LDR_RECV_NONE:
        case SEND_UNI_REQ_RECV_REP:
        case SEND_UNI_REQ_RECV_LDR_REP:
        case SEND_UNI_REP_LDR_RECV_UNI_REQ:
          assert(false);
        default: assert(false);
      }

      if (!qp_meta->mcast_send)
        group_to_send_to[flow_i] = groups_per_flow[flow_i];
    }
    if (qp_meta->mcast_recv) {
      assert(flow_i < flow_num);
      recv_q_depth[flow_i] = qp_meta->recv_q_depth;
      recvs_from_flow[flow_i] = true;
    }
  }
  
  
  ctx->mcast_cb = create_mcast_cb(flow_num, recv_qp_num, send_num,
                         groups_per_flow, recv_q_depth,
                         group_to_send_to,
                         recvs_from_flow,
                         ctx->local_ip,
                         ctx->recv_buffer,
                         ctx->total_recv_buf_size, ctx->t_id);
  
}

static void init_rdma_ctx(context_t *ctx, hrd_ctrl_blk_t *cb)
{
  ctx->rdma_ctx = (rdma_context_t *) malloc(sizeof(rdma_context_t));
  ctx->rdma_ctx->pd = cb->pd;
  ctx->rdma_ctx->recv_mr = cb->dgram_buf_mr;
  ctx->rdma_ctx->local_id = malloc(ctx->qp_num * sizeof(uint16_t));
  for (int qp_i = 0; qp_i < ctx->qp_num; ++qp_i) {
    ctx->rdma_ctx->local_id[qp_i] = hrd_get_local_lid(cb->dgram_qp[qp_i]->context,
                                                      cb->dev_port_id);
  }
}

static void ctx_prepost_recvs(context_t *ctx)
{
  for (int qp_i = 0; qp_i < ctx->qp_num; ++qp_i) {
    per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_i];
    post_recvs_with_recv_info(qp_meta->recv_info,
                              qp_meta->recv_wr_num);
  }
}

static void ctx_qp_meta_mfs(per_qp_meta_t *qp_meta,
                            recv_handler_t recv_handler,
                            send_helper_t send_helper,
                            recv_kvs_t recv_kvs,
                            insert_helper_t insert_helper)
{
  qp_meta->mfs = malloc(sizeof(mf_t));
  qp_meta->mfs->recv_handler = recv_handler;
  qp_meta->mfs->send_helper = send_helper;
  qp_meta->mfs->recv_kvs = recv_kvs;
  qp_meta->mfs->insert_helper = insert_helper;

}

static void ctx_qp_meta_mirror_buffers(per_qp_meta_t *qp_meta,
                                       uint32_t max_size,
                                       uint16_t fifo_num)
{
  qp_meta->mirror_remote_recv_fifo =
    fifo_constructor(max_size, sizeof(uint16_t), false, 0, fifo_num);
}


static void set_up_ctx(context_t *ctx)
{
  ctx->total_recv_buf_size = 0;
  for (int qp_i = 0; qp_i < ctx->qp_num; ++qp_i) {
    ctx->total_recv_buf_size += ctx->qp_meta[qp_i].recv_buf_size;
  }
  if (ENABLE_ASSERTIONS) printf("total w_size %u \n ", ctx->total_recv_buf_size);
  hrd_ctrl_blk_t *cb =
    hrd_ctrl_blk_init(ctx->t_id,	/* local_hid */
                                   0, -1, /* port_index, numa_node_id */
                                   0, 0,	/* #conn qps, uc */
                                   NULL, 0, -1,	/* prealloc conn recv_buf, recv_buf capacity, key */
                                   ctx->qp_num, ctx->total_recv_buf_size,	/* num_dgram_qps, dgram_buf_size */
                                   MASTER_SHM_KEY + ctx->t_id, /* key */
                                   get_recv_q_depths(ctx->qp_meta, ctx->qp_num),
                                   get_send_q_depths(ctx->qp_meta, ctx->qp_num)); /* Depth of the dgram RECV Q*/

  ctx->cb = cb;
  ctx->recv_buffer = (void*) cb->dgram_buf;
  init_rdma_ctx(ctx, cb);
  init_ctx_send_mrs(ctx);
  set_per_qp_meta_recv_fifos(ctx);
  set_up_ctx_mcast(ctx);
  set_up_ctx_qps(ctx);
  init_ctx_recv_infos(ctx);
  ctx_prepost_recvs(ctx);
  check_ctx(ctx);
}

static context_t *create_ctx(uint8_t m_id, uint16_t t_id, 
                             uint16_t qp_num, char* local_ip)
{
  context_t *ctx = calloc(1, sizeof(context_t));
  ctx->t_id = t_id;
  ctx->m_id = (uint8_t) m_id;
  ctx->qp_num = qp_num;
  ctx->qp_meta = calloc(qp_num, sizeof(per_qp_meta_t));
  ctx->local_ip = malloc(16);
  strcpy(ctx->local_ip, local_ip);
  return ctx;
}

#endif //ODYSSEY_NETWORK_CONTEXT_H

#include "zk_util.h"
#include "zk_inline_util.h"
#include "init_connect.h"
#include "trace_util.h"
#include "rdma_gen_util.h"

void *follower(void *arg)
{
  struct thread_params params = *(struct thread_params *) arg;
  uint32_t g_id = (uint32_t)( machine_id > LEADER_MACHINE ? ((machine_id - 1) * FOLLOWERS_PER_MACHINE) + params.id :
                  (machine_id * FOLLOWERS_PER_MACHINE) + params.id);
  uint8_t flr_id = (uint8_t) machine_id; // (machine_id > LEADER_MACHINE ? (machine_id - 1) : machine_id);
  uint16_t t_id = (uint16_t) params.id;
  if (t_id == 0) my_printf(yellow, "FOLLOWER-id %d \n", flr_id);
  uint16_t remote_ldr_thread = t_id;

  protocol_t protocol = FOLLOWER;


  int *recv_q_depths, *send_q_depths;
  set_up_queue_depths_ldr_flr(&recv_q_depths, &send_q_depths, protocol);
  struct hrd_ctrl_blk *cb = hrd_ctrl_blk_init(t_id,	/* local_hid */
                                              0, -1, /* port_index, numa_node_id */
                                              0, 0,	/* #conn qps, uc */
                                              NULL, 0, -1,	/* prealloc conn buf, buf size, key */
                                              LEADER_QP_NUM, FLR_BUF_SIZE,	/* num_dgram_qps, dgram_buf_size */
                                              MASTER_SHM_KEY + t_id, /* key */
                                              recv_q_depths, send_q_depths); /* Depth of the dgram RECV Q*/

  uint32_t prep_push_ptr = 0, prep_pull_ptr = 0;
  uint32_t com_push_ptr = 0, com_pull_ptr = 0;
  volatile zk_prep_mes_ud_t *prep_buffer = (volatile zk_prep_mes_ud_t *)(cb->dgram_buf);
  zk_com_mes_ud_t *com_buffer = (zk_com_mes_ud_t *)(cb->dgram_buf + FLR_PREP_BUF_SIZE);

  /* ---------------------------------------------------------------------------
  ------------------------------MULTICAST SET UP-------------------------------
  ---------------------------------------------------------------------------*/

  struct mcast_info *mcast_data;
  struct mcast_essentials *mcast = NULL;
  // need to init mcast before sync, such that we can post recvs
  if (ENABLE_MULTICAST == 1) {
      zk_init_multicast(&mcast_data, &mcast, t_id, cb, protocol);
      assert(mcast != NULL);
  }

  struct ibv_cq *prep_recv_cq = ENABLE_MULTICAST == 1 ? mcast->recv_cq[PREP_MCAST_QP] : cb->dgram_recv_cq[PREP_ACK_QP_ID];
  struct ibv_qp *prep_recv_qp = ENABLE_MULTICAST == 1 ? mcast->recv_qp[PREP_MCAST_QP] : cb->dgram_qp[PREP_ACK_QP_ID];
  struct ibv_cq *com_recv_cq = ENABLE_MULTICAST == 1 ? mcast->recv_cq[COM_MCAST_QP] : cb->dgram_recv_cq[COMMIT_W_QP_ID];
  struct ibv_qp *com_recv_qp = ENABLE_MULTICAST == 1 ? mcast->recv_qp[COM_MCAST_QP] : cb->dgram_qp[COMMIT_W_QP_ID];
  uint32_t lkey = ENABLE_MULTICAST == 1 ?  mcast->recv_mr->lkey : cb->dgram_buf_mr->lkey;
  /* Fill the RECV queues that receive the Commits and Prepares, (we need to do this early) */
  if (WRITE_RATIO > 0) {
    pre_post_recvs(&prep_push_ptr, prep_recv_qp, lkey, (void *) prep_buffer,
                   FLR_PREP_BUF_SLOTS, FLR_MAX_RECV_PREP_WRS, PREP_ACK_QP_ID, (uint32_t)FLR_PREP_RECV_SIZE);
    pre_post_recvs(&com_push_ptr, com_recv_qp, lkey, (void *) com_buffer,
                   FLR_COM_BUF_SLOTS, FLR_MAX_RECV_COM_WRS, COMMIT_W_QP_ID, (uint32_t)FLR_COM_RECV_SIZE);
  }
  /* -----------------------------------------------------
  --------------CONNECT WITH LEADER-----------------------
  ---------------------------------------------------------*/
  setup_connections_and_spawn_stats_thread(g_id, cb, t_id);
  if (MULTICAST_TESTING == 1) multicast_testing(mcast, t_id, cb, COMMIT_W_QP_ID);

  /* -----------------------------------------------------
  --------------DECLARATIONS------------------------------
  ---------------------------------------------------------*/
  // PREP_ACK_QP_ID 0: send ACKS -- receive Preparess
  struct ibv_send_wr ack_send_wr[FLR_MAX_ACK_WRS];
  struct ibv_sge ack_send_sgl[FLR_MAX_ACK_WRS], prep_recv_sgl[FLR_MAX_RECV_PREP_WRS];
  struct ibv_wc prep_recv_wc[FLR_MAX_RECV_PREP_WRS];
  struct ibv_recv_wr prep_recv_wr[FLR_MAX_RECV_PREP_WRS];

  // PREP_ACK_QP_ID 1: send Writes  -- receive Commits
  struct ibv_send_wr w_send_wr[FLR_MAX_W_WRS];
  struct ibv_sge w_send_sgl[FLR_MAX_W_WRS], com_recv_sgl[FLR_MAX_RECV_COM_WRS];
  struct ibv_wc com_recv_wc[FLR_MAX_RECV_COM_WRS];
  struct ibv_recv_wr com_recv_wr[FLR_MAX_RECV_COM_WRS];

  // FC_QP_ID 2: send Credits  (Follower does not receive credits)
  struct ibv_send_wr credit_send_wr[FLR_MAX_CREDIT_WRS];
  struct ibv_sge credit_send_sgl;
  uint16_t credits = W_CREDITS;

  uint32_t credit_debug_cnt = 0, outstanding_writes = 0, trace_iter = 0;
  uint64_t sent_ack_tx = 0, credit_tx = 0, w_tx = 0;

  latency_info_t latency_info = {
    .measured_req_flag = NO_REQ,
    .measured_sess_id = 0,
  };


  zk_resp_t *resp = (zk_resp_t *) malloc(ZK_TRACE_BATCH * sizeof(zk_resp_t));
  struct ibv_mr *w_mr;
  zk_trace_op_t *ops = (zk_trace_op_t *) memalign(4096, ZK_TRACE_BATCH *  sizeof(zk_trace_op_t));
  recv_info_t *prep_recv_info, *com_recv_info;
  prep_recv_info = init_recv_info(cb, prep_push_ptr, FLR_PREP_BUF_SLOTS,
                                  (uint32_t) FLR_PREP_RECV_SIZE, FLR_MAX_RECV_PREP_WRS, prep_recv_qp,
                                  FLR_MAX_RECV_PREP_WRS,
                                  prep_recv_wr, prep_recv_sgl,
                                  (void*) prep_buffer);
  com_recv_info = init_recv_info(cb, com_push_ptr, FLR_COM_BUF_SLOTS,
                                 (uint32_t) FLR_COM_RECV_SIZE, FLR_MAX_RECV_COM_WRS,
                                 com_recv_qp, FLR_MAX_RECV_COM_WRS,
                                 com_recv_wr, com_recv_sgl,
                                 (void*) com_buffer);

  p_writes_t *p_writes = set_up_pending_writes(FLR_PENDING_WRITES, NULL, NULL, NULL, protocol);
  p_acks_t *p_acks = (p_acks_t *) calloc(1, sizeof(p_acks_t));
  zk_ack_mes_t *ack = (zk_ack_mes_t *) calloc(1, sizeof(zk_ack_mes_t));
    
  if (!FLR_W_ENABLE_INLINING)
    w_mr = register_buffer(cb->pd, p_writes->w_fifo->fifo, W_FIFO_SIZE * sizeof(zk_w_mes_t));

  struct fifo *remote_w_buf;
  init_fifo(&remote_w_buf, LEADER_W_BUF_SLOTS * sizeof(uint16_t), 1);
  struct fifo *prep_buf_mirror;
  init_fifo(&prep_buf_mirror, FLR_PREP_BUF_SLOTS * sizeof(uint16_t), 1);

  /* ---------------------------------------------------------------------------
  ------------------------------INITIALIZE STATIC STRUCTURES--------------------
    ---------------------------------------------------------------------------*/
  // SEND AND RECEIVE WRs
  set_up_follower_WRs(ack_send_wr, ack_send_sgl, prep_recv_wr, prep_recv_sgl, w_send_wr, w_send_sgl,
                      com_recv_wr, com_recv_sgl, remote_ldr_thread, cb, w_mr, mcast);
  flr_set_up_credit_WRs(credit_send_wr, &credit_send_sgl, cb, flr_id, FLR_MAX_CREDIT_WRS, t_id);
  // TRACE
  trace_t *trace;
  if (!ENABLE_CLIENTS)
    trace = trace_init(t_id);

  /* ---------------------------------------------------------------------------
  ------------------------------LATENCY AND DEBUG-----------------------------------
  ---------------------------------------------------------------------------*/
  uint32_t wait_for_gid_dbg_counter = 0, completed_but_not_polled_coms = 0,
    completed_but_not_polled_preps = 0,
    wait_for_prepares_dbg_counter = 0, wait_for_coms_dbg_counter = 0;
  uint16_t last_session = 0;
  struct timespec start, end;
  uint16_t debug_ptr = 0;
  if (t_id == 0) my_printf(green, "Follower %d  reached the loop %u \n", t_id, p_acks->acks_to_send);
  /* ---------------------------------------------------------------------------
  ------------------------------START LOOP--------------------------------
  ---------------------------------------------------------------------------*/
  while(true) {
    if (t_stats[t_id].received_preps_mes_num > 0 && FLR_CHECK_DBG_COUNTERS)
      flr_check_debug_cntrs(&credit_debug_cnt, &wait_for_coms_dbg_counter,
                            &wait_for_prepares_dbg_counter,
                            &wait_for_gid_dbg_counter, prep_buffer ,prep_pull_ptr, p_writes, t_id);



    if (PUT_A_MACHINE_TO_SLEEP && (machine_id == MACHINE_THAT_SLEEPS) &&
        (t_stats[WORKERS_PER_MACHINE -1].cache_hits_per_thread > 4 * MILLION)) {
      if (t_id == 0) my_printf(yellow, "Machine performs scheduled failure\n");
      exit(0);
    }
  /* ---------------------------------------------------------------------------
  ------------------------------ POLL FOR PREPARES--------------------------
  ---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      flr_poll_for_prepares(prep_buffer, &prep_pull_ptr, p_writes, p_acks, prep_recv_cq,
                            prep_recv_wc, prep_recv_info, prep_buf_mirror, &completed_but_not_polled_preps,
                            t_id, flr_id, &wait_for_prepares_dbg_counter);


  /* ---------------------------------------------------------------------------
  ------------------------------SEND ACKS-------------------------------------
  ---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      send_acks_to_ldr(p_writes, ack_send_wr, ack_send_sgl, &sent_ack_tx, cb,
                       prep_recv_info, flr_id,  ack, p_acks, t_id);

    /* ---------------------------------------------------------------------------
    ------------------------------POLL FOR COMMITS---------------------------------
    ---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      poll_for_coms(com_buffer, &com_pull_ptr, p_writes, &credits, com_recv_cq,
                    com_recv_wc, com_recv_info, cb, credit_send_wr, &credit_tx,
                    remote_w_buf, &completed_but_not_polled_coms,
                    t_id, flr_id, &wait_for_coms_dbg_counter);

    /* ---------------------------------------------------------------------------
    ------------------------------PROPAGATE UPDATES---------------------------------
    ---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      flr_propagate_updates(p_writes, p_acks, resp, prep_buf_mirror, &latency_info, t_id, &wait_for_gid_dbg_counter);


  /* ---------------------------------------------------------------------------
  ------------------------------PROBE THE CACHE--------------------------------------
  ---------------------------------------------------------------------------*/

  // Propagate the updates before probing the cache
    trace_iter = zk_batch_from_trace_to_KVS(trace_iter, t_id, trace, ops, flr_id,
                                            p_writes, resp, &latency_info, &last_session, protocol);



  /* ---------------------------------------------------------------------------
  ------------------------------SEND WRITES TO THE LEADER---------------------------
  ---------------------------------------------------------------------------*/
  if (WRITE_RATIO > 0)
    send_writes_to_the_ldr(p_writes, &credits, cb, w_send_sgl, w_send_wr, &w_tx, remote_w_buf,
    t_id, &outstanding_writes);
  }
  return NULL;
}

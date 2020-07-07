#include "zk_util.h"
#include "zk_inline_util.h"
#include "init_connect.h"
#include "rdma_gen_util.h"
#include "trace_util.h"


void *leader(void *arg)
{
	struct thread_params params = *(struct thread_params *) arg;
	uint16_t t_id = (uint16_t) params.id;
  uint32_t g_id = get_gid((uint8_t) machine_id, t_id);
  uint16_t follower_id = t_id;

	if (ENABLE_MULTICAST == 1 && t_id == 0)
		my_printf(cyan, "MULTICAST IS ENABLED\n");
	protocol_t protocol = LEADER;

	int *recv_q_depths, *send_q_depths;
  set_up_queue_depths_ldr_flr(&recv_q_depths, &send_q_depths, protocol);
	struct hrd_ctrl_blk *cb = hrd_ctrl_blk_init(t_id,	/* local_hid */
												0, -1, /* port_index, numa_node_id */
												0, 0,	/* #conn qps, uc */
												NULL, 0, -1,	/* prealloc conn buf, buf size, key */
												LEADER_QP_NUM, LEADER_BUF_SIZE,	/* num_dgram_qps, dgram_buf_size */
												MASTER_SHM_KEY + t_id, /* key */
												recv_q_depths, send_q_depths); /* Depth of the dgram RECV Q*/

  uint32_t ack_buf_push_ptr = 0, ack_buf_pull_ptr = 0;
  uint32_t w_buf_push_ptr = 0, w_buf_pull_ptr = 0;
  zk_ack_mes_ud_t *ack_buffer = (zk_ack_mes_ud_t *)(cb->dgram_buf);
  volatile zk_w_mes_ud_t *w_buffer =
    (volatile zk_w_mes_ud_t *)(cb->dgram_buf + LEADER_ACK_BUF_SIZE);
	/* ---------------------------------------------------------------------------
	------------------------------MULTICAST SET UP-------------------------------
	---------------------------------------------------------------------------*/

	struct mcast_info *mcast_data = NULL;
	struct mcast_essentials *mcast = NULL;
	// need to init mcast before sync, such that we can post recvs
	if (ENABLE_MULTICAST) {
		zk_init_multicast(&mcast_data, &mcast, t_id, cb, protocol);
		assert(mcast != NULL);
	}
	/* Fill the RECV queue that receives the Broadcasts, we need to do this early */
	if (WRITE_RATIO > 0) {
    // Pre post receives only for writes
    pre_post_recvs(&w_buf_push_ptr, cb->dgram_qp[COMMIT_W_QP_ID], cb->dgram_buf_mr->lkey, (void *)w_buffer,
                   LEADER_W_BUF_SLOTS, LDR_MAX_RECV_W_WRS, COMMIT_W_QP_ID, LDR_W_RECV_SIZE);
  }

	/* -----------------------------------------------------
	--------------CONNECT WITH FOLLOWERS-----------------------
	---------------------------------------------------------*/
  setup_connections_and_spawn_stats_thread(g_id, cb, t_id);
	if (MULTICAST_TESTING == 1) multicast_testing(mcast, t_id, cb, COMMIT_W_QP_ID);

	/* -----------------------------------------------------
	--------------DECLARATIONS------------------------------
	---------------------------------------------------------*/
  // PREP_ACK_QP_ID 0: send Prepares -- receive ACKs
	struct ibv_send_wr prep_send_wr[LDR_MAX_PREP_WRS];
	struct ibv_sge prep_send_sgl[MAX_BCAST_BATCH], ack_recv_sgl[LDR_MAX_RECV_ACK_WRS];
	struct ibv_wc ack_recv_wc[LDR_MAX_RECV_ACK_WRS];
	struct ibv_recv_wr ack_recv_wr[LDR_MAX_RECV_ACK_WRS];

  // W_QP_ID 1: send Commits  -- receive Writes
  struct ibv_send_wr com_send_wr[LDR_MAX_COM_WRS];
  struct ibv_sge com_send_sgl[MAX_BCAST_BATCH], w_recv_sgl[LDR_MAX_RECV_W_WRS];
  struct ibv_wc w_recv_wc[LDR_MAX_RECV_W_WRS];
  struct ibv_recv_wr w_recv_wr[LDR_MAX_RECV_W_WRS];

  // FC_QP_ID 2: receive Credits (LDR Does not send credits)
  struct ibv_sge credit_recv_sgl;
  struct ibv_wc credit_wc[LDR_MAX_CREDIT_RECV];
  struct ibv_recv_wr credit_recv_wr[LDR_MAX_CREDIT_RECV];

 	uint16_t credits[LDR_VC_NUM][MACHINE_NUM];
	uint32_t trace_iter = 0;
  uint64_t prep_br_tx = 0, commit_br_tx = 0;

  recv_info_t *w_recv_info, *ack_recv_info, *cred_recv_info;
  w_recv_info =  init_recv_info(cb, w_buf_push_ptr, LEADER_W_BUF_SLOTS,
                                (uint32_t) LDR_W_RECV_SIZE, LDR_MAX_RECV_W_WRS,
                                cb->dgram_qp[COMMIT_W_QP_ID],
                                LDR_MAX_RECV_W_WRS,
                                w_recv_wr, w_recv_sgl,
                                (void*) w_buffer);

  ack_recv_info = init_recv_info(cb, ack_buf_push_ptr, LEADER_ACK_BUF_SLOTS,
                                 (uint32_t) LDR_ACK_RECV_SIZE, 0, cb->dgram_qp[PREP_ACK_QP_ID],
                                 LDR_MAX_RECV_ACK_WRS, ack_recv_wr, ack_recv_sgl,
                                 (void*) ack_buffer);

  cred_recv_info = init_recv_info(cb, 0, 0, 64, 0, cb->dgram_qp[FC_QP_ID],
                                 LDR_MAX_CREDIT_RECV, credit_recv_wr, &credit_recv_sgl,
                                 (void*) cb->dgram_buf);


	latency_info_t latency_info = {
			.measured_req_flag = NO_REQ,
			.measured_sess_id = 0,
	};

	zk_trace_op_t *ops = (zk_trace_op_t *) calloc((size_t) MAX_OP_BATCH, sizeof(zk_trace_op_t));
  zk_resp_t *resp = (zk_resp_t*) calloc((size_t) MAX_OP_BATCH, sizeof(zk_resp_t));
  zk_com_fifo_t *com_fifo = set_up_ldr_ops(resp, t_id);
	struct ibv_mr *prep_mr, *com_mr;



  p_writes_t *p_writes = set_up_pending_writes(LEADER_PENDING_WRITES, prep_send_wr,
                                               com_send_wr, credits, protocol);
  void *prep_buf = (void *) p_writes->prep_fifo->prep_message;
  set_up_ldr_mrs(&prep_mr, prep_buf, &com_mr, (void *)com_fifo->commits, cb);

  // There are no explicit credits and therefore we need to represent the remote prepare buffer somehow,
  // such that we can interpret the incoming acks correctly
  struct fifo *remote_prep_buf;
  init_fifo(&remote_prep_buf, FLR_PREP_BUF_SLOTS * sizeof(uint16_t), FOLLOWER_MACHINE_NUM);
  uint16_t *fifo = (uint16_t *)remote_prep_buf[FOLLOWER_MACHINE_NUM -1].fifo;
  assert(fifo[FLR_PREP_BUF_SLOTS -1] == 0);

	/* ---------------------------------------------------------------------------
	------------------------------INITIALIZE STATIC STRUCTUREs--------------------
		---------------------------------------------------------------------------*/
  for (uint8_t m_id = 0; m_id < MACHINE_NUM; m_id++) {
    credits[PREP_VC][m_id] = PREPARE_CREDITS;
    credits[COMM_VC][m_id] = COMMIT_CREDITS;
  }
  set_up_ldr_WRs(prep_send_wr, prep_send_sgl,
                  com_send_wr, com_send_sgl,
                  t_id, follower_id, prep_mr, com_mr, mcast);

	// TRACE
	trace_t *trace = NULL;
  if (!ENABLE_CLIENTS)
    trace = trace_init(t_id);

	/* ---------------------------------------------------------------------------
	------------------------------LATENCY AND DEBUG-----------------------------------
	---------------------------------------------------------------------------*/
  uint16_t last_session = 0;
  uint32_t wait_for_gid_dbg_counter = 0, wait_for_acks_dbg_counter = 0;
  uint32_t credit_debug_cnt[LDR_VC_NUM] = {0}, time_out_cnt[LDR_VC_NUM] = {0};
  uint32_t outstanding_prepares = 0, completed_but_not_polled_writes = 0;
	struct timespec start, end;
  if (t_id == 0) my_printf(green, "Leader %d  reached the loop \n", t_id);

	/* ---------------------------------------------------------------------------
	------------------------------START LOOP--------------------------------
	---------------------------------------------------------------------------*/
	while(true) {

     if (ENABLE_ASSERTIONS)
       ldr_check_debug_cntrs(credit_debug_cnt, &wait_for_acks_dbg_counter,
                             &wait_for_gid_dbg_counter, p_writes, t_id);

		/* ---------------------------------------------------------------------------
		------------------------------ POLL FOR ACKS--------------------------------
		---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      ldr_poll_for_acks(ack_buffer, &ack_buf_pull_ptr, p_writes,
                        credits, cb->dgram_recv_cq[PREP_ACK_QP_ID], ack_recv_wc, ack_recv_info,
                        remote_prep_buf,
                        t_id, &wait_for_acks_dbg_counter, &outstanding_prepares);

/* ---------------------------------------------------------------------------
		------------------------------ PROPAGATE UPDATES--------------------------
		---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      /* After propagating the acked messages we push their l_id to a prep_message buffer
       * to send the commits and clear the p_write buffer space. The reason behind that
       * is that we do not want to wait for the commit broadcast to happen to clear the
       * buffer space for new writes*/
      ldr_propagate_updates(p_writes, com_fifo, resp, &latency_info, t_id, &wait_for_gid_dbg_counter);


    /* ---------------------------------------------------------------------------
		------------------------------ BROADCAST COMMITS--------------------------
		---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      broadcast_commits(p_writes, credits, cb, com_fifo,
                        &commit_br_tx, credit_wc,
                        com_send_sgl, com_send_wr, cred_recv_info, time_out_cnt,
                        w_recv_info, t_id);
    /* ---------------------------------------------------------------------------
    ------------------------------PROBE THE CACHE--------------------------------------
    ---------------------------------------------------------------------------*/


    // Get a new batch from the trace, pass it through the cache and create
    // the appropriate prepare messages
		trace_iter = zk_batch_from_trace_to_KVS(trace_iter, t_id, trace, ops,
                                            (uint8_t) FOLLOWER_MACHINE_NUM, p_writes, resp,
                                            &latency_info,  &last_session, protocol);

    /* ---------------------------------------------------------------------------
		------------------------------POLL FOR REMOTE WRITES--------------------------
		---------------------------------------------------------------------------*/
    // get local and remote writes back to back to increase the write batch
    if (WRITE_RATIO > 0)
      poll_for_writes(w_buffer, &w_buf_pull_ptr, p_writes, cb->dgram_recv_cq[COMMIT_W_QP_ID],
                      w_recv_wc, w_recv_info, &completed_but_not_polled_writes, t_id);

    /* ---------------------------------------------------------------------------
		------------------------------GET GLOBAL WRITE IDS--------------------------
		---------------------------------------------------------------------------*/
    // Assign a global write  id to each new write
    if (WRITE_RATIO > 0) zk_get_g_ids(p_writes, t_id);

    if (ENABLE_ASSERTIONS) check_ldr_p_states(p_writes, t_id);
		/* ---------------------------------------------------------------------------
		------------------------------BROADCASTS--------------------------------------
		---------------------------------------------------------------------------*/
		if (WRITE_RATIO > 0)
			/* Poll for credits - Perform broadcasts
				 Post the appropriate number of credit receives before sending anything */
      broadcast_prepares(p_writes, credits, cb, prep_send_sgl, prep_send_wr, &prep_br_tx, ack_recv_info,
                         remote_prep_buf, time_out_cnt, &outstanding_prepares, t_id);
    if (ENABLE_ASSERTIONS) {
      assert(p_writes->size <= LEADER_PENDING_WRITES);
      for (uint16_t i = 0; i < LEADER_PENDING_WRITES - p_writes->size; i++) {
        uint16_t ptr = (p_writes->push_ptr + i) % LEADER_PENDING_WRITES;
        assert (p_writes->w_state[ptr] == INVALID);
      }
    }


	}
	return NULL;
}


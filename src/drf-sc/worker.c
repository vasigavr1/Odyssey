#include "util.h"
#include "inline_util.h"

void *worker(void *arg)
{
	struct thread_params params = *(struct thread_params *) arg;
	uint16_t t_id = (uint16_t)params.id;
  uint16_t gid = (uint16_t) ((machine_id * WORKERS_PER_MACHINE) + t_id);

	if (ENABLE_MULTICAST == 1 && t_id == 0) {
		cyan_printf("MULTICAST IS ENABLED. PLEASE DISABLE IT AS IT IS NOT WORKING\n");
		assert(false);
	}
  //cyan_printf("Worker %u is running \n", t_id);

	int *recv_q_depths, *send_q_depths;
  set_up_queue_depths(&recv_q_depths, &send_q_depths);
	struct hrd_ctrl_blk *cb = hrd_ctrl_blk_init(t_id,	/* local_hid */
												0, -1, /* port_index, numa_node_id */
												0, 0,	/* #conn qps, uc */
												NULL, 0, -1,	/* prealloc conn buf, buf w_size, key */
												QP_NUM, TOTAL_BUF_SIZE,	/* num_dgram_qps, dgram_buf_size */
												MASTER_SHM_KEY + t_id, /* key */
												recv_q_depths, send_q_depths); /* Depth of the dgram RECV Q*/

  uint32_t ack_buf_push_ptr = 0, ack_buf_pull_ptr = 0,
    w_buf_push_ptr = 0, w_buf_pull_ptr = 0,  r_buf_push_ptr = 0, r_buf_pull_ptr = 0,
    r_rep_buf_push_ptr = 0, r_rep_buf_pull_ptr = 0;
  struct ack_message_ud_req *ack_buffer = (struct ack_message_ud_req *)(cb->dgram_buf);
  volatile struct  w_message_ud_req *w_buffer =
    (volatile struct w_message_ud_req *)(cb->dgram_buf + ACK_BUF_SIZE);
  volatile struct  r_message_ud_req *r_buffer =
    (volatile struct r_message_ud_req *)(cb->dgram_buf + ACK_BUF_SIZE + W_BUF_SIZE);
  volatile struct  r_rep_message_ud_req *r_rep_buffer =
    (volatile struct r_rep_message_ud_req *)(cb->dgram_buf + ACK_BUF_SIZE + W_BUF_SIZE + R_BUF_SIZE);
	/* ---------------------------------------------------------------------------
	------------------------------PREPOST RECVS-------------------------------
	---------------------------------------------------------------------------*/
	/* Fill the RECV queue that receives the Broadcasts, we need to do this early */
  // Pre post receives for writes
  pre_post_recvs(&w_buf_push_ptr, cb->dgram_qp[W_QP_ID], cb->dgram_buf_mr->lkey, (void *)w_buffer,
                   W_BUF_SLOTS, MAX_RECV_W_WRS, W_QP_ID, (uint32_t) W_RECV_SIZE);
  // Pre post receives for reads
  pre_post_recvs(&r_buf_push_ptr, cb->dgram_qp[R_QP_ID], cb->dgram_buf_mr->lkey, (void *)r_buffer,
                 R_BUF_SLOTS, MAX_RECV_R_WRS, R_QP_ID, R_RECV_SIZE);

  // Initialize the RMW struct, before anyone tries to touch it
  if (t_id == 0) set_up_rmw_struct();
	/* -----------------------------------------------------
	--------------CONNECT WITH ALL MACHINES-----------------------
	---------------------------------------------------------*/
	setup_connections_and_spawn_stats_thread(gid, cb);

	/* -----------------------------------------------------
	--------------DECLARATIONS------------------------------
	---------------------------------------------------------*/
  // R_QP_ID 0: send Reads -- receive Reads
  struct ibv_send_wr r_send_wr[MAX_R_WRS];
  struct ibv_sge r_send_sgl[MAX_BCAST_BATCH];
  struct ibv_wc r_recv_wc[MAX_RECV_R_WRS];

  // R_REP_QP_ID 1: send Read Replies  -- receive Read Replies
  struct ibv_send_wr r_rep_send_wr[MAX_R_REP_WRS];
  struct ibv_sge r_rep_send_sgl[MAX_R_REP_WRS];
  struct ibv_wc r_rep_recv_wc[MAX_RECV_R_REP_WRS];

  // W_QP_ID 2: Send Writes receive Writes
  struct ibv_send_wr w_send_wr[MAX_W_WRS];
  struct ibv_sge w_send_sgl[MAX_BCAST_BATCH];
  struct ibv_wc w_recv_wc[MAX_RECV_W_WRS];

  // ACK_QP_ID 3: send ACKs -- receive ACKs
  struct ibv_send_wr ack_send_wr[MAX_ACK_WRS];
  struct ibv_sge ack_send_sgl[MAX_ACK_WRS];
  struct ibv_wc ack_recv_wc[MAX_RECV_ACK_WRS];


 	uint16_t credits[VC_NUM][MACHINE_NUM];
  uint64_t r_br_tx = 0, w_br_tx = 0, r_rep_tx = 0, ack_tx = 0;
	uint32_t trace_iter = 0;

  struct recv_info *r_recv_info, *r_rep_recv_info, *w_recv_info, *ack_recv_info ;
  init_recv_info(cb, &r_recv_info, r_buf_push_ptr, R_BUF_SLOTS,
                 (uint32_t) R_RECV_SIZE, 0, cb->dgram_qp[R_QP_ID], MAX_RECV_R_WRS,
                 (void*) r_buffer);
  init_recv_info(cb, &r_rep_recv_info, r_rep_buf_push_ptr, R_REP_BUF_SLOTS,
                 (uint32_t) R_REP_RECV_SIZE, 0, cb->dgram_qp[R_REP_QP_ID], MAX_RECV_R_REP_WRS,
                 (void*) r_rep_buffer);

  init_recv_info(cb, &w_recv_info, w_buf_push_ptr, W_BUF_SLOTS,
                 (uint32_t) W_RECV_SIZE, MAX_RECV_W_WRS,  cb->dgram_qp[W_QP_ID],
                 MAX_RECV_W_WRS, (void*) w_buffer);

  init_recv_info(cb, &ack_recv_info, ack_buf_push_ptr, ACK_BUF_SLOTS,
                 (uint32_t) ACK_RECV_SIZE, 0, cb->dgram_qp[ACK_QP_ID], MAX_RECV_ACK_WRS,
                 (void*) ack_buffer);

  struct ack_message acks[MACHINE_NUM] = {0};
  for (uint16_t i = 0; i < MACHINE_NUM; i++) {
    acks[i].m_id = (uint8_t) machine_id;
    acks[i].opcode = CACHE_OP_ACK;
  }
  struct pending_ops *p_ops;
  struct ibv_mr *r_mr, *w_mr, *r_rep_mr;
  set_up_pending_ops(&p_ops, PENDING_WRITES, PENDING_READS);
  void *r_fifo_buf = p_ops->r_fifo->r_message;
  void *w_fifo_buf = p_ops->w_fifo->w_message;
  void *r_rep_fifo_buf = (void *)p_ops->r_rep_fifo->r_rep_message;
  set_up_mr(&r_mr, r_fifo_buf, R_ENABLE_INLINING, R_FIFO_SIZE * sizeof(struct r_message), cb);
  set_up_mr(&w_mr, w_fifo_buf, W_ENABLE_INLINING, W_FIFO_SIZE * sizeof(struct w_message), cb);
  set_up_mr(&r_rep_mr, r_rep_fifo_buf, R_REP_ENABLE_INLINING, R_REP_FIFO_SIZE * sizeof(struct r_rep_message), cb);

  struct trace_op *ops = (struct trace_op *) calloc(MAX_OP_BATCH, sizeof(struct trace_op));
  randomize_op_values(ops, t_id);
  struct cache_resp *resp = (struct cache_resp *) malloc(MAX_OP_BATCH * sizeof(struct cache_resp));
  set_up_bcast_WRs(w_send_wr, w_send_sgl, r_send_wr, r_send_sgl,
                   t_id, cb, w_mr, r_mr);
  set_up_ack_n_r_rep_WRs(ack_send_wr, ack_send_sgl, r_rep_send_wr, r_rep_send_sgl,
                         cb, r_rep_mr, acks, t_id);
  set_up_credits(credits);
  assert(credits[R_VC][0] == R_CREDITS && credits[W_VC][0] == W_CREDITS);
  struct quorum_info *q_info;
  set_up_q_info(&q_info);



	// TRACE
	struct trace_command *trace;
  if (!ENABLE_CLIENTS)
	  trace_init((void **)&trace, t_id);

	/* ---------------------------------------------------------------------------
	------------------------------LATENCY AND DEBUG-----------------------------------
	---------------------------------------------------------------------------*/
  struct latency_flags latency_info = {
    .measured_req_flag = NO_REQ,
    .measured_sess_id = 0,
  };
  uint32_t waiting_dbg_counter[QP_NUM] = {0};
  uint32_t credit_debug_cnt[VC_NUM] = {0}, time_out_cnt[VC_NUM] = {0};
  struct session_dbg *ses_dbg;
  if (DEBUG_SESSIONS) {
    ses_dbg = (struct session_dbg *) malloc(sizeof(struct session_dbg));
    memset(ses_dbg, 0, sizeof(struct session_dbg));
  }
  uint32_t outstanding_writes = 0, outstanding_reads = 0;
  uint64_t debug_lids = 0;
  // helper for polling writes: in a corner failure-realted case,
  // it may be that not all avaialble writes can be polled due to the unavailability of the acks
  uint32_t completed_but_not_polled_writes = 0;
  //uint32_t client_req_pull_ptr[SESSIONS_PER_THREAD] = {0};

	if (t_id == 0) green_printf("Worker %d  reached the loop \n", t_id);
  bool slept = false;
  //fprintf(stderr, "Worker %d  reached the loop \n", t_id);

	/* ---------------------------------------------------------------------------
	------------------------------START LOOP--------------------------------
	---------------------------------------------------------------------------*/
	while(true) {
     if (ENABLE_ASSERTIONS && CHECK_DBG_COUNTERS)
       check_debug_cntrs(credit_debug_cnt, waiting_dbg_counter, p_ops,
                         (void *) cb->dgram_buf, r_buf_pull_ptr,
                         w_buf_pull_ptr, ack_buf_pull_ptr, r_rep_buf_pull_ptr, t_id);



    if (PUT_A_MACHINE_TO_SLEEP && (machine_id == MACHINE_THAT_SLEEPS) &&
      (t_stats[WORKERS_PER_MACHINE -1].cache_hits_per_thread > 100000) && (!slept)) {
      uint seconds = 30;
      if (t_id == 0) yellow_printf("Workers are going to sleep for %u secs\n", seconds);
      //exit(1);
      sleep(seconds); slept = true;
      yellow_printf("Worker %u is back\n", t_id);
    }
    if (ENABLE_INFO_DUMP_ON_STALL && print_for_debug) {
      print_verbouse_debug_info(p_ops, t_id, credits);
    }

    /* ---------------------------------------------------------------------------
		------------------------------ POLL FOR WRITES--------------------------
		---------------------------------------------------------------------------*/
    poll_for_writes(w_buffer, &w_buf_pull_ptr, p_ops, cb->dgram_recv_cq[W_QP_ID],
                    w_recv_wc, w_recv_info, acks, &completed_but_not_polled_writes, t_id);

    /* ---------------------------------------------------------------------------
       ------------------------------ SEND ACKS----------------------------------
       ---------------------------------------------------------------------------*/

    send_acks(ack_send_wr, &ack_tx, cb,  w_recv_info, acks, t_id);

    /* ---------------------------------------------------------------------------
		------------------------------ POLL FOR READS--------------------------
		---------------------------------------------------------------------------*/
    poll_for_reads(r_buffer, &r_buf_pull_ptr, p_ops, cb->dgram_recv_cq[R_QP_ID],
                    r_recv_wc, t_id, waiting_dbg_counter);

    /* ---------------------------------------------------------------------------
		------------------------------ SEND READ REPLIES--------------------------
		---------------------------------------------------------------------------*/

    send_r_reps(p_ops, cb, r_rep_send_wr, r_rep_send_sgl, r_recv_info, w_recv_info, &r_rep_tx, t_id);

    /* ---------------------------------------------------------------------------
		------------------------------ POLL FOR READ REPLIES--------------------------
		---------------------------------------------------------------------------*/

    poll_for_read_replies(r_rep_buffer, &r_rep_buf_pull_ptr, p_ops, credits,
                          cb->dgram_recv_cq[R_REP_QP_ID], r_rep_recv_wc,
                          r_rep_recv_info, t_id, &outstanding_reads, waiting_dbg_counter);

    /* ---------------------------------------------------------------------------
		------------------------------ COMMIT READS----------------------------------
		---------------------------------------------------------------------------*/
    // Either commit a read or convert it into a write
    commit_reads(p_ops, &latency_info, t_id);

    /* ---------------------------------------------------------------------------
		------------------------------ INSPECT RMWS----------------------------------
		---------------------------------------------------------------------------*/
    if (ENABLE_RMWS)
      inspect_rmws(p_ops, t_id);


    /* ---------------------------------------------------------------------------
    ------------------------------ POLL FOR ACKS--------------------------------
    ---------------------------------------------------------------------------*/
    // if (WRITE_RATIO > 0)
    poll_acks(ack_buffer, &ack_buf_pull_ptr, p_ops, credits, cb->dgram_recv_cq[ACK_QP_ID],
              ack_recv_wc, ack_recv_info, &latency_info,
              t_id, waiting_dbg_counter, &outstanding_writes);

    /* ---------------------------------------------------------------------------
    ------------------------------PROBE THE CACHE--------------------------------------
    ---------------------------------------------------------------------------*/

    //if (!ENABLE_CLIENTS)
    // Get a new batch from the trace, pass it through the cache and create
    // the appropriate write/r_rep messages
		//trace_iter = batch_from_trace_to_cache(trace_iter, t_id, trace, ops,
                                           //p_ops, resp, &latency_info,
                                          // ses_dbg);
    //else
    trace_iter = batch_requests_to_cache(t_id,
                                         trace_iter, trace, ops,
                                         p_ops, resp, &latency_info,
                                         ses_dbg);

    /* ---------------------------------------------------------------------------
		------------------------------BROADCAST READS--------------------------
		---------------------------------------------------------------------------*/
    // Perform the r_rep broadcasts
    broadcast_reads(p_ops, credits, cb, q_info, credit_debug_cnt, time_out_cnt,
                    r_send_sgl, r_send_wr, w_send_wr,
                    &r_br_tx, r_rep_recv_info, t_id, &outstanding_reads);

    /* ---------------------------------------------------------------------------
		------------------------------BROADCAST WRITES--------------------------
		---------------------------------------------------------------------------*/
    // Perform the write broadcasts
    //if (WRITE_RATIO > 0)
    broadcast_writes(p_ops, q_info, credits, cb, credit_debug_cnt, time_out_cnt,
                     w_send_sgl, r_send_wr, w_send_wr, &w_br_tx,
                     ack_recv_info, r_rep_recv_info, t_id, &outstanding_writes, &debug_lids);
	}
	return NULL;
}


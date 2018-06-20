#include "util.h"
#include "inline_util.h"

void *worker(void *arg)
{
	struct thread_params params = *(struct thread_params *) arg;
	uint16_t t_id = (uint16_t)params.id;
  uint16_t gid = (uint16_t)(machine_id * WORKERS_PER_MACHINE) + t_id;

	if (ENABLE_MULTICAST == 1 && t_id == 0) {
		cyan_printf("MULTICAST IS ENABLED. PLEASE DISABLE IT AS IT IS NOT WORKING\n");
		assert(false);
	}

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
                   W_BUF_SLOTS, MAX_RECV_W_WRS, W_QP_ID, W_RECV_SIZE);
  // Pre post receives for reads
  pre_post_recvs(&r_buf_push_ptr, cb->dgram_qp[R_QP_ID], cb->dgram_buf_mr->lkey, (void *)r_buffer,
                 R_BUF_SLOTS, MAX_RECV_R_WRS, R_QP_ID, R_RECV_SIZE);

	/* -----------------------------------------------------
	--------------CONNECT WITH ALL MACHINES-----------------------
	---------------------------------------------------------*/
	setup_connections_and_spawn_stats_thread(gid, cb);

	/* -----------------------------------------------------
	--------------DECLARATIONS------------------------------
	---------------------------------------------------------*/
  // R_QP_ID 0: send Reads -- receive Reads
  struct ibv_send_wr r_send_wr[MAX_R_WRS];
  struct ibv_sge r_send_sgl[MAX_BCAST_BATCH], r_recv_sgl[MAX_RECV_R_WRS];
  struct ibv_wc r_recv_wc[MAX_RECV_R_WRS];
  struct ibv_recv_wr r_recv_wr[MAX_RECV_R_WRS];

  // R_REP_QP_ID 1: send Read Replies  -- receive Read Replies
  struct ibv_send_wr r_rep_send_wr[MAX_R_REP_WRS];
  struct ibv_sge r_rep_send_sgl[MAX_R_REP_WRS], r_rep_recv_sgl[MAX_RECV_R_REP_WRS];
  struct ibv_wc r_rep_recv_wc[MAX_RECV_R_REP_WRS];
  struct ibv_recv_wr r_rep_recv_wr[MAX_RECV_R_REP_WRS];

  // W_QP_ID 2: Send Writes receive Writes
  struct ibv_send_wr w_send_wr[MAX_W_WRS];
  struct ibv_sge w_send_sgl[MAX_BCAST_BATCH], w_recv_sgl[MAX_RECV_W_WRS];
  struct ibv_wc w_recv_wc[MAX_RECV_W_WRS];
  struct ibv_recv_wr w_recv_wr[MAX_RECV_W_WRS];

  // ACK_QP_ID 3: send ACKs -- receive ACKs
  struct ibv_send_wr ack_send_wr[MAX_ACK_WRS];
  struct ibv_sge ack_send_sgl[MAX_ACK_WRS], ack_recv_sgl[MAX_RECV_ACK_WRS];
  struct ibv_wc ack_recv_wc[MAX_RECV_ACK_WRS];
  struct ibv_recv_wr ack_recv_wr[MAX_RECV_ACK_WRS];


 	uint16_t credits[VC_NUM][MACHINE_NUM];
  uint64_t r_br_tx = 0, w_br_tx = 0, r_rep_tx = 0, ack_tx = 0;


	uint32_t trace_iter = 0;

  struct recv_info *w_recv_info, *ack_recv_info, *r_recv_info, *r_rep_recv_info;
  init_recv_info(&w_recv_info, w_buf_push_ptr, W_BUF_SLOTS,
                 (uint32_t)W_RECV_SIZE, MAX_RECV_W_WRS, w_recv_wr, cb->dgram_qp[W_QP_ID],
                 w_recv_sgl, (void*) w_buffer);
  init_recv_info(&ack_recv_info, ack_buf_push_ptr, ACK_BUF_SLOTS,
                 (uint32_t)ACK_RECV_SIZE, 0, ack_recv_wr, cb->dgram_qp[ACK_QP_ID], ack_recv_sgl,
                 (void*) ack_buffer);
  init_recv_info(&r_recv_info, r_buf_push_ptr, R_BUF_SLOTS,
                 (uint32_t)R_RECV_SIZE, 0, r_recv_wr, cb->dgram_qp[R_QP_ID], r_recv_sgl,
                 (void*) r_buffer);
  init_recv_info(&r_rep_recv_info, r_rep_buf_push_ptr, R_REP_BUF_SLOTS,
                 (uint32_t)R_REP_RECV_SIZE, 0, r_rep_recv_wr, cb->dgram_qp[R_REP_QP_ID], r_rep_recv_sgl,
                 (void*) r_rep_buffer);

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

  struct cache_op *ops= (struct cache_op *) malloc(MAX_OP_BATCH * sizeof(struct cache_op));
  struct mica_resp *resp = (struct mica_resp *) malloc(MAX_OP_BATCH * sizeof(struct mica_resp));
  set_up_bcast_WRs(w_send_wr, w_send_sgl, r_send_wr, r_send_sgl, w_recv_wr, w_recv_sgl,
                   r_recv_wr, r_recv_sgl,t_id, cb, w_mr, r_mr);
  set_up_ack_n_r_rep_WRs(ack_send_wr, ack_send_sgl, r_rep_send_wr, r_rep_send_sgl, ack_recv_wr, ack_recv_sgl,
                         r_rep_recv_wr, r_rep_recv_sgl, cb, r_rep_mr, acks, t_id);
  set_up_credits(credits);
  struct quorum_info *q_info;
  set_up_q_info(&q_info);

	// TRACE
	struct trace_command_uni *trace;
	trace_init((void **)&trace, t_id);

	/* ---------------------------------------------------------------------------
	------------------------------LATENCY AND DEBUG-----------------------------------
	---------------------------------------------------------------------------*/
  uint32_t waiting_dbg_counter[QP_NUM] = {0};
  uint32_t credit_debug_cnt[VC_NUM] = {0}, time_out_cnt[VC_NUM] = {0};
  uint32_t outstanding_writes = 0, outstanding_reads = 0;
	struct timespec start, end;
	if (t_id == 0) green_printf("Worker %d  reached the loop \n", t_id);
  bool slept = false;

	/* ---------------------------------------------------------------------------
	------------------------------START LOOP--------------------------------
	---------------------------------------------------------------------------*/
	while(true) {


     if (ENABLE_ASSERTIONS && CHECK_DBG_COUNTERS)
       check_debug_cntrs(credit_debug_cnt, waiting_dbg_counter, p_ops, (void *) cb->dgram_buf, r_buf_pull_ptr,
                         w_buf_pull_ptr, ack_buf_pull_ptr, r_rep_buf_pull_ptr, t_id);

    if (PUT_A_MACHINE_TO_SLEEP && (machine_id == MACHINE_THAT_SLEEPS) &&
      (t_stats[t_id].cache_hits_per_thread > M_16) && (!slept)) {
      uint seconds = 10;
      yellow_printf("Worker %u is going to sleep for %u secs\n", t_id, seconds);
      sleep(seconds); slept = true;
    }
    if (ENABLE_INFO_DUMP_ON_STALL && print_for_debug) {
      uint16_t i;
      green_printf("---DEBUG INFO---------\n");
      yellow_printf("1. ---SESSIONS--- \n");
      if (p_ops->all_sessions_stalled) yellow_printf("All sessions are stalled \n");
      else yellow_printf("There are available sessions \n");
      for (i = 0; i < SESSIONS_PER_THREAD; i++)
        printf("S%u: %d ", i, p_ops->session_has_pending_op[i]);
      printf("\n");
      cyan_printf("2. ---CREDITS--- \n");
      for (i = 0; i < MACHINE_NUM; i++)
        cyan_printf("Credits for machine %u: %u R and %u W \n", i, credits[R_VC][i], credits[W_VC][i]);
      printf("\n");
      green_printf("3. ---FIFOS--- \n");
      green_printf("W_size: %u \nw_push_ptr %u \nw_pull_ptr %u\n", p_ops->w_size, p_ops->w_push_ptr, p_ops->w_pull_ptr);
      green_printf("R_size: %u \nr_push_ptr %u \nr_pull_ptr %u\n", p_ops->r_size, p_ops->r_push_ptr, p_ops->r_pull_ptr);

      yellow_printf("Cache hits: %u \nReads: %u \nWrites: %u \nReleases: %u \nAcquires: %u \n",
                    t_stats[t_id].cache_hits_per_thread, t_stats[t_id].reads_per_thread,
                    t_stats[t_id].writes_per_thread, t_stats[t_id].releases_per_thread,
                    t_stats[t_id].acquires_per_thread);
      print_for_debug = false;
    }

    /* ---------------------------------------------------------------------------
		------------------------------ POLL FOR WRITES--------------------------
		---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      poll_for_writes(w_buffer, &w_buf_pull_ptr, p_ops, cb->dgram_recv_cq[W_QP_ID],
                      w_recv_wc, w_recv_info, acks, t_id);

    /* ---------------------------------------------------------------------------
       ------------------------------ SEND ACKS----------------------------------
       ---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      send_acks(ack_send_wr, &ack_tx, cb,  w_recv_info, acks, t_id);

    /* ---------------------------------------------------------------------------
		------------------------------ POLL FOR READS--------------------------
		---------------------------------------------------------------------------*/
    if (WRITE_RATIO < 1000 || ENABLE_LIN)
      poll_for_reads(r_buffer, &r_buf_pull_ptr, p_ops, cb->dgram_recv_cq[R_QP_ID],
                    r_recv_wc, r_rep_recv_info, acks, t_id, waiting_dbg_counter);

    /* ---------------------------------------------------------------------------
		------------------------------ SEND READ REPLIES--------------------------
		---------------------------------------------------------------------------*/
    if (WRITE_RATIO < 1000 || ENABLE_LIN)
      send_r_reps(p_ops, cb, r_rep_send_wr, r_rep_send_sgl, r_recv_info, &r_rep_tx, t_id);

    /* ---------------------------------------------------------------------------
		------------------------------ POLL FOR READ REPLIES--------------------------
		---------------------------------------------------------------------------*/
    if (WRITE_RATIO < 1000 || ENABLE_LIN)
      poll_for_read_replies(r_rep_buffer, &r_rep_buf_pull_ptr, p_ops, credits, cb->dgram_recv_cq[R_REP_QP_ID],
                          r_rep_recv_wc, r_rep_recv_info, t_id, &outstanding_reads, waiting_dbg_counter);

    /* ---------------------------------------------------------------------------
		------------------------------ COMMIT READS----------------------------------
		---------------------------------------------------------------------------*/
    // Either commit a read or convert it into a write
    if (WRITE_RATIO < 1000 || ENABLE_LIN)
      commit_reads(p_ops, t_id);

    /* ---------------------------------------------------------------------------
    ------------------------------ POLL FOR ACKS--------------------------------
    ---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      poll_acks(ack_buffer, &ack_buf_pull_ptr, p_ops, credits, cb->dgram_recv_cq[ACK_QP_ID], ack_recv_wc,
                ack_recv_info, t_id, waiting_dbg_counter, &outstanding_writes);

    /* ---------------------------------------------------------------------------
    ------------------------------PROBE THE CACHE--------------------------------------
    ---------------------------------------------------------------------------*/

    // Get a new batch from the trace, pass it through the cache and create
    // the appropriate write/r_rep messages
		trace_iter = batch_from_trace_to_cache(trace_iter, t_id, trace, ops,
                                           p_ops, resp);

    /* ---------------------------------------------------------------------------
		------------------------------BROADCAST READS--------------------------
		---------------------------------------------------------------------------*/
    // Perform the r_rep broadcasts
    if (WRITE_RATIO < 1000 || ENABLE_LIN)
      broadcast_reads(p_ops, credits, cb, q_info, credit_debug_cnt, time_out_cnt, r_send_sgl, r_send_wr, w_send_wr,
                      &r_br_tx, r_rep_recv_info, t_id, &outstanding_reads);

    /* ---------------------------------------------------------------------------
		------------------------------BROADCAST WRITES--------------------------
		---------------------------------------------------------------------------*/
    // Perform the write broadcasts
    if (WRITE_RATIO > 0)
      broadcast_writes(p_ops, q_info, credits, cb, credit_debug_cnt, time_out_cnt,
                       w_send_sgl, r_send_wr, w_send_wr, &w_br_tx,
                       ack_recv_info, t_id, &outstanding_writes);





	}
	return NULL;
}


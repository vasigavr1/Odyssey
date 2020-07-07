#ifndef INLINE_UTILS_H
#define INLINE_UTILS_H


#include "zk_kvs_util.h"
#include "zk_debug_util.h"
#include "zk_reservation_stations_util.h_.h"

/* ---------------------------------------------------------------------------
//------------------------------TRACE --------------------------------
//---------------------------------------------------------------------------*/


// Both Leader and Followers use this to read the trace, propagate reqs to the cache and maintain their prepare/write fifos
static inline uint32_t zk_batch_from_trace_to_KVS(uint32_t trace_iter, uint16_t t_id, trace_t *trace,
                                                  zk_trace_op_t *ops, uint8_t flr_id,
                                                  p_writes_t *p_writes, zk_resp_t *resp,
                                                  latency_info_t *latency_info,
                                                  uint16_t *last_session_, protocol_t protocol)
{

  if (protocol == FOLLOWER && MAKE_FOLLOWERS_PASSIVE) return 0;
  uint16_t op_i = 0, last_session = *last_session_;
  int working_session = -1;
  // if there are clients the "all_sessions_stalled" flag is not used,
  // so we need not bother checking it
  if (!ENABLE_CLIENTS && p_writes->all_sessions_stalled) {
    return trace_iter;
  }
  for (uint16_t i = 0; i < SESSIONS_PER_THREAD; i++) {
    uint16_t sess_i = (uint16_t)((last_session + i) % SESSIONS_PER_THREAD);
    if (pull_request_from_this_session(p_writes->stalled[sess_i], sess_i, t_id)) {
      working_session = sess_i;
      break;
    }
  }
  if (ENABLE_CLIENTS) {
    if (working_session == -1) return trace_iter;
  }
  else if (ENABLE_ASSERTIONS) assert(working_session != -1);

  bool passed_over_all_sessions = false;
  /// main loop
  while (op_i < ZK_TRACE_BATCH && !passed_over_all_sessions) {

    zk_fill_trace_op(&trace[trace_iter], &ops[op_i], op_i, working_session, protocol,
                     p_writes, flr_id, latency_info, t_id);
    while (!pull_request_from_this_session(p_writes->stalled[working_session],
                                           (uint16_t) working_session, t_id)) {

      MOD_INCR(working_session, SESSIONS_PER_THREAD);
      if (working_session == last_session) {
        passed_over_all_sessions = true;
        // If clients are used the condition does not guarantee that sessions are stalled
        if (!ENABLE_CLIENTS) p_writes->all_sessions_stalled = true;
        break;
      }
    }
    resp[op_i].type = EMPTY;
    if (!ENABLE_CLIENTS) {
      trace_iter++;
      if (trace[trace_iter].opcode == NOP) trace_iter = 0;
    }
    op_i++;
  }
  //printf("Session %u pulled: ops %u, req_array ptr %u \n",
  //       working_session, op_i, ops[0].index_to_req_array);
  *last_session_ = (uint16_t) working_session;
  t_stats[t_id].cache_hits_per_thread += op_i;
  zk_KVS_batch_op_trace(op_i, ops, resp, t_id);
  if (MEASURE_LATENCY && machine_id == LATENCY_MACHINE && t_id == LATENCY_THREAD &&
      latency_info->measured_req_flag == READ_REQ)
    report_latency(latency_info);

  for (uint16_t i = 0; i < op_i; i++) {
    // my_printf(green, "After: OP_i %u -> session %u \n", i, *(uint32_t *) &ops[i]);
    if (resp[i].type == KVS_MISS)  {
      my_printf(green, "KVS %u: bkt %u, server %u, tag %u \n", i,
                ops[i].key.bkt, ops[i].key.server, ops[i].key.tag);
      assert(false);
      continue;
    }
    // check_version_after_batching_trace_to_cache(&ops[i], &resp[i], t_id);
    // Local reads
    if (ops[i].opcode == KVS_OP_GET) {
      //check_state_with_allowed_flags(2, interface[t_id].req_array[ops[i].session_id][ops[i].index_to_req_array].state, IN_PROGRESS_REQ);
      //assert(interface[t_id].req_array[ops[i].session_id][ops[i].index_to_req_array].state == IN_PROGRESS_REQ);
      signal_completion_to_client(ops[i].session_id, ops[i].index_to_req_array, t_id);
    }
  }

  return trace_iter;
}


/* ---------------------------------------------------------------------------
//------------------------------ LEADER SPECIFIC -----------------------------
//---------------------------------------------------------------------------*/


// Leader calls this to handout global ids to pending writes
static inline void zk_get_g_ids(p_writes_t *p_writes, uint16_t t_id)
{
	uint16_t unordered_writes_num = (uint16_t) ((LEADER_PENDING_WRITES + p_writes->push_ptr - p_writes->unordered_ptr)
																	% LEADER_PENDING_WRITES);
  if (unordered_writes_num == 0) return;
	uint64_t id = atomic_fetch_add_explicit(&global_w_id, (uint64_t) unordered_writes_num, memory_order_relaxed);
  if (ENABLE_STAT_COUNTING) {
    t_stats[t_id].batches_per_thread++;
    t_stats[t_id].total_writes += unordered_writes_num;
  }

	for (uint16_t i = 0; i < unordered_writes_num; ++i) {
    assert(p_writes->unordered_ptr == ((LEADER_PENDING_WRITES + p_writes->push_ptr - unordered_writes_num + i)
                                      % LEADER_PENDING_WRITES));
    uint32_t unordered_ptr = p_writes->unordered_ptr;
		p_writes->g_id[unordered_ptr] = id + i;
		zk_prepare_t *prep = p_writes->ptrs_to_ops[unordered_ptr];
		prep->g_id = p_writes->g_id[unordered_ptr];
    MOD_INCR(p_writes->unordered_ptr, LEADER_PENDING_WRITES);
	}
  p_writes->highest_g_id_taken = id + unordered_writes_num - 1;


	if (ENABLE_ASSERTIONS)
    assert(p_writes->unordered_ptr == p_writes->push_ptr);

}

/* ---------------------------------------------------------------------------
//------------------------------ LEADER MAIN LOOP -----------------------------
//---------------------------------------------------------------------------*/



static inline void zk_increase_prep_credits(uint16_t credits[][MACHINE_NUM], zk_ack_mes_t *ack,
                                            struct fifo *remote_prep_buf, uint16_t t_id)
{
  assert(LEADER_MACHINE == 0);
  credits[PREP_VC][ack->follower_id] +=
    remove_from_the_mirrored_buffer(remote_prep_buf, ack->ack_num, t_id, ack->follower_id - 1, FLR_PREP_BUF_SLOTS);

  if (ENABLE_ASSERTIONS) assert(credits[PREP_VC][ack->follower_id - 1] <= PREPARE_CREDITS);
}

static inline uint32_t zk_find_the_first_prepare_that_gets_acked(uint16_t *ack_num,
                                                                 uint64_t l_id, p_writes_t *p_writes,
                                                                 uint64_t pull_lid, uint16_t t_id)
{

  if (pull_lid >= l_id) {
    (*ack_num) -= (pull_lid - l_id);
    if (ENABLE_ASSERTIONS) assert(*ack_num > 0 && *ack_num <= FLR_PENDING_WRITES);
    return p_writes->pull_ptr;
  }
  else { // l_id > pull_lid
    return (uint32_t) (p_writes->pull_ptr + (l_id - pull_lid)) % LEADER_PENDING_WRITES;
  }
}

static inline void zk_apply_acks(uint16_t ack_num, uint32_t ack_ptr,
                                 uint64_t l_id, p_writes_t *p_writes,
                                 uint64_t pull_lid, uint32_t *outstanding_prepares,
                                 uint16_t t_id)
{
  for (uint16_t ack_i = 0; ack_i < ack_num; ack_i++) {
    if (ENABLE_ASSERTIONS && (ack_ptr == p_writes->push_ptr)) {
      uint32_t origin_ack_ptr = (uint32_t) (ack_ptr - ack_i + LEADER_PENDING_WRITES) % LEADER_PENDING_WRITES;
      my_printf(red, "Origin ack_ptr %u/%u, acks %u/%u, pull_ptr %u, push_ptr % u, size %u \n",
                origin_ack_ptr,  (p_writes->pull_ptr + (l_id - pull_lid)) % LEADER_PENDING_WRITES,
                ack_i, ack_num, p_writes->pull_ptr, p_writes->push_ptr, p_writes->size);
    }
    p_writes->acks_seen[ack_ptr]++;
    if (p_writes->acks_seen[ack_ptr] == LDR_QUORUM_OF_ACKS) {
      if (ENABLE_ASSERTIONS) (*outstanding_prepares)--;
//        printf("Leader %d valid ack %u/%u write at ptr %d with g_id %lu is ready \n",
//               t_id, ack_i, ack_num,  ack_ptr, p_writes->g_id[ack_ptr]);
      p_writes->w_state[ack_ptr] = READY;

    }
    MOD_INCR(ack_ptr, LEADER_PENDING_WRITES);
  }
}

// Leader polls for acks
static inline void ldr_poll_for_acks(zk_ack_mes_ud_t *incoming_acks, uint32_t *pull_ptr,
                                     p_writes_t *p_writes,
                                     uint16_t credits[][MACHINE_NUM],
                                     struct ibv_cq *ack_recv_cq, struct ibv_wc *ack_recv_wc,
                                     recv_info_t *ack_recv_info, struct fifo *remote_prep_buf,
                                     uint16_t t_id, uint32_t *dbg_counter,
                                     uint32_t *outstanding_prepares)
{
	uint32_t buf_ptr = *pull_ptr;
	int polled_messages = 0;
  int completed_messages =
    find_how_many_messages_can_be_polled(ack_recv_cq, ack_recv_wc,
                                         NULL, LEADER_ACK_BUF_SLOTS, t_id);
  if (completed_messages <= 0) {
    if (*outstanding_prepares > 0) (*dbg_counter)++;
    return;
  }
  while (polled_messages < completed_messages) {
		zk_ack_mes_t *ack = &incoming_acks[buf_ptr].ack;
		uint16_t ack_num = ack->ack_num;
		uint64_t l_id = ack->l_id;
		uint64_t pull_lid = p_writes->local_w_id; // l_id at the pull pointer
		uint32_t ack_ptr; // a pointer in the FIFO, from where ack should be added
    zk_check_polled_ack_and_print(ack, ack_num, pull_lid, buf_ptr, t_id);
    zk_increase_prep_credits(credits, ack, remote_prep_buf, t_id);

    // bookkeep early, because the ack may get skipped
    MOD_INCR(buf_ptr, LEADER_ACK_BUF_SLOTS);
    polled_messages++;
    // if the pending write FIFO is empty it means the acks are for committed messages.
    // in which case we only need to increase credits
    if (p_writes->size == 0 ) continue;
    if (pull_lid >= l_id && (pull_lid - l_id) >= ack_num) continue;

    zk_check_ack_l_id_is_small_enough(ack_num, l_id, p_writes, pull_lid, t_id);
    ack_ptr = zk_find_the_first_prepare_that_gets_acked(&ack_num, l_id, p_writes, pull_lid, t_id);

    // Apply the acks that refer to stored writes
    zk_apply_acks(ack_num, ack_ptr, l_id, p_writes, pull_lid,
                  outstanding_prepares, t_id);
	} // while
	*pull_ptr = buf_ptr;
  zk_debug_info_bookkeep(completed_messages, polled_messages, dbg_counter,
                         ack_recv_info, outstanding_prepares, t_id);
	ack_recv_info->posted_recvs -= polled_messages;
}



// Leader propagates Updates that have seen all acks to the KVS
static inline void ldr_propagate_updates(p_writes_t *p_writes, zk_com_fifo_t *com_fifo,
                                         zk_resp_t *resp, latency_info_t *latency_info,
                                         uint16_t t_id, uint32_t *dbg_counter)
{
//  printf("Ldr %d propagating updates \n", t_id);
	uint16_t update_op_i = 0;
  // remember the starting point to use it when writing the KVS
	uint32_t starting_pull_ptr = p_writes->pull_ptr;

	// Read the latest committed g_id
	uint64_t committed_g_id = atomic_load_explicit(&committed_global_w_id, memory_order_relaxed);
	while(p_writes->w_state[p_writes->pull_ptr] == READY) {
    // the commit prep_message is full: that may be too restricting,
    // but it does not affect performance or correctness
		if (com_fifo->size == COMMIT_FIFO_SIZE) break;
    if (!is_expected_g_id_ready(p_writes, &committed_g_id, &update_op_i,
                                dbg_counter, LEADER_PENDING_WRITES, LEADER, t_id))
      break;
 	}
	if (update_op_i > 0) {
    zk_create_commit_message(com_fifo, p_writes->local_w_id, update_op_i);
		p_writes->local_w_id += update_op_i; // advance the local_w_id

    if (ENABLE_ASSERTIONS) assert(p_writes->size >= update_op_i);
    p_writes->size -= update_op_i;
    zk_KVS_batch_op_updates((uint16_t) update_op_i, p_writes->ptrs_to_ops, starting_pull_ptr,
                            LEADER_PENDING_WRITES, false, t_id);
		atomic_store_explicit(&committed_global_w_id, committed_g_id, memory_order_relaxed);
    zk_take_latency_measurement_for_writes(latency_info, t_id);
    zk_signal_completion_and_bookkeepfor_writes(p_writes, update_op_i, starting_pull_ptr,
                                                LEADER_PENDING_WRITES, LEADER, latency_info, t_id);
	}
}


// Poll for incoming write requests from followers
static inline void poll_for_writes(volatile zk_w_mes_ud_t *incoming_ws, uint32_t *pull_ptr,
																	 p_writes_t *p_writes,
																	 struct ibv_cq *w_recv_cq, struct ibv_wc *w_recv_wc,
                                   recv_info_t *w_recv_info,
                                   uint32_t *completed_but_not_polled_writes,
                                   uint16_t t_id)
{
  int completed_messages =
    find_how_many_messages_can_be_polled(w_recv_cq, w_recv_wc,
                                         completed_but_not_polled_writes,
                                         LEADER_W_BUF_SLOTS, t_id);
  if (completed_messages <= 0) return;

	if (p_writes->size == LEADER_PENDING_WRITES) return;
	if (ENABLE_ASSERTIONS) assert(p_writes->size < LEADER_PENDING_WRITES);
	uint32_t index = *pull_ptr;
	uint32_t polled_messages = 0;
	// Start polling
  while (polled_messages < completed_messages) {
	//while (incoming_ws[index].w_mes.write[0].w_num > 0) {
   // wait_for_the_entire_write(&incoming_ws[index].w_mes, t_id, index);
		 if (DEBUG_WRITES) printf("Leader sees a write Opcode %d at offset %d  \n",
                              incoming_ws[index].w_mes.write[0].opcode, index);
		zk_w_mes_t *w_mes = (zk_w_mes_t *) &incoming_ws[index].w_mes;
		uint8_t w_num = w_mes->write[0].w_num;
    if (p_writes->size + w_num > LEADER_PENDING_WRITES) {
      (*completed_but_not_polled_writes) = completed_messages - polled_messages;
      break;
    }
		for (uint16_t i = 0; i < w_num; i++) {
      zk_write_t *write = &w_mes->write[i];
      if (ENABLE_ASSERTIONS) if(write->opcode != KVS_OP_PUT)
          my_printf(red, "Opcode %u, i %u/%u \n",write->opcode, i, w_num);
      if (DEBUG_WRITES)
        printf("Poll for writes passes session id %u \n", write->sess_id);
      ldr_insert_write(p_writes, (void*) write, write->sess_id,
                       false, t_id);
      write->opcode = 0;
		}
    if (ENABLE_STAT_COUNTING) {
      t_stats[t_id].received_writes += w_num;
      t_stats[t_id].received_writes_mes_num++;
    }
    incoming_ws[index].w_mes.write[0].w_num = 0;
    MOD_INCR(index, LEADER_W_BUF_SLOTS);
    polled_messages++;
	}
  (*pull_ptr) = index;

	if (polled_messages > 0) {
    //poll_cq(w_recv_cq, polled_messages, w_recv_wc, "polling for writes");
    w_recv_info->posted_recvs -= polled_messages;
    //post_recvs_with_recv_info(w_recv_info, polled_messages);
  }
}



/* ---------------------------------------------------------------------------
//------------------------------ BROADCASTS -----------------------------
//---------------------------------------------------------------------------*/




// Leader broadcasts commits
static inline void broadcast_commits(p_writes_t *p_writes, uint16_t credits[][MACHINE_NUM],
                                     hrd_ctrl_blk_t *cb,
                                     zk_com_fifo_t *com_fifo, uint64_t *commit_br_tx,
																		 struct ibv_wc *credit_wc,
                                     struct ibv_sge *com_send_sgl, struct ibv_send_wr *com_send_wr,
                                     recv_info_t * cred_recv_info, uint32_t *time_out_cnt,
																		 recv_info_t *w_recv_info, uint16_t t_id)
{

  if (com_fifo->size == 0) return;
  uint8_t vc = COMM_VC;
  uint16_t  br_i = 0, credit_recv_counter = 0, mes_sent = 0, available_credits = 0;
  ldr_poll_credits(cb->dgram_recv_cq[FC_QP_ID], credit_wc, credits[vc],
                   cred_recv_info, p_writes->q_info, t_id);

  if (!check_bcast_credits(credits[vc], p_writes->q_info, &time_out_cnt[vc],
                           &available_credits, 1,  t_id)) {
    if (ENABLE_STAT_COUNTING) t_stats[t_id].stalled_com_credit++;
    return;
  }

  while (com_fifo->size > 0 && mes_sent < available_credits) {
		zk_com_mes_t *com_mes = &com_fifo->commits[com_fifo->pull_ptr];
    // Create the broadcast messages
    forge_commit_wrs(com_mes, p_writes->q_info, t_id, br_i, cb, com_send_sgl,
                     com_send_wr,  commit_br_tx, credits);
		com_fifo->size--;
		MOD_INCR(com_fifo->pull_ptr, COMMIT_FIFO_SIZE);
    br_i++;
    mes_sent++;
    zk_checks_and_stats_on_bcasting_commits(com_fifo, com_mes, br_i, t_id);
    if ((*commit_br_tx) % FLR_CREDITS_IN_MESSAGE == 0) credit_recv_counter++;
    if (br_i == MAX_BCAST_BATCH) {
      post_recvs_with_recv_info(cred_recv_info, LDR_MAX_CREDIT_RECV - cred_recv_info->posted_recvs);
      post_quorum_broadasts_and_recvs(w_recv_info, LDR_MAX_RECV_W_WRS - w_recv_info->posted_recvs,
                                      p_writes->q_info, br_i, *commit_br_tx, com_send_wr,
                                      cb->dgram_qp[COMMIT_W_QP_ID], COM_ENABLE_INLINING);
      br_i = 0;
    }
  }
	if (br_i > 0) {
    post_recvs_with_recv_info(cred_recv_info, LDR_MAX_CREDIT_RECV - cred_recv_info->posted_recvs);
    post_quorum_broadasts_and_recvs(w_recv_info, LDR_MAX_RECV_W_WRS - w_recv_info->posted_recvs,
                                    p_writes->q_info, br_i, *commit_br_tx, com_send_wr,
                                    cb->dgram_qp[COMMIT_W_QP_ID], COM_ENABLE_INLINING);
	}
	if (ENABLE_ASSERTIONS) assert(w_recv_info->posted_recvs <= LDR_MAX_RECV_W_WRS);
  if (mes_sent > 0) decrease_credits(credits, p_writes->q_info, mes_sent, vc);
}






// Leader Broadcasts its Prepares
static inline void broadcast_prepares(p_writes_t *p_writes,
																			uint16_t credits[][MACHINE_NUM], struct hrd_ctrl_blk *cb,
																			struct ibv_sge *prep_send_sgl, struct ibv_send_wr *prep_send_wr,
																			uint64_t *prep_br_tx, recv_info_t *ack_recv_info,
                                      struct fifo *remote_prep_buf, uint32_t *time_out_cnt,
                                      uint32_t *outstanding_prepares, uint16_t t_id)
{
	uint8_t vc = PREP_VC;
	uint16_t br_i = 0, mes_sent = 0, available_credits = 0;
	uint32_t bcast_pull_ptr = p_writes->prep_fifo->bcast_pull_ptr;

  if (p_writes->prep_fifo->bcast_size == 0)  return;
  else if (!check_bcast_credits(credits[vc], p_writes->q_info, &time_out_cnt[vc],
                                &available_credits, 1,  t_id)) return;
	while (p_writes->prep_fifo->bcast_size > 0 && mes_sent < available_credits) {
    if (DEBUG_PREPARES)
      printf("LDR %d has %u bcasts to send credits %d\n", t_id,
             p_writes->prep_fifo->bcast_size, credits[PREP_VC][1]);
		// Create the broadcast messages

		forge_prep_wr((uint16_t) bcast_pull_ptr, p_writes, cb,  prep_send_sgl,
                  prep_send_wr, prep_br_tx, br_i, credits, vc, t_id);
		br_i++;
    mes_sent++;
    uint8_t coalesce_num = p_writes->prep_fifo->prep_message[bcast_pull_ptr].coalesce_num;
    add_to_the_mirrored_buffer(remote_prep_buf, coalesce_num, FOLLOWER_MACHINE_NUM, FLR_PREP_BUF_SLOTS, p_writes->q_info);
    zk_checks_and_stats_on_bcasting_prepares(p_writes, coalesce_num, outstanding_prepares, t_id);
    zk_reset_prep_message(p_writes, coalesce_num,t_id);
    p_writes->prep_fifo->bcast_size -= coalesce_num;
    MOD_INCR(bcast_pull_ptr, PREP_FIFO_SIZE);
		if (br_i == MAX_BCAST_BATCH) {
      post_quorum_broadasts_and_recvs(ack_recv_info, LDR_MAX_RECV_ACK_WRS - ack_recv_info->posted_recvs,
                                      p_writes->q_info, br_i, *prep_br_tx, prep_send_wr, cb->dgram_qp[PREP_ACK_QP_ID],
                                      LEADER_PREPARE_ENABLE_INLINING);
      br_i = 0;
		}
	}
  if (br_i > 0) {
    post_quorum_broadasts_and_recvs(ack_recv_info, LDR_MAX_RECV_ACK_WRS - ack_recv_info->posted_recvs,
                                    p_writes->q_info, br_i, *prep_br_tx, prep_send_wr, cb->dgram_qp[PREP_ACK_QP_ID],
                                    LEADER_PREPARE_ENABLE_INLINING);
  }
  if (ENABLE_ASSERTIONS) assert(ack_recv_info->posted_recvs <= LDR_MAX_RECV_ACK_WRS);
	p_writes->prep_fifo->bcast_pull_ptr = bcast_pull_ptr;
  if (mes_sent > 0) decrease_credits(credits, p_writes->q_info, mes_sent, vc);
}



/* ---------------------------------------------------------------------------
//------------------------------ FOLLOWER MAIN LOOP -----------------------------
//---------------------------------------------------------------------------*/

// Poll for prepare messages
static inline void flr_poll_for_prepares(volatile zk_prep_mes_ud_t *incoming_preps, uint32_t *pull_ptr,
                                         p_writes_t *p_writes, struct pending_acks *p_acks,
                                         struct ibv_cq *prep_recv_cq, struct ibv_wc *prep_recv_wc,
                                         recv_info_t *prep_recv_info, struct fifo *prep_buf_mirror,
                                         uint32_t *completed_but_not_polled_preps,
                                         uint16_t t_id, uint8_t flr_id, uint32_t *wait_for_prepares_dbg_counter)
{
	uint16_t polled_messages = 0;
	if (prep_buf_mirror->size == MAX_PREP_BUF_SLOTS_TO_BE_POLLED) {
    return;
  }
	uint32_t buf_ptr = *pull_ptr;
  int completed_messages =
    find_how_many_messages_can_be_polled(prep_recv_cq, prep_recv_wc,
                                         completed_but_not_polled_preps,
                                         FLR_PREP_BUF_SLOTS, t_id);
  if (completed_messages <= 0) {
    zk_increment_wait_for_preps_cntr(p_writes, p_acks, wait_for_prepares_dbg_counter);
    return;
  }
  while (polled_messages < completed_messages) {
    zk_prep_mes_t *prep_mes = (zk_prep_mes_t *) &incoming_preps[buf_ptr].prepare;
    uint8_t coalesce_num = prep_mes->coalesce_num;
		zk_prepare_t *prepare = prep_mes->prepare;
    uint32_t incoming_l_id = prep_mes->l_id;
    uint64_t expected_l_id = p_writes->local_w_id + p_writes->size;
    if (prep_buf_mirror->size == MAX_PREP_BUF_SLOTS_TO_BE_POLLED) break;
		if (p_writes->size + coalesce_num > FLR_PENDING_WRITES) break;
    zk_check_polled_prep_and_print(prep_mes, p_writes, coalesce_num, buf_ptr,
                                   incoming_l_id, expected_l_id, incoming_preps, t_id);

		p_acks->acks_to_send+= coalesce_num; // lids are in order so ack them
    add_to_the_mirrored_buffer(prep_buf_mirror, coalesce_num, 1, FLR_PREP_BUF_SLOTS, p_writes->q_info);
    ///Loop throug prepares inside the message
		for (uint8_t prep_i = 0; prep_i < coalesce_num; prep_i++) {
      zk_check_prepare_and_print(&prepare[prep_i], p_writes, prep_i, t_id);
      fill_p_writes_entry(p_writes, &prepare[prep_i], flr_id, t_id);
  		MOD_INCR(p_writes->push_ptr, FLR_PENDING_WRITES);
			p_writes->size++;
		} ///

		if (ENABLE_ASSERTIONS) prep_mes->opcode = 0;
		MOD_INCR(buf_ptr, FLR_PREP_BUF_SLOTS);
		polled_messages++;
	}
  (*completed_but_not_polled_preps) = (uint32_t) (completed_messages - polled_messages);
  *pull_ptr = buf_ptr;
	prep_recv_info->posted_recvs -= polled_messages;
  zk_checks_after_polling_prepares(p_writes, wait_for_prepares_dbg_counter, polled_messages,
                                   prep_recv_info, p_acks,  t_id);
}


// Send a batched ack that denotes the first local write id and the number of subsequent lid that are being acked
static inline void send_acks_to_ldr(p_writes_t *p_writes, struct ibv_send_wr *ack_send_wr,
																		struct ibv_sge *ack_send_sgl, uint64_t *sent_ack_tx,
																		struct hrd_ctrl_blk *cb, recv_info_t *prep_recv_info,
																		uint8_t flr_id, zk_ack_mes_t *ack,
																		struct pending_acks *p_acks, uint16_t t_id)
{
  if (p_acks->acks_to_send == 0) return;
	struct ibv_send_wr *bad_send_wr;


	ack->opcode = KVS_OP_ACK;
	ack->follower_id = flr_id;
	ack->ack_num = (uint16_t) p_acks->acks_to_send;
	uint64_t l_id_to_send = p_writes->local_w_id + p_acks->slots_ahead;
  for (uint32_t i = 0; i < ack->ack_num; i++) {
    uint16_t w_ptr = (uint16_t) ((p_writes->pull_ptr + p_acks->slots_ahead + i) % FLR_PENDING_WRITES);
    if (ENABLE_ASSERTIONS) assert(p_writes->w_state[w_ptr] == VALID);
    p_writes->w_state[w_ptr] = SENT;
  }
	ack->l_id = l_id_to_send;
	p_acks->slots_ahead += p_acks->acks_to_send;
	p_acks->acks_to_send = 0;
	ack_send_sgl->addr = (uint64_t) (uintptr_t) ack;
  check_stats_prints_when_sending_acks(ack, p_writes, p_acks, l_id_to_send, t_id);
  selective_signaling_for_unicast(sent_ack_tx, ACK_SEND_SS_BATCH, ack_send_wr,
                                  0, cb->dgram_send_cq[PREP_ACK_QP_ID], true,
                                  "sending acks", t_id);
	// RECEIVES for prepares
	uint32_t posted_recvs = prep_recv_info->posted_recvs;
	uint32_t recvs_to_post_num = FLR_MAX_RECV_PREP_WRS - posted_recvs;
	if (recvs_to_post_num > 0) {
    post_recvs_with_recv_info(prep_recv_info, recvs_to_post_num);
    checks_and_prints_posting_recvs_for_preps(prep_recv_info, recvs_to_post_num, t_id);
  }
	// SEND the ack
	int ret = ibv_post_send(cb->dgram_qp[PREP_ACK_QP_ID], &ack_send_wr[0], &bad_send_wr);
	CPE(ret, "ACK ibv_post_send error", ret);
}


//Send credits for the commits
static inline void send_credits_for_commits(recv_info_t *com_recv_info, struct hrd_ctrl_blk *cb,
                                            struct ibv_send_wr *credit_wr, uint64_t *credit_tx,
                                            uint16_t credit_num, uint16_t t_id)
{
  struct ibv_send_wr *bad_send_wr;
  // RECEIVES FOR COMMITS
  uint32_t recvs_to_post_num = (uint32_t) (credit_num * FLR_CREDITS_IN_MESSAGE);
  if (ENABLE_ASSERTIONS) assert(recvs_to_post_num < FLR_MAX_RECV_COM_WRS);
  post_recvs_with_recv_info(com_recv_info, recvs_to_post_num);
  //printf("FLR %d posting %u recvs and has a total of %u recvs for commits \n",
	//		    t_id, recvs_to_post_num,  com_recv_info->posted_recvs);

  for (uint16_t credit_wr_i = 0; credit_wr_i < credit_num; credit_wr_i++) {
    selective_signaling_for_unicast(credit_tx, COM_CREDIT_SS_BATCH, credit_wr,
                                    credit_wr_i, cb->dgram_send_cq[FC_QP_ID], true,
                                    "sending credits", t_id);
  }
  credit_wr[credit_num - 1].next = NULL;
  //my_printf(yellow, "I am sending %d credit message(s)\n", credit_num);
  int ret = ibv_post_send(cb->dgram_qp[FC_QP_ID], &credit_wr[0], &bad_send_wr);
  CPE(ret, "ibv_post_send error in credits", ret);
}



// Send the local writes to the ldr
static inline void send_writes_to_the_ldr(p_writes_t *p_writes,
                                          uint16_t *credits, struct hrd_ctrl_blk *cb,
                                          struct ibv_sge *w_send_sgl, struct ibv_send_wr *w_send_wr,
                                          uint64_t *w_tx, struct fifo *remote_w_buf,
                                          uint16_t t_id, uint32_t *outstanding_writes)
{
  struct ibv_send_wr *bad_send_wr;
  uint16_t w_i = 0;

  while (p_writes->w_fifo->size > 0 && (*credits) > 0) {
    if (DEBUG_WRITES)
      printf("FLR %d has %u writes to send credits %d\n", t_id, p_writes->w_fifo->size, *credits);
    // Create the messages
    forge_w_wr(p_writes, cb, w_send_sgl, w_send_wr, w_tx, w_i, *credits, t_id);
    zk_w_mes_t *w_mes_fifo = (zk_w_mes_t *) p_writes->w_fifo->fifo;
    uint32_t w_ptr = p_writes->w_fifo->pull_ptr;
    zk_w_mes_t *w_mes = &w_mes_fifo[w_ptr];
    uint16_t coalesce_num = w_mes->write[0].w_num;
    (*credits)--;
    checks_and_stats_when_sending_write(p_writes, coalesce_num, outstanding_writes, t_id);
    reset_write_mes(p_writes, w_mes_fifo, coalesce_num, t_id);
    add_to_the_mirrored_buffer(remote_w_buf, (uint8_t) coalesce_num, 1,
                               LEADER_W_BUF_SLOTS, p_writes->q_info);
    MOD_INCR(p_writes->w_fifo->pull_ptr, W_FIFO_SIZE);
    p_writes->w_fifo->size -= coalesce_num;
    w_i++;
  }
  if (w_i > 0) {
    w_send_wr[w_i - 1].next = NULL;
    int ret = ibv_post_send(cb->dgram_qp[COMMIT_W_QP_ID], &w_send_wr[0], &bad_send_wr);
    CPE(ret, "Broadcast ibv_post_send error", ret);
  }


}

// Leader polls for acks
static inline void poll_for_coms(zk_com_mes_ud_t *incoming_coms, uint32_t *pull_ptr,
                                 p_writes_t *p_writes, uint16_t *credits,
                                 struct ibv_cq * com_recv_cq, struct ibv_wc *com_recv_wc,
                                 recv_info_t *com_recv_info, struct hrd_ctrl_blk *cb,
                                 struct ibv_send_wr *credit_wr, uint64_t *credit_tx,
                                 struct fifo *remote_w_buf,
                                 uint32_t *completed_but_not_polled_coms,
                                 uint16_t t_id, uint8_t flr_id, uint32_t *dbg_counter)
{
  uint32_t buf_ptr = *pull_ptr;
  uint32_t polled_messages = 0;

  int completed_messages =
    find_how_many_messages_can_be_polled(com_recv_cq, com_recv_wc,
                                         completed_but_not_polled_coms,
                                         FLR_COM_BUF_SLOTS, t_id);
  if (completed_messages <= 0) {
    if (ENABLE_ASSERTIONS  && p_writes->size > 0) (*dbg_counter)++;
    return;
  }
  while (polled_messages < completed_messages) {
    zk_com_mes_t *com = &incoming_coms[buf_ptr].com;
    uint16_t com_num = com->com_num;
    uint64_t l_id = com->l_id;
    uint64_t pull_lid = p_writes->local_w_id; // l_id at the pull pointer
    zk_check_polled_commit_and_print(com, p_writes, buf_ptr,
                                     l_id, pull_lid, com_num, t_id);
    // This must always hold: l_id >= pull_lid,
    // because we need the commit to advance the pull_lid
    uint16_t com_ptr = (uint16_t)
      ((p_writes->pull_ptr + (l_id - pull_lid)) % FLR_PENDING_WRITES);
    /// loop through each commit
    for (uint16_t com_i = 0; com_i < com_num; com_i++) {
      if (zk_write_not_ready(com, com_ptr, com_i, com_num, p_writes, t_id))
        goto END_WHILE;

      assert(l_id + com_i - pull_lid < FLR_PENDING_WRITES);
			p_writes->w_state[com_ptr] = READY;
      flr_increases_write_credits(p_writes, com_ptr, remote_w_buf,
                                  credits, flr_id, t_id);
      MOD_INCR(com_ptr, FLR_PENDING_WRITES);
    } ///

    if (ENABLE_ASSERTIONS) com->opcode = 0;
		MOD_INCR(buf_ptr, FLR_COM_BUF_SLOTS);
    if (ENABLE_STAT_COUNTING) {
      t_stats[t_id].received_coms += com_num;
      t_stats[t_id].received_coms_mes_num++;
    }
    if (buf_ptr % FLR_CREDITS_IN_MESSAGE == 0)
      send_credits_for_commits(com_recv_info, cb, credit_wr, credit_tx, 1, t_id);
		polled_messages++;
  } // while
  END_WHILE: *pull_ptr = buf_ptr;
  (*completed_but_not_polled_coms) = completed_messages - polled_messages;
  zk_checks_after_polling_commits(dbg_counter, polled_messages, com_recv_info);
  com_recv_info->posted_recvs -= polled_messages;
}


// Follower propagates Updates that have seen all acks to the KVS
static inline void flr_propagate_updates(p_writes_t *p_writes, struct pending_acks *p_acks,
                                         zk_resp_t *resp, struct fifo *prep_buf_mirror,
                                         latency_info_t *latency_info,
                                         uint16_t t_id, uint32_t *dbg_counter)
{
	uint16_t update_op_i = 0;
  // remember the starting point to use it when writing the KVS
	uint32_t starting_pull_ptr = p_writes->pull_ptr;
	// Read the latest committed g_id
	uint64_t committed_g_id = atomic_load_explicit(&committed_global_w_id, memory_order_relaxed);
  flr_increase_counter_if_waiting_for_commit(p_writes, committed_g_id, t_id);

	while(p_writes->w_state[p_writes->pull_ptr] == READY) {
    if (!is_expected_g_id_ready(p_writes, &committed_g_id, &update_op_i,
                                dbg_counter, FLR_PENDING_WRITES, FOLLOWER, t_id))
      break;
  }

	if (update_op_i > 0) {
    remove_from_the_mirrored_buffer(prep_buf_mirror, update_op_i, t_id, 0, FLR_PREP_BUF_SLOTS);
		p_writes->local_w_id += update_op_i; // advance the local_w_id
		if (ENABLE_ASSERTIONS) {
			assert(p_writes->size >= update_op_i);
			assert(p_acks->slots_ahead >= update_op_i);
		}
		p_acks->slots_ahead -= update_op_i;
		p_writes->size -= update_op_i;
    zk_KVS_batch_op_updates((uint16_t) update_op_i, p_writes->ptrs_to_ops,  starting_pull_ptr,
                            FLR_PENDING_WRITES, true, t_id);
		atomic_store_explicit(&committed_global_w_id, committed_g_id, memory_order_relaxed);
    zk_take_latency_measurement_for_writes(latency_info, t_id);

    zk_signal_completion_and_bookkeepfor_writes(p_writes, update_op_i, starting_pull_ptr,
                                                FLR_PENDING_WRITES, FOLLOWER, latency_info, t_id);
	}
}




#endif /* INLINE_UTILS_H */

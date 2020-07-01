#ifndef INLINE_UTILS_H
#define INLINE_UTILS_H

#include "../general_util/inline_util.h"
#include "zk_kvs_util.h"
#include "zk_debug_util.h"
#include "zk_reservation_stations_util.h_.h"



/* ---------------------------------------------------------------------------
//------------------------------ LEADER SPECIFIC -----------------------------
//---------------------------------------------------------------------------*/

static inline void checks_when_leader_creates_write(zk_prep_mes_t *preps, uint32_t prep_ptr,
                                                    uint32_t inside_prep_ptr, p_writes_t *p_writes,
                                                    uint32_t w_ptr, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (inside_prep_ptr == 0) {
      uint32_t message_l_id = preps[prep_ptr].l_id;
      if (message_l_id > MAX_PREP_COALESCE) {
        uint32_t prev_prep_ptr = (prep_ptr + PREP_FIFO_SIZE - 1) % PREP_FIFO_SIZE;
        uint32_t prev_l_id = preps[prev_prep_ptr].l_id;
        uint8_t prev_coalesce = preps[prev_prep_ptr].coalesce_num;
        if (message_l_id != prev_l_id + prev_coalesce) {
          my_printf(red, "Current message l_id %u, previous message l_id %u , previous coalesce %u\n",
                    message_l_id, prev_l_id, prev_coalesce);
        }
      }
    }
    if (p_writes->w_state[w_ptr] != INVALID)
      my_printf(red, "Leader %u w_state %d at w_ptr %u, g_id %lu, cache hits %lu, size %u \n",
                t_id, p_writes->w_state[w_ptr], w_ptr, p_writes->g_id[w_ptr],
                t_stats[t_id].cache_hits_per_thread, p_writes->size);
			//printf("Sent %d, Valid %d, Ready %d \n", SENT, VALID, READY);
    assert(p_writes->w_state[w_ptr] == INVALID);

  }
}

static inline void fill_prep(zk_prepare_t *prep, mica_key_t key, uint8_t opcode, uint8_t val_len,
                             uint8_t *value, uint8_t flr_id, uint16_t session_id)
{
  prep->key = key;
  prep->opcode = opcode;
  prep->val_len = val_len;
  memcpy(prep->value, value, (size_t) VALUE_SIZE);
  prep->flr_id = flr_id; // means it's a leader message
  prep->sess_id = session_id;
}

static inline void fill_write(zk_write_t *write, mica_key_t key, uint8_t opcode, uint8_t val_len,
                              uint8_t *value, uint8_t flr_id, uint32_t session_id)
{
  write->key = key;
  write->opcode = opcode;
  write->val_len = val_len;
  memcpy(write->value, value, (size_t) VALUE_SIZE);
  write->flr_id = flr_id; // means it's a leader message
  write->sess_id = session_id;
}

// Insert a new local or remote write to the leader pending writes
 static inline void ldr_insert_write(p_writes_t *p_writes, void *source, uint32_t session_id,
                                     bool local, uint16_t t_id)
{
  zk_prep_mes_t *preps = p_writes->prep_fifo->prep_message;
  uint32_t prep_ptr = p_writes->prep_fifo->push_ptr;
  uint32_t inside_prep_ptr = preps[prep_ptr].coalesce_num;
  uint32_t w_ptr = p_writes->push_ptr;
  zk_prepare_t *prep = &preps[prep_ptr].prepare[inside_prep_ptr];

  if (local) {
    zk_trace_op_t *op = (zk_trace_op_t *) source;
    fill_prep(prep, op->key, op->opcode, op->val_len, op->value,
              FOLLOWER_MACHINE_NUM, (uint16_t) session_id);
  }
  else {
    zk_write_t *rem_write = (zk_write_t *) source;
    fill_prep(prep, rem_write->key, rem_write->opcode, rem_write->val_len, rem_write->value,
              rem_write->flr_id, (uint16_t) rem_write->sess_id);
  }

  // link it with p_writes to lead it later it to the KVS
  p_writes->ptrs_to_ops[w_ptr] = prep;

  // If it's the first message give it an lid
  if (inside_prep_ptr == 0) {
    p_writes->prep_fifo->backward_ptrs[prep_ptr] = w_ptr;
    uint32_t message_l_id = (uint32_t) (p_writes->local_w_id + p_writes->size);
    preps[prep_ptr].l_id = (uint32_t) message_l_id;
  }
  checks_when_leader_creates_write(preps, prep_ptr, inside_prep_ptr, p_writes, w_ptr, t_id);

  // Bookkeeping
  p_writes->w_state[w_ptr] = VALID;
  p_writes->is_local[w_ptr] = local;
  p_writes->session_id[w_ptr] = (uint32_t) session_id;
  MOD_INCR(p_writes->push_ptr, LEADER_PENDING_WRITES);
  p_writes->size++;
  p_writes->prep_fifo->bcast_size++;
  preps[prep_ptr].coalesce_num++;
  if (preps[prep_ptr].coalesce_num == MAX_PREP_COALESCE) {
    MOD_INCR(p_writes->prep_fifo->push_ptr, PREP_FIFO_SIZE);
    preps[p_writes->prep_fifo->push_ptr].coalesce_num = 0;
  }
}

// Follower inserts a new local write to the write fifo it maintains (for the writes that it propagates to the leader)
static inline void flr_insert_write(p_writes_t *p_writes, zk_trace_op_t *op, uint32_t session_id,
                                    uint8_t flr_id, uint16_t t_id)
{
  zk_w_mes_t *w_mes = (zk_w_mes_t *) p_writes->w_fifo->fifo;
  uint32_t w_ptr = p_writes->w_fifo->push_ptr;
  uint32_t inside_prep_ptr = w_mes[w_ptr].write[0].w_num;
  zk_write_t *write = &w_mes[w_ptr].write[inside_prep_ptr];

  fill_write(write, op->key, op->opcode, op->val_len, op->value,
             flr_id, session_id);
  //    printf("Passed session id %u to the op in message %u, with inside ptr %u\n",
  //           *(uint32_t*)w_mes[w_ptr].write[inside_prep_ptr].session_id, w_ptr, inside_prep_ptr);
  p_writes->w_fifo->size++;
  w_mes[w_ptr].write[0].w_num++;

  if (w_mes[w_ptr].write[0].w_num == MAX_W_COALESCE) {
    MOD_INCR(p_writes->w_fifo->push_ptr, W_FIFO_SIZE);
    w_mes[p_writes->w_fifo->push_ptr].write[0].w_num = 0;
   //my_printf(yellow, "Zeroing when in cache at pointer %u \n", p_writes->w_fifo->push_ptr);
  }
}


static inline void zk_fill_trace_op(trace_t *trace, zk_trace_op_t *ops, uint16_t op_i,
                                    int working_session, protocol_t protocol,
                                    p_writes_t *p_writes, uint8_t flr_id,
                                    latency_info_t *latency_info, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) assert(trace->opcode != NOP);
  bool is_update = (trace->opcode == KVS_OP_PUT) ? (uint8_t) 1 : (uint8_t) 0;
  *(uint128 *) &ops[op_i].key = *(uint128 *) trace->key_hash;
  ops[op_i].opcode = is_update ? (uint8_t) KVS_OP_PUT : (uint8_t) KVS_OP_GET;
  assert(is_update);
  ops[op_i].val_len = is_update ? (uint8_t) (VALUE_SIZE >> SHIFT_BITS) : (uint8_t) 0;
  if (MEASURE_LATENCY) start_measurement(latency_info, (uint32_t) working_session, t_id, ops[op_i].opcode);
  if (is_update) {
    switch (protocol) {
      case FOLLOWER:
        flr_insert_write(p_writes, &ops[op_i],
                         (uint32_t) working_session, flr_id, t_id);
        break;
      case LEADER:
        ldr_insert_write(p_writes, (void *) &ops[op_i],
                         (uint32_t) working_session, true, t_id);
        break;
      default:
        if (ENABLE_ASSERTIONS) assert(false);
    }
    p_writes->stalled[working_session] = true;
  }
  if (ENABLE_ASSERTIONS == 1) {
    assert(WRITE_RATIO > 0 || is_update == 0);
    if (is_update) assert(ops[op_i].val_len > 0);
  }
}

// Both Leader and Followers use this to read the trace, propagate reqs to the cache and maintain their prepare/write fifos
static inline uint32_t zk_batch_from_trace_to_KVS(uint32_t trace_iter, uint16_t t_id, trace_t *trace,
                                                  zk_trace_op_t *ops, uint8_t flr_id,
                                                  p_writes_t *p_writes, zk_resp_t *resp,
                                                  latency_info_t *latency_info,
                                                  uint16_t *last_session_, protocol_t protocol)
{
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
  //  my_printf(green, "op_i %d , trace_iter %d, trace[trace_iter].opcode %d \n", op_i, trace_iter, trace[trace_iter].opcode);
  /// main loop
  while (op_i < ZK_TRACE_BATCH && !passed_over_all_sessions) {

    zk_fill_trace_op(&trace[trace_iter], ops, op_i, working_session, protocol,
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
    // printf("thread %d t_id next working session %d\n total ops %d\n", t_id, working_session, op_i);
    resp[op_i].type = EMPTY;
    trace_iter++;
    if (trace[trace_iter].opcode == NOP) trace_iter = 0;
    op_i++;
  }

  *last_session_ = (uint16_t) working_session;
  t_stats[t_id].cache_hits_per_thread += op_i;
  zk_KVS_batch_op_trace(op_i, ops, resp, t_id);
  if (MEASURE_LATENCY && machine_id == LATENCY_MACHINE && t_id == LATENCY_THREAD &&
      latency_info->measured_req_flag == READ_REQ)
    report_latency(latency_info);
  return trace_iter;
}

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
    //uint16_t unordered_ptr = (uint16_t) ((LEADER_PENDING_WRITES + p_writes->push_ptr - unordered_writes_num + i)
    //                         % LEADER_PENDING_WRITES);
    uint32_t unordered_ptr = p_writes->unordered_ptr;
		p_writes->g_id[unordered_ptr] = id + i;
		zk_prepare_t *prep = p_writes->ptrs_to_ops[unordered_ptr];
		prep->g_id = (uint32_t) p_writes->g_id[unordered_ptr];
    MOD_INCR(p_writes->unordered_ptr, LEADER_PENDING_WRITES);
	}
  p_writes->highest_g_id_taken = id + unordered_writes_num - 1;
	if (id > B_4) {
    if (MEASURE_LATENCY && machine_id == LATENCY_MACHINE) print_latency_stats();
    assert(false);
  }
//  if (unordered_writes_num > 0)
//    printf("Thread %d got id %lu to id %lu for its write for %u writes \n",
//           t_id,  id, id + unordered_writes_num - 1,  unordered_writes_num);
	if (ENABLE_ASSERTIONS)
    assert(p_writes->unordered_ptr == p_writes->push_ptr);

}

/* ---------------------------------------------------------------------------
//------------------------------ LEADER MAIN LOOP -----------------------------
//---------------------------------------------------------------------------*/



static inline void zk_increase_prep_credits(uint16_t credits[][FOLLOWER_MACHINE_NUM], zk_ack_mes_t *ack,
                                            struct fifo *remote_prep_buf, uint16_t t_id)
{
  credits[PREP_VC][ack->follower_id] +=
    remove_from_the_mirrored_buffer(remote_prep_buf, ack->ack_num, t_id, ack->follower_id, FLR_PREP_BUF_SLOTS);

  if (ENABLE_ASSERTIONS) assert(credits[PREP_VC][ack->follower_id] <= PREPARE_CREDITS);
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
                                     uint16_t credits[][FOLLOWER_MACHINE_NUM],
                                     struct ibv_cq *ack_recv_cq, struct ibv_wc *ack_recv_wc,
                                     struct recv_info *ack_recv_info, struct fifo *remote_prep_buf,
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


// Add the acked gid to the appropriate commit message
static inline void forge_commit_message(zk_com_fifo_t *com_fifo, uint64_t l_id, uint16_t update_op_i)
{
	//l_id refers the oldest write to commit (writes go from l_id to l_id + update_op_i)
	uint16_t com_mes_i = com_fifo->push_ptr;
  assert(com_mes_i < COMMIT_FIFO_SIZE);
	uint16_t last_com_mes_i;
	zk_com_mes_t *commit_messages = com_fifo->commits;
	//if (unlikely(update_op_i ))
  assert(com_fifo->size <= COMMIT_FIFO_SIZE);
	// see if the new batch can fit with the previous one -- no reason why it should not
	if (com_fifo->size > 0) {
		last_com_mes_i = (COMMIT_FIFO_SIZE + com_mes_i - 1) % COMMIT_FIFO_SIZE;
		zk_com_mes_t *last_commit = &commit_messages[last_com_mes_i];
		uint64_t last_l_id = last_commit->l_id;
		last_l_id += last_commit->com_num;
		if (last_l_id == l_id) {
			if (last_commit->com_num + update_op_i <= MAX_LIDS_IN_A_COMMIT) {
				last_commit->com_num += update_op_i;
				return;
			}
		}
	}
	//otherwise push a new commit
	zk_com_mes_t *new_commit = &commit_messages[com_mes_i];
	new_commit->l_id = l_id;
	new_commit->com_num = update_op_i;
	com_fifo->size++;
	MOD_INCR(com_fifo->push_ptr, COMMIT_FIFO_SIZE);
	if (ENABLE_ASSERTIONS) {
		assert(com_fifo->size <= COMMIT_FIFO_SIZE );
	}
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
    if (!is_expected_g_id_ready(p_writes, committed_g_id, dbg_counter, LEADER, t_id))
      break;
    bookkeep_after_finding_expected_gid(p_writes, latency_info,
                                        LEADER, dbg_counter, t_id);



    MOD_INCR(p_writes->pull_ptr, LEADER_PENDING_WRITES);
    update_op_i++;
    committed_g_id++; // this is getting checked in every iteration, so it's needed
	}
	if (update_op_i > 0) {
		forge_commit_message(com_fifo, p_writes->local_w_id, update_op_i);
		p_writes->local_w_id += update_op_i; // advance the local_w_id

    if (ENABLE_ASSERTIONS) assert(p_writes->size >= update_op_i);
    p_writes->size -= update_op_i;
    zk_KVS_batch_op_updates((uint16_t) update_op_i, p_writes->ptrs_to_ops, starting_pull_ptr,
                            LEADER_PENDING_WRITES, false, t_id);
		atomic_store_explicit(&committed_global_w_id, committed_g_id, memory_order_relaxed);
    zk_take_latency_measurement_for_writes(p_writes, latency_info, t_id);
	}
}

// Wait until the entire write is there
static inline void wait_for_the_entire_write(volatile zk_w_mes_t *w_mes,
                                               uint16_t t_id, uint32_t index)
{
  uint32_t debug_cntr = 0;
  assert(w_mes->write[w_mes->write[0].w_num - 1].opcode == KVS_OP_PUT);
  while (w_mes->write[w_mes->write[0].w_num - 1].opcode != KVS_OP_PUT) {
    if (ENABLE_ASSERTIONS) {
      debug_cntr++;
      if (debug_cntr == B_4_) {
        my_printf(red, "Flr %d stuck waiting for a write to come index %u prep id %u\n",
                   t_id, index, w_mes->write[0].w_num - 1);
        print_ldr_stats(t_id);
        debug_cntr = 0;
      }
    }
  }
}

// Poll for incoming write requests from followers
static inline void poll_for_writes(volatile zk_w_mes_ud_t *incoming_ws, uint32_t *pull_ptr,
																	 p_writes_t *p_writes,
																	 struct ibv_cq *w_recv_cq, struct ibv_wc *w_recv_wc,
                                   struct recv_info *w_recv_info,
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
    post_recvs_with_recv_info(w_recv_info, polled_messages);
  }
}



/* ---------------------------------------------------------------------------
//------------------------------ BROADCASTS -----------------------------
//---------------------------------------------------------------------------*/

// Poll for credits and increment the credits according to the protocol
static inline void ldr_poll_credits(struct ibv_cq* credit_recv_cq, struct ibv_wc* credit_wc,
                                    uint16_t credits[][FOLLOWER_MACHINE_NUM])
{
  int credits_found = 0;
  credits_found = ibv_poll_cq(credit_recv_cq, LDR_MAX_CREDIT_RECV, credit_wc);
  if(credits_found > 0) {
    if(unlikely(credit_wc[credits_found - 1].status != 0)) {
      fprintf(stderr, "Bad wc status when polling for credits to send a broadcast %d\n", credit_wc[credits_found -1].status);
      assert(false);
    }
    for (uint32_t j = 0; j < credits_found; j++) {
      credits[COMMIT_W_QP_ID][credit_wc[j].imm_data]+= FLR_CREDITS_IN_MESSAGE;
    }

  }
  else if(unlikely(credits_found < 0)) {
    printf("ERROR In the credit CQ\n"); exit(0);
  }
}

//Checks if there are enough credits to perform a broadcast
static inline bool check_bcast_credits(uint16_t credits[][FOLLOWER_MACHINE_NUM], hrd_ctrl_blk_t* cb,
                                       struct ibv_wc* credit_wc,
                                       uint32_t* credit_debug_cnt, uint8_t vc)
{
  bool poll_for_credits = false;
  uint16_t j;
  for (j = 0; j < FOLLOWER_MACHINE_NUM; j++) {
    if (credits[vc][j] == 0) {
      poll_for_credits = true;
      break;
    }
  }
  if (ENABLE_ASSERTIONS && vc == PREP_VC ) {
    if (poll_for_credits) credit_debug_cnt[vc]++;
    else credit_debug_cnt[vc] = 0;
  }
  // There are no explicit credit messages for the prepare messages
  if (vc == PREP_VC) return !poll_for_credits;

  if (poll_for_credits)
    ldr_poll_credits(cb->dgram_recv_cq[FC_QP_ID], credit_wc, credits);
  // We polled for credits, if we did not find enough just break
  for (j = 0; j < FOLLOWER_MACHINE_NUM; j++) {
    if (credits[vc][j] == 0) {
      if (ENABLE_ASSERTIONS) credit_debug_cnt[vc]++;
      return false;
    }
  }
  credit_debug_cnt[vc] = 0;
  return true;
}


// Form Broadcast work requests for the leader
static inline void forge_commit_wrs(zk_com_mes_t *com_mes, uint16_t t_id,
                                   uint16_t br_i, struct hrd_ctrl_blk *cb, struct ibv_sge *com_send_sgl,
                                   struct ibv_send_wr *com_send_wr,  long *commit_br_tx,
                                    uint16_t credits[][FOLLOWER_MACHINE_NUM])
{
  struct ibv_wc signal_send_wc;
  com_send_sgl[br_i].addr = (uint64_t) (uintptr_t) com_mes;
  com_send_sgl[br_i].length = LDR_COM_SEND_SIZE;
	if (ENABLE_ASSERTIONS) {
    assert(com_send_sgl[br_i].length <= LDR_COM_SEND_SIZE);
    if (!USE_QUORUM)
      assert(com_mes->com_num <= LEADER_PENDING_WRITES);
  }
//  my_printf(green, "Leader %d : I BROADCAST a message with %d commits with %d credits: %d \n",
//               t_id, com_mes->com_num, com_mes->opcode, credits[COMM_VC][0]);

  // Do a Signaled Send every BROADCAST_SS_BATCH broadcasts (BROADCAST_SS_BATCH * (MACHINE_NUM - 1) messages)
  if ((*commit_br_tx) % COM_BCAST_SS_BATCH == 0) com_send_wr[0].send_flags |= IBV_SEND_SIGNALED;
  (*commit_br_tx)++;
  if((*commit_br_tx) % COM_BCAST_SS_BATCH == COM_BCAST_SS_BATCH - 1) {
    hrd_poll_cq(cb->dgram_send_cq[COMMIT_W_QP_ID], 1, &signal_send_wc);
  }
  // Have the last message of each broadcast pointing to the first message of the next bcast
  if (br_i > 0)
    com_send_wr[(br_i * MESSAGES_IN_BCAST) - 1].next = &com_send_wr[br_i * MESSAGES_IN_BCAST];
}

// Broadcast logic uses this function to post appropriate number of credit recvs before sending broadcasts
static inline void post_recvs_and_batch_bcasts_to_NIC(uint16_t br_i, struct hrd_ctrl_blk *cb,
                                                      struct ibv_send_wr *send_wr,
                                                      struct ibv_recv_wr *credit_recv_wr,
                                                      uint16_t *credit_recv_counter, uint8_t qp_id)
{
  uint16_t j;
  int ret;
  struct ibv_send_wr *bad_send_wr;
  struct ibv_recv_wr *bad_recv_wr;
  uint32_t max_credit_recvs = LDR_MAX_CREDIT_RECV;

  if (*credit_recv_counter > 0) { // Must post receives for credits
    if (ENABLE_ASSERTIONS == 1) assert ((*credit_recv_counter) * FOLLOWER_MACHINE_NUM <= max_credit_recvs);
    for (j = 0; j < FOLLOWER_MACHINE_NUM * (*credit_recv_counter); j++) {
      credit_recv_wr[j].next = (j == (FOLLOWER_MACHINE_NUM * (*credit_recv_counter)) - 1) ?
                               NULL : &credit_recv_wr[j + 1];
    }
//    my_printf(cyan, "Leader posting a credit receive\n");
    ret = ibv_post_recv(cb->dgram_qp[FC_QP_ID], &credit_recv_wr[0], &bad_recv_wr);
    CPE(ret, "ibv_post_recv error: posting recvs for credits before broadcasting", ret);
    *credit_recv_counter = 0;
  }

  // Batch the broadcasts to the NIC
  if (br_i > 0) {
    send_wr[(br_i * MESSAGES_IN_BCAST) - 1].next = NULL;

    ret = ibv_post_send(cb->dgram_qp[qp_id], &send_wr[0], &bad_send_wr);
    CPE(ret, "Broadcast ibv_post_send error", ret);
  }
}




// Leader broadcasts commits
static inline void broadcast_commits(uint16_t credits[][FOLLOWER_MACHINE_NUM], struct hrd_ctrl_blk *cb,
                                     zk_com_fifo_t *com_fifo, long *commit_br_tx,
																		 uint32_t *credit_debug_cnt, struct ibv_wc *credit_wc,
                                     struct ibv_sge *com_send_sgl, struct ibv_send_wr *com_send_wr,
                                     struct ibv_recv_wr *credit_recv_wr,
																		 struct recv_info *w_recv_info, uint16_t t_id)
{

  if (com_fifo->size == 0) return;
  //printf("Ldr %d bcasting commits \n", t_id);
  uint8_t vc = COMM_VC;
  uint16_t  br_i = 0, credit_recv_counter = 0;
  while (com_fifo->size > 0) {
    // Check if there are enough credits for a Broadcast
    if (!check_bcast_credits(credits, cb, credit_wc, credit_debug_cnt, vc)) {
      if (ENABLE_STAT_COUNTING) t_stats[t_id].stalled_com_credit++;
      break;
    }
		zk_com_mes_t *com_mes = &com_fifo->commits[com_fifo->pull_ptr];
    // Create the broadcast messages
    forge_commit_wrs(com_mes, t_id, br_i, cb, com_send_sgl,
                     com_send_wr,  commit_br_tx, credits);
    (*commit_br_tx)++;
    for (uint16_t j = 0; j < FOLLOWER_MACHINE_NUM; j++) { credits[COMM_VC][j]--; }
		//commits_sent += com_mes->com_num;
		com_fifo->size--;
		// Don't zero the com_num because there may be no INLINING
		MOD_INCR(com_fifo->pull_ptr, COMMIT_FIFO_SIZE);
    br_i++;
		if (ENABLE_ASSERTIONS) {
			assert(br_i <= COMMIT_CREDITS);
      assert(com_fifo != NULL);
      if (com_fifo->size > COMMIT_FIFO_SIZE)
        printf("com fifo size %u/%d \n", com_fifo->size, COMMIT_FIFO_SIZE);
			assert(com_fifo->size <= COMMIT_FIFO_SIZE);
			assert(com_mes->com_num > 0 && com_mes->com_num <= MAX_LIDS_IN_A_COMMIT);
		}
    if (ENABLE_STAT_COUNTING) {
      t_stats[t_id].coms_sent += com_mes->com_num;
      t_stats[t_id].coms_sent_mes_num++;
    }
    if ((*commit_br_tx) % FLR_CREDITS_IN_MESSAGE == 0) credit_recv_counter++;
    if (br_i == MAX_BCAST_BATCH) {
      uint32_t recvs_to_post_num = LDR_MAX_RECV_W_WRS - w_recv_info->posted_recvs;
      post_recvs_with_recv_info(w_recv_info, recvs_to_post_num);
      w_recv_info->posted_recvs += recvs_to_post_num;
      //printf("Broadcasting %u commits \n", br_i);
      post_recvs_and_batch_bcasts_to_NIC(br_i, cb, com_send_wr, credit_recv_wr, &credit_recv_counter, COMMIT_W_QP_ID);
			com_send_wr[0].send_flags = COM_ENABLE_INLINING == 1 ? IBV_SEND_INLINE : 0;
      br_i = 0;
    }
  }
	if (br_i > 0) {
    //printf("Broadcasting %u commits \n", br_i);
    uint32_t recvs_to_post_num = LDR_MAX_RECV_W_WRS - w_recv_info->posted_recvs;
    post_recvs_with_recv_info(w_recv_info, recvs_to_post_num);
    w_recv_info->posted_recvs += recvs_to_post_num;
    for (int com_i = 0; com_i < MESSAGES_IN_BCAST; ++com_i) {
      if (com_i < MESSAGES_IN_BCAST - 1)
        assert(com_send_wr[com_i].next == &com_send_wr[com_i + 1]);
      else assert(com_send_wr[com_i].next == NULL);
      assert(com_send_wr[com_i].num_sge == 1);
      assert(com_send_wr[com_i].sg_list == com_send_sgl);
      assert(com_send_wr[com_i].opcode == IBV_WR_SEND);
    }

		post_recvs_and_batch_bcasts_to_NIC(br_i, cb, com_send_wr, credit_recv_wr, &credit_recv_counter, COMMIT_W_QP_ID);
		com_send_wr[0].send_flags = COM_ENABLE_INLINING == 1 ? IBV_SEND_INLINE : 0;
	}
	if (ENABLE_ASSERTIONS) assert(w_recv_info->posted_recvs <= LDR_MAX_RECV_W_WRS);

}



// Form the Broadcast work request for the prepare
static inline void forge_prep_wr(uint16_t prep_i, p_writes_t *p_writes,
																 struct hrd_ctrl_blk *cb, struct ibv_sge *send_sgl,
																 struct ibv_send_wr *send_wr, long *prep_br_tx,
																 uint16_t br_i, uint16_t credits[][FOLLOWER_MACHINE_NUM],
																 uint8_t vc, uint16_t t_id) {
	uint16_t i;
	struct ibv_wc signal_send_wc;
	zk_prep_mes_t *prep = &p_writes->prep_fifo->prep_message[prep_i];
	uint32_t backward_ptr = p_writes->prep_fifo->backward_ptrs[prep_i];
	uint16_t coalesce_num = prep->coalesce_num;
  send_sgl[br_i].length = PREP_MES_HEADER + coalesce_num * sizeof(zk_prepare_t);
  send_sgl[br_i].addr = (uint64_t) (uintptr_t) prep;
	for (i = 0; i < coalesce_num; i++) {
		p_writes->w_state[(backward_ptr + i) % LEADER_PENDING_WRITES] = SENT;
    if (DEBUG_PREPARES)
		  printf("Prepare %d, val-len %u, message size %d\n", i, prep->prepare[i].val_len,
             send_sgl[br_i].length);
		if (ENABLE_ASSERTIONS) {
			assert(prep->prepare[i].val_len == VALUE_SIZE >> SHIFT_BITS);
			assert(prep->prepare[i].opcode == KVS_OP_PUT);
		}

	}

  if (DEBUG_PREPARES)
	  my_printf(green, "Leader %d : I BROADCAST a prepare message %d of %u prepares with size %u,  with  credits: %d, lid: %u  \n",
							   t_id, prep->opcode, coalesce_num, send_sgl[br_i].length, credits[vc][0], prep->l_id);
	// Do a Signaled Send every PREP_BCAST_SS_BATCH broadcasts (PREP_BCAST_SS_BATCH * (MACHINE_NUM - 1) messages)
	if ((*prep_br_tx) % PREP_BCAST_SS_BATCH == 0) send_wr[0].send_flags |= IBV_SEND_SIGNALED;
	(*prep_br_tx)++;
	if ((*prep_br_tx) % PREP_BCAST_SS_BATCH == PREP_BCAST_SS_BATCH - 1) {
//    printf("Leader %u POLLING for a send completion in prepares \n", t_id);
		poll_cq(cb->dgram_send_cq[PREP_ACK_QP_ID], 1, &signal_send_wc, "sneding prepares");
	}
	// Have the last message of each broadcast pointing to the first message of the next bcast
	if (br_i > 0)
		send_wr[(br_i * MESSAGES_IN_BCAST) - 1].next = &send_wr[br_i * MESSAGES_IN_BCAST];

}


// Leader Broadcasts its Prepares
static inline void broadcast_prepares(p_writes_t *p_writes,
																			uint16_t credits[][FOLLOWER_MACHINE_NUM], struct hrd_ctrl_blk *cb,
																			struct ibv_wc *credit_wc, uint32_t *credit_debug_cnt,
																			struct ibv_sge *prep_send_sgl, struct ibv_send_wr *prep_send_wr,
																			long *prep_br_tx, struct recv_info *ack_recv_info, struct fifo *remote_prep_buf,
                                      uint16_t t_id, uint32_t *outstanding_prepares)
{
//  printf("Ldr %d bcasting prepares \n", t_id);
	uint8_t vc = PREP_VC;
	uint16_t br_i = 0, j, credit_recv_counter = 0;
	uint32_t bcast_pull_ptr = p_writes->prep_fifo->bcast_pull_ptr;
//	uint32_t posted_recvs = ack_recv_info->posted_recvs;

	while (p_writes->prep_fifo->bcast_size > 0) {

		if (!check_bcast_credits(credits, cb, credit_wc, credit_debug_cnt, vc))
			break;
    if (DEBUG_PREPARES)
      printf("LDR %d has %u bcasts to send credits %d\n",t_id, p_writes->prep_fifo->bcast_size, credits[PREP_VC][0]);
		// Create the broadcast messages

		forge_prep_wr((uint16_t) bcast_pull_ptr, p_writes, cb,  prep_send_sgl, prep_send_wr, prep_br_tx, br_i, credits, vc, t_id);
		br_i++;
    uint8_t coalesce_num = p_writes->prep_fifo->prep_message[bcast_pull_ptr].coalesce_num;
    add_to_the_mirrored_buffer(remote_prep_buf, coalesce_num, FOLLOWER_MACHINE_NUM, FLR_PREP_BUF_SLOTS);
    for (uint16_t i = 0; i < FOLLOWER_MACHINE_NUM; i++) credits[PREP_VC][i]--;
    if (ENABLE_ASSERTIONS) {
      assert( p_writes->prep_fifo->bcast_size >= coalesce_num);
      (*outstanding_prepares) += coalesce_num;
    }
    if (ENABLE_STAT_COUNTING) {
      t_stats[t_id].preps_sent += coalesce_num;
      t_stats[t_id].preps_sent_mes_num++;
    }
    // This message has been sent do not add other prepares to it!
    if (coalesce_num < MAX_PREP_COALESCE) {
//      my_printf(yellow, "Broadcasting prep with coalesce num %u \n", coalesce_num);
      MOD_INCR(p_writes->prep_fifo->push_ptr, PREP_FIFO_SIZE);
      p_writes->prep_fifo->prep_message[p_writes->prep_fifo->push_ptr].coalesce_num = 0;
    }
    p_writes->prep_fifo->bcast_size -= coalesce_num;
//    preps_sent += coalesce_num;
    MOD_INCR(bcast_pull_ptr, PREP_FIFO_SIZE);
		if (br_i == MAX_BCAST_BATCH) {
      uint32_t recvs_to_post_num = LDR_MAX_RECV_ACK_WRS - ack_recv_info->posted_recvs;
      //printf("Ldr %d posting %d recvs\n",t_id,  recvs_to_post_num);
      if (recvs_to_post_num) post_recvs_with_recv_info(ack_recv_info, recvs_to_post_num);
      ack_recv_info->posted_recvs += recvs_to_post_num;
			post_recvs_and_batch_bcasts_to_NIC(br_i, cb, prep_send_wr, NULL, &credit_recv_counter, PREP_ACK_QP_ID);
			br_i = 0;
			prep_send_wr[0].send_flags = LEADER_PREPARE_ENABLE_INLINING == 1 ? IBV_SEND_INLINE : 0;
		}
	}
  if (br_i > 0) {
    uint32_t recvs_to_post_num = LDR_MAX_RECV_ACK_WRS - ack_recv_info->posted_recvs;
    //printf("Ldr %d posting %d recvs (2nd round)\n",t_id,  recvs_to_post_num);
    if (recvs_to_post_num) post_recvs_with_recv_info(ack_recv_info, recvs_to_post_num);
    ack_recv_info->posted_recvs += recvs_to_post_num;
    post_recvs_and_batch_bcasts_to_NIC(br_i, cb, prep_send_wr, NULL, &credit_recv_counter, PREP_ACK_QP_ID);
		prep_send_wr[0].send_flags = LEADER_PREPARE_ENABLE_INLINING == 1 ? IBV_SEND_INLINE : 0;
  }
  if (ENABLE_ASSERTIONS) assert(ack_recv_info->posted_recvs <= LDR_MAX_RECV_ACK_WRS);
	p_writes->prep_fifo->bcast_pull_ptr = bcast_pull_ptr;
}



/* ---------------------------------------------------------------------------
//------------------------------ FOLLOWER MAIN LOOP -----------------------------
//---------------------------------------------------------------------------*/

// Spin until you make sure the entire message is there
// (Experience shows that if this hits, the message has come and has been falsely deleted)
static inline bool wait_for_the_entire_prepare(volatile zk_prep_mes_t *prep_mes,
                                               uint16_t t_id, uint32_t index, p_writes_t *p_writes)
{
  uint8_t coalesce_num = prep_mes->coalesce_num;
  uint32_t debug_cntr = 0;
  while (prep_mes->coalesce_num == 0) {
    if (ENABLE_ASSERTIONS) {
      debug_cntr++;
      if (debug_cntr == B_4_) {
        my_printf(red, "Flr %d stuck waiting for a prep coalesce num to not be zero, index %u, coalesce %u\n",
                   t_id, index, prep_mes->coalesce_num);
        print_flr_stats(t_id);
        debug_cntr = 0;
        return false;
      }
    }
  }
  while (prep_mes->prepare[prep_mes->coalesce_num - 1].opcode != KVS_OP_PUT) {
    if (ENABLE_ASSERTIONS) {
      debug_cntr++;
      if (debug_cntr == B_4_) {
        my_printf(red, "Flr %d stuck waiting for a prepare to come index %u prep id %u l_id %u\n",
                   t_id, index, coalesce_num - 1, prep_mes->l_id);
        for (uint16_t i = 0; i < coalesce_num; i++) {
          uint32_t session_id;
          session_id = prep_mes->prepare[i].sess_id;
          my_printf(green, "Prepare %u, flr_id %u, session id %u opcode %u, val_len %u,  \n",
          i, prep_mes->prepare[i].flr_id, session_id,
                       prep_mes->prepare[i].opcode, prep_mes->prepare[i].val_len);
        }
        for (uint16_t i = 0; i < p_writes->size; i++) {
          uint16_t ptr = (uint16_t) ((p_writes->pull_ptr + i) % FLR_PENDING_WRITES);
          if (p_writes->ptrs_to_ops[ptr] == prep_mes->prepare) {
            my_printf(red, "write %ptr already points to that op \n");
          }
        }
        print_flr_stats(t_id);
        //exit(0);
        debug_cntr = 0;
        return false;
      }
    }
  }
  return true;
}


// Poll for prepare messages
static inline void flr_poll_for_prepares(volatile zk_prep_mes_ud_t *incoming_preps, uint32_t *pull_ptr,
                                         p_writes_t *p_writes, struct pending_acks *p_acks,
                                         struct ibv_cq *prep_recv_cq, struct ibv_wc *prep_recv_wc,
                                         struct recv_info *prep_recv_info, struct fifo *prep_buf_mirror,
                                         uint32_t *completed_but_not_polled_preps,
                                         uint16_t t_id, uint8_t flr_id, uint32_t *dbg_counter)
{
	uint16_t polled_messages = 0;
	if (prep_buf_mirror->size == MAX_PREP_BUF_SLOTS_TO_BE_POLLED) {
    //printf("this is stopping me \n");
    return;
  }
	uint32_t index = *pull_ptr;
  int completed_messages =
    find_how_many_messages_can_be_polled(prep_recv_cq, prep_recv_wc,
                                         completed_but_not_polled_preps,
                                         FLR_PREP_BUF_SLOTS, t_id);
  if (completed_messages <= 0) return;
  while (polled_messages < completed_messages) {
	//while((incoming_preps[index].prepare.opcode == KVS_OP_PUT) &&
   // (prep_buf_mirror->size < MAX_PREP_BUF_SLOTS_TO_BE_POLLED)) {
    if (prep_buf_mirror->size == MAX_PREP_BUF_SLOTS_TO_BE_POLLED) break;
    // wait for the entire message
    //if (!wait_for_the_entire_prepare(&incoming_preps[index].prepare, t_id, index, p_writes)) break;
    zk_prep_mes_t *prep_mes = (zk_prep_mes_t *) &incoming_preps[index].prepare;
    uint8_t coalesce_num = prep_mes->coalesce_num;
		zk_prepare_t *prepare = prep_mes->prepare;
		if (p_writes->size + coalesce_num > FLR_PENDING_WRITES) break;
		uint32_t incoming_l_id = prep_mes->l_id;
		uint64_t expected_l_id = p_writes->local_w_id + p_writes->size;
    if (DEBUG_PREPARES)
      my_printf(green, "Flr %d sees a prep message with %d prepares at index %u l_id %u, expected lid %lu \n",
                t_id, coalesce_num, index, incoming_l_id, expected_l_id);
		if (FLR_DISALLOW_OUT_OF_ORDER_PREPARES) {
      if (expected_l_id != (uint64_t) incoming_l_id) {
       my_printf(red, "flr %u expected l_id  %lu and received %u \n",
                  t_id, expected_l_id, incoming_l_id);
        uint32_t dbg = M_256 + 2 ;
        flr_check_debug_cntrs(&dbg, &dbg, &dbg, &dbg, incoming_preps, index, p_writes, t_id);
        //print_flr_stats(t_id);
        assert(false);
      }
    }
		if (ENABLE_ASSERTIONS) assert(expected_l_id <= (uint64_t) incoming_l_id);
    if (ENABLE_STAT_COUNTING) {
      t_stats[t_id].received_preps += coalesce_num;
      t_stats[t_id].received_preps_mes_num++;
    }

		uint32_t extra_slots = 0;
		// OUT-OF-ORDER message
		if (expected_l_id < (uint64_t) incoming_l_id) {
			extra_slots = (uint64_t) incoming_l_id - expected_l_id;
			if (p_writes->size + extra_slots + coalesce_num > FLR_PENDING_WRITES) break;
		}
		else p_acks->acks_to_send+= coalesce_num; // lids are in order so ack them

		if (ENABLE_ASSERTIONS) assert(coalesce_num > 0 && coalesce_num <= MAX_PREP_COALESCE);
    add_to_the_mirrored_buffer(prep_buf_mirror, coalesce_num, 1, FLR_PREP_BUF_SLOTS);
		for (uint8_t prep_i = 0; prep_i < coalesce_num; prep_i++) {
			uint32_t push_ptr = (p_writes->push_ptr + extra_slots) % FLR_PENDING_WRITES;
			p_writes->ptrs_to_ops[push_ptr] = &prepare[prep_i];
			p_writes->g_id[push_ptr] = (uint64_t) prepare[prep_i].g_id;
			p_writes->flr_id[push_ptr] = prepare[prep_i].flr_id;
			assert(p_writes->flr_id[push_ptr] <=  FOLLOWER_MACHINE_NUM);
//			my_printf(green, "Flr %u, prep_i %u new write at ptr %u with g_id %lu and flr id %u, value_len %u \n",
//									 t_id, prep_i, push_ptr, p_writes->g_id[push_ptr], p_writes->flr_id[push_ptr],
//									 prepare[prep_i].val_len);
			if (ENABLE_ASSERTIONS) {
				assert(prepare[prep_i].val_len == VALUE_SIZE >> SHIFT_BITS);
				assert(p_writes->w_state[push_ptr] == INVALID);
			}
			// if the req originates from this follower
			if (prepare[prep_i].flr_id == flr_id)  {
				p_writes->is_local[push_ptr] = true;
        p_writes->session_id[push_ptr] = prepare[prep_i].sess_id;
//        printf("A prepare polled for local session %u/%u, push_ptr %u\n", p_writes->session_id[push_ptr], sess, push_ptr);
			}
			else p_writes->is_local[push_ptr] = false;
			p_writes->w_state[push_ptr] = VALID;
			if (extra_slots == 0) { // FIFO style insert
				MOD_INCR(p_writes->push_ptr, FLR_PENDING_WRITES);
				p_writes->size++;
			}
			else extra_slots++; // forward insert
		}
		// Because of out-of-order messages it may be that the next expected message has already been seen and stored
		while ((p_writes->w_state[p_writes->push_ptr] == VALID) &&
           (p_writes->size < FLR_PENDING_WRITES)) {
			if (FLR_DISALLOW_OUT_OF_ORDER_PREPARES) assert(false);
			MOD_INCR(p_writes->push_ptr, FLR_PENDING_WRITES);
			p_writes->size++;
			p_acks->acks_to_send++;
		}
		incoming_preps[index].prepare.opcode = 0;
    incoming_preps[index].prepare.coalesce_num = 0;
		MOD_INCR(index, FLR_PREP_BUF_SLOTS);
		polled_messages++;
	}
  (*completed_but_not_polled_preps) = completed_messages - polled_messages;
  *pull_ptr = index;
	if (polled_messages > 0) {
    if (ENABLE_ASSERTIONS) (*dbg_counter) = 0;
  }
  else if (ENABLE_ASSERTIONS && p_acks->acks_to_send == 0 && p_writes->size == 0) (*dbg_counter)++;
  if (ENABLE_STAT_COUNTING && p_acks->acks_to_send == 0 && p_writes->size == 0) t_stats[t_id].stalled_ack_prep++;
	if (ENABLE_ASSERTIONS) assert(prep_recv_info->posted_recvs >= polled_messages);
	prep_recv_info->posted_recvs -= polled_messages;
  if (ENABLE_ASSERTIONS) assert(prep_recv_info->posted_recvs <= FLR_MAX_RECV_PREP_WRS);

}


// Send a batched ack that denotes the first local write id and the number of subsequent lid that are being acked
static inline void send_acks_to_ldr(p_writes_t *p_writes, struct ibv_send_wr *ack_send_wr,
																		struct ibv_sge *ack_send_sgl, long *sent_ack_tx,
																		struct hrd_ctrl_blk *cb, struct recv_info *prep_recv_info,
																		uint8_t flr_id, zk_ack_mes_t *ack,
																		struct pending_acks *p_acks, uint16_t t_id)
{
  if (p_acks->acks_to_send == 0) return;
	struct ibv_wc signal_send_wc;
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
  if (ENABLE_ASSERTIONS) {
    assert(ack->l_id == l_id_to_send);
    assert (p_acks->slots_ahead <= p_writes->size);
  }
	p_acks->slots_ahead += p_acks->acks_to_send;
	p_acks->acks_to_send = 0;
	ack_send_sgl->addr = (uint64_t) (uintptr_t) ack;
  if (ENABLE_STAT_COUNTING) {
    t_stats[t_id].acks_sent += ack->ack_num;
    t_stats[t_id].acks_sent_mes_num++;
  }
  if (DEBUG_ACKS)
    my_printf(yellow, "Flr %d is sending an ack for lid %lu and ack num %d and flr id %d, p_writes size %u/%d \n",
                  t_id, l_id_to_send, ack->ack_num, ack->follower_id, p_writes->size, FLR_PENDING_WRITES);
  if (ENABLE_ASSERTIONS) assert(ack->ack_num > 0 && ack->ack_num <= FLR_PENDING_WRITES);
	if ((*sent_ack_tx) % ACK_SEND_SS_BATCH == 0) {
		ack_send_wr->send_flags |= IBV_SEND_SIGNALED;
		// if (local_client_id == 0) my_printf(green, "Sending ack %llu signaled \n", *sent_ack_tx);
	}
	else ack_send_wr->send_flags = IBV_SEND_INLINE;
	if((*sent_ack_tx) % ACK_SEND_SS_BATCH == ACK_SEND_SS_BATCH - 1) {
		// if (local_client_id == 0) my_printf(green, "Polling for ack  %llu \n", *sent_ack_tx);
		poll_cq(cb->dgram_send_cq[PREP_ACK_QP_ID], 1, &signal_send_wc, "sending acks");
	}

	(*sent_ack_tx)++; // Selective signaling

	// RECEIVES for prepares
	uint32_t posted_recvs = prep_recv_info->posted_recvs;
	uint32_t recvs_to_post_num = FLR_MAX_RECV_PREP_WRS - posted_recvs;
	if (recvs_to_post_num > 0) {
    post_recvs_with_recv_info(prep_recv_info, recvs_to_post_num);
    prep_recv_info->posted_recvs += recvs_to_post_num;
//    printf("FLR %d posting %u recvs and has a total of %u recvs for prepares \n",
//           t_id, recvs_to_post_num,  prep_recv_info->posted_recvs);
    if (ENABLE_ASSERTIONS) {
      assert(recvs_to_post_num <= FLR_MAX_RECV_PREP_WRS);
      assert(prep_recv_info->posted_recvs <= FLR_MAX_RECV_PREP_WRS);
    }
  }
	// SEND the ack
	int ret = ibv_post_send(cb->dgram_qp[PREP_ACK_QP_ID], &ack_send_wr[0], &bad_send_wr);
	CPE(ret, "ACK ibv_post_send error", ret);
}


//Send credits for the commits
static inline void send_credits_for_commits(struct recv_info *com_recv_info, struct hrd_ctrl_blk *cb,
                                            struct ibv_send_wr *credit_wr, long *credit_tx,
                                            uint16_t credit_num, uint16_t t_id)
{
  struct ibv_send_wr *bad_send_wr;
  struct ibv_wc signal_send_wc;
  // RECEIVES FOR COMMITS
  uint32_t recvs_to_post_num = (uint32_t) (credit_num * FLR_CREDITS_IN_MESSAGE);
  if (ENABLE_ASSERTIONS) assert(recvs_to_post_num < FLR_MAX_RECV_COM_WRS);
  post_recvs_with_recv_info(com_recv_info, recvs_to_post_num);
    com_recv_info->posted_recvs += recvs_to_post_num;
//		printf("FLR %d posting %u recvs and has a total of %u recvs for commits \n",
//					 t_id, recvs_to_post_num,  com_recv_info->posted_recvs);

  for (uint16_t credit_wr_i = 0; credit_wr_i < credit_num; credit_wr_i++) {
    if (((*credit_tx) % COM_CREDIT_SS_BATCH) == 0) {
     credit_wr->send_flags |= IBV_SEND_SIGNALED;
    } else credit_wr->send_flags = IBV_SEND_INLINE;
    if (((*credit_tx) % COM_CREDIT_SS_BATCH) == COM_CREDIT_SS_BATCH - 1) {
     poll_cq(cb->dgram_send_cq[FC_QP_ID], 1, &signal_send_wc, "sending credits");
    }
  }
  (*credit_tx)++;
  credit_wr[credit_num - 1].next = NULL;
//   my_printf(yellow, "I am sending %d credit message(s)\n", credit_num);
  int ret = ibv_post_send(cb->dgram_qp[FC_QP_ID], &credit_wr[0], &bad_send_wr);
  CPE(ret, "ibv_post_send error in credits", ret);
}



// Form the Write work request for the write
static inline void forge_w_wr(p_writes_t *p_writes,
                              struct hrd_ctrl_blk *cb, struct ibv_sge *w_send_sgl,
                              struct ibv_send_wr *w_send_wr, long *w_tx,
                              uint16_t w_i, uint16_t *credits,
                              uint16_t t_id) {
  uint16_t i;
  struct ibv_wc signal_send_wc;
  zk_w_mes_t *w_mes_fifo = (zk_w_mes_t *) p_writes->w_fifo->fifo;
  uint32_t w_ptr = p_writes->w_fifo->pull_ptr;
  zk_w_mes_t *w_mes = &w_mes_fifo[w_ptr];
  uint16_t coalesce_num = w_mes->write[0].w_num;
  if (ENABLE_ASSERTIONS) assert(coalesce_num > 0);
  w_send_sgl[w_i].length = coalesce_num * sizeof(zk_write_t);
  w_send_sgl[w_i].addr = (uint64_t) (uintptr_t) w_mes;

  for (i = 0; i < coalesce_num; i++) {
    if (DEBUG_WRITES)
      printf("Write %d, session id %u, val-len %u, message size %d\n", i,
             w_mes->write[i].sess_id,
             w_mes->write[i].val_len,
             w_send_sgl[w_i].length);
    if (ENABLE_ASSERTIONS) {
      assert(w_mes->write[i].val_len == VALUE_SIZE >> SHIFT_BITS);
      assert(w_mes->write[i].opcode == KVS_OP_PUT);
    }
  }
  if (FLR_W_ENABLE_INLINING) w_send_wr[w_i].send_flags = IBV_SEND_INLINE;
  else w_send_wr[w_i].send_flags = 0;
  if (DEBUG_WRITES)
    my_printf(green, "Follower %d : I sent a write message %d of %u writes with size %u,  with  credits: %d \n",
                 t_id, w_mes->write->opcode, coalesce_num, w_send_sgl[w_i].length, *credits);
  if ((*w_tx) % WRITE_SS_BATCH == 0) w_send_wr[w_i].send_flags |= IBV_SEND_SIGNALED;
  (*w_tx)++;
  (*credits)--;
  if ((*w_tx) % WRITE_SS_BATCH == WRITE_SS_BATCH - 1) {
    // printf("Leader %u POLLING for a send completion in writes \n", t_id);
    poll_cq(cb->dgram_send_cq[COMMIT_W_QP_ID], 1, &signal_send_wc, "sending writes");
  }
  // Have the last message point to the current message
  if (w_i > 0)
    w_send_wr[w_i - 1].next = &w_send_wr[w_i];

}


// Send the local writes to the ldr
static inline void send_writes_to_the_ldr(p_writes_t *p_writes,
                                          uint16_t *credits, struct hrd_ctrl_blk *cb,
                                          struct ibv_sge *w_send_sgl, struct ibv_send_wr *w_send_wr,
                                          long *w_tx, struct fifo *remote_w_buf,
                                          uint16_t t_id, uint32_t *outstanding_writes)
{
//  printf("Ldr %d bcasting prepares \n", t_id);
  struct ibv_send_wr *bad_send_wr;
  uint16_t w_i = 0;

  while (p_writes->w_fifo->size > 0 && (*credits) > 0) {
    if (DEBUG_WRITES)
      printf("FLR %d has %u writes to send credits %d\n", t_id, p_writes->w_fifo->size, *credits);
    // Create the messages
    forge_w_wr(p_writes, cb, w_send_sgl, w_send_wr, w_tx, w_i, credits, t_id);
    zk_w_mes_t *w_mes_fifo = (zk_w_mes_t *) p_writes->w_fifo->fifo;
    uint32_t w_ptr = p_writes->w_fifo->pull_ptr;
    zk_w_mes_t *w_mes = &w_mes_fifo[w_ptr];
    uint16_t coalesce_num = w_mes->write[0].w_num;

    if (ENABLE_ASSERTIONS) {
      assert(p_writes->w_fifo->size >= coalesce_num);
      (*outstanding_writes) += coalesce_num;
    }
    if (ENABLE_STAT_COUNTING) {
      t_stats[t_id].writes_sent += coalesce_num;
      t_stats[t_id].writes_sent_mes_num++;
    }
    // This message has been sent do not add other writes to it!
    if (coalesce_num < MAX_W_COALESCE) {
      MOD_INCR(p_writes->w_fifo->push_ptr, W_FIFO_SIZE);
      w_mes_fifo[p_writes->w_fifo->push_ptr].write[0].w_num = 0;
//      my_printf(yellow, "Zeroing when sending at pointer %u \n", p_writes->prep_fifo->push_ptr);

    }
    add_to_the_mirrored_buffer(remote_w_buf, coalesce_num, 1, LEADER_W_BUF_SLOTS);
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
                                 struct recv_info *com_recv_info, struct hrd_ctrl_blk *cb,
                                 struct ibv_send_wr *credit_wr, long *credit_tx,
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

    assert((l_id - pull_lid) < FLR_PENDING_WRITES);
    assert(l_id + com_num - pull_lid < FLR_PENDING_WRITES);
    // This must always hold: l_id >= pull_lid,
    // because we need the commit to advance the pull_lid
    uint16_t com_ptr = (uint16_t)
      ((p_writes->pull_ptr + (l_id - pull_lid)) % FLR_PENDING_WRITES);
    /// loop through each commit
    for (uint16_t com_i = 0; com_i < com_num; com_i++) {
      if (zk_write_not_yet_acked(com, com_ptr, com_i, com_num, p_writes, t_id))
        goto END_WHILE;

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
  (*completed_but_not_polled_coms) = completed_messages - polled_messages;
  END_WHILE: *pull_ptr = buf_ptr;
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
    if (!is_expected_g_id_ready(p_writes, committed_g_id, dbg_counter, FOLLOWER, t_id))
      break;

    bookkeep_after_finding_expected_gid(p_writes, latency_info,
                                        FOLLOWER, dbg_counter,
                                        t_id);
    MOD_INCR(p_writes->pull_ptr, FLR_PENDING_WRITES);
    update_op_i++;
    committed_g_id++; // this is getting checked in every iteration, so it's needed
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
    zk_take_latency_measurement_for_writes(p_writes, latency_info, t_id);
	}
}




#endif /* INLINE_UTILS_H */

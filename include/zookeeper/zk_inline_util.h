#ifndef INLINE_UTILS_H
#define INLINE_UTILS_H

#include "kvs.h"
#include "zk_main.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>

/* ---------------------------------------------------------------------------
------------------------------UTILITY --------------------------------------
---------------------------------------------------------------------------*/

// swap 2 pointerss
static inline void swap_pointers(void** ptr_1, void** ptr_2)
{
	void* tmp = *ptr_1;
	*ptr_1 = *ptr_2;
	*ptr_2 = tmp;
}

// Swap 3 pointers in a cirular fashion
static inline void circulate_pointers(void** ptr_1, void** ptr_2, void** ptr_3)
{
	void* tmp = *ptr_1;
	*ptr_1 = *ptr_2;
	*ptr_2 = *ptr_3;
	*ptr_3 = tmp;
}

// Check whether 2 key hashes are equal
static inline bool keys_are_equal(mica_key_t* key1, mica_key_t* key2) {
	return (key1->bkt    == key2->bkt &&
			key1->server == key2->server &&
			key1->tag    == key2->tag);
}


// A condition to be used to trigger periodic (but rare) measurements
static inline bool trigger_measurement(uint16_t local_client_id)
{
	return t_stats[local_client_id].cache_hits_per_thread % K_32 > 0 &&
		   t_stats[local_client_id].cache_hits_per_thread % K_32 <= CACHE_BATCH_SIZE &&
		   local_client_id == 0 && machine_id == MACHINE_NUM -1;
}

//Add latency to histogram (in microseconds)
static inline void bookkeep_latency(int useconds, req_type rt){
  uint32_t** latency_counter;
  switch (rt){
    case HOT_READ_REQ:
      latency_counter = &latency_count.hot_reads;
      if (useconds > latency_count.max_read_lat) latency_count.max_read_lat = (uint32_t) useconds;
      break;
    case HOT_WRITE_REQ:
      latency_counter = &latency_count.hot_writes;
      if (useconds > latency_count.max_write_lat) latency_count.max_write_lat = (uint32_t) useconds;
      break;
    default: assert(0);
  }
  latency_count.total_measurements++;

  if (useconds > MAX_LATENCY)
    (*latency_counter)[LATENCY_BUCKETS]++;
  else
    (*latency_counter)[useconds / (MAX_LATENCY / LATENCY_BUCKETS)]++;
}

// Poll for the local reqs completion to measure a local req's latency
static inline void poll_local_req_for_latency_measurement(struct latency_flags* latency_info, struct timespec* start,
														  struct local_latency* local_measure)
{

	if ((MEASURE_LATENCY == 1) && ((latency_info->measured_req_flag) == LOCAL_REQ) && ((local_measure->local_latency_start_polling) == 1))
		if (*(local_measure->flag_to_poll) == 0) {
			struct timespec end;
			clock_gettime(CLOCK_MONOTONIC, &end);
			int useconds = ((end.tv_sec - start->tv_sec) * 1000000) +
						   ((end.tv_nsec - start->tv_nsec) / 1000);
			if (ENABLE_ASSERTIONS) assert(useconds > 0);
      //			yellow_printf("Latency of a local req, region %d, flag ptr %llu: %d us\n",
      //										local_measure->measured_local_region, local_measure->flag_to_poll, useconds);
			bookkeep_latency(useconds, LOCAL_REQ);
			latency_info->measured_req_flag = NO_REQ;
			local_measure->measured_local_region = -1;
			local_measure->local_latency_start_polling = 0;
			local_measure->flag_to_poll = NULL;

		}
}

static inline void report_remote_latency(struct latency_flags* latency_info, uint16_t prev_rem_req_i,
										 struct ibv_wc* wc, struct timespec* start)
{
	uint16_t i;
	for (i = 0; i < prev_rem_req_i; i++) {
		//	 printf("Looking for the req\n" );
		if (wc[i].imm_data == REMOTE_LATENCY_MARK) {
			struct timespec end;
			clock_gettime(CLOCK_MONOTONIC, &end);
			int useconds = ((end.tv_sec - start->tv_sec) * 1000000) +
						   ((end.tv_nsec - start->tv_nsec) / 1000);  //(end.tv_nsec - start->tv_nsec) / 1000;
			if (ENABLE_ASSERTIONS) assert(useconds > 0);
			//		printf("Latency of a Remote read %u us\n", useconds);
			bookkeep_latency(useconds, REMOTE_REQ);
			(latency_info->measured_req_flag) = NO_REQ;
			break;
		}
	}
}


// The follower sends a write to the leader and tags it with a session id. When the leaders sends a prepare,
// it includes that session id and a flr id
// the follower inspects the flr id, such that it can unblock the session id, if the write originated locally
// we hijack that connection for the latency, remembering the session that gets stuck on a write
static inline void change_latency_tag(struct latency_flags *latency_info, struct pending_writes *p_writes,
                                      uint16_t t_id)
{
  if (latency_info->measured_req_flag == HOT_WRITE_REQ_BEFORE_CACHE &&
      machine_id == LATENCY_MACHINE && t_id == LATENCY_THREAD &&
      latency_info->last_measured_sess_id == p_writes->session_id[p_writes->pull_ptr])
    latency_info-> measured_req_flag = HOT_WRITE_REQ;
}

// Both writes and reads of zookeeper use this to report their latency
static inline void report_latency(struct latency_flags* latency_info)
{
  struct timespec end;
  clock_gettime(CLOCK_MONOTONIC, &end);
  int useconds = ((end.tv_sec - latency_info->start.tv_sec) * 1000000) +
                 ((end.tv_nsec - latency_info->start.tv_nsec) / 1000);  //(end.tv_nsec - start->tv_nsec) / 1000;
  //if (ENABLE_ASSERTIONS) assert(useconds > 0);
  		//printf("Latency of a reqof type %d is  %u us\n", latency_info->measured_req_flag, useconds);
  bookkeep_latency(useconds, latency_info->measured_req_flag);
  (latency_info->measured_req_flag) = NO_REQ;
}


// Necessary bookkeeping to initiate the latency measurement
static inline void start_measurement(struct latency_flags* latency_info, uint32_t sess_id, uint16_t t_id,
                                     uint8_t opcode, bool is_ldr) {
  uint8_t compare_op = MEASURE_READ_LATENCY ? CACHE_OP_GET : CACHE_OP_PUT;
  if ((latency_info->measured_req_flag) == NO_REQ) {
    if (t_stats[t_id].cache_hits_per_thread > M_1 && t_id == LATENCY_THREAD && machine_id == LATENCY_MACHINE
        && sess_id == 1) {
      //printf("tag a key for latency measurement \n");
      if (opcode == CACHE_OP_GET) latency_info->measured_req_flag = HOT_READ_REQ;
      else if (opcode == CACHE_OP_PUT) {
        latency_info->measured_req_flag = is_ldr ? HOT_WRITE_REQ : HOT_WRITE_REQ_BEFORE_CACHE;
        latency_info->last_measured_sess_id = sess_id;

      }
      else if (ENABLE_ASSERTIONS) assert(false);
      //green_printf("Measuring a req %llu, opcode %d, flag %d op_i %d \n",
			//					 t_stats[t_id].cache_hits_per_thread, opcode, latency_info->measured_req_flag, latency_info->last_measured_sess_id);

      clock_gettime(CLOCK_MONOTONIC, &latency_info->start);
      if (ENABLE_ASSERTIONS) assert(latency_info->measured_req_flag != NO_REQ);
    }
  }
}



/* ---------------------------------------------------------------------------
//------------------------------ ZOOKEEPER GENERIC -----------------------------
//---------------------------------------------------------------------------*/


// Post Receives for acknowledgements
static inline void post_recvs_with_recv_info(struct recv_info *recv, uint32_t recv_num)
{
  if (recv_num == 0) return;
  uint16_t j;
  struct ibv_recv_wr *bad_recv_wr;
  for (j = 0; j < recv_num; j++) {
    recv->recv_sgl[j].addr = (uintptr_t) recv->buf + (recv->push_ptr * recv->slot_size);
//    printf("Posting a receive at push ptr %u at address %lu \n", recv->push_ptr, recv->recv_sgl[j].addr);
    MOD_ADD(recv->push_ptr, recv->buf_slots);
    recv->recv_wr[j].next = (j == recv_num - 1) ?
                            NULL : &recv->recv_wr[j + 1];
  }
  int ret = ibv_post_recv(recv->recv_qp, &recv->recv_wr[0], &bad_recv_wr);
  CPE(ret, "ibv_post_recv error", ret);
}

// Generic function to mirror buffer spaces--used when elements are added
static inline void add_to_the_mirrored_buffer(struct fifo *mirror_buf, uint8_t coalesce_num, uint16_t number_of_fifos,
                                              uint32_t max_size)
{
  for (uint16_t i = 0; i < number_of_fifos; i++) {
    uint32_t push_ptr = mirror_buf[i].push_ptr;
    uint16_t *fifo = (uint16_t *) mirror_buf[i].fifo;
    fifo[push_ptr] = (uint16_t)coalesce_num;
    MOD_ADD(mirror_buf[i].push_ptr, max_size);
    mirror_buf[i].size++;
    if (ENABLE_ASSERTIONS) assert(mirror_buf[i].size <= max_size);
  }
}

// Generic function to mirror buffer spaces--used when elements are removed
static inline uint16_t remove_from_the_mirrored_buffer(struct fifo *mirror_buf_, uint16_t remove_num,
                                                       uint16_t t_id, uint8_t fifo_id, uint32_t max_size)
{
  struct fifo *mirror_buf = &mirror_buf_[fifo_id];
  uint16_t *fifo = (uint16_t *)mirror_buf->fifo;
  uint16_t new_credits = 0;
  if (ENABLE_ASSERTIONS && mirror_buf->size == 0) {
    red_printf("remove_num %u, ,mirror_buf->pull_ptr %u fifo_id %u  \n",
               remove_num, mirror_buf->pull_ptr, fifo_id);
    assert(false);
  }
  while (remove_num > 0) {
    uint32_t pull_ptr = mirror_buf->pull_ptr;
    if (fifo[pull_ptr] <= remove_num) {
      remove_num -= fifo[pull_ptr];
      MOD_ADD(mirror_buf->pull_ptr, max_size);
      if (ENABLE_ASSERTIONS && mirror_buf->size == 0) {
        red_printf("remove_num %u, ,mirror_buf->pull_ptr %u fifo_id %u  \n",
                   remove_num, mirror_buf->pull_ptr, fifo_id);
        assert(false);
      }
      mirror_buf->size--;
      new_credits++;
    }
    else {
      fifo[pull_ptr] -= remove_num;
      remove_num = 0;
    }
  }
  return new_credits;
}

/* ---------------------------------------------------------------------------
//------------------------------ ZOOKEEPER DEBUGGING -----------------------------
//---------------------------------------------------------------------------*/


static inline void print_ldr_stats (uint16_t t_id)
{

  yellow_printf("Prepares sent %ld/%ld \n", t_stats[t_id].preps_sent_mes_num, t_stats[t_id].preps_sent );
  yellow_printf("Acks Received %ld/%ld \n", t_stats[t_id].received_acks_mes_num, t_stats[t_id].received_acks );
  yellow_printf("Commits sent %ld/%ld \n", t_stats[t_id].coms_sent_mes_num, t_stats[t_id].coms_sent );
}

static inline void print_flr_stats (uint16_t t_id)
{

  yellow_printf("Prepares received %ld/%ld \n", t_stats[t_id].received_preps_mes_num, t_stats[t_id].received_preps );
  yellow_printf("Acks sent %ld/%ld \n", t_stats[t_id].acks_sent_mes_num, t_stats[t_id].acks_sent );
  yellow_printf("Commits received %ld/%ld \n", t_stats[t_id].received_coms_mes_num, t_stats[t_id].received_coms );
}

// Leader checks its debug counters
static inline void ldr_check_debug_cntrs(uint32_t *credit_debug_cnt, uint32_t *wait_for_acks_dbg_counter,
                                         uint32_t *wait_for_gid_dbg_counter,struct pending_writes *p_writes,
                                         uint16_t t_id)
{
  if (unlikely((*wait_for_gid_dbg_counter) > M_16)) {
    red_printf("Leader %d waits for the g_id, committed g_id %lu \n", t_id, committed_global_w_id);
    print_ldr_stats(t_id);
    (*wait_for_gid_dbg_counter) = 0;
  }
  if (unlikely((*wait_for_acks_dbg_counter) > M_16)) {
    red_printf("Leader %d waits for acks, committed g_id %lu \n", t_id, committed_global_w_id);
    cyan_printf("Sent lid %u and state %d\n", p_writes->local_w_id, p_writes->w_state[p_writes->pull_ptr]);
    print_ldr_stats(t_id);
    (*wait_for_acks_dbg_counter) = 0;
    exit(0);
  }
  if (unlikely(credit_debug_cnt[PREP_VC] > M_16)) {
    red_printf("Leader %d lacks prep credits, committed g_id %lu \n", t_id, committed_global_w_id);
    print_ldr_stats(t_id);
    credit_debug_cnt[PREP_VC] = 0;
  }
  if (unlikely(credit_debug_cnt[COMM_VC] > M_16)) {
    red_printf("Leader %d lacks comm credits, committed g_id %lu \n", t_id, committed_global_w_id);
    print_ldr_stats(t_id);
    credit_debug_cnt[COMM_VC] = 0;
  }
}

// Follower checks its debug counters
static inline void flr_check_debug_cntrs(uint32_t *credit_debug_cnt, uint32_t *wait_for_coms_dbg_counter,
                                         uint32_t *wait_for_preps_dbg_counter,
                                         uint32_t *wait_for_gid_dbg_counter, volatile struct prep_message_ud_req *prep_buf,
                                         uint32_t pull_ptr, struct pending_writes *p_writes, uint16_t t_id)
{

  if (unlikely((*wait_for_preps_dbg_counter) > M_256)) {
    red_printf("Follower %d waits for preps, committed g_id %lu \n", t_id, committed_global_w_id);
    struct prepare *prep = (struct prepare *)&prep_buf[pull_ptr].prepare.prepare;
    uint32_t l_id = *(uint32_t *)prep_buf[pull_ptr].prepare.l_id;
    uint32_t g_id = *(uint32_t *)prep->g_id;
    uint8_t message_opc = prep_buf[pull_ptr].prepare.opcode;
    cyan_printf("Flr %d, polling on index %u,polled opc %u, 1st write opcode: %u, l_id %u, first g_id %u, expected l_id %u\n",
                t_id, pull_ptr, message_opc, prep->opcode, l_id, g_id, p_writes->local_w_id);
    MOD_ADD(pull_ptr, FLR_PREP_BUF_SLOTS);
    prep = (struct prepare *)&prep_buf[pull_ptr].prepare.prepare;
    l_id = *(uint32_t *)prep_buf[pull_ptr].prepare.l_id;
    g_id = *(uint32_t *)prep->g_id;
    message_opc = prep_buf[pull_ptr].prepare.opcode;
    cyan_printf("Next index %u,polled opc %u, 1st write opcode: %u, l_id %u, first g_id %u, expected l_id %u\n",
                pull_ptr, message_opc, prep->opcode, l_id, g_id, p_writes->local_w_id);
    for (int i = 0; i < FLR_PREP_BUF_SLOTS; ++i) {
      if (prep_buf[i].prepare.opcode == CACHE_OP_PUT) {
        green_printf("GOOD OPCODE in index %d, l_id %u \n", i, *(uint32_t *)prep_buf[i].prepare.l_id);
      }
      else red_printf("BAD OPCODE in index %d, l_id %u \n", i, *(uint32_t *)prep_buf[i].prepare.l_id);

    }

    print_flr_stats(t_id);
    (*wait_for_preps_dbg_counter) = 0;
//    exit(0);
  }
  if (unlikely((*wait_for_gid_dbg_counter) > M_128)) {
    red_printf("Follower %d waits for the g_id, committed g_id %lu \n", t_id, committed_global_w_id);
    print_flr_stats(t_id);
    (*wait_for_gid_dbg_counter) = 0;
  }
  if (unlikely((*wait_for_coms_dbg_counter) > M_128)) {
    red_printf("Follower %d waits for coms, committed g_id %lu \n", t_id, committed_global_w_id);
    print_flr_stats(t_id);
    (*wait_for_coms_dbg_counter) = 0;
  }
  if (unlikely((*credit_debug_cnt) > M_128)) {
    red_printf("Follower %d lacks write credits, committed g_id %lu \n", t_id, committed_global_w_id);
    print_flr_stats(t_id);
    (*credit_debug_cnt) = 0;
  }
}

// Check the states of pending writes
static inline void check_ldr_p_states(struct pending_writes *p_writes, uint16_t t_id)
{
  assert(p_writes->size <= LEADER_PENDING_WRITES);
  for (uint16_t w_i = 0; w_i < LEADER_PENDING_WRITES - p_writes->size; w_i++) {
    uint16_t ptr = (p_writes->push_ptr + w_i) % LEADER_PENDING_WRITES;
    if (p_writes->w_state[ptr] != INVALID) {
      red_printf("LDR %d push ptr %u, pull ptr %u, size %u, state %d at ptr %u \n",
                 t_id, p_writes->push_ptr, p_writes->pull_ptr, p_writes->size, p_writes->w_state[ptr], ptr);
      print_ldr_stats(t_id);
      exit(0);
    }
  }
}


/* ---------------------------------------------------------------------------
//------------------------------ LEADER SPECIFIC -----------------------------
//---------------------------------------------------------------------------*/

// Insert a new local or remote write to the leader pending writes
 static inline void ldr_insert_write(struct pending_writes *p_writes, struct prepare *write, uint32_t session_id,
                                     bool local, uint8_t flr_id, uint16_t t_id)
{
  struct prep_message *preps = p_writes->prep_fifo->prep_message;
  uint32_t prep_ptr = p_writes->prep_fifo->push_ptr;
  uint32_t inside_prep_ptr = preps[prep_ptr].coalesce_num;
  uint16_t w_ptr = p_writes->push_ptr;

  memcpy(preps[prep_ptr].prepare[inside_prep_ptr].key, (write->key), KEY_SIZE + 2 + VALUE_SIZE);
  p_writes->ptrs_to_ops[w_ptr] = &preps[prep_ptr].prepare[inside_prep_ptr];
  preps[prep_ptr].prepare[inside_prep_ptr].flr_id = flr_id; //FOLLOWER_MACHINE_NUM means it's a leader message
  if (!local) memcpy(preps[prep_ptr].prepare[inside_prep_ptr].session_id, &session_id, 3);
//  else memset(preps[prep_ptr].prepare[inside_prep_ptr].session_id, 0, 3);
  if (inside_prep_ptr == 0) {
    p_writes->prep_fifo->backward_ptrs[prep_ptr] = w_ptr;
    uint32_t message_l_id = (uint32_t) (p_writes->local_w_id + p_writes->size);
    if (ENABLE_ASSERTIONS) {
      if (message_l_id > MAX_PREP_COALESCE) {
        uint32_t prev_prep_ptr = (prep_ptr + PREP_FIFO_SIZE - 1) % PREP_FIFO_SIZE;
        uint32_t prev_l_id = *(uint32_t *) preps[prev_prep_ptr].l_id;
        uint8_t prev_coalesce = preps[prev_prep_ptr].coalesce_num;
        if (message_l_id != prev_l_id + prev_coalesce) {
          red_printf("Current message l_id %u, previous message l_id %u , previous coalesce %u\n",
                     message_l_id, prev_l_id, prev_coalesce);
        }
      }
    }
    memcpy(preps[prep_ptr].l_id, &message_l_id, 4);
  }

  if (ENABLE_ASSERTIONS) {
    if (p_writes->w_state[w_ptr] != INVALID)
      red_printf("Leader %u w_state %d at w_ptr %u, g_id %lu, cache hits %lu, size %u \n",
                 t_id, p_writes->w_state[w_ptr], w_ptr, p_writes->g_id[w_ptr],
                 t_stats[t_id].cache_hits_per_thread, p_writes->size);
//					printf("Sent %d, Valid %d, Ready %d \n", SENT, VALID, READY);
    assert(p_writes->w_state[w_ptr] == INVALID);

    assert(keys_are_equal((mica_key_t *) &preps[prep_ptr].prepare[inside_prep_ptr],
                          (mica_key_t *)write));
  }
  p_writes->w_state[w_ptr] = VALID;
  if (local) p_writes->session_has_pending_write[session_id] = true;
  p_writes->is_local[w_ptr] = local;
  p_writes->session_id[w_ptr] = (uint32_t) session_id;
  MOD_ADD(p_writes->push_ptr, LEADER_PENDING_WRITES);
  p_writes->size++;
  p_writes->prep_fifo->bcast_size++;

  preps[prep_ptr].coalesce_num++;
  if (preps[prep_ptr].coalesce_num == MAX_PREP_COALESCE) {
//				p_writes->prep_fifo->size++;
//				if (ENABLE_ASSERTIONS) assert(p_writes->prep_fifo->size <= PREP_FIFO_SIZE);
    MOD_ADD(p_writes->prep_fifo->push_ptr, PREP_FIFO_SIZE);
    preps[p_writes->prep_fifo->push_ptr].coalesce_num = 0;

  }
}

// Follower inserts a new local write to the write fifo it maintains (for the writes that it propagates to the leader)
static inline void flr_insert_write(struct pending_writes *p_writes, struct write *write, uint32_t session_id,
                                    uint8_t flr_id, uint16_t t_id)
{
  // printf("Thread %d found a write for session %d\n", t_id, working_session);
  // SET UP the prepare message helpers
  struct w_message *w_mes = p_writes->w_fifo->fifo;
  uint32_t w_ptr = p_writes->w_fifo->push_ptr;
  uint32_t inside_prep_ptr = w_mes[w_ptr].write[0].w_num;


  memcpy(w_mes[w_ptr].write[inside_prep_ptr].key, (write->key), KEY_SIZE + 2 + VALUE_SIZE);
  w_mes[w_ptr].write[inside_prep_ptr].flr_id = flr_id;
  memcpy(w_mes[w_ptr].write[inside_prep_ptr].session_id, &session_id, sizeof(uint32_t));
  //    printf("Passed session id %u to the write in message %u, with inside ptr %u\n",
  //           *(uint32_t*)w_mes[w_ptr].write[inside_prep_ptr].session_id, w_ptr, inside_prep_ptr);
  if (ENABLE_ASSERTIONS) {
    assert(keys_are_equal((mica_key_t *) &w_mes[w_ptr].write[inside_prep_ptr], (mica_key_t *)write));
  }
  p_writes->session_has_pending_write[session_id] = true;
  p_writes->w_fifo->size++;
  w_mes[w_ptr].write[0].w_num++;

  if (w_mes[w_ptr].write[0].w_num == MAX_W_COALESCE) {
    MOD_ADD(p_writes->w_fifo->push_ptr, W_FIFO_SIZE);
    w_mes[p_writes->w_fifo->push_ptr].write[0].w_num = 0;
   //yellow_printf("Zeroing when in cache at pointer %u \n", p_writes->w_fifo->push_ptr);
  }
}

// Both Leader and Followers use this to read the trace, propagate reqs to the cache and maintain their prepare/write fifos
static inline uint32_t batch_from_trace_to_cache(uint32_t trace_iter, uint32_t t_id, struct trace_command *trace,
                                                 struct cache_op *ops, uint8_t flr_id,
                                                 struct pending_writes *p_writes, struct mica_resp *resp,
                                                 struct latency_flags *latency_info,
                                                 int protocol)
{
  int i = 0, op_i = 0;
  uint8_t is_update = 0;
  int working_session = -1;
  if (p_writes->all_sessions_stalled) return trace_iter;
  for (i = 0; i < SESSIONS_PER_THREAD; i++) {
    if (!p_writes->session_has_pending_write[i]) {
      working_session = i;
      break;
    }
  }
//  printf("working session = %d\n", working_session);
  if (ENABLE_ASSERTIONS) assert(working_session != -1);

  //  green_printf("op_i %d , trace_iter %d, trace[trace_iter].opcode %d \n", op_i, trace_iter, trace[trace_iter].opcode);
  while (op_i < CACHE_BATCH_SIZE && working_session < SESSIONS_PER_THREAD) {
    if (ENABLE_ASSERTIONS) assert(trace[trace_iter].opcode != NOP);
    is_update = (trace[trace_iter].opcode == WRITE_OP) ? (uint8_t) 1 : (uint8_t) 0;
    *(uint128 *) &ops[op_i] = trace[trace_iter].key_hash;
    ops[op_i].opcode = is_update ? (uint8_t) CACHE_OP_PUT : (uint8_t) CACHE_OP_GET;
    ops[op_i].val_len = is_update ? (uint8_t) (VALUE_SIZE >> SHIFT_BITS) : (uint8_t) 0;
    if (MEASURE_LATENCY) start_measurement(latency_info, working_session, t_id, ops[op_i].opcode, protocol == LEADER);
    if (is_update) {
      if (protocol == LEADER) ldr_insert_write(p_writes, (struct prepare *)&ops[op_i], (uint32_t)working_session, true, FOLLOWER_MACHINE_NUM, t_id);
      else if (protocol == FOLLOWER) flr_insert_write(p_writes, (struct write *)&ops[op_i], (uint32_t)working_session, flr_id, t_id);
      else if (ENABLE_ASSERTIONS) assert(false);
      while (p_writes->session_has_pending_write[working_session]) {
        working_session++;
        if (working_session == SESSIONS_PER_THREAD) {
          p_writes->all_sessions_stalled = true;
          break;
        }
      }
      // printf("thread %d t_id next working session %d\n total ops %d\n", t_id, working_session, op_i);
    }

    if (ENABLE_ASSERTIONS == 1) {
      assert(WRITE_RATIO > 0 || is_update == 0);
      if (is_update) assert(ops[op_i].val_len > 0);
    }
    resp[op_i].type = EMPTY;
    trace_iter++;
    if (trace[trace_iter].opcode == NOP) trace_iter = 0;
    op_i++;
  }
  t_stats[t_id].cache_hits_per_thread += op_i;
  cache_batch_op_trace(op_i, t_id, &ops, resp);
  if (MEASURE_LATENCY && machine_id == LATENCY_MACHINE && t_id == LATENCY_THREAD &&
      latency_info->measured_req_flag == HOT_READ_REQ)
    report_latency(latency_info);
  return trace_iter;
}

// Leader calls this to handout global ids to pending writes
static inline void get_wids(struct pending_writes *p_writes, uint16_t t_id)
{
	uint16_t unordered_writes_num = (LEADER_PENDING_WRITES + p_writes->push_ptr - p_writes->unordered_ptr)
																	% LEADER_PENDING_WRITES;
  if (unordered_writes_num == 0) return;
	uint64_t id = atomic_fetch_add_explicit(&global_w_id, (uint64_t)unordered_writes_num, memory_order_relaxed);
  if (ENABLE_STAT_COUNTING) {
    t_stats[t_id].batches_per_thread++;
    t_stats[t_id].total_writes += unordered_writes_num;
  }
	int i;
	for (i = 0; i < unordered_writes_num; ++i) {
    uint16_t unordered_ptr =  (LEADER_PENDING_WRITES + p_writes->push_ptr - unordered_writes_num + i)
                              % LEADER_PENDING_WRITES;
		p_writes->g_id[unordered_ptr] = id + i;
		struct prepare *prep = (struct prepare*) p_writes->ptrs_to_ops[unordered_ptr];
		memcpy(prep->g_id, &p_writes->g_id[unordered_ptr], 4);
	}
	if (id > B_4) {
    if (MEASURE_LATENCY && machine_id == LATENCY_MACHINE) print_latency_stats();
    assert(false);
  }
//  if (unordered_writes_num > 0)
//    printf("Thread %d got id %lu to id %lu for its write for %u writes \n",
//           t_id,  id, id + unordered_writes_num - 1,  unordered_writes_num);
	if (ENABLE_ASSERTIONS)
		assert((p_writes->unordered_ptr + unordered_writes_num) % LEADER_PENDING_WRITES == p_writes->push_ptr);
	p_writes->unordered_ptr = p_writes->push_ptr;
}

/* ---------------------------------------------------------------------------
//------------------------------ LEADER MAIN LOOP -----------------------------
//---------------------------------------------------------------------------*/


// Spin until you know the entire message is there
static inline void wait_for_the_entire_ack(volatile struct ack_message *ack,
                                           uint16_t t_id, uint32_t index)
{
  uint32_t debug_cntr = 0;
  while (ack->ack_num  == 0) {
    if (ENABLE_ASSERTIONS) {
      debug_cntr++;
      if (debug_cntr > M_128) {
        red_printf("Ldr %d stuck waiting for an ack to come index %u ack_num %u\n",
                   t_id, index, ack->ack_num);
        print_ldr_stats(t_id);
        exit(0); debug_cntr = 0;
      }
    }
  }
}

// Leader polls for acks
static inline void poll_for_acks(struct ack_message_ud_req *incoming_acks, uint32_t *pull_ptr,
																 struct pending_writes *p_writes,
																 uint16_t credits[][FOLLOWER_MACHINE_NUM],
																 struct ibv_cq * ack_recv_cq, struct ibv_wc *ack_recv_wc,
																 struct recv_info *ack_recv_info, struct fifo *remote_prep_buf,
                                 uint16_t t_id, uint32_t *dbg_counter,
                                 uint32_t *outstanding_prepares)
{
	uint32_t index = *pull_ptr;
	uint32_t polled_messages = 0;
  int completed_messages =  ibv_poll_cq(ack_recv_cq, LEADER_ACK_BUF_SLOTS, ack_recv_wc);
  if (completed_messages <= 0) return;
  while (polled_messages < completed_messages) {
	//while (incoming_acks[index].ack.opcode == CACHE_OP_ACK) {
		volatile struct ack_message *ack = &incoming_acks[index].ack;
    //wait_for_the_entire_ack(ack, t_id, index);
		uint16_t ack_num = ack->ack_num;
    if (ENABLE_ASSERTIONS) {
      assert (ack->opcode == CACHE_OP_ACK);
      assert(ack_num > 0 && ack_num <= FLR_PENDING_WRITES);
      assert(ack->follower_id < FOLLOWER_MACHINE_NUM);
    }
		MOD_ADD(index, LEADER_ACK_BUF_SLOTS);
		polled_messages++;

		uint64_t l_id = *(uint64_t *) (ack->local_id);
		uint64_t pull_lid = p_writes->local_w_id; // l_id at the pull pointer
		uint16_t ack_ptr; // a pointer in the FIFO, from where ack should be added
    if (DEBUG_ACKS)
      yellow_printf("Leader %d ack opcode %d with %d acks for l_id %lu, oldest lid %lu, at offset %d from flr %u \n",
                    t_id, ack->opcode, ack_num, l_id, pull_lid, index, ack->follower_id);
    if (ENABLE_STAT_COUNTING) {
      t_stats[t_id].received_acks += ack_num;
      t_stats[t_id].received_acks_mes_num++;
    }
    credits[PREP_VC][ack->follower_id] +=
      remove_from_the_mirrored_buffer(remote_prep_buf, ack->ack_num, t_id, ack->follower_id, FLR_PREP_BUF_SLOTS);
    // if the pending write FIFO is empty it means the acks are for committed messages.
    if (p_writes->size == 0 ) {
      if (!USE_QUORUM) assert(false);
      memset((void*)ack, 0, FLR_ACK_SEND_SIZE); continue;
    }

		if (ENABLE_ASSERTIONS) {
      assert(l_id + ack_num <= pull_lid + p_writes->size);
      if ((l_id + ack_num < pull_lid) && (!USE_QUORUM)) {
        red_printf("l_id %u, ack_num %u, pull_lid %u \n", l_id, ack_num, pull_lid);
        exit(0);
      }
    }
		if (pull_lid >= l_id) {
      if ((pull_lid - l_id) >= ack_num) {memset((void*)ack, 0, FLR_ACK_SEND_SIZE); continue;}
			ack_num -= (pull_lid - l_id);
      if (ENABLE_ASSERTIONS) assert(ack_num > 0 && ack_num <= FLR_PENDING_WRITES);
			ack_ptr = p_writes->pull_ptr;
		}
		else { // l_id > pull_lid
			ack_ptr = (p_writes->pull_ptr + (l_id - pull_lid)) % LEADER_PENDING_WRITES;
		}
    // Increase credits only for the acks that get applied
		for (uint16_t ack_i = 0; ack_i < ack_num; ack_i++) {
      if (ENABLE_ASSERTIONS && (ack_ptr == p_writes->push_ptr)) {
        uint16_t origin_ack_ptr = (ack_ptr - ack_i + LEADER_PENDING_WRITES) % LEADER_PENDING_WRITES;
        red_printf("Origin ack_ptr %u/%u, acks %u/%u, pull_ptr %u, push_ptr % u, size %u \n",
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
			MOD_ADD(ack_ptr, LEADER_PENDING_WRITES);
		}
		if (ENABLE_ASSERTIONS) assert(credits[PREP_VC][ack->follower_id] <= PREPARE_CREDITS);
		memset((void*)ack, 0, FLR_ACK_SEND_SIZE); // need to delete all the g_ids
	} // while

	*pull_ptr = index;
	// Poll for the completion of the receives
  if (polled_messages > 0) {
    //hrd_poll_cq(ack_recv_cq, polled_messages, ack_recv_wc);
    if (ENABLE_ASSERTIONS) (*dbg_counter) = 0;
  }
  else {
    if (ENABLE_ASSERTIONS && (*outstanding_prepares) > 0) (*dbg_counter)++;
    if (ENABLE_STAT_COUNTING && (*outstanding_prepares) > 0) t_stats[t_id].stalled_ack_prep++;
  }
	if (ENABLE_ASSERTIONS) assert(ack_recv_info->posted_recvs >= polled_messages);
	ack_recv_info->posted_recvs -= polled_messages;
}


// Add the acked gid to the appropriate commit message
static inline void forge_commit_message(struct commit_fifo *com_fifo, uint64_t l_id, uint16_t update_op_i)
{
	//l_id refers the oldest write to commit (werites go from l_id to l_id + update_op_i)
	uint16_t com_mes_i = com_fifo->push_ptr;
	uint16_t last_com_mes_i;
	struct com_message *commit_messages = com_fifo->commits;
	if (unlikely(update_op_i ))

	// see if the new batch can fit with the previous one -- no reason why it should not
	if (com_fifo->size > 0) {
		last_com_mes_i = (COMMIT_FIFO_SIZE + com_mes_i - 1) % COMMIT_FIFO_SIZE;
		struct com_message *last_commit = &commit_messages[last_com_mes_i];
		uint64_t last_l_id = *(uint64_t *) &last_commit->l_id;
		last_l_id += last_commit->com_num;
		if (last_l_id == l_id) {
			if (last_commit->com_num + update_op_i <= MAX_LIDS_IN_A_COMMIT) {
				last_commit->com_num += update_op_i;
				return;
			}
		}
	}
	//otherwise push a new commit
	struct com_message *new_commit = &commit_messages[com_mes_i];
	memcpy(new_commit->l_id, &l_id, sizeof(uint64_t));
	new_commit->com_num = update_op_i;
	com_fifo->size++;
	MOD_ADD(com_fifo->push_ptr, COMMIT_FIFO_SIZE);
	if (ENABLE_ASSERTIONS) {
		assert(com_fifo->size <= COMMIT_FIFO_SIZE );
	}
}

// Leader propagates Updates that have seen all acks to the KVS
static inline void propagate_updates(struct pending_writes *p_writes, struct commit_fifo *com_fifo,
																		 struct mica_resp *resp, struct latency_flags *latency_info,
                                     uint16_t t_id, uint32_t *dbg_counter)
{
//  printf("Ldr %d propagating updates \n", t_id);
	uint16_t update_op_i = 0;
	uint32_t pull_ptr = p_writes->pull_ptr;

	// Read the latest committed g_id
	uint64_t committed_g_id = atomic_load_explicit(&committed_global_w_id, memory_order_relaxed);
	while(p_writes->w_state[p_writes->pull_ptr] == READY) {
		if (com_fifo->size == COMMIT_FIFO_SIZE) break; // the commit prep_message is full: that may be too restricting, but it does not affect performance or correctness
		if (p_writes->g_id[p_writes->pull_ptr] != committed_g_id + 1 && (!DISABLE_GID_ORDERING)) {
			if (ENABLE_ASSERTIONS) (*dbg_counter)++;
      if (ENABLE_STAT_COUNTING) t_stats[t_id].stalled_gid++;
			break;
		}
//    printf("Leader found updates to propagate\n");
		p_writes->w_state[p_writes->pull_ptr] = INVALID;
    p_writes->acks_seen[p_writes->pull_ptr] = 0;
		if (p_writes->is_local[p_writes->pull_ptr]) {
      if (DEBUG_WRITES) cyan_printf("Ldr %u freeing session %u \n", t_id,
                               p_writes->session_id[p_writes->pull_ptr]);
			p_writes->session_has_pending_write[p_writes->session_id[p_writes->pull_ptr]] = false;
			p_writes->all_sessions_stalled = false;
			p_writes->is_local[p_writes->pull_ptr] = false;
		}
		MOD_ADD(p_writes->pull_ptr, LEADER_PENDING_WRITES);
		update_op_i++;
		committed_g_id++;
	}
	if (update_op_i > 0) {
    if (ENABLE_ASSERTIONS) (*dbg_counter) = 0;
		forge_commit_message(com_fifo, p_writes->local_w_id, update_op_i);
		p_writes->local_w_id += update_op_i; // advance the local_w_id
    if (ENABLE_ASSERTIONS) assert(p_writes->size >= update_op_i);
    p_writes->size -= update_op_i;
    if (!DISABLE_UPDATING_KVS)
      cache_batch_op_updates((uint32_t) update_op_i, 0, p_writes->ptrs_to_ops, resp, pull_ptr, LEADER_PENDING_WRITES, false);
		atomic_store_explicit(&committed_global_w_id, committed_g_id, memory_order_relaxed);
    if (MEASURE_LATENCY && latency_info->measured_req_flag == HOT_WRITE_REQ &&
        machine_id == LATENCY_MACHINE && t_id == LATENCY_THREAD &&
        latency_info->last_measured_sess_id < p_writes->local_w_id)
      report_latency(latency_info);
//    if (t_id == 0)  yellow_printf("Committed global id %lu \n", committed_g_id);
	}
}

// Wait until the entire write is there
static inline void wait_for_the_entire_write(volatile struct w_message *w_mes,
                                               uint16_t t_id, uint32_t index)
{
  uint32_t debug_cntr = 0;
  while (w_mes->write[w_mes->write[0].w_num - 1].opcode != CACHE_OP_PUT) {
    if (ENABLE_ASSERTIONS) {
      debug_cntr++;
      if (debug_cntr == B_4_) {
        red_printf("Flr %d stuck waiting for a write to come index %u prep id %u\n",
                   t_id, index, w_mes->write[0].w_num - 1);
        print_ldr_stats(t_id);
        debug_cntr = 0;
      }
    }
  }
}

// Poll for incoming write requests from followers
static inline void poll_for_writes(volatile struct w_message_ud_req *incoming_ws, uint32_t *pull_ptr,
																	 struct pending_writes *p_writes,
																	 struct ibv_cq *w_recv_cq, struct ibv_wc *w_recv_wc,
                                   struct recv_info *w_recv_info, uint16_t t_id)
{
	if (p_writes->size == LEADER_PENDING_WRITES) return;
	if (ENABLE_ASSERTIONS) assert(p_writes->size < LEADER_PENDING_WRITES);
	uint32_t index = *pull_ptr;
	uint32_t polled_messages = 0;
	// Start polling
	while (incoming_ws[index].w_mes.write[0].w_num > 0) {
    wait_for_the_entire_write(&incoming_ws[index].w_mes, t_id, index);
		 if (DEBUG_WRITES) printf("Leader sees a write Opcode %d at offset %d  \n",
                              incoming_ws[index].w_mes.write[0].opcode, index);
		struct w_message *w_mes = (struct w_message*) &incoming_ws[index].w_mes;
		uint8_t w_num = w_mes->write[0].w_num;
    if (p_writes->size + w_num > LEADER_PENDING_WRITES) break;
		for (uint16_t i = 0; i < w_num; i++) {
      struct write *write = &w_mes->write[i];
      if (ENABLE_ASSERTIONS) if(write->opcode != CACHE_OP_PUT)
          red_printf("Opcode %u, i %u/%u \n",write->opcode, i, w_num);
      if (DEBUG_WRITES)
        printf("POll for writes passes session id %u \n", *(uint32_t *)write->session_id);
      ldr_insert_write(p_writes, (struct prepare *)write, *(uint32_t *)write->session_id,
                       false, write->flr_id, t_id);
      write->opcode = 0;
		}
    if (ENABLE_STAT_COUNTING) {
      t_stats[t_id].received_writes += w_num;
      t_stats[t_id].received_writes_mes_num++;
    }
    incoming_ws[index].w_mes.write[0].w_num = 0;
    MOD_ADD(index, LEADER_W_BUF_SLOTS);
    polled_messages++;
	}
  (*pull_ptr) = index;
	// Poll for the completion of the receives
	if (polled_messages > 0) {
    hrd_poll_cq(w_recv_cq, polled_messages, w_recv_wc);
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
    if(unlikely(credit_wc[credits_found -1].status != 0)) {
      fprintf(stderr, "Bad wc status when polling for credits to send a broadcast %d\n", credit_wc[credits_found -1].status);
      exit(0);
    }
    for (uint32_t j = 0; j < credits_found; j++) {
      credits[COMMIT_W_QP_ID][credit_wc[j].imm_data]+= FLR_CREDITS_IN_MESSAGE;
    }

  }
  else if(unlikely(credits_found < 0)) {
    printf("ERROR In the credit CQ\n"); exit(0);
  }
}

//Checks if there are enough credits to perform a broadcast -- protocol independent
static inline bool check_bcast_credits(uint16_t credits[][FOLLOWER_MACHINE_NUM], struct hrd_ctrl_blk* cb, struct ibv_wc* credit_wc,
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
static inline void forge_commit_wrs(struct com_message *com_mes, uint16_t t_id,
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
//  green_printf("Leader %d : I BROADCAST a message with %d commits with %d credits: %d \n",
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
//    cyan_printf("Leader posting a credit receive\n");
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
                                     struct commit_fifo *com_fifo, long *commit_br_tx,
																		 uint32_t *credit_debug_cnt, struct ibv_wc *credit_wc,
                                     struct ibv_sge *com_send_sgl, struct ibv_send_wr *com_send_wr,
                                     struct ibv_recv_wr *credit_recv_wr,
																		 struct recv_info *w_recv_info, uint16_t t_id)
{
//  printf("Ldr %d bcasting commits \n", t_id);
  uint8_t vc = COMM_VC;
  uint16_t  br_i = 0, credit_recv_counter = 0;
  while (com_fifo->size > 0) {
    // Check if there are enough credits for a Broadcast
    if (!check_bcast_credits(credits, cb, credit_wc, credit_debug_cnt, vc)) {
      if (ENABLE_STAT_COUNTING) t_stats[t_id].stalled_com_credit++;
      break;
    }
		struct com_message *com_mes = &com_fifo->commits[com_fifo->pull_ptr];
    // Create the broadcast messages
    forge_commit_wrs(com_mes, t_id, br_i, cb, com_send_sgl,
                     com_send_wr,  commit_br_tx, credits);
    (*commit_br_tx)++;
    for (uint16_t j = 0; j < FOLLOWER_MACHINE_NUM; j++) { credits[COMM_VC][j]--; }
		//commits_sent += com_mes->com_num;
		com_fifo->size--;
		// Don't zero the com_num because there may be no INLINING
		MOD_ADD(com_fifo->pull_ptr, COMMIT_FIFO_SIZE);
    br_i++;
		if (ENABLE_ASSERTIONS) {
			assert(br_i <= COMMIT_CREDITS);
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

      post_recvs_and_batch_bcasts_to_NIC(br_i, cb, com_send_wr, credit_recv_wr, &credit_recv_counter, COMMIT_W_QP_ID);
			com_send_wr[0].send_flags = COM_ENABLE_INLINING == 1 ? IBV_SEND_INLINE : 0;
      br_i = 0;
    }
  }
	if (br_i > 0) {
    uint32_t recvs_to_post_num = LDR_MAX_RECV_W_WRS - w_recv_info->posted_recvs;
    post_recvs_with_recv_info(w_recv_info, recvs_to_post_num);
    w_recv_info->posted_recvs += recvs_to_post_num;
		post_recvs_and_batch_bcasts_to_NIC(br_i, cb, com_send_wr, credit_recv_wr, &credit_recv_counter, COMMIT_W_QP_ID);
		com_send_wr[0].send_flags = COM_ENABLE_INLINING == 1 ? IBV_SEND_INLINE : 0;
	}
	if (ENABLE_ASSERTIONS) assert(w_recv_info->posted_recvs <= LDR_MAX_RECV_W_WRS);

}



// Form the Broadcast work request for the prepare
static inline void forge_prep_wr(uint16_t prep_i, struct pending_writes *p_writes,
																 struct hrd_ctrl_blk *cb, struct ibv_sge *send_sgl,
																 struct ibv_send_wr *send_wr, long *prep_br_tx,
																 uint16_t br_i, uint16_t credits[][FOLLOWER_MACHINE_NUM],
																 uint8_t vc, uint16_t t_id) {
	uint16_t i;
	struct ibv_wc signal_send_wc;
	struct prep_message *prep = &p_writes->prep_fifo->prep_message[prep_i];
	uint32_t backward_ptr = p_writes->prep_fifo->backward_ptrs[prep_i];
	uint16_t coalesce_num = prep->coalesce_num;
  send_sgl[br_i].length = PREP_MES_HEADER + coalesce_num * sizeof(struct prepare);
  send_sgl[br_i].addr = (uint64_t) (uintptr_t) prep;
	for (i = 0; i < coalesce_num; i++) {
		p_writes->w_state[(backward_ptr + i) % LEADER_PENDING_WRITES] = SENT;
    if (DEBUG_PREPARES)
		  printf("Prepare %d, val-len %u, message size %d\n", i, prep->prepare[i].val_len,
             send_sgl[br_i].length);
		if (ENABLE_ASSERTIONS) {
			assert(prep->prepare[i].val_len == VALUE_SIZE >> SHIFT_BITS);
			assert(prep->prepare[i].opcode == CACHE_OP_PUT);
		}

	}

  if (DEBUG_PREPARES)
	  green_printf("Leader %d : I BROADCAST a prepare message %d of %u prepares with size %u,  with  credits: %d, lid: %u  \n",
							   t_id, prep->opcode, coalesce_num, send_sgl[br_i].length, credits[vc][0],  *(uint32_t*)prep->l_id);
	// Do a Signaled Send every PREP_BCAST_SS_BATCH broadcasts (PREP_BCAST_SS_BATCH * (MACHINE_NUM - 1) messages)
	if ((*prep_br_tx) % PREP_BCAST_SS_BATCH == 0) send_wr[0].send_flags |= IBV_SEND_SIGNALED;
	(*prep_br_tx)++;
	if ((*prep_br_tx) % PREP_BCAST_SS_BATCH == PREP_BCAST_SS_BATCH - 1) {
//    printf("Leader %u POLLING for a send completion in prepares \n", t_id);
		hrd_poll_cq(cb->dgram_send_cq[PREP_ACK_QP_ID], 1, &signal_send_wc);
	}
	// Have the last message of each broadcast pointing to the first message of the next bcast
	if (br_i > 0)
		send_wr[(br_i * MESSAGES_IN_BCAST) - 1].next = &send_wr[br_i * MESSAGES_IN_BCAST];

}


// Leader Broadcasts its Prepares
static inline void broadcast_prepares(struct pending_writes *p_writes,
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

		forge_prep_wr(bcast_pull_ptr, p_writes, cb,  prep_send_sgl, prep_send_wr, prep_br_tx, br_i, credits, vc, t_id);
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
//      yellow_printf("Broadcasting prep with coalesce num %u \n", coalesce_num);
      MOD_ADD(p_writes->prep_fifo->push_ptr, PREP_FIFO_SIZE);
      p_writes->prep_fifo->prep_message[p_writes->prep_fifo->push_ptr].coalesce_num = 0;
    }
    p_writes->prep_fifo->bcast_size -= coalesce_num;
//    preps_sent += coalesce_num;
    MOD_ADD(bcast_pull_ptr, PREP_FIFO_SIZE);
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
static inline bool wait_for_the_entire_prepare(volatile struct prep_message *prep_mes,
                                               uint16_t t_id, uint32_t index, struct pending_writes *p_writes)
{
  uint8_t coalesce_num = prep_mes->coalesce_num;
  uint32_t debug_cntr = 0;
  while (prep_mes->coalesce_num == 0) {
    if (ENABLE_ASSERTIONS) {
      debug_cntr++;
      if (debug_cntr == B_4_) {
        red_printf("Flr %d stuck waiting for a prep coalesce num to not be zero, index %u, coalesce %u\n",
                   t_id, index, prep_mes->coalesce_num);
        print_flr_stats(t_id);
        debug_cntr = 0;
        return false;
      }
    }
  }
  while (prep_mes->prepare[prep_mes->coalesce_num - 1].opcode != CACHE_OP_PUT) {
    if (ENABLE_ASSERTIONS) {
      debug_cntr++;
      if (debug_cntr == B_4_) {
        red_printf("Flr %d stuck waiting for a prepare to come index %u prep id %u l_id %u\n",
                   t_id, index, coalesce_num - 1, *(uint32_t *)prep_mes->l_id);
        for (uint16_t i = 0; i < coalesce_num; i++) {
          uint32_t session_id;
          memcpy(&session_id, (uint8_t*)prep_mes->prepare[i].session_id, 3);
          green_printf("Prepare %u, flr_id %u, session id %u opcode %u, val_len %u,  \n",
          i, prep_mes->prepare[i].flr_id, session_id,
                       prep_mes->prepare[i].opcode, prep_mes->prepare[i].val_len);
        }
        for (uint16_t i = 0; i < p_writes->size; i++) {
          uint16_t ptr = (p_writes->pull_ptr + i) % FLR_PENDING_WRITES;
          if (p_writes->ptrs_to_ops[ptr] == prep_mes->prepare) {
            red_printf("write %ptr already points to that op \n");
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
static inline void poll_for_prepares(volatile struct prep_message_ud_req *incoming_preps, uint32_t *pull_ptr,
																		 struct pending_writes *p_writes, struct pending_acks *p_acks,
																		 struct ibv_cq *prep_recv_cq, struct ibv_wc *prep_recv_wc,
																		 struct recv_info *prep_recv_info, struct fifo *prep_buf_mirror,
                                     uint16_t t_id, uint8_t flr_id, uint32_t *dbg_counter)
{
	uint16_t polled_messages = 0;
	if (prep_buf_mirror->size == MAX_PREP_BUF_SLOTS_TO_BE_POLLED) return;
	uint32_t index = *pull_ptr;
	while((incoming_preps[index].prepare.opcode == CACHE_OP_PUT) &&
    (prep_buf_mirror->size < MAX_PREP_BUF_SLOTS_TO_BE_POLLED)) {
    // wait for the entire message
    if (!wait_for_the_entire_prepare(&incoming_preps[index].prepare, t_id, index, p_writes)) break;
    struct prep_message *prep_mes = (struct prep_message *) &incoming_preps[index].prepare;
    uint8_t coalesce_num = prep_mes->coalesce_num;
		struct prepare *prepare = prep_mes->prepare;
		if (p_writes->size + coalesce_num > FLR_PENDING_WRITES) break;
		uint32_t incoming_l_id = *(uint32_t *)prep_mes->l_id;
		uint64_t expected_l_id = p_writes->local_w_id + p_writes->size;
    if (DEBUG_PREPARES)
      printf("Flr %d sees a prep message with %d prepares at index %u l_id %u, expected lid %lu \n",
             t_id, coalesce_num, index, incoming_l_id, expected_l_id);
		if (FLR_DISALLOW_OUT_OF_ORDER_PREPARES) {
      if (expected_l_id != (uint64_t) incoming_l_id) {
       red_printf("flr %u expected l_id  %lu and received %u \n",
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
			memcpy(&p_writes->g_id[push_ptr], prepare[prep_i].g_id, sizeof(uint32_t));
			p_writes->flr_id[push_ptr] = prepare[prep_i].flr_id;
			assert(p_writes->flr_id[push_ptr] <=  FOLLOWER_MACHINE_NUM);
//			green_printf("Flr %u, prep_i %u new write at ptr %u with g_id %lu and flr id %u, value_len %u \n",
//									 t_id, prep_i, push_ptr, p_writes->g_id[push_ptr], p_writes->flr_id[push_ptr],
//									 prepare[prep_i].val_len);
			if (ENABLE_ASSERTIONS) {
				assert(prepare[prep_i].val_len == VALUE_SIZE >> SHIFT_BITS);
				assert(p_writes->w_state[push_ptr] == INVALID);
			}
			// if the req originates from this follower
			if (prepare[prep_i].flr_id == flr_id)  {
				p_writes->is_local[push_ptr] = true;
				memcpy(&p_writes->session_id[push_ptr], prepare[prep_i].session_id, 3 * sizeof(uint8_t));
//        printf("A prepare polled for local session %u/%u, push_ptr %u\n", p_writes->session_id[push_ptr], sess, push_ptr);
			}
			else p_writes->is_local[push_ptr] = false;
			p_writes->w_state[push_ptr] = VALID;
			if (extra_slots == 0) { // FIFO style insert
				MOD_ADD(p_writes->push_ptr, FLR_PENDING_WRITES);
				p_writes->size++;
			}
			else extra_slots++; // forward insert
		}
		// Because of out-of-order messages it may be that the next expected message has already been seen and stored
		while ((p_writes->w_state[p_writes->push_ptr] ==  VALID) &&  (p_writes->size < FLR_PENDING_WRITES)) {
			if (FLR_DISALLOW_OUT_OF_ORDER_PREPARES) assert(false);
			MOD_ADD(p_writes->push_ptr, FLR_PENDING_WRITES);
			p_writes->size++;
			p_acks->acks_to_send++;
		}
		incoming_preps[index].prepare.opcode = 0;
    incoming_preps[index].prepare.coalesce_num = 0;
		MOD_ADD(index, FLR_PREP_BUF_SLOTS);
		polled_messages++;
	}
  *pull_ptr = index;
	if (polled_messages > 0) {
    hrd_poll_cq(prep_recv_cq, polled_messages, prep_recv_wc);
    if (ENABLE_ASSERTIONS) (*dbg_counter) = 0;
  }
  else if (ENABLE_ASSERTIONS && p_acks->acks_to_send == 0 && p_writes->size == 0) (*dbg_counter)++;
  if (ENABLE_STAT_COUNTING && p_acks->acks_to_send == 0 && p_writes->size == 0) t_stats[t_id].stalled_ack_prep++;
	if (ENABLE_ASSERTIONS) assert(prep_recv_info->posted_recvs >= polled_messages);
	prep_recv_info->posted_recvs -= polled_messages;
  if (ENABLE_ASSERTIONS) assert(prep_recv_info->posted_recvs <= FLR_MAX_RECV_PREP_WRS);

}


// Send a batched ack that denotes the first local write id and the number of subsequent lid that are being acked
static inline void send_acks_to_ldr(struct pending_writes *p_writes, struct ibv_send_wr *ack_send_wr,
																		struct ibv_sge *ack_send_sgl, long *sent_ack_tx,
																		struct hrd_ctrl_blk *cb, struct recv_info *prep_recv_info,
																		uint8_t flr_id, struct ack_message *ack,
																		struct pending_acks *p_acks, uint16_t t_id)
{
  if (p_acks->acks_to_send == 0) return;
	struct ibv_wc signal_send_wc;
	struct ibv_send_wr *bad_send_wr;


	ack->opcode = CACHE_OP_ACK;
	ack->follower_id = flr_id;
	ack->ack_num = p_acks->acks_to_send;
	uint64_t l_id_to_send = p_writes->local_w_id + p_acks->slots_ahead;
  for (uint32_t i = 0; i < ack->ack_num; i++) {
    uint16_t w_ptr = (p_writes->pull_ptr + p_acks->slots_ahead + i) % FLR_PENDING_WRITES;
    if (ENABLE_ASSERTIONS) assert(p_writes->w_state[w_ptr] == VALID);
    p_writes->w_state[w_ptr] = SENT;
  }
	memcpy(ack->local_id, &l_id_to_send, sizeof(uint64_t));
  if (ENABLE_ASSERTIONS) {
    assert(*(uint64_t *)ack->local_id == l_id_to_send);
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
    yellow_printf("Flr %d is sending an ack for lid %lu and ack num %d and flr id %d, p_writes size %u/%d \n",
                  t_id, l_id_to_send, ack->ack_num, ack->follower_id, p_writes->size, FLR_PENDING_WRITES);
  if (ENABLE_ASSERTIONS) assert(ack->ack_num > 0 && ack->ack_num <= FLR_PENDING_WRITES);
	if ((*sent_ack_tx) % ACK_SEND_SS_BATCH == 0) {
		ack_send_wr->send_flags |= IBV_SEND_SIGNALED;
		// if (local_client_id == 0) green_printf("Sending ack %llu signaled \n", *sent_ack_tx);
	}
	else ack_send_wr->send_flags = IBV_SEND_INLINE;
	if((*sent_ack_tx) % ACK_SEND_SS_BATCH == ACK_SEND_SS_BATCH - 1) {
		// if (local_client_id == 0) green_printf("Polling for ack  %llu \n", *sent_ack_tx);
		hrd_poll_cq(cb->dgram_send_cq[PREP_ACK_QP_ID], 1, &signal_send_wc);
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
     hrd_poll_cq(cb->dgram_send_cq[FC_QP_ID], 1, &signal_send_wc);
    }
  }
  (*credit_tx)++;
  credit_wr[credit_num - 1].next = NULL;
//   yellow_printf("I am sending %d credit message(s)\n", credit_num);
  int ret = ibv_post_send(cb->dgram_qp[FC_QP_ID], &credit_wr[0], &bad_send_wr);
  CPE(ret, "ibv_post_send error in credits", ret);
}



// Form the Write work request for the write
static inline void forge_w_wr(struct pending_writes *p_writes,
                              struct hrd_ctrl_blk *cb, struct ibv_sge *w_send_sgl,
                              struct ibv_send_wr *w_send_wr, long *w_tx,
                              uint16_t w_i, uint16_t *credits,
                              uint16_t t_id) {
  uint16_t i;
  struct ibv_wc signal_send_wc;
  struct w_message *w_mes_fifo = p_writes->w_fifo->fifo;
  uint32_t w_ptr = p_writes->w_fifo->pull_ptr;
  struct w_message *w_mes = &w_mes_fifo[w_ptr];
  uint16_t coalesce_num = w_mes->write[0].w_num;
  if (ENABLE_ASSERTIONS) assert(coalesce_num > 0);
  w_send_sgl[w_i].length = coalesce_num * sizeof(struct write);
  w_send_sgl[w_i].addr = (uint64_t) (uintptr_t) w_mes;

  for (i = 0; i < coalesce_num; i++) {
    if (DEBUG_WRITES)
      printf("Write %d, session id %u, val-len %u, message size %d\n", i,
             *(uint32_t *)w_mes->write[i].session_id,
             w_mes->write[i].val_len,
             w_send_sgl[w_i].length);
    if (ENABLE_ASSERTIONS) {
      assert(w_mes->write[i].val_len == VALUE_SIZE >> SHIFT_BITS);
      assert(w_mes->write[i].opcode == CACHE_OP_PUT);
    }
  }
  if (FLR_W_ENABLE_INLINING) w_send_wr[w_i].send_flags = IBV_SEND_INLINE;
  else w_send_wr[w_i].send_flags = 0;
  if (DEBUG_WRITES)
    green_printf("Follower %d : I sent a write message %d of %u writes with size %u,  with  credits: %d \n",
                 t_id, w_mes->write->opcode, coalesce_num, w_send_sgl[w_i].length, *credits);
  if ((*w_tx) % WRITE_SS_BATCH == 0) w_send_wr[w_i].send_flags |= IBV_SEND_SIGNALED;
  (*w_tx)++;
  (*credits)--;
  if ((*w_tx) % WRITE_SS_BATCH == WRITE_SS_BATCH - 1) {
    // printf("Leader %u POLLING for a send completion in writes \n", t_id);
    hrd_poll_cq(cb->dgram_send_cq[COMMIT_W_QP_ID], 1, &signal_send_wc);
  }
  // Have the last message point to the current message
  if (w_i > 0)
    w_send_wr[w_i - 1].next = &w_send_wr[w_i];

}


// Send the local writes to the ldr
static inline void send_writes_to_the_ldr(struct pending_writes *p_writes,
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
    struct w_message *w_mes_fifo = p_writes->w_fifo->fifo;
    uint32_t w_ptr = p_writes->w_fifo->pull_ptr;
    struct w_message *w_mes = &w_mes_fifo[w_ptr];
    uint16_t coalesce_num = w_mes->write[0].w_num;

    //mirror_remote_buffer(remote_prep_buf, credits, coalesce_num); // TODO FIX THE MIRRORING
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
      MOD_ADD(p_writes->w_fifo->push_ptr, W_FIFO_SIZE);
      w_mes_fifo[p_writes->w_fifo->push_ptr].write[0].w_num = 0;
//      yellow_printf("Zeroing when sending at pointer %u \n", p_writes->prep_fifo->push_ptr);

    }
    add_to_the_mirrored_buffer(remote_w_buf, coalesce_num, 1, LEADER_W_BUF_SLOTS);
    MOD_ADD(p_writes->w_fifo->pull_ptr, W_FIFO_SIZE);
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
static inline void poll_for_coms(struct com_message_ud_req *incoming_coms, uint32_t *pull_ptr,
                                 struct pending_writes *p_writes, uint16_t *credits,
                                 struct ibv_cq * com_recv_cq, struct ibv_wc *com_recv_wc,
                                 struct recv_info *com_recv_info, struct hrd_ctrl_blk *cb,
                                 struct ibv_send_wr *credit_wr, long *credit_tx,
                                 struct fifo *remote_w_buf,
                                 uint16_t t_id, uint8_t flr_id, uint32_t *dbg_counter)
{
  uint32_t index = *pull_ptr;
  uint32_t polled_messages = 0;
  while (incoming_coms[index].com.opcode == CACHE_OP_PUT) {
    // No need to wait for the entire message because the opcode is the last field
    struct com_message *com = &incoming_coms[index].com;
    uint16_t com_num = com->com_num;
    uint64_t l_id = *(uint64_t *) (com->l_id);
    uint64_t pull_lid = p_writes->local_w_id; // l_id at the pull pointer
    uint16_t com_ptr; // a pointer in the FIFO, where the committed write lives
//    yellow_printf("Flr %d com opcode %d with %d coms for l_id %lu, oldest lid %lu, at offset %d at address %u \n",
//                  t_id, com->opcode, com_num, l_id, pull_lid, index, &(incoming_coms[index]));
    if (ENABLE_ASSERTIONS) {
			if ((pull_lid > l_id) || ((l_id + com_num > pull_lid + p_writes->size) && (!USE_QUORUM))) {
        red_printf("Flr %d, COMMIT: received lid %lu, com_num %u, pull_lid %lu, p_writes size  %u \n",
                   t_id, l_id, com_num, pull_lid, p_writes->size);
        print_ldr_stats(t_id);
        exit(0);
      }
      assert(com_num > 0 && com_num <= MAX_LIDS_IN_A_COMMIT);
		}

    // This must always hold: l_id >= pull_lid
		com_ptr = (p_writes->pull_ptr + (l_id - pull_lid)) % FLR_PENDING_WRITES;
//    if (p_writes->w_state[(com_ptr + com_num - 1) % FLR_PENDING_WRITES] == VALID) return;
    for (uint16_t com_i = 0; com_i < com_num; com_i++) {
//      printf("Flr %d valid com %u/%u write at ptr %d with g_id %lu is ready \n",
//               t_id, com_i, com_num,  com_ptr, p_writes->g_id[com_ptr]);
      // it may be that a commit refers to a subset of writes that we have seen,
      // but we need to commit that subset to avoid a deadlock
      if (USE_QUORUM) {
        if (p_writes->w_state[com_ptr] != SENT) {
          com->com_num -= com_i;
          l_id += com_i;
          memcpy(com->l_id, &l_id, sizeof(uint64_t));
          goto END_WHILE;
        }
      }
      else if (ENABLE_ASSERTIONS) assert(p_writes->w_state[com_ptr] == SENT);
			p_writes->w_state[com_ptr] = READY;
			if (p_writes->flr_id[com_ptr] == flr_id) {
        (*credits) += remove_from_the_mirrored_buffer(remote_w_buf, 1, t_id, 0, LEADER_W_BUF_SLOTS);
        if (DEBUG_WRITES)
          yellow_printf("Received a credit, credits: %u \n", *credits);
        } // TODO DO THE MIRRORING
      MOD_ADD(com_ptr, FLR_PENDING_WRITES);
    }
    if (ENABLE_ASSERTIONS) assert(*credits <= W_CREDITS);
    memset(com, 0, LDR_COM_SEND_SIZE); // need to delete the l_id too
		MOD_ADD(index, FLR_COM_BUF_SLOTS);
    if (ENABLE_STAT_COUNTING) {
      t_stats[t_id].received_coms += com_num;
      t_stats[t_id].received_coms_mes_num++;
    }
    if (index % FLR_CREDITS_IN_MESSAGE == 0)
      send_credits_for_commits(com_recv_info, cb, credit_wr, credit_tx, 1, t_id);
		polled_messages++;
  } // while
  END_WHILE: *pull_ptr = index;
  // Poll for the completion of the receives
  if (polled_messages > 0) {
    hrd_poll_cq(com_recv_cq, polled_messages, com_recv_wc);
    if (ENABLE_ASSERTIONS) (*dbg_counter) = 0;
  }
  else {
    if (ENABLE_ASSERTIONS  && p_writes->size > 0) (*dbg_counter)++;
  }
  if (ENABLE_ASSERTIONS) assert(com_recv_info->posted_recvs >= polled_messages);
  com_recv_info->posted_recvs -= polled_messages;
}


// Follower propagate sUpdates that have seen all acks to the KVS
static inline void flr_propagate_updates(struct pending_writes *p_writes, struct pending_acks *p_acks,
                                         struct mica_resp *resp, struct fifo *prep_buf_mirror,
                                         struct latency_flags *latency_info,
                                         uint16_t t_id, uint32_t *dbg_counter)
{
	uint16_t update_op_i = 0;
	uint32_t pull_ptr = p_writes->pull_ptr;

	// Read the latest committed g_id
	uint64_t committed_g_id = atomic_load_explicit(&committed_global_w_id, memory_order_relaxed);
  if (ENABLE_STAT_COUNTING) {
    if ((p_writes->g_id[p_writes->pull_ptr] == committed_g_id + 1) && (p_writes->w_state[p_writes->pull_ptr] == SENT))
      t_stats[t_id].stalled_com_credit++;
  }
	while(p_writes->w_state[p_writes->pull_ptr] == READY) {
		if (p_writes->g_id[p_writes->pull_ptr] != committed_g_id + 1 && (!DISABLE_GID_ORDERING)) {
      if (ENABLE_ASSERTIONS) (*dbg_counter)++;
      if (ENABLE_STAT_COUNTING) t_stats[t_id].stalled_gid++;
      break;
    }
//    printf("Follower found updates to propagate\n");
		p_writes->w_state[p_writes->pull_ptr] = INVALID;
		if (p_writes->is_local[p_writes->pull_ptr]) {
      if (DEBUG_WRITES)
        cyan_printf("Found a local req freeing session %d \n", p_writes->session_id[p_writes->pull_ptr]);
			p_writes->session_has_pending_write[p_writes->session_id[p_writes->pull_ptr]] = false;
			p_writes->all_sessions_stalled = false;
			p_writes->is_local[p_writes->pull_ptr] = false;
      if (MEASURE_LATENCY) change_latency_tag(latency_info, p_writes, t_id);
      if (MEASURE_LATENCY && latency_info->measured_req_flag == HOT_WRITE_REQ_BEFORE_CACHE &&
          machine_id == LATENCY_MACHINE && t_id == LATENCY_THREAD &&
          latency_info->last_measured_sess_id == p_writes->session_id[p_writes->pull_ptr])
        latency_info->measured_req_flag = HOT_WRITE_REQ;
		}
		MOD_ADD(p_writes->pull_ptr, FLR_PENDING_WRITES);
		update_op_i++;
		committed_g_id++;
	}
	if (update_op_i > 0) {
    remove_from_the_mirrored_buffer(prep_buf_mirror, update_op_i, t_id, 0, FLR_PREP_BUF_SLOTS);
    if (ENABLE_ASSERTIONS)(*dbg_counter) = 0;
		p_writes->local_w_id += update_op_i; // advance the local_w_id
		if (ENABLE_ASSERTIONS) {
			assert(p_writes->size >= update_op_i);
			assert(p_acks->slots_ahead >= update_op_i);
		}
		p_acks->slots_ahead -= update_op_i;
		p_writes->size -= update_op_i;
		if (!DISABLE_UPDATING_KVS)
      cache_batch_op_updates((uint32_t) update_op_i, 0, p_writes->ptrs_to_ops, resp, pull_ptr, FLR_PENDING_WRITES, true);
    else
      for (uint16_t i = 0; i < update_op_i; i++) p_writes->ptrs_to_ops[(pull_ptr + i) % FLR_PENDING_WRITES]->opcode = 5;

		atomic_store_explicit(&committed_global_w_id, committed_g_id, memory_order_relaxed);
    if (MEASURE_LATENCY && latency_info->measured_req_flag == HOT_WRITE_REQ &&
        machine_id == LATENCY_MACHINE && t_id == LATENCY_THREAD )
      report_latency(latency_info);
//		yellow_printf("Committed global id %lu \n", committed_g_id);
	}
}




#endif /* INLINE_UTILS_H */

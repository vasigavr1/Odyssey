#ifndef INLINE_UTILS_H
#define INLINE_UTILS_H

#include "cache.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <optik_mod.h>

/* ---------------------------------------------------------------------------
------------------------------UTILITY --------------------------------------
---------------------------------------------------------------------------*/
static inline void my_assert(bool cond, const char *message)
{
  if (ENABLE_ASSERTIONS) {
    if (!cond) {
      red_printf("%s\n", message);
      assert(false);
    }
  }
}

static inline void print_version(const uint32_t version)
{
  yellow_printf("Version: %u\n", version);
}


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
static inline bool keys_are_equal(struct cache_key* key1, struct cache_key* key2) {
	return (key1->bkt    == key2->bkt &&
			key1->server == key2->server &&
			key1->tag    == key2->tag) ? true : false;
}

// Check whether 2 key hashes are equal
static inline bool true_keys_are_equal(struct key* key1, struct key* key2) {
  return (key1->bkt    == key2->bkt &&
          key1->server == key2->server &&
          key1->tag    == key2->tag) ? true : false;
}

// Compares two network timestamps, returns SMALLER if ts1 < ts2
static inline enum ts_compare compare_netw_ts(struct network_ts_tuple *ts1, struct network_ts_tuple *ts2)
{
  if ((*(uint32_t *)ts1->version == *(uint32_t *)ts2->version) &&
      (ts1->m_id == ts2->m_id))
    return EQUAL;
  else if ((*(uint32_t *)ts1->version < *(uint32_t *)ts2->version) ||
          ((*(uint32_t *)ts1->version == *(uint32_t *)ts2->version) &&
           (ts1->m_id < ts2->m_id)))
    return SMALLER;
  else if  ((*(uint32_t *)ts1->version > *(uint32_t *)ts2->version) ||
           ((*(uint32_t *)ts1->version == *(uint32_t *)ts2->version)) &&
            (ts1->m_id > ts2->m_id))
    return GREATER;

  return ERROR;
}

// Compares two timestamps, returns SMALLER if ts1 < ts2
static inline enum ts_compare compare_ts(struct ts_tuple *ts1, struct ts_tuple *ts2)
{
  if ((ts1->version == ts2->version) &&
      (ts1->m_id == ts2->m_id))
    return EQUAL;
  else if ((ts1->version < ts2->version) ||
           ((ts1->version == ts2->version) &&
            (ts1->m_id < ts2->m_id)))
    return SMALLER;
  else if  ((ts1->version > ts2->version) ||
            ((ts1->version == ts2->version)) &&
            (ts1->m_id > ts2->m_id))
    return GREATER;
  return ERROR;
}

// Compares a network ts with a regular ts, returns SMALLER if ts1 < ts2
static inline enum ts_compare compare_netw_ts_with_ts(struct network_ts_tuple *ts1, struct ts_tuple *ts2)
{
  if ((*(uint32_t *)ts1->version == ts2->version) &&
      (ts1->m_id == ts2->m_id))
    return EQUAL;
  else if ((*(uint32_t *)ts1->version < ts2->version) ||
           ((*(uint32_t *)ts1->version == ts2->version) &&
            (ts1->m_id < ts2->m_id)))
    return SMALLER;
  else if  ((*(uint32_t *)ts1->version > ts2->version) ||
            ((*(uint32_t *)ts1->version == ts2->version)) &&
            (ts1->m_id > ts2->m_id))
    return GREATER;

  return ERROR;
}

static inline enum ts_compare compare_meta_ts_with_ts(cache_meta *ts1, struct ts_tuple *ts2) {
  if ((ts1->version == ts2->version) &&
      (ts1->m_id == ts2->m_id))
    return EQUAL;
  else if ((ts1->version < ts2->version) ||
           ((ts1->version == ts2->version) &&
            (ts1->m_id < ts2->m_id)))
    return SMALLER;
  else if ((ts1->version > ts2->version) ||
           ((ts1->version == ts2->version)) &&
           (ts1->m_id > ts2->m_id))
    return GREATER;

  return ERROR;
}


// First arguement is the network ts
static inline void assign_ts_to_netw_ts(struct network_ts_tuple *ts1, struct ts_tuple *ts2)
{
  ts1->m_id = ts2->m_id;
  *(uint32_t*) ts1->version = ts2->version;
}

// First arguement is the ts
static inline void assign_netw_ts_to_ts(struct ts_tuple *ts1, struct network_ts_tuple *ts2)
{
  ts1->m_id = ts2->m_id;
  ts1->version = *(uint32_t*) ts2->version;
}


// Check whether 2 keys (including the metadata) are equal
static inline uint8_t keys_and_meta_are_equal(struct cache_key* key1, struct cache_key* key2) {
	return (uint8_t) ((key1->bkt    == key2->bkt &&
			key1->server == key2->server &&
			key1->tag    == key2->tag &&
			key1->meta.version == key2->meta.version) ? 1 : 0);
}

// A condition to be used to trigger periodic (but rare) measurements
static inline bool trigger_measurement(uint16_t local_client_id)
{
	return t_stats[local_client_id].cache_hits_per_thread % K_32 > 0 &&
		   t_stats[local_client_id].cache_hits_per_thread % K_32 <= 500 &&
		   local_client_id == 0 && machine_id == MACHINE_NUM -1;
}

// Calculate the thread global id
static inline uint16_t get_gid(uint8_t m_id, uint16_t t_id)
{
  return (uint16_t) (m_id * WORKERS_PER_MACHINE + t_id);
}

// Calculate the machineid out of the thread global id
static inline uint8_t gid_to_mid(uint16_t g_id)
{
  return (uint8_t) (g_id / WORKERS_PER_MACHINE);
}

// Convert a machine id to a "remote machine id"
static inline uint8_t  mid_to_rmid(uint8_t m_id)
{
  return m_id < machine_id ? m_id : (uint8_t)(m_id - 1);
}

// Convert a "remote machine id" to a machine id
static inline uint8_t rmid_to_mid(uint8_t rm_id)
{
  return rm_id < machine_id ? rm_id : (uint8_t)(rm_id + 1);
}

// Calculate the global session id
static inline uint16_t get_glob_sess_id(uint8_t m_id, uint16_t t_id, uint16_t sess_id)
{
  return (uint16_t) ((m_id * SESSIONS_PER_MACHINE) +
                     (t_id * SESSIONS_PER_THREAD)  +
                      sess_id);
}

// Get the machine id out of a global session id
static inline uint8_t glob_ses_id_to_m_id(uint16_t glob_sess_id)
{
  return (uint8_t) (glob_sess_id / SESSIONS_PER_MACHINE);
}

// Get the machine id out of a global session id
static inline uint16_t glob_ses_id_to_t_id(uint16_t glob_sess_id)
{
  return (uint16_t) ((glob_sess_id % SESSIONS_PER_MACHINE) / SESSIONS_PER_THREAD);
}

// Get the sess id out of a global session id
static inline uint16_t glob_ses_id_to_sess_id(uint16_t glob_sess_id)
{
  return (uint16_t) ((glob_sess_id % SESSIONS_PER_MACHINE) % SESSIONS_PER_THREAD);
}

static inline bool is_global_ses_id_local(uint16_t glob_sess_id, uint16_t t_id)
{
  return glob_ses_id_to_t_id(glob_sess_id) == t_id &&
         glob_ses_id_to_m_id(glob_sess_id) == machine_id;
}
// Generic CAS
static inline bool cas_a_state(atomic_uint_fast8_t * state, uint8_t old_state, uint8_t new_state, uint16_t t_id)
{
  return atomic_compare_exchange_strong(state, (atomic_uint_fast8_t *) &old_state,
                                        (atomic_uint_fast8_t) new_state);
}

static inline bool rmw_ids_are_equal(struct rmw_id *id1, struct rmw_id *id2)
{
  return id1->glob_sess_id == id2->glob_sess_id && id1->id == id2->id;
}

static inline bool rmw_id_is_equal_with_id_and_glob_sess_id(struct rmw_id *id1, uint64_t id, uint16_t glob_sess_id)
{
  return id1->glob_sess_id == glob_sess_id && id1->id == id;
}

static inline void assign_second_rmw_id_to_first(struct rmw_id* rmw_id1, struct rmw_id* rmw_id2)
{
  rmw_id1->id = rmw_id2->id;
  rmw_id1->glob_sess_id = rmw_id2->glob_sess_id;
}

// assign second argument to the first
static inline void assign_rmw_id_to_net_rmw_id(struct net_rmw_id* rmw_id1, struct rmw_id* rmw_id2)
{
  rmw_id1->id = rmw_id2->id;
  rmw_id1->glob_sess_id = rmw_id2->glob_sess_id;
}

// assign second argument to the first
static inline void assign_net_rmw_id_to_rmw_id(struct rmw_id* rmw_id1, struct net_rmw_id* rmw_id2)
{
  rmw_id1->id = rmw_id2->id;
  rmw_id1->glob_sess_id = rmw_id2->glob_sess_id;
}

static inline void swap_rmw_ids(struct rmw_id* rmw_id1, struct rmw_id* rmw_id2)
{
  struct rmw_id  tmp = *rmw_id1;
  assign_second_rmw_id_to_first(rmw_id1, rmw_id2);
  assign_second_rmw_id_to_first(rmw_id2, &tmp);
}

static inline uint8_t sum_of_reps(struct rmw_local_entry* loc_entry)
{
  return loc_entry->rmw_reps.acks + loc_entry->rmw_reps.rmw_id_commited +
         loc_entry->rmw_reps.log_too_small + loc_entry->rmw_reps.already_accepted +
         loc_entry->rmw_reps.ts_stale + loc_entry->rmw_reps.seen_higher_prop;
}

/* ---------------------------------------------------------------------------
//------------------------------ RDMA GENERIC -----------------------------
//---------------------------------------------------------------------------*/
// Post Receives for acknowledgements
static inline void post_recvs_with_recv_info(struct recv_info *recv, uint32_t recv_num)
{
  if (recv_num == 0) return;
  uint16_t j;
  struct ibv_recv_wr *bad_recv_wr;
  for (j = 0; j < recv_num; j++) {
    recv->recv_sgl[j].addr = (uintptr_t) recv->buf + (recv->push_ptr * recv->slot_size);
    //printf("Posting a receive at push ptr %u at address %lu \n", recv->w_push_ptr, recv->recv_sgl[j].addr);
    MOD_ADD(recv->push_ptr, recv->buf_slots);
    recv->recv_wr[j].next = (j == recv_num - 1) ?
                            NULL : &recv->recv_wr[j + 1];
  }
  int ret = ibv_post_recv(recv->recv_qp, &recv->recv_wr[0], &bad_recv_wr);
  if (ENABLE_ASSERTIONS) CPE(ret, "ibv_post_recv error", ret);
}

/* Fill @wc with @num_comps comps from this @cq. Exit on error. */
static inline uint32_t poll_cq(struct ibv_cq *cq, int num_comps, struct ibv_wc *wc, uint8_t caller_flag)
{
  int comps = 0;
  uint32_t debug_cnt = 0;
  while(comps < num_comps) {
    if (ENABLE_ASSERTIONS && debug_cnt > M_256) {
      printf("Someone is stuck waiting for a completion %d / %d , type %u  \n", comps, num_comps, caller_flag );
      debug_cnt = 0;
    }
    int new_comps = ibv_poll_cq(cq, num_comps - comps, &wc[comps]);
    if(new_comps != 0) {
//			 printf("I see completions %d\n", new_comps);
      /* Ideally, we should check from comps -> new_comps - 1 */
      if(ENABLE_ASSERTIONS && wc[comps].status != 0) {
        fprintf(stderr, "Bad wc status %d\n", wc[comps].status);
        exit(0);
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


/* ---------------------------------------------------------------------------
//------------------------------ LATENCY MEASUREMENTS-------------------------
//---------------------------------------------------------------------------*/

//Add latency to histogram (in microseconds)
static inline void bookkeep_latency(int useconds, req_type rt){
  uint32_t** latency_counter;
  switch (rt){
    case ACQUIRE:
      latency_counter = &latency_count.acquires;
      if (useconds > latency_count.max_acq_lat) {
        latency_count.max_acq_lat = (uint32_t) useconds;
        //green_printf("Found max acq latency: %u/%d \n",
        //             latency_count.max_acq_lat, useconds);
      }
      break;
    case RELEASE:
      latency_counter = &latency_count.releases;
      if (useconds > latency_count.max_rel_lat) {
        latency_count.max_rel_lat = (uint32_t) useconds;
        //yellow_printf("Found max rel latency: %u/%d \n", latency_count.max_rel_lat, useconds);
      }
      break;
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

//
static inline void report_latency(struct latency_flags* latency_info)
{
  struct timespec end;
  clock_gettime(CLOCK_MONOTONIC, &end);
  int useconds = ((end.tv_sec - latency_info->start.tv_sec) * MILLION) +
                 ((end.tv_nsec - latency_info->start.tv_nsec) / 1000);  //(end.tv_nsec - start->tv_nsec) / 1000;
  if (ENABLE_ASSERTIONS) assert(useconds > 0);
  //if (useconds > 1000)
//    printf("Latency of a req of type %s is %u us, sess %u , thread reqs/ measured reqs: %ld \n",
//           latency_info->measured_req_flag == RELEASE ? "RELEASE" : "ACQUIRE",
//           useconds, latency_info->measured_sess_id,
//           t_stats[0].cache_hits_per_thread / (latency_count.total_measurements + 1));
  bookkeep_latency(useconds, latency_info->measured_req_flag);
  (latency_info->measured_req_flag) = NO_REQ;
}

// Necessary bookkeeping to initiate the latency measurement
static inline void start_measurement(struct latency_flags* latency_info, uint32_t sess_id, uint16_t t_id,
                                     uint8_t opcode) {
  uint8_t compare_op = MEASURE_READ_LATENCY ? OP_ACQUIRE : OP_RELEASE ;
  if ((latency_info->measured_req_flag) == NO_REQ) {
    if (t_stats[t_id].cache_hits_per_thread > M_1 &&
      (MEASURE_READ_LATENCY == 2 || opcode == compare_op) &&
      t_id == LATENCY_THREAD && machine_id == LATENCY_MACHINE) {
      //printf("tag a key for latency measurement \n");
      //if (opcode == CACHE_OP_GET) latency_info->measured_req_flag = HOT_READ_REQ;
     // else if (opcode == CACHE_OP_PUT) {
      //  latency_info->measured_req_flag = HOT_WRITE_REQ;
      //}
      //else
      if (opcode == OP_RELEASE)
        latency_info->measured_req_flag = RELEASE;
      else if (opcode == OP_ACQUIRE) latency_info->measured_req_flag = ACQUIRE;
      else if (ENABLE_ASSERTIONS) assert(false);
      //green_printf("Measuring a req %llu, opcode %d, flag %d op_i %d \n",
      //					 t_stats[t_id].cache_hits_per_thread, opcode, latency_info->measured_req_flag, latency_info->measured_sess_id);
      latency_info->measured_sess_id = sess_id;
      clock_gettime(CLOCK_MONOTONIC, &latency_info->start);
      if (ENABLE_ASSERTIONS) assert(latency_info->measured_req_flag != NO_REQ);
    }
  }
}


/* ---------------------------------------------------------------------------
//------------------------------DEBUGGING-------------------------------------
//---------------------------------------------------------------------------*/
static inline void check_version(uint32_t version, const char *message) {
  if (ENABLE_ASSERTIONS) {


    if (version == 0 || version % 2 != 0) {
      red_printf("Version %u %s\n", version, message);
    }
    assert(version > 0);
    assert(version % 2 == 0);
  }
}

static inline void print_wrkr_stats (uint16_t t_id)
{
  green_printf("WORKER %u SENT MESSAGES \n", t_id);
  yellow_printf("Writes sent %ld/%ld \n", t_stats[t_id].writes_sent_mes_num, t_stats[t_id].writes_sent);
  yellow_printf("Acks sent %ld/%ld \n", t_stats[t_id].acks_sent_mes_num, t_stats[t_id].acks_sent);
  yellow_printf("Reads sent %ld/%ld \n", t_stats[t_id].reads_sent_mes_num, t_stats[t_id].reads_sent);
  yellow_printf("R_reps sent %ld/%ld \n", t_stats[t_id].r_reps_sent_mes_num, t_stats[t_id].r_reps_sent);
  green_printf("WORKER %u RECEIVED MESSAGES \n", t_id);
  //yellow_printf("Writes sent %ld/%ld \n", t_stats[g_id].writes_sent_mes_num, t_stats[g_id].writes_sent);
  //yellow_printf("Acks sent %ld/%ld \n", t_stats[g_id].acks_sent_mes_num, t_stats[g_id].acks_sent);
  yellow_printf("Reads received %ld/%ld \n", t_stats[t_id].received_reads_mes_num, t_stats[t_id].received_reads);
  yellow_printf("R_reps received %ld/%ld \n", t_stats[t_id].received_r_reps_mes_num, t_stats[t_id].received_r_reps);

  for (uint8_t i = 0; i < MACHINE_NUM; i++) {
    if (i == machine_id) continue;
    cyan_printf("FROM/ TO MACHINE %u \n", i);
    yellow_printf("Acks Received %lu/%lu from machine id %u \n", t_stats[t_id].per_worker_acks_mes_received[i],
                  t_stats[t_id].per_worker_acks_received[i], i);
    yellow_printf("Writes Received %lu from machine id %u\n", t_stats[t_id].per_worker_writes_received[i], i);
    yellow_printf("Acks Sent %lu/%lu to machine id %u \n", t_stats[t_id].per_worker_acks_mes_sent[i],
                  t_stats[t_id].per_worker_acks_sent[i], i);

  }
//  yellow_printf("Reads sent %ld/%ld \n", t_stats[g_id].r_reps_sent_mes_num, t_stats[g_id].r_reps_sent );
}


// Leader checks its debug counters
static inline void check_debug_cntrs(uint32_t *credit_debug_cnt, uint32_t *wait_dbg_counter,
                                     struct pending_ops *p_ops, void *buf,
                                     uint32_t r_pull_ptr, uint32_t w_pull_ptr,
                                     uint32_t ack_pull_ptr, uint32_t r_rep_pull_ptr,
                                     uint16_t t_id)
{

//  volatile struct  w_message_ud_req *w_buffer =
//    (volatile struct w_message_ud_req *)(buf + ACK_BUF_SIZE);
//  volatile struct  r_message_ud_req *r_buffer =
//    (volatile struct r_message_ud_req *)(cb->dgram_buf + ACK_BUF_SIZE + W_BUF_SIZE);

  // ACKS
  if (unlikely(wait_dbg_counter[ACK_QP_ID] > M_512)) {
    red_printf("Worker %d waits for acks \n", t_id);
    if (VERBOSE_DBG_COUNTER) {
      struct ack_message_ud_req *ack_buf = (struct ack_message_ud_req *) (buf);
      struct ack_message *ack = &ack_buf[ack_pull_ptr].ack;
      uint64_t l_id = *(uint64_t *) ack->local_id;
      uint8_t message_opc = ack->opcode;
      cyan_printf("Wrkr %d, polling on index %u, polled opc %u, 1st ack opcode: %u, l_id %lu, expected l_id %lu\n",
                  t_id, ack_pull_ptr, message_opc, ack->opcode, l_id, p_ops->local_w_id);
      MOD_ADD(ack_pull_ptr, ACK_BUF_SLOTS);
      ack = &ack_buf[ack_pull_ptr].ack;
      l_id = *(uint64_t *) ack->local_id;
      message_opc = ack->opcode;
      cyan_printf("Next index %u,polled opc %u, 1st ack opcode: %u, l_id %lu, expected l_id %lu\n",
                  ack_pull_ptr, message_opc, ack->opcode, l_id, p_ops->local_w_id);
      for (int i = 0; i < ACK_BUF_SLOTS; ++i) {
        if (ack_buf[i].ack.opcode == CACHE_OP_ACK) {
          green_printf("GOOD OPCODE in index %d, l_id %u \n", i, *(uint64_t *) ack_buf[i].ack.local_id);
        } else
          red_printf("BAD OPCODE in index %d, l_id %u, from machine: %u  \n", i, *(uint64_t *) ack_buf[i].ack.local_id,
                     ack_buf[i].ack.m_id);

      }
    }
    print_wrkr_stats(t_id);
    wait_dbg_counter[ACK_QP_ID] = 0;
    //exit(0);
  }
  // R_REPS
  if (unlikely(wait_dbg_counter[R_REP_QP_ID] > M_512)) {
    red_printf("Worker %d waits for r_reps \n", t_id);
    if (VERBOSE_DBG_COUNTER) {
      struct r_rep_message_ud_req *r_rep_buf =
        (struct r_rep_message_ud_req *) (buf + ACK_BUF_SIZE + W_BUF_SIZE + R_BUF_SIZE);
      struct r_rep_message *r_rep = &r_rep_buf[r_rep_pull_ptr].r_rep_mes;
      uint64_t l_id = *(uint64_t *) r_rep->l_id;
      uint8_t message_opc = r_rep->opcode;
      cyan_printf("Wrkr %d, polling on index %u, polled opc %u, 1st r_rep opcode: %u, l_id %lu, expected l_id %lu\n",
                  t_id, r_rep_pull_ptr, message_opc, r_rep->opcode, l_id, p_ops->local_r_id);
      MOD_ADD(r_rep_pull_ptr, R_REP_BUF_SLOTS);
      r_rep = &r_rep_buf[r_rep_pull_ptr].r_rep_mes;
      l_id = *(uint64_t *) r_rep->l_id;
      message_opc = r_rep->opcode;
      cyan_printf("Next index %u,polled opc %u, 1st r_rep opcode: %u, l_id %lu, expected l_id %lu\n",
                  r_rep_pull_ptr, message_opc, r_rep->opcode, l_id, p_ops->local_r_id);
      for (int i = 0; i < R_REP_BUF_SLOTS; ++i) {
        if (r_rep_buf[i].r_rep_mes.opcode == READ_REPLY) {
          green_printf("GOOD OPCODE in index %d, l_id %u \n", i, *(uint64_t *) r_rep_buf[i].r_rep_mes.l_id);
        } else
          red_printf("BAD OPCODE in index %d, l_id %u, from machine: %u  \n", i,
                     *(uint64_t *) r_rep_buf[i].r_rep_mes.l_id,
                     r_rep_buf[i].r_rep_mes.m_id);

      }
    }
    print_wrkr_stats(t_id);
    wait_dbg_counter[R_REP_QP_ID] = 0;
    //exit(0);
  }
  if (unlikely(wait_dbg_counter[R_QP_ID] > M_512)) {
    red_printf("Worker %d waits for reads \n", t_id);
    print_wrkr_stats(t_id);
    wait_dbg_counter[R_QP_ID] = 0;
  }
  if (unlikely(credit_debug_cnt[W_VC] > M_512)) {
    red_printf("Worker %d lacks write credits \n", t_id);
    print_wrkr_stats(t_id);
    credit_debug_cnt[W_VC] = 0;
  }
  if (unlikely(credit_debug_cnt[R_VC] > M_512)) {
    red_printf("Worker %d lacks read credits \n", t_id);
    print_wrkr_stats(t_id);
    credit_debug_cnt[R_VC] = 0;
  }
}

static inline void debug_and_count_stats_when_broadcasting_writes
  (struct pending_ops *p_ops, uint32_t bcast_pull_ptr,
   uint8_t coalesce_num, uint16_t t_id, uint64_t* expected_l_id_to_send,
   uint16_t br_i, uint32_t *outstanding_writes)
{
  bool is_accept = p_ops->w_fifo->w_message[bcast_pull_ptr].write[0].opcode == ACCEPT_OP;
  if (ENABLE_ASSERTIONS) {
    if (!is_accept) {
      uint64_t lid_to_send = *(uint64_t *) p_ops->w_fifo->w_message[bcast_pull_ptr].l_id;
      if (lid_to_send != (*expected_l_id_to_send)) {
        red_printf("Wrkr %u, expected l_id %lu lid_to send %u, opcode %u \n",
                   t_id, (*expected_l_id_to_send), lid_to_send,
                   p_ops->w_fifo->w_message[bcast_pull_ptr].write[0].opcode );
        assert(false);
      }
      (*expected_l_id_to_send) = lid_to_send + coalesce_num;
    }
    if (coalesce_num == 0) {
      red_printf("Wrkr %u coalesce_num is %u, bcast_size %u, w_size %u, push_ptr %u, pull_ptr %u"
                   " mes fifo push_ptr %u, mes fifo pull ptr %u l_id %lu"
                   " bcast_pull_ptr %u, br_i %u\n",
                 t_id, coalesce_num, p_ops->w_fifo->bcast_size,
                 p_ops->w_size,
                 p_ops->w_push_ptr, p_ops->w_pull_ptr,
                 p_ops->w_fifo->push_ptr, p_ops->w_fifo->bcast_pull_ptr,
                 *(uint64_t *)p_ops->w_fifo->w_message[bcast_pull_ptr].l_id,
                 bcast_pull_ptr, br_i);
    }
    assert(coalesce_num > 0);
    assert(p_ops->w_fifo->bcast_size >= coalesce_num);
    (*outstanding_writes) += coalesce_num;
  }
  if (ENABLE_STAT_COUNTING) {
    bool is_commit = p_ops->w_fifo->w_message[bcast_pull_ptr].write[0].opcode == COMMIT_OP;
    if (is_accept) t_stats[t_id].accepts_sent++;
    else if (is_commit) t_stats[t_id].commits_sent++;
    else {
      t_stats[t_id].writes_sent += coalesce_num;
      t_stats[t_id].writes_sent_mes_num++;
    }
  }
}


// Perform some basic checks when inserting a write to a fresh message
static inline void debug_checks_when_inserting_a_write
  (const uint8_t source, const uint8_t inside_w_ptr, const uint32_t w_mes_ptr,
   struct w_message *w_mes, const uint64_t message_l_id,
   struct pending_ops *p_ops, const uint32_t w_ptr, const uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    // In a fresh message check that the lid is consistent with the previous message
    if (inside_w_ptr == 0) {
      if (message_l_id > MAX_W_COALESCE) {
        uint32_t prev_w_mes_ptr = (w_mes_ptr + W_FIFO_SIZE - 1) % W_FIFO_SIZE;
        bool prev_mes_is_accept = w_mes[prev_w_mes_ptr].write[0].opcode == ACCEPT_OP;
        uint64_t prev_l_id = *(uint64_t *) w_mes[prev_w_mes_ptr].l_id;
        uint8_t prev_coalesce = w_mes[prev_w_mes_ptr].w_num;
        if (!prev_mes_is_accept && message_l_id != prev_l_id + prev_coalesce) {
          red_printf("Worker %u: Current message l_id %u, previous message l_id %u , previous coalesce %u\n",
                     t_id, message_l_id, prev_l_id, prev_coalesce);
        }
      }
    }

    // Check the versions
    assert ((*(uint32_t *) w_mes[w_mes_ptr].write[inside_w_ptr].version) < B_4_EXACT);
    if ((*(uint32_t *) w_mes[w_mes_ptr].write[inside_w_ptr].version) % 2 != 0) {
      red_printf("Worker %u: Version to insert %u, comes from read %u \n", t_id,
                 *(uint32_t *) w_mes[w_mes_ptr].write[inside_w_ptr].version, source);
      assert (false);
    }
    // Check that the buffer is not occupied
    if (unlikely(p_ops->w_state[w_ptr] != INVALID))
      red_printf("Worker %u w_state %d at w_ptr %u, cache hits %lu, w_size %u \n",
                 t_id, p_ops->w_state[w_ptr], w_ptr,
                 t_stats[t_id].cache_hits_per_thread, p_ops->w_size);
    //					printf("Sent %d, Valid %d, Ready %d \n", SENT, VALID, READY);
    assert(p_ops->w_state[w_ptr] == INVALID);
  }

}

// When forging a write (which the accept hijack)
static inline void checks_when_forging_an_accept(struct accept_message *acc_mes, struct ibv_sge *send_sgl,
                                                 uint16_t br_i, uint8_t coalesce_num, uint16_t t_id)
{
  assert(coalesce_num <= MAX_ACC_COALESCE);
  assert(coalesce_num > 0);
  for (uint8_t  i = 0; i < coalesce_num; i++) {
    if (DEBUG_RMW)
      printf("Worker: %u, Accept %d, val-len %u, message w_size %d\n", t_id, i, acc_mes->acc[i].val_len,
             send_sgl[br_i].length);
    if (ENABLE_ASSERTIONS) {
      assert(acc_mes->acc[i].val_len == VALUE_SIZE >> SHIFT_BITS);
      assert(acc_mes->acc[i].opcode == ACCEPT_OP);
    }
  }
}

// When forging a write (which the accept hijack)
static inline void checks_when_forging_a_commit(struct commit_message *com_mes, struct ibv_sge *send_sgl,
                                                 uint16_t br_i, uint8_t coalesce_num, uint16_t t_id)
{
  assert(coalesce_num <= MAX_COM_COALESCE);
  assert(coalesce_num > 0);
  for (uint8_t  i = 0; i < coalesce_num; i++) {
    if (DEBUG_RMW)
      printf("Worker: %u, Commit %d, val-len %u, message w_size %d\n", t_id, i, com_mes->com[i].val_len,
             send_sgl[br_i].length);
    if (ENABLE_ASSERTIONS) {
      assert(com_mes->com[i].val_len == VALUE_SIZE >> SHIFT_BITS);
      assert(com_mes->com[i].opcode == COMMIT_OP);
    }
  }
}


static inline void checks_when_forging_a_write(struct w_message *w_mes, struct ibv_sge *send_sgl,
                                               uint16_t br_i, uint8_t coalesce_num, uint16_t t_id)
{
  for (uint8_t  i = 0; i < coalesce_num; i++) {
    if (DEBUG_WRITES)
      printf("Worker: %u, Write %d, val-len %u, message w_size %d\n", t_id, i, w_mes->write[i].val_len,
             send_sgl[br_i].length);
    if (ENABLE_ASSERTIONS) {
      assert(w_mes->write[i].val_len == VALUE_SIZE >> SHIFT_BITS);
      assert(w_mes->write[i].opcode == CACHE_OP_PUT ||
             w_mes->write[i].opcode == OP_RELEASE ||
             w_mes->write[i].opcode == OP_ACQUIRE ||
             w_mes->write[i].opcode == OP_RELEASE_SECOND_ROUND);
      //assert(w_mes->write[i].m_id == machine_id); // not true because reads get converted to writes with random m_ids
    }
  }
}


static inline void print_thread_stats(uint16_t t_id) {

    yellow_printf("Cache hits: %u \nReads: %lu \nWrites: %lu \nReleases: %lu \nAcquires: %lu \nQ Reads: %lu "
                    "\nRectified keys %lu\n",
                  t_stats[t_id].cache_hits_per_thread, t_stats[t_id].reads_per_thread,
                  t_stats[t_id].writes_per_thread, t_stats[t_id].releases_per_thread,
                  t_stats[t_id].acquires_per_thread, t_stats[t_id].quorum_reads,
                  t_stats[t_id].rectified_keys);
}


static inline void print_verbouse_debug_info(struct pending_ops *p_ops, uint16_t t_id, uint16_t credits[][MACHINE_NUM])
{
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

static inline void print_mica_key(struct cache_key *key)
{

}

static inline void print_true_key(struct key *key)
{
  printf("bkt: %u, server: %u, tag : %u, \n", key->bkt,key->server, key->tag);
}


//The purpose of this is to save some space in function that polls read replies
static inline void print_and_check_mes_when_polling_r_reps(struct r_rep_message *r_rep_mes,
                                                           uint32_t index, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(r_rep_mes->opcode == READ_REPLY || r_rep_mes->opcode == PROP_REPLY ||
           r_rep_mes->opcode == ACCEPT_REPLY);
    my_assert(r_rep_mes->m_id < MACHINE_NUM, "Received r_rep with m_id >= Machine_NUM");
    my_assert(r_rep_mes->coalesce_num > 0, "Received r_rep with coalesce num = 0");
  }

  if ((DEBUG_READS && (r_rep_mes->opcode == READ_REPLY)) ||
      (DEBUG_RMW   && (r_rep_mes->opcode == PROP_REPLY || r_rep_mes->opcode == ACCEPT_REPLY))) {
    yellow_printf("Worker %u sees a READ REPLY: %d at offset %d, l_id %lu, from machine "
                    "%u with %u replies first opc %u\n",
                  t_id, r_rep_mes->opcode, index,
                  *(uint64_t *) r_rep_mes->l_id,
                  r_rep_mes->m_id, r_rep_mes->coalesce_num, r_rep_mes->r_rep[0].opcode);
  }
}

static inline void increase_credits_when_polling_r_reps(uint16_t credits[][MACHINE_NUM], bool is_propose,
                                                     bool is_accept, uint8_t rem_m_id, uint16_t t_id)
{
  if (!is_accept) credits[R_VC][rem_m_id]++;
  else credits[W_VC][rem_m_id]++;
  if (ENABLE_ASSERTIONS) {
    assert(credits[R_VC][rem_m_id] <= R_CREDITS);
    assert(credits[W_VC][rem_m_id] <= W_CREDITS);
    assert(!(is_accept && is_propose));
  }
}

// Debug session
static inline void debug_sessions(struct session_dbg *ses_dbg, struct pending_ops *p_ops,
                                  uint32_t sess_id, uint16_t t_id)
{
  if (DEBUG_SESSIONS) {
    //assert(p_ops->prop_info->entry[sess_id].state != INVALID_RMW);
    ses_dbg->dbg_cnt[sess_id]++;
    if (ses_dbg->dbg_cnt[sess_id] == DEBUG_SESS_COUNTER) {
      if (sess_id == 0) red_printf("Wrkr %u Session %u seems to be stuck \n", t_id, sess_id);
      ses_dbg->dbg_cnt[sess_id] = 0;
    }
  }
}

// Debug session
static inline void debug_all_sessions(struct session_dbg *ses_dbg, struct pending_ops *p_ops,
                                      uint16_t t_id)
{
  if (DEBUG_SESSIONS) {
    for (uint16_t sess_id = 0; sess_id < SESSIONS_PER_THREAD; sess_id++) {
      ses_dbg->dbg_cnt[sess_id]++;
      assert(p_ops->prop_info->entry[sess_id].state != INVALID_RMW);
      if (ses_dbg->dbg_cnt[sess_id] == DEBUG_SESS_COUNTER) {
        if (sess_id == 0) {
          red_printf("Wrkr %u Session %u seems to be stuck-- all stuck \n", t_id, sess_id);
          for (uint16_t i = 0; i < SESSIONS_PER_THREAD; i++)
            printf("%u - %u- %u - %u, ", ses_dbg->dbg_cnt[i],
                   p_ops->prop_info->entry[sess_id].state, p_ops->prop_info->entry[sess_id].back_off_cntr,
                   p_ops->prop_info->entry[sess_id].index_to_rmw);
          printf ("\n");
        }
        ses_dbg->dbg_cnt[sess_id] = 0;
      }
    }
  }
}


// Prints out information about the participants
static inline void print_q_info(struct quorum_info *q_info)
{
  yellow_printf("-----QUORUM INFO----- \n");
  green_printf("Active m_ids: \n");
  for (uint8_t i = 0; i < q_info->active_num; i++) {
    green_printf("%u) %u \n", i, q_info->active_ids[i]);
  }
  red_printf("Missing m_ids: \n");
  for (uint8_t i = 0; i < q_info->missing_num; i++) {
    red_printf("%u) %u \n", i, q_info->missing_ids[i]);
  }
  yellow_printf("Send vector : ");
  for (uint8_t i = 0; i < REM_MACH_NUM; i++) {
    yellow_printf("%d ", q_info->send_vector[i]);
  }
  yellow_printf("\n First rm_id: %u, Last rm_id: %u \n",
                q_info->first_active_rm_id, q_info->last_active_rm_id);
}

// From commit reads
static inline void checks_when_commiting_a_read(struct pending_ops *p_ops, uint32_t pull_ptr,
                                                bool acq_second_round_to_flip_bit, bool insert_write_flag,
                                                bool write_local_kvs, uint16_t t_id)
{
  if (acq_second_round_to_flip_bit) assert(p_ops->virt_r_size < MAX_ALLOWED_R_SIZE);
  assert(p_ops->read_info[pull_ptr].opcode == OP_ACQUIRE ||
         p_ops->read_info[pull_ptr].opcode == OP_ACQUIRE_FLIP_BIT ||
         p_ops->read_info[pull_ptr].opcode == CACHE_OP_GET ||
         p_ops->read_info[pull_ptr].opcode == OP_RELEASE ||
         p_ops->read_info[pull_ptr].opcode == CACHE_OP_PUT);
  if (p_ops->read_info[pull_ptr].opcode == OP_ACQUIRE_FLIP_BIT)
    assert(!acq_second_round_to_flip_bit && !insert_write_flag && !write_local_kvs);
  if(p_ops->read_info[pull_ptr].opcode == CACHE_OP_GET) assert(epoch_id > 0);
  if (p_ops->read_info[pull_ptr].opcode == CACHE_OP_GET)
    assert(!acq_second_round_to_flip_bit && !insert_write_flag);
  if (p_ops->read_info[pull_ptr].opcode == OP_RELEASE)
    assert(!acq_second_round_to_flip_bit && insert_write_flag && !write_local_kvs);
  if (p_ops->read_info[pull_ptr].opcode == CACHE_OP_PUT)
    assert(!acq_second_round_to_flip_bit && insert_write_flag && write_local_kvs);
  if (p_ops->read_info[pull_ptr].opcode == OP_ACQUIRE_FLIP_BIT)
    assert(!acq_second_round_to_flip_bit && !insert_write_flag && !write_local_kvs);

  if (DEBUG_READS || DEBUG_TS)
    green_printf("Committing read at index %u, it has seen %u times the same timestamp\n",
                 pull_ptr, p_ops->read_info[pull_ptr].times_seen_ts);

}

//
static inline void check_read_fifo_metadata(struct pending_ops *p_ops, struct r_message *r_mes,
                                            uint16_t t_id)
{
  assert(p_ops->virt_r_size <= MAX_ALLOWED_R_SIZE);
  assert(p_ops->r_size <= p_ops->virt_r_size);
  assert(r_mes->coalesce_num <= MAX_R_COALESCE);
  assert(p_ops->r_session_id[p_ops->r_push_ptr] <= SESSIONS_PER_THREAD);
}

static inline void check_global_sess_id(uint8_t machine_id, uint16_t t_id,
                                        uint16_t session_id, uint16_t glob_sess_id)
{
  assert(glob_ses_id_to_m_id(glob_sess_id) == machine_id);
  assert(glob_ses_id_to_t_id(glob_sess_id) == t_id);
  assert(glob_ses_id_to_sess_id(glob_sess_id) == session_id);
}


// Do some preliminary checks for the write message,
// the while loop is there to wait for the entire message to be written (currently not useful)
static inline void check_the_polled_write_message(volatile struct w_message *w_mes,
                                                  uint32_t index, uint32_t polled_writes, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    uint32_t debug_cntr = 0;
    if (w_mes->w_num == 0) {
      red_printf("Wrkr %u received a write with w_num %u, op %u from machine %u with lid %lu \n",
                 t_id, w_mes->w_num, w_mes->write[0].opcode, w_mes->m_id, *(uint64_t *) w_mes->l_id);
      assert(false);
    }
    while (w_mes->write[w_mes->w_num - 1].opcode != CACHE_OP_PUT) {
      if (w_mes->write[w_mes->w_num - 1].opcode == OP_RELEASE) return;
      if (w_mes->write[w_mes->w_num - 1].opcode == OP_ACQUIRE) return;
      if (w_mes->write[w_mes->w_num - 1].opcode == OP_RELEASE_BIT_VECTOR) return;
      if (w_mes->write[w_mes->w_num - 1].opcode == ACCEPT_OP) return;
      if (w_mes->write[w_mes->w_num - 1].opcode == COMMIT_OP) return;
      if (ENABLE_ASSERTIONS) {
        assert(false);
        debug_cntr++;
        if (debug_cntr == B_4_) {
          red_printf("Wrkr %d stuck waiting for a write to come index %u coalesce id %u\n",
                     t_id, index, w_mes->w_num - 1);
          print_wrkr_stats(t_id);
          debug_cntr = 0;
        }
      }
    }
    if (polled_writes + w_mes->w_num > MAX_INCOMING_W) {
      assert(false);
    }
  }
}

// When polling for writes
static inline void check_a_polled_write(struct write* write, uint16_t w_i, uint16_t w_num, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (write->opcode != CACHE_OP_PUT && write->opcode != OP_RELEASE &&
        write->opcode != OP_ACQUIRE && write->opcode != ACCEPT_OP &&
        write->opcode != OP_RELEASE_BIT_VECTOR && write->opcode != COMMIT_OP)
      red_printf("Wrkr %u Receiving write : Opcode %u, i %u/%u \n", t_id, write->opcode, w_i, w_num);
    if ((*(uint32_t *) write->version) % 2 != 0) {
      red_printf("Wrkr %u :Odd version %u, m_id %u \n", t_id, *(uint32_t *) write->version, write->m_id);
    }
  }
}


// When polling for writes
static inline void print_polled_write_message_info(struct w_message *w_mes, uint32_t index, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (DEBUG_WRITES && w_mes->write[0].opcode != ACCEPT_OP)
      printf("Worker %u sees a write Opcode %d at offset %d, l_id %lu  \n",
             t_id, w_mes->write[0].opcode, index, *(uint64_t *) w_mes->l_id);
    else if (DEBUG_RMW && w_mes->write[0].opcode == ACCEPT_OP) {
      struct accept_message *acc_mes = (struct accept_message *) w_mes;
      printf("Worker %u sees an Accept: opcode %d at offset %d, rmw_id %lu, "
               "glob_sess_id %u, log_no %u, coalesce_num %u \n",
             t_id, acc_mes->acc[0].opcode, index, acc_mes->acc[0].t_rmw_id,
             *(uint16_t *) acc_mes->acc[0].glob_sess_id, *(uint32_t *) acc_mes->acc[0].log_no,
             acc_mes->acc_num);
    }
    else if (DEBUG_RMW && w_mes->write[0].opcode == COMMIT_OP) {
      struct commit_message *com_mes = (struct commit_message *) w_mes;
      printf("Worker %u sees a Commit: opcode %d at offset %d, l_id %lu, "
               "glob_sess_id %u, log_no %u, coalesce_num %u \n",
             t_id, com_mes->com[0].opcode, index, *(uint64_t *) com_mes->com[0].t_rmw_id,
             *(uint16_t *) com_mes->com[0].glob_sess_id, *(uint32_t *) com_mes->com[0].log_no,
             com_mes->com_num);
    }
  }
}


static inline void count_stats_on_receiving_w_mes_reset_w_num(struct w_message *w_mes,
                                                              uint8_t w_num, uint16_t t_id)
{
  if (ENABLE_STAT_COUNTING) {
    if (ENABLE_ASSERTIONS) t_stats[t_id].per_worker_writes_received[w_mes->m_id] += w_num;
    t_stats[t_id].received_writes += w_num;
    t_stats[t_id].received_writes_mes_num++;
  }
  if (ENABLE_ASSERTIONS) w_mes->w_num = 0;
}

static inline void check_accept_mes(struct accept_message *acc_mes)
{
  assert(acc_mes->acc_num == 0); // the w_num gets reset after polling a write
  assert(acc_mes->m_id < MACHINE_NUM);
  assert(acc_mes->acc[0].opcode == ACCEPT_OP);
}

// Called when inserting a read reply
static inline void check_the_read_opcode(uint8_t read_opcode)
{
  assert(read_opcode == CACHE_OP_GET || read_opcode == OP_ACQUIRE || read_opcode == OP_ACQUIRE_FLIP_BIT ||
         read_opcode == PROPOSE_OP || read_opcode == ACCEPT_OP || read_opcode == OP_ACQUIRE_FP ||
         read_opcode == CACHE_OP_GET_TS);
}

// Called when forging a read reply work request
static inline void checks_and_prints_when_forging_r_rep_wr(uint8_t coalesce_num, uint16_t mes_i,
                                                           struct ibv_sge *send_sgl, uint32_t r_rep_i,
                                                           struct r_rep_message *r_rep_mes,
                                                           struct r_rep_fifo *r_rep_fifo,
                                                           uint16_t t_id)
{
  if (DEBUG_READS) {
    for (uint16_t i = 0; i < coalesce_num; i++)
      yellow_printf("Wrkr: %u, Read Reply no %d, opcode :%u message mes_size %d \n",
                    t_id, i, r_rep_mes->opcode, send_sgl[mes_i].length);
    green_printf("Wrkr %d : I send a READ REPLY message of %u r reps with mes_size %u, with lid: %u to machine %u \n",
                 t_id, coalesce_num, send_sgl[mes_i].length,
                 *(uint64_t *) r_rep_mes->l_id, r_rep_fifo->rem_m_id[r_rep_i]);
  }
  if (ENABLE_ASSERTIONS) {
    assert(send_sgl[mes_i].length < MTU);
    assert(send_sgl[mes_i].length <= R_REP_SEND_SIZE);
    assert(r_rep_fifo->rem_m_id[r_rep_i] < MACHINE_NUM);
    assert(coalesce_num > 0);
  }
}

// called when sending read replies
static inline void print_check_count_stats_when_sending_r_rep(struct r_rep_fifo *r_rep_fifo,
                                                              uint8_t coalesce_num,
                                                              uint16_t mes_i, uint16_t t_id)
{
  if (DEBUG_READS)
    printf("Wrkr %d has %u read replies to send \n", t_id, r_rep_fifo->total_size);
  if (ENABLE_ASSERTIONS) {
    assert(r_rep_fifo->total_size >= coalesce_num);
    assert(mes_i < MAX_R_REP_WRS);
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[t_id].r_reps_sent += coalesce_num;
    t_stats[t_id].r_reps_sent_mes_num++;
  }
}

// check the local id of a read reply
static inline void check_r_rep_l_id(uint64_t l_id, uint8_t r_rep_num, uint64_t pull_lid,
                                    uint32_t r_size, uint16_t t_id)
{
  assert(l_id + r_rep_num <= pull_lid + r_size);
  if ((l_id + r_rep_num < pull_lid) && (!USE_QUORUM)) {
    red_printf("Wrkr :%u Error on the l_id of a received read reply: "
                 "l_id %u, r_rep_num %u, pull_lid %u, r_size %u \n", t_id, l_id, r_rep_num, pull_lid, r_size);
    assert(false);
  }
}

// spin waiting for the r_rep
static inline void wait_until_the_entire_r_rep(volatile struct r_rep_big *stuck_r_rep,
                                               volatile struct r_rep_message_ud_req *r_rep_buf,
                                               uint16_t r_rep_i, uint32_t r_rep_pull_ptr,
                                               struct pending_ops *p_ops, uint16_t stuck_byte_ptr,
                                               uint16_t t_id)
{
  uint32_t debug_cntr = 0;
  while (stuck_r_rep->opcode < TS_SMALLER || stuck_r_rep->opcode > TS_GREATER) {
    if (ENABLE_ASSERTIONS) {
      assert(false);
      debug_cntr++;
      if (debug_cntr == M_512_) {
        red_printf("Wrkr %d stuck waiting for a r_rep_mes to come, stuck opcode %u, stuck byte_ptr %u, r_rep_i %u, stuck ptr %lu  \n",
                   t_id, stuck_r_rep->opcode, stuck_byte_ptr, r_rep_i, stuck_r_rep);
        r_rep_pull_ptr = (r_rep_pull_ptr - 1) % R_REP_BUF_SLOTS;
        volatile struct r_rep_message *r_rep_mes = &r_rep_buf[r_rep_pull_ptr].r_rep_mes;
        uint64_t l_id = *(uint64_t *) r_rep_mes->l_id;
        uint8_t message_opc = r_rep_mes->opcode;
        cyan_printf("Wrkr %d, polling on index %u, polled opc %u, 1st r_rep_mes opcode: %u, l_id %lu, expected l_id %lu\n",
                    t_id, r_rep_pull_ptr, message_opc, r_rep_mes->opcode, l_id, p_ops->local_r_id);

        uint16_t byte_ptr = R_REP_MES_HEADER;
        for (uint16_t i = 0; i < r_rep_mes->coalesce_num; i++) {
          struct r_rep_big *r_rep = (struct r_rep_big *) (((void *) r_rep_mes) + byte_ptr);
          yellow_printf("R_rep %u/%u, opcode %u, byte_ptr %u , ptr %lu/%lu\n", i, r_rep_mes->coalesce_num, r_rep->opcode, byte_ptr,
                        r_rep, stuck_r_rep);
          if (r_rep->opcode == TS_GREATER) byte_ptr += R_REP_SIZE;
          else if (r_rep->opcode == TS_GREATER_TS_ONLY) byte_ptr += R_REP_ONLY_TS_SIZE;
          else byte_ptr++;
        }
        for (int i = 0; i < R_REP_BUF_SLOTS; ++i) {
          if (r_rep_buf[i].r_rep_mes.opcode == READ_REPLY) {
            green_printf("GOOD OPCODE in index %d, l_id %u \n", i, *(uint64_t *) r_rep_buf[i].r_rep_mes.l_id);
          } else
            red_printf("BAD OPCODE in index %d, l_id %u, from machine: %u  \n", i,
                       *(uint64_t *) r_rep_buf[i].r_rep_mes.l_id,
                       r_rep_buf[i].r_rep_mes.m_id);
        }
        print_wrkr_stats(t_id);
        debug_cntr = 0;
      }
    }
  }
}


// Check when inserting a read
static inline void check_previous_read_lid(uint8_t source, struct cache_op *read, uint64_t message_l_id,
                                           struct r_message *r_mes, uint32_t r_mes_ptr, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (source == FROM_TRACE) assert(read->opcode != PROPOSE_OP);
    if (message_l_id > MAX_R_COALESCE) {
      uint32_t prev_r_mes_ptr = (r_mes_ptr + R_FIFO_SIZE - 1) % R_FIFO_SIZE;
      if (r_mes[prev_r_mes_ptr].read[0].opcode != PROPOSE_OP) {
        uint64_t prev_l_id = *(uint64_t *) r_mes[prev_r_mes_ptr].l_id;
        uint8_t prev_coalesce = r_mes[prev_r_mes_ptr].coalesce_num;
        if (message_l_id != prev_l_id + prev_coalesce) {
          red_printf("Wrkr: %u Read: Current message l_id %u, previous message l_id %u , "
                       "prev push ptr %u, current push ptr %u ,previous coalesce %u, previous opcode %u \n",
                     t_id, message_l_id, prev_l_id, prev_r_mes_ptr, r_mes_ptr,
                     prev_coalesce, r_mes[prev_r_mes_ptr].read[0].opcode);
        }
      }
    }
  }
}

// Check when inserting a read
static inline void check_read_state_and_key(struct pending_ops *p_ops, uint32_t r_ptr, uint8_t source, struct r_message *r_mes,
                                            struct cache_op *read, uint32_t r_mes_ptr, uint8_t inside_r_ptr, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (p_ops->r_state[r_ptr] != INVALID)
      red_printf("Worker %u r_state %d at r_ptr %u, cache hits %lu, r_size %u \n",
                 t_id, p_ops->r_state[r_ptr], r_ptr,
                 t_stats[t_id].cache_hits_per_thread, p_ops->r_size);
    //printf("Sent %d, Valid %d, Ready %d \n", SENT, VALID, READY);
    assert(p_ops->r_state[r_ptr] == INVALID);
    if (source == FROM_TRACE)
      assert(keys_are_equal((struct cache_key *) (((void *) &r_mes[r_mes_ptr].read[inside_r_ptr]) - 3),
                            (struct cache_key *) read));
  }
}

// Check if the key in the entry matches the key in the KVS
static inline void check_entry_validity_with_cache_op(struct cache_op *op, uint32_t entry)
{
  if (ENABLE_ASSERTIONS) {
    struct key *key = (struct key *) (((void *) op) + sizeof(cache_meta));
    struct key *entry_key = &rmw.entry[entry].key;
    if (!true_keys_are_equal(key, entry_key)) {
      print_true_key(key);
      print_true_key(entry_key);
      assert(false);
    }
  }
}

// Returns true if the incoming key and the entry key are equal
static inline bool check_entry_validity_with_key(struct key *incoming_key, uint32_t entry)
{
  if (ENABLE_ASSERTIONS) {
    struct key *entry_key = &rmw.entry[entry].key;
    return true_keys_are_equal(incoming_key, entry_key);
  }
  return true;
}

// When polling an ack message
static inline void check_ack_message_count_stats(struct pending_ops* p_ops, struct ack_message* ack,
                                                 uint32_t index, uint16_t ack_num, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(ack_num > 0);
    assert(ack->opcode == CACHE_OP_ACK);
    //      wait_for_the_entire_ack((volatile struct ack_message *)ack, t_id, index);
    assert(ack->m_id < MACHINE_NUM);
    uint64_t l_id = *(uint64_t *) (ack->local_id);
    uint64_t pull_lid = p_ops->local_w_id;
    assert(l_id + ack_num <= pull_lid + p_ops->w_size);
    if ((l_id + ack_num < pull_lid) && (!USE_QUORUM)) {
      red_printf("l_id %u, ack_num %u, pull_lid %u \n", l_id, ack_num, pull_lid);
      assert(false);
    }
    if (DEBUG_ACKS)
      yellow_printf(
        "Wrkr %d  polled ack opcode %d with %d acks for l_id %lu, oldest lid %lu, at offset %d from machine %u \n",
        t_id, ack->opcode, ack_num, l_id, pull_lid, index, ack->m_id);
  }
  if (ENABLE_STAT_COUNTING) {
    if (ENABLE_ASSERTIONS) {
      t_stats[t_id].per_worker_acks_received[ack->m_id] += ack_num;
      t_stats[t_id].per_worker_acks_mes_received[ack->m_id]++;
    }
    t_stats[t_id].received_acks += ack_num;
    t_stats[t_id].received_acks_mes_num++;
  }
}


// When polling acks: more precisely when inspecting each l_id acked
static inline void  check_ack_and_print(struct pending_ops* p_ops, uint16_t ack_i, uint32_t ack_ptr,
                                        uint16_t  ack_num, uint64_t l_id, uint64_t pull_lid, uint16_t t_id) {
  if (ENABLE_ASSERTIONS) {
    if (DEBUG_WRITES && (ack_ptr == p_ops->w_push_ptr)) {
      uint32_t origin_ack_ptr = (ack_ptr - ack_i + PENDING_WRITES) % PENDING_WRITES;
      red_printf("Worker %u: Origin ack_ptr %u/%u, acks %u/%u, w_pull_ptr %u, w_push_ptr % u, w_size %u \n",
                 t_id, origin_ack_ptr, (p_ops->w_pull_ptr + (l_id - pull_lid)) % PENDING_WRITES,
                 ack_i, ack_num, p_ops->w_pull_ptr, p_ops->w_push_ptr, p_ops->w_size);

    }
    assert(p_ops->acks_seen[ack_ptr] < REM_MACH_NUM);
  }
}

// Check the key of the cache_op, the KV and the global RMW entry
static inline void check_keys_with_two_cache_ops(struct cache_op* op, struct cache_op* kv_ptr,
                                                 uint32_t entry)
{
  if (ENABLE_ASSERTIONS) {
    struct key *rmw_entry_key = &rmw.entry[entry].key;
    struct key *op_key = (struct key *) (((void *) op) + sizeof(cache_meta));
    struct key *kv_key = (struct key *) (((void *) kv_ptr) + sizeof(cache_meta));
    if (!(true_keys_are_equal(rmw_entry_key, kv_key) &&
          (true_keys_are_equal(rmw_entry_key, op_key)) &&
          (true_keys_are_equal(kv_key, op_key)))) {
      print_true_key(rmw_entry_key);
      print_true_key(kv_key);
      print_true_key(op_key);
      printf("entry %u/%u kv ptr %p \n", entry, *(uint32_t *) kv_ptr->value, (void *)kv_ptr->value);
      assert(false);
    }
  }
}

// Check the key of the cache_op, the KV and the global RMW entry
static inline void check_keys_with_one_cache_op(struct key *com_key, struct cache_op *kv_ptr,
                                                uint32_t entry)
{
  if (ENABLE_ASSERTIONS) {
    struct key *rmw_entry_key = &rmw.entry[entry].key;
    struct key *kv_key = (struct key *) (((void *) kv_ptr) + sizeof(cache_meta));
    if (!(true_keys_are_equal(rmw_entry_key, kv_key) &&
          (true_keys_are_equal(rmw_entry_key, com_key)) &&
          (true_keys_are_equal(kv_key, com_key)))) {
      print_true_key(rmw_entry_key);
      print_true_key(kv_key);
      print_true_key(com_key);
      printf("entry %u/%u kv ptr %p \n", entry, *(uint32_t *) kv_ptr->value, (void *)kv_ptr->value);
      assert(false);
    }
  }
}

// Before batching to cache we give all ops an odd version, check if it were changed
static inline void check_version_after_batching_trace_to_cache(struct cache_op* op,
                                                               struct cache_resp* resp, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (op->key.meta.version % 2 != 0) {
      red_printf("Wrkr %u, Trace to cache: Version not even: %u, opcode %u, resp %u \n",
      t_id, op->key.meta.version, op->opcode, resp->type);
    }
    my_assert(op->key.meta.version % 2 == 0, "Trace to cache: Version must be even after cache");
  }
}

// When removing writes
static inline void check_after_removing_writes(struct pending_ops* p_ops, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (p_ops->w_state[p_ops->w_pull_ptr] >= READY_COMMIT) {
      red_printf("W state = %u at ptr  %u, size: %u \n",
                 p_ops->w_state[p_ops->w_pull_ptr], p_ops->w_pull_ptr, p_ops->w_size);
      assert(false);
    }
    if (p_ops->w_state[(p_ops->w_pull_ptr + 1) % PENDING_WRITES] >= READY_COMMIT) {
      red_printf("W state = %u at ptr %u, push ptr %u , size: %u \n",
                 p_ops->w_state[(p_ops->w_pull_ptr + 1) % PENDING_WRITES], (p_ops->w_pull_ptr + 1) % PENDING_WRITES,
                 p_ops->w_push_ptr, p_ops->w_size);
      red_printf("W state = %u at ptr  %u, size: %u \n",
                 p_ops->w_state[p_ops->w_pull_ptr], p_ops->w_pull_ptr, p_ops->w_size);
    }
  }
}

// Check that the counter for propose replies add up(SAME FOR ACCEPTS AND PROPS)
static inline void check_sum_of_reps(struct rmw_local_entry *loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->rmw_reps.tot_replies == sum_of_reps(loc_entry));
    assert(loc_entry->rmw_reps.tot_replies <= REM_MACH_NUM);
  }
}

// when a ptr is passed as an rmw rep, makes sure it's valid
static inline void check_ptr_is_valid_rmw_rep(struct rmw_rep_last_committed* rmw_rep)
{
  if (ENABLE_ASSERTIONS) {
    assert(rmw_rep->opcode == RMW_ID_COMMITTED || rmw_rep->opcode == LOG_TOO_SMALL);
    if ((*(uint32_t *) rmw_rep->ts.version % 2  != 0) )
      red_printf("Checking the ptr to rmw_rep, version %u \n", (*(uint32_t *) rmw_rep->ts.version));
    assert(*(uint32_t *) rmw_rep->ts.version % 2  == 0 );
    if (rmw_rep->opcode == RMW_ID_COMMITTED ) assert(*(uint32_t *) rmw_rep->ts.version > 0 ); // it can be 0 if LOG_TOO_SMALL and the other side has not yet committed log 0
    assert(*(uint16_t *) rmw_rep->glob_sess_id < GLOBAL_SESSION_NUM);
  }
}

static inline void check_loc_entry_metadata_is_reset(struct rmw_local_entry* loc_entry)
{
  if (ENABLE_ASSERTIONS) { // make sure the loc_entry is correctly set-up
    if (loc_entry->help_loc_entry == NULL) {
      //red_printf("The help_loc_ptr is NULL. The reason is typically that help_loc_entry was passed to the function "
        //           "instead of loc entry to check \n");
      assert(loc_entry->state == INVALID_RMW);
    }
    else {
      assert(loc_entry->help_loc_entry->state == INVALID_RMW);
      assert(loc_entry->rmw_reps.tot_replies == 0);
      assert(loc_entry->back_off_cntr == 0);
    }
  }
}

// first argument here should be the state and then a bunch of allowed flags
static inline void check_state_with_allowed_flags(int num_of_flags, ...)
{
  if (ENABLE_ASSERTIONS) {
    va_list valist;
    va_start(valist, num_of_flags);
    bool found = false;
    int state = va_arg(valist, int);
    assert(num_of_flags < 10);
    int flags[10];
    for (uint8_t i = 0; i < num_of_flags - 1; i++) {
      flags[i] = va_arg(valist, int);
      if (state == flags[i]) found = true;
    }
    if (!found) {
      red_printf("Checking state failed state: %u, Allowed flags: \n", state);
      for (uint8_t i = 0; i < num_of_flags - 1; i++) {
        red_printf("%u ", flags[i]);
      }
      red_printf("\n");
      assert(false);
    }

    va_end(valist);
  }
}

// first argument here should be the state and then a bunch of disallowed flags
static inline void check_state_with_disallowed_flags(int num_of_flags, ...)
{
  cyan_printf("Checking state\n");
  if (ENABLE_ASSERTIONS) {
    va_list valist;
    va_start(valist, num_of_flags);
    bool found = false;
    int state = va_arg(valist, int);
    assert(num_of_flags < 10);
    int flags[10];
    for (uint8_t i = 0; i < num_of_flags - 1; i++) {
      flags[i] = va_arg(valist, int);
      if (state == flags[i]) found = true;
    }
    if (found) {
      red_printf("Checking state failed state: %u, Disallowed flags: \n", state);
      for (uint8_t i = 0; i < num_of_flags - 1; i++) {
        red_printf("%u ", flags[i]);
      }
      red_printf("\n");
      assert(false);
    }

    va_end(valist);
  }
}

// When going to ack an accept/propose because the log it refers to is higher than what we are working on
static inline void check_that_log_is_too_high(struct rmw_entry *glob_entry, uint32_t log_no)
{
  if (ENABLE_ASSERTIONS) {
    assert(log_no > glob_entry->last_committed_log_no);
    if (log_no == glob_entry->last_committed_log_no + 1) {
      if (glob_entry->state != INVALID_RMW) {
        red_printf("Checking_that_log_is to high: log_no %u/%u, glob committed last_log %u, state %u \n",
                   log_no, glob_entry->log_no, glob_entry->last_committed_log_no, glob_entry->state);
        assert(false);
      }
    } else if (glob_entry->state != INVALID_RMW)
      assert(glob_entry->last_committed_log_no + 1);
  }
}

//
static inline void check_log_nos_of_glob_entry(struct rmw_entry *glob_entry, char* message, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (glob_entry->state != INVALID_RMW) {
      if (rmw_ids_are_equal(&glob_entry->last_committed_rmw_id, &glob_entry->rmw_id)) {
        red_printf("Wrkr %u Last committed rmw id is equal to current, Glob_entry state %u, com log/log %u/%u "
                     "rmw id %u/%u, glob_sess id %u/%u : %s \n",
                   t_id, glob_entry->state, glob_entry->last_committed_log_no, glob_entry->log_no,
                   glob_entry->last_committed_rmw_id.id, glob_entry->rmw_id.id,
                   glob_entry->last_committed_rmw_id.glob_sess_id,
                   glob_entry->rmw_id.glob_sess_id, message);
        assert(false);
      }

      if (glob_entry->last_committed_log_no >= glob_entry->log_no) {
        red_printf("Wrkr %u t_id, Glob_entry state %u, com log/log %u/%u : %s \n",
                   t_id, glob_entry->state, glob_entry->last_committed_log_no, glob_entry->log_no, message);
        assert(false);
      }
    }
  }
}

//
static inline void check_for_same_ts_as_already_proposed(struct cache_op *kv_ptr, struct propose *prop, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->opcode == KEY_HAS_BEEN_RMWED) {
      struct rmw_entry *glob_entry = &rmw.entry[*(uint32_t *) kv_ptr->value];
      if (glob_entry->state == PROPOSED) {
        if (compare_netw_ts_with_ts(&prop->ts, &glob_entry->new_ts) == EQUAL) {
          red_printf("Wrkr %u Received a proposal with same TS as an already acked proposal, "
                       " prop log/glob log %u/%u, glob sess %u/%u, rmw_id %u/%u, version %u/%u, m_id %u/%u \n",
                     t_id, *(uint32_t *) prop->log_no, glob_entry->log_no, *(uint16_t *) prop->glob_sess_id,
                     glob_entry->rmw_id.glob_sess_id, *(uint64_t *) prop->t_rmw_id, glob_entry->rmw_id.id,
                     *(uint32_t *) prop->ts.version, glob_entry->new_ts.version, prop->ts.m_id, glob_entry->new_ts.m_id);
          assert(false);
        }
      }
    }

  }
}

static inline void verify_paxos(struct rmw_local_entry *loc_entry, uint16_t t_id)
{
  if (VERIFY_PAXOS && is_global_ses_id_local(loc_entry->rmw_id.glob_sess_id, t_id)) {
    //if (committed_log_no != *(uint32_t *)loc_entry->value_to_write)
    //  red_printf ("vale_to write/log no %u/%u",
     //             *(uint32_t *)loc_entry->value_to_write, committed_log_no );
    fprintf(rmw_verify_fp[t_id], "%u %u %u \n", loc_entry->key.bkt, 0, loc_entry->accepted_log_no);
  }
}

/* ---------------------------------------------------------------------------
//------------------------------ CONF BITS HANDLERS---------------------------
//---------------------------------------------------------------------------*/
/* 1. On detecting a failure bring the corresponding machine's bit_vec to DOWN STABLE
 * 2. On receiving a Release with a bit vector bring all the bits set in the vector to DOWN STABLE
 * 3. On inserting an Acquire from the trace you find out that a remote Release has raised
 *    the conf bit for the local machine; it is necessary to increase the epoch id because the local machine
 *    may be the common machine of the release quorum and acquire quorum and thus the failure may not be visible
 *    to the acquire by the rest of the nodes it will reach. At that point the acquire must raise its conf bit
 *    to UP_STABLE, such that it can catch further failures
 * 4. On receiving the first round of an Acquire, bring the bit to DOWN_TRANSIENT_OWNED
 *    if it's DOWN _STABLE and give the acquire ownership of the bit.
 *    Subsequent Acquires will all get ownership of the bit: each bit has
 *    SESSIONS_PER_THREAD * WORKERS_PER_MACHINE owner-slots, such that it can accommodate all
 *    possible acquires from each machine
 * 5. On receiving the second round of an Acquire that owns a bit (DOWN_TRANSIENT_OWNED)
 *    bring that bit to UP_STABLE*/

// Covers 1 & 2. Set a bit to new_state. 1 is not covered directly by this function.
// function "set_send_and_conf_bit_after_detecting_failure" calls this one though to
// be more efficient with debugging information
static inline void set_conf_bit_to_new_state(const uint16_t t_id, const uint16_t m_id, uint8_t new_state)
{
  if (conf_bit_vec[m_id].bit != new_state) {
    while (!atomic_flag_test_and_set_explicit(&conf_bit_vec[m_id].lock, memory_order_acquire));
    conf_bit_vec[m_id].bit = new_state;
    atomic_flag_clear_explicit(&conf_bit_vec[m_id].lock, memory_order_release);
  }
}


// 3. Query the conf bit vector for the local bit when starting an acquire
//    if some remote release has raised the bit, then incrase epoch id and flip the bit
static inline void on_starting_an_acquire_query_the_conf(const uint16_t t_id)
{
  if (unlikely(conf_bit_vec[machine_id].bit == DOWN_STABLE)) {
    epoch_id++;
    set_conf_bit_to_new_state(t_id, (uint16_t) machine_id, UP_STABLE);
    if (DEBUG_BIT_VECS)
      yellow_printf("Thread %u, acquire increases the epoch id "
                      "as a remote release has notified the machine it has lost messages, new epoch id %u\n", t_id, epoch_id);
  }
}

// 4. On receiving the first round of an Acquire bring the bit to DOWN_TRANSIENT_OWNED
// and register the acquire's local_r_id as one of the bit's owners
// Return the opcode an acquire should have going in to the cache
// If it found no failure it should be OP_ACQUIRE
// If it found a failure it should be OP_ACQUIRE_FP
static inline uint8_t take_ownership_of_a_conf_bit(const uint16_t t_id, const uint64_t local_r_id,
                                                const uint16_t acq_m_id)
{
  if (conf_bit_vec[acq_m_id].bit == UP_STABLE) return OP_ACQUIRE;
  if (DEBUG_BIT_VECS)
    yellow_printf("Wrkr %u An acquire from machine %u  is looking to take ownership "
                    "of a bit: local_r_id %u \n", t_id, acq_m_id, local_r_id);
  bool owned_a_failure = false;
  // if it's down, own it, even if it is already owned

  while (!atomic_flag_test_and_set_explicit(&conf_bit_vec[acq_m_id].lock, memory_order_acquire));
  if (conf_bit_vec[acq_m_id].bit != UP_STABLE) {
    uint32_t ses_i = conf_bit_vec[acq_m_id].sess_num[t_id];
    conf_bit_vec[acq_m_id].owners[t_id][ses_i] = local_r_id;
    MOD_ADD(conf_bit_vec[acq_m_id].sess_num[t_id], SESSIONS_PER_THREAD);
    owned_a_failure = true;
    conf_bit_vec[acq_m_id].bit = DOWN_TRANSIENT_OWNED;
  }
  atomic_flag_clear_explicit(&conf_bit_vec[acq_m_id].lock, memory_order_release);

  if (DEBUG_BIT_VECS && owned_a_failure) {
    uint32_t ses_i = (conf_bit_vec[acq_m_id].sess_num[t_id] +SESSIONS_PER_THREAD - 1) % SESSIONS_PER_THREAD;
    green_printf("Wrkr %u acquire from machine %u got ownership of its failure, "
                   "bit %u  owned t_id %u, owned local_r_id %u/%u \n",
                 t_id, acq_m_id, conf_bit_vec[acq_m_id].bit,
                 t_id, local_r_id, conf_bit_vec[acq_m_id].owners[t_id][ses_i]);
  }
  return  OP_ACQUIRE_FP;
}

// 5. On receiving the second round of an Acquire that owns a bit (DOWN_TRANSIENT_OWNED)
//    bring that bit to UP_STABLE
static inline void raise_conf_bit_iff_owned(const uint16_t t_id, const uint64_t local_r_id,
                                            const uint16_t acq_m_id)
{
  if (DEBUG_BIT_VECS)
    yellow_printf("Wrkr %u An acquire from machine %u  is looking if it owns a failure local_r_id %u \n",
                  t_id, acq_m_id, local_r_id);
  // First change the state  of the owned bits
  bool bit_gets_flipped = false;
  if (conf_bit_vec[acq_m_id].bit != DOWN_TRANSIENT_OWNED) {
    return;
  }
  // Grab the lock
  while (!atomic_flag_test_and_set_explicit(&conf_bit_vec[acq_m_id].lock, memory_order_acquire));
  if (conf_bit_vec[acq_m_id].bit == DOWN_TRANSIENT_OWNED) {
    uint32_t max_sess = conf_bit_vec[acq_m_id].sess_num[t_id];
    for (uint32_t ses_i = 0; ses_i <= max_sess; ses_i++) {
      if (conf_bit_vec[acq_m_id].owners[t_id][ses_i] == local_r_id) {
        conf_bit_vec[acq_m_id].bit = UP_STABLE;
        bit_gets_flipped = true;
        break;
      }
    }
    if (bit_gets_flipped)  // "invalidate" all owners
      memset(conf_bit_vec[acq_m_id].sess_num, 0, WORKERS_PER_MACHINE * sizeof(*conf_bit_vec[acq_m_id].sess_num));
  }
  atomic_flag_clear_explicit(&conf_bit_vec[acq_m_id].lock, memory_order_release);


  if (DEBUG_BIT_VECS && bit_gets_flipped) {
    assert(conf_bit_vec[acq_m_id].bit == UP_STABLE);
    green_printf("Wrkr %u Acquire  from machine %u had ownership of its failure bit %u/%d, "
                   "owned t_id %u, owned local_w_id %u\n",
                 t_id, acq_m_id, conf_bit_vec[acq_m_id].bit, UP_STABLE,
                 t_id, local_r_id);
  }

}


/* ---------------------------------------------------------------------------
//------------------------------ SEND BITS HANDLERS---------------------------------------
//---------------------------------------------------------------------------*/
/* 1. On Detecting a failure bring the corresponding machine's bit_vec to DOWN STABLE
 * 2. On Sending a Release containing a failure, convert the corresponding bits to
 *    Transient and give ownership if there is none
 * 3. On the Release quoromizing a failure transition the corresponding bit_vec to UP_STABLE
 *    iff the Release owns them
 * 4. Create the bit vector to be sent with the first round of a Release */

/*
 * It may appear that the lock is not useful bit it is: to protect the t_id and local_w_id
 * if the bit was to be set to DOWN_TRANSIENT_OWNED by a mere CAS lock-free then when raising
 * an owned bit a CAS on the bit would have to be used then. But how can you be sure that you are
 * the owner and not someone else is not the owner with same local_w_id but has not yet changed the t_id?
 * Lock is used to protect owner_local_wid  and owner_t_id: there are other ways but this is the cleaner
 * */

// 1. Detect a failure: Bring a given bit of send bit_vec to state DOWN_STABLE
// this also calls the function to change the config_bit_vector
static inline void set_send_and_conf_bit_after_detecting_failure(const uint16_t t_id, const uint16_t m_id)
{
  if (DEBUG_BIT_VECS)
    yellow_printf("Wrkr %u handles Send and conf bit vec after failure to machine %u,"
                    " send bit %u, state %u, conf_bit %u \n",
                  t_id, m_id, send_bit_vector.bit_vec[m_id].bit, send_bit_vector.state,
                  conf_bit_vec[m_id].bit);

  if (send_bit_vector.bit_vec[m_id].bit != DOWN_STABLE) {
    while (!atomic_flag_test_and_set_explicit(&send_bit_vector.bit_vec[m_id].lock, memory_order_acquire));
    send_bit_vector.bit_vec[m_id].bit = DOWN_STABLE;
    atomic_flag_clear_explicit(&send_bit_vector.bit_vec[m_id].lock, memory_order_release);
  }

  // Do the exact same for the conf bit
  set_conf_bit_to_new_state(t_id, m_id, DOWN_STABLE);

  if (send_bit_vector.state != DOWN_STABLE) {
    while (!atomic_flag_test_and_set_explicit(&send_bit_vector.state_lock, memory_order_acquire));
    send_bit_vector.state = DOWN_STABLE;
    atomic_flag_clear_explicit(&send_bit_vector.state_lock, memory_order_release);
  }
  if (DEBUG_BIT_VECS)
    green_printf("Wrkr %u After: send bit %u, state %u, conf_bit %u \n",
                 t_id, send_bit_vector.bit_vec[m_id].bit, send_bit_vector.state,
                 conf_bit_vec[m_id].bit);
}

//2. On Sending a Release containing a failure
static inline void take_ownership_of_send_bits(const uint16_t t_id, const uint64_t local_w_id)
{
  if (DEBUG_BIT_VECS)
    yellow_printf("Wrkr %u A release is looking to take ownership of failures local_w_id %u \n", t_id, local_w_id);
  for (uint16_t m_i = 0; m_i < MACHINE_NUM; m_i++) {
    bool debug_flag = false;
    // if it's down but not owned then own it
    if (send_bit_vector.bit_vec[m_i].bit == DOWN_STABLE) {
      while (!atomic_flag_test_and_set_explicit(&send_bit_vector.bit_vec[m_i].lock, memory_order_acquire));
      if (send_bit_vector.bit_vec[m_i].bit == DOWN_STABLE) {
        send_bit_vector.bit_vec[m_i].bit = DOWN_TRANSIENT_OWNED;
        send_bit_vector.bit_vec[m_i].owner_t_id = t_id;
        send_bit_vector.bit_vec[m_i].owner_local_wr_id = local_w_id;
        if (DEBUG_BIT_VECS) debug_flag = true;
      }
      atomic_flag_clear_explicit(&send_bit_vector.bit_vec[m_i].lock, memory_order_release);
    }
    if (DEBUG_BIT_VECS && debug_flag)
      green_printf("Wrkr %u got ownership of failure on m_id %u, bit %u  owned t_id %u, owned local_w_id %u \n",
                   t_id, m_i, send_bit_vector.bit_vec[m_i].bit, send_bit_vector.bit_vec[m_i].owner_t_id,
                   send_bit_vector.bit_vec[m_i].owner_local_wr_id);
  }
}

// 3. After the frist round of a Release raise the bit_vec iff owned
static inline void raise_send_bit_iff_owned(const uint16_t t_id, const uint64_t local_w_id)
{
  if (DEBUG_BIT_VECS)
    yellow_printf("Wrkr %u A release is looking if it owns a failure local_w_id %u, total state %u \n",
                  t_id, local_w_id, send_bit_vector.state);
  bool there_are_failed_machines = false;
  // First change the state  of the owned bits
  for (uint16_t m_i = 0; m_i < MACHINE_NUM; m_i++) {
    bool debug_flag = false;
    // Compare and Swap on the bit (DOWN_TRANSIENT_OWNED --> UP STABLE)
    if (send_bit_vector.bit_vec[m_i].bit == DOWN_TRANSIENT_OWNED) { // if the bit_vec is owned
      if (send_bit_vector.bit_vec[m_i].owner_local_wr_id == local_w_id &&
          send_bit_vector.bit_vec[m_i].owner_t_id == t_id) { // if it is owned by me

        // Grab the lock
        while (!atomic_flag_test_and_set_explicit(&send_bit_vector.bit_vec[m_i].lock, memory_order_acquire));
          if (send_bit_vector.bit_vec[m_i].bit == DOWN_TRANSIENT_OWNED) { // Check again if the bit_vec is owned
            if (send_bit_vector.bit_vec[m_i].owner_local_wr_id == local_w_id &&
                send_bit_vector.bit_vec[m_i].owner_t_id == t_id) { // if it is owned by me
              send_bit_vector.bit_vec[m_i].bit = UP_STABLE;
              debug_flag = true;
            }
          }
        atomic_flag_clear_explicit(&send_bit_vector.bit_vec[m_i].lock, memory_order_release);
      }
      if (DEBUG_BIT_VECS && debug_flag)
        green_printf("Wrkr %u Release had ownership of failure on m_id %u, bit %u  owned t_id %u, owned local_w_id %u \n",
                     t_id, m_i, send_bit_vector.bit_vec[m_i].bit, send_bit_vector.bit_vec[m_i].owner_t_id,
                     send_bit_vector.bit_vec[m_i].owner_local_wr_id);
    }
    if (send_bit_vector.bit_vec[m_i].bit != UP_STABLE) there_are_failed_machines = true; // look out for any failed machines
  }

  if (!there_are_failed_machines && send_bit_vector.state != UP_STABLE) { // if no failed machines try to change state
    bool debug_flag = false;
    while (!atomic_flag_test_and_set_explicit(&send_bit_vector.state_lock, memory_order_acquire));
    for (uint16_t m_i = 0; m_i < MACHINE_NUM; m_i++) {
      if (send_bit_vector.bit_vec[m_i].bit != UP_STABLE) {
        there_are_failed_machines = true;
        break;
      }
    }
    if (!there_are_failed_machines) {
      send_bit_vector.state = UP_STABLE;
      debug_flag = true;
    }
    atomic_flag_clear_explicit(&send_bit_vector.state_lock, memory_order_release);
    if (DEBUG_BIT_VECS  && debug_flag)
      yellow_printf("Wrkr %u Release local_w_id %u, raised the total state %u \n",
                    t_id, local_w_id, send_bit_vector.state);
  }
}

// 4. Create a bit_vec vector to be put inside the release
static inline void create_bit_vector(uint8_t *bit_vector_to_send, uint16_t t_id)
{
  uint64_t bit_vect = 0;
  for (uint16_t i = 0; i < MACHINE_NUM; i++) {
    if (i == machine_id) continue;
    if (send_bit_vector.bit_vec[i].bit != UP_STABLE)
      bit_vect = bit_vect | machine_bit_id[i];
  }
  memcpy(bit_vector_to_send, (void *) &bit_vect, SEND_CONF_VEC_SIZE);
}


/* ---------------------------------------------------------------------------
//------------------------------ DRF-SPECIFIC UTILITY-------------------------
//---------------------------------------------------------------------------*/

// Increment the per-request counters
static inline void increment_per_req_counters(uint8_t opcode, uint16_t t_id)
{
  if (ENABLE_STAT_COUNTING) {
    if (opcode == CACHE_OP_PUT) t_stats[t_id].writes_per_thread++;
    else if (opcode == CACHE_OP_GET) t_stats[t_id].reads_per_thread++;
    else if (opcode == OP_ACQUIRE) t_stats[t_id].acquires_per_thread++;
    else if (opcode == OP_RELEASE) t_stats[t_id].releases_per_thread++;
  }
}

// In case of a miss in the KVS clean up the op, sessions and what not
static inline void clean_up_on_KVS_miss(struct cache_op *op, struct pending_ops *p_ops,
                                        struct latency_flags *latency_info, uint16_t t_id)
{
  if (op->opcode == OP_RELEASE || op->opcode == OP_ACQUIRE) {
    uint32_t session_id = 0;
    memcpy(&session_id, op, SESSION_BYTES);
    yellow_printf("Cache_miss, session %u \n", session_id);
    if (ENABLE_ASSERTIONS) assert(session_id < SESSIONS_PER_THREAD);
    p_ops->session_has_pending_op[session_id] = false;
    p_ops->all_sessions_stalled = false;
    t_stats[t_id].cache_hits_per_thread--;
    if (MEASURE_LATENCY && t_id == LATENCY_THREAD && machine_id == LATENCY_MACHINE &&
        latency_info->measured_req_flag != NO_REQ &&
        session_id == latency_info->measured_sess_id)
      latency_info->measured_req_flag = NO_REQ;
  }
}

// when committing register global_sess id as committed
static inline void register_committed_global_sess_id (uint16_t glob_sess_id, uint64_t rmw_id, uint16_t t_id)
{
  uint64_t tmp_rmw_id, debug_cntr = 0;
  do {
    if (ENABLE_ASSERTIONS) {
      debug_cntr++;
      if (debug_cntr > 100) {
        red_printf("stuck on registering glob sess id %u \n", debug_cntr);
        debug_cntr = 0;
      }
    }
    tmp_rmw_id = committed_glob_sess_rmw_id[glob_sess_id];
    if (rmw_id <= tmp_rmw_id) return;
  } while (atomic_compare_exchange_strong(&committed_glob_sess_rmw_id[glob_sess_id], &tmp_rmw_id, rmw_id));
}


// Fill a write message with a commit
static inline void fill_commit_message(struct commit_message* com_mes, struct rmw_local_entry* loc_entry, uint16_t t_id)
{
  struct commit *com = &com_mes->com[0];
  com->ts.m_id = loc_entry->new_ts.m_id;
  *(uint32_t *) com->ts.version = loc_entry->new_ts.version;
  memcpy(com->key, &loc_entry->key, TRUE_KEY_SIZE);
  com->opcode = COMMIT_OP;
  memcpy(com->value, loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
  *(uint64_t *) com->t_rmw_id = loc_entry->rmw_id.id;
  *(uint16_t *) com->glob_sess_id = loc_entry->rmw_id.glob_sess_id;
  *(uint32_t *) com->log_no = loc_entry->log_no;
  if (ENABLE_ASSERTIONS) {
    assert(*(uint32_t *) com->log_no > 0);
    assert(*(uint64_t *) com->t_rmw_id > 0);
  }
}

// Set up the message depending on where it comes from: trace, 2nd round of release, 2nd round of read etc.
static inline void write_bookkeeping_in_insertion_based_on_source
                  (struct pending_ops *p_ops, struct cache_op *write, const uint8_t source,
                   const uint32_t incoming_pull_ptr, uint8_t *inside_w_ptr_, uint32_t *w_mes_ptr_,
                   struct w_message *w_mes, struct read_info *r_info, const uint16_t t_id)
{
  my_assert(*inside_w_ptr_ < MAX_W_COALESCE, "Inside pointer must not point to the last message");
  uint8_t inside_w_ptr = *inside_w_ptr_;
  uint32_t w_mes_ptr = *w_mes_ptr_;
  my_assert(source <= FROM_COMMIT, "When inserting a write source is too high. Have you enabled lin writes?");

  if (source == FROM_TRACE) {
    memcpy(w_mes[w_mes_ptr].write[inside_w_ptr].version, (void *) &write->key.meta.version,
           4 + TRUE_KEY_SIZE + 2 + VALUE_SIZE);
    w_mes[w_mes_ptr].write[inside_w_ptr].m_id = (uint8_t) machine_id;
  }
  else if (unlikely(source == RELEASE_THIRD)) { // Second round of a release
    memcpy(&w_mes[w_mes_ptr].write[inside_w_ptr].m_id, (void *) &write->key.meta.m_id, W_SIZE);
    w_mes[w_mes_ptr].write[inside_w_ptr].opcode = OP_RELEASE_SECOND_ROUND;
    //if (DEBUG_SESSIONS)
     // cyan_printf("Wrkr %u: Changing the opcode from %u to %u of write %u of w_mes %u \n",
     //             t_id, write->opcode, w_mes[w_mes_ptr].write[inside_w_ptr].opcode, inside_w_ptr, w_mes_ptr);
    if (ENABLE_ASSERTIONS) assert (w_mes[w_mes_ptr].write[inside_w_ptr].m_id == (uint8_t) machine_id);
    if (DEBUG_QUORUM) {
      printf("Thread %u: Second round release, from ptr: %u to ptr %u, key: ", t_id, incoming_pull_ptr, p_ops->w_push_ptr);
      print_true_key((struct key*)w_mes[w_mes_ptr].write[inside_w_ptr].key);
    }
  }
  else if (source == FROM_COMMIT) {
    // always use a new slot for the commit
    if (inside_w_ptr > 0) {
      MOD_ADD(p_ops->w_fifo->push_ptr, W_FIFO_SIZE);
      w_mes_ptr = p_ops->w_fifo->push_ptr;
      w_mes[w_mes_ptr].w_num = 0;
      inside_w_ptr = 0;
    }
    fill_commit_message((struct commit_message*) &w_mes[w_mes_ptr], (struct rmw_local_entry*) write, t_id);
  }
  else { //source = FROM_READ: 2nd round of read/write/acquire/release
    // if the write is a release put it on a new message to
    // guarantee it is not batched with writes from the same session
    if (r_info->opcode == OP_RELEASE && inside_w_ptr > 0 && !EMULATE_ABD) {
      MOD_ADD(p_ops->w_fifo->push_ptr, W_FIFO_SIZE);
      w_mes_ptr = p_ops->w_fifo->push_ptr;
      w_mes[w_mes_ptr].w_num = 0;
      inside_w_ptr = 0;
    }
    memcpy(&w_mes[w_mes_ptr].write[inside_w_ptr], &r_info->ts_to_read, TS_TUPLE_SIZE + TRUE_KEY_SIZE);
    memcpy(w_mes[w_mes_ptr].write[inside_w_ptr].value, r_info->value, VALUE_SIZE);
    w_mes[w_mes_ptr].write[inside_w_ptr].opcode = r_info->opcode;
    if (ENABLE_ASSERTIONS) {
      assert(source == FROM_READ);
      if (!(r_info->opcode == CACHE_OP_PUT ||
            r_info->opcode == OP_RELEASE ||
            r_info->opcode == OP_ACQUIRE))
        red_printf("Wrkr %u Wrong opcode %u in the read_info when inserting a read \n",
                   t_id, r_info->opcode);
    }
  }
  // Make sure the pointed values are correct
  (*inside_w_ptr_) = inside_w_ptr;
  (*w_mes_ptr_) = w_mes_ptr;

}

// fill a read reply when insterting a read reply
static inline void fill_read_reply( struct r_rep_big *r_rep,struct r_rep_fifo * r_rep_fifo,
                                    struct network_ts_tuple *local_ts, struct network_ts_tuple *remote_ts,
                                    void *value, uint8_t r_rep_flag, uint16_t t_id) {
  enum ts_compare ts_comp = compare_netw_ts(local_ts, remote_ts);
  if (machine_id == 0 && R_TO_W_DEBUG) {
    if (ts_comp == EQUAL)
      green_printf("L/R:  m_id: %u/%u version %u/%u \n", local_ts->m_id, remote_ts->m_id,
                   *(uint32_t *) local_ts->version, *(uint32_t *) remote_ts->version);
    else
      red_printf("L/R:  m_id: %u/%u version %u/%u \n", local_ts->m_id, remote_ts->m_id,
                 *(uint32_t *) local_ts->version, *(uint32_t *) remote_ts->version);
  }
  switch (ts_comp) {
    case SMALLER: // local is smaller than remote
      //if (DEBUG_TS) printf("Read TS is smaller \n");
      r_rep->opcode = TS_SMALLER;
      break;
    case EQUAL:
      //if (DEBUG_TS) /printf("Read TS are equal \n");
      r_rep->opcode = TS_EQUAL;
      break;
    case GREATER: // local is greater than remote
      memcpy(&r_rep->ts, local_ts, TS_TUPLE_SIZE);
      if (r_rep_flag == READ_TS) {
        //This does not need the value, as it is going to do a write eventually
        r_rep->opcode = TS_GREATER_TS_ONLY;
        r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] += (R_REP_ONLY_TS_SIZE - R_REP_SMALL_SIZE);
      } else {
        if (DEBUG_TS) printf("Read TS is greater \n");
        r_rep->opcode = TS_GREATER;
        memcpy(r_rep->value, value, VALUE_SIZE);
        r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] += (R_REP_SIZE - R_REP_SMALL_SIZE);
      }
      break;
    default:
      if (ENABLE_ASSERTIONS) assert(false);
  }
}

// When forging a write
static inline void add_failure_to_release(struct pending_ops *p_ops,
                                          struct w_message *w_mes, uint32_t backward_ptr,
                                          uint16_t t_id)
{
  uint8_t bit_vector_to_send[SEND_CONF_VEC_SIZE] = {0};
  create_bit_vector(bit_vector_to_send, t_id);
  if (*(uint16_t *) bit_vector_to_send > 0) {
    // Save the overloaded bytes in some buffer, such that they can be used in the second round of the release
    memcpy(&p_ops->overwritten_values[SEND_CONF_VEC_SIZE * backward_ptr], w_mes->write[0].value,
           SEND_CONF_VEC_SIZE);
    memcpy(w_mes->write[0].value, &bit_vector_to_send, SEND_CONF_VEC_SIZE);
    // l_d can be used raw, because Release is guaranteed to be the first message
    take_ownership_of_send_bits(t_id, *(uint64_t *) w_mes->l_id);
    if (DEBUG_QUORUM)
      green_printf("Wrkr %u Sending a release with a vector bit_vec %u \n", t_id, *(uint16_t *) bit_vector_to_send);
    w_mes->write[0].opcode = OP_RELEASE_BIT_VECTOR;
    p_ops->ptrs_to_local_w[backward_ptr] = &w_mes->write[0];
    //if (DEBUG_SESSIONS)
    //  cyan_printf("Wrkr %u release is from session %u, session has pending op: %u\n",
    //             t_id, p_ops->w_session_id[backward_ptr],
    //             p_ops->session_has_pending_op[p_ops->w_session_id[backward_ptr]]);

  }
}

// When forging a write
static inline void set_w_state_for_each_write(struct pending_ops *p_ops,
                                              struct w_message *w_mes, uint32_t backward_ptr,
                                              uint8_t coalesce_num, uint16_t t_id)
{
  for (uint8_t i = 0; i < coalesce_num; i++) {
    if (unlikely(w_mes->write[i].opcode == OP_RELEASE_SECOND_ROUND)) {
      if (DEBUG_QUORUM) green_printf("Thread %u Changing the op of the second round of a release \n", t_id);
      w_mes->write[i].opcode = OP_RELEASE;
    }
    uint8_t w_state;
    if (w_mes->write[i].opcode == COMMIT_OP) w_state = SENT_COMMIT;
    else if (w_mes->write[i].opcode == CACHE_OP_PUT) w_state = SENT_PUT;
    else if (unlikely(w_mes->write[i].opcode == OP_RELEASE_BIT_VECTOR))
      w_state = SENT_BIT_VECTOR;
    else w_state = SENT_RELEASE; // Release or second round of acquire!!
    if (ENABLE_ASSERTIONS) if (w_state == OP_RELEASE_BIT_VECTOR) assert(i == 0);
    p_ops->w_state[(backward_ptr + i) % PENDING_WRITES] = w_state;
  }
}

// When committing reads
static inline void set_flags_before_committing_a_read(struct pending_ops *p_ops, uint32_t pull_ptr,
                                                      bool *acq_second_round_to_flip_bit, bool *insert_write_flag,
                                                      bool *write_local_kvs, uint16_t t_id)
{
  (*acq_second_round_to_flip_bit) = p_ops->read_info[pull_ptr].fp_detected;
  (*insert_write_flag) = (p_ops->read_info[pull_ptr].opcode != CACHE_OP_GET)  &&
                      (p_ops->read_info[pull_ptr].opcode == OP_RELEASE || p_ops->read_info[pull_ptr].opcode == CACHE_OP_PUT ||
                      (!p_ops->read_info[pull_ptr].seen_larger_ts && p_ops->read_info[pull_ptr].times_seen_ts < REMOTE_QUORUM) ||
                      (p_ops->read_info[pull_ptr].seen_larger_ts && p_ops->read_info[pull_ptr].times_seen_ts <= REMOTE_QUORUM));
  (*write_local_kvs) = (p_ops->read_info[pull_ptr].opcode != OP_RELEASE) &&
                       (p_ops->read_info[pull_ptr].seen_larger_ts ||
                       (p_ops->read_info[pull_ptr].opcode == CACHE_OP_GET) || // a simple read is quorum only if the kvs epoch is behind..
                       (p_ops->read_info[pull_ptr].opcode == CACHE_OP_PUT));  // out-of-epoch write
}

// In case of an out-of-epoch write that found a bigger TS --NOT NEEDED
static inline void rectify_version_of_w_mes(struct pending_ops *p_ops, struct read_info *r_info,
                                             uint32_t tmp_version, uint16_t t_id)
{
//  if (ENABLE_ASSERTIONS) {
//    red_printf("Worker: %u, KVS has bigger version %u than read-info %u -> w_message must "
//               "be rectified in position w_mes: %u, inside_ptr: %u\n",
//               t_id, *(uint32_t *) r_info->ts_to_read.version, tmp_version,
//               r_info->w_mes_ptr, r_info->inside_w_ptr);
//    assert(*(uint32_t *)p_ops->w_fifo->w_message[r_info->w_mes_ptr].write[r_info->inside_w_ptr].version == tmp_version);
//  }
//  memcpy(p_ops->w_fifo->w_message[r_info->w_mes_ptr].write[r_info->inside_w_ptr].version, r_info->ts_to_read.version, 4);
}

// returns true if the key was found
static inline bool search_out_of_epoch_writes(struct pending_ops *p_ops, struct key *read_key, uint16_t t_id, void **val_ptr)
{
  struct pending_out_of_epoch_writes *writes = p_ops->p_ooe_writes;
  uint32_t w_i = writes->pull_ptr;
  for (uint32_t i = 0; i < writes->size; i++) {
    if (true_keys_are_equal((struct key*)p_ops->read_info[writes->r_info_ptrs[w_i]].key, read_key)) {
      *val_ptr = (void*) p_ops->read_info[writes->r_info_ptrs[w_i]].value;
      //red_printf("Wrkr %u: Forwarding value from out-of-epoch write, read key: ", t_id);
      //print_true_key(read_key);
      //red_printf("write key: "); print_true_key((struct key*)p_ops->read_info[writes->r_info_ptrs[w_i]].key);
      //red_printf("size: %u, push_ptr %u, pull_ptr %u, r_info ptr %u \n",
      //          writes->size, writes->push_ptr, writes->pull_ptr, writes->r_info_ptrs[w_i]);
      return true;
    }
    MOD_ADD(w_i, PENDING_READS);
  }
  return false;
}

//fill the reply entry with last_commited RMW-id, TS, value and log number
static inline void fill_reply_entry_with_committed_RMW (struct cache_op *kv_ptr, uint32_t entry,
                                                        struct rmw_help_entry *rep_entry)
{
  rep_entry->ts.m_id = kv_ptr->key.meta.m_id;
  rep_entry->ts.version = kv_ptr->key.meta.version - 1;
  memcpy(rep_entry->value, kv_ptr->value + BYTES_OVERRIDEN_IN_KVS_VALUE, (size_t) RMW_VALUE_SIZE);
  rep_entry->log_no = rmw.entry[entry].last_committed_log_no;
  rep_entry->rmw_id = rmw.entry[entry].last_committed_rmw_id;
}

// Accepts contain the rmw-id of the last committed log no. Attempt to register it.
static inline void register_last_committed_rmw_id_by_remote_accept(struct rmw_entry *glob_entry,
                                                                   struct accept *acc , uint16_t t_id) {

  if (glob_entry->last_committed_log_no < (*(uint32_t *)acc->log_no) - 1) {
    register_committed_global_sess_id(acc->last_committed_rmw_id.glob_sess_id, acc->last_committed_rmw_id.id, t_id);
  }
  else if (ENABLE_ASSERTIONS && (glob_entry->last_committed_log_no == (*(uint32_t *)acc->log_no) - 1)) {
    assert(glob_entry->last_committed_rmw_id.id == acc->last_committed_rmw_id.id &&
           glob_entry->last_committed_rmw_id.glob_sess_id == acc->last_committed_rmw_id.glob_sess_id);
    assert(committed_glob_sess_rmw_id[acc->last_committed_rmw_id.glob_sess_id] >= acc->last_committed_rmw_id.id);
  }
}

// Check the global RMW-id structure, to see if an RMW has already been committed
static inline bool the_rmw_has_committed(uint16_t glob_sess_id, uint64_t rmw_l_id, uint16_t t_id, uint8_t *flag)
{
  if (committed_glob_sess_rmw_id[glob_sess_id] >= rmw_l_id) {
    if (DEBUG_RMW)
      green_printf("Worker %u: A Remote machine  is trying a propose with global sess_id %u, "
                     "rmw_id %lu, that has been already committed \n",
                   t_id, glob_sess_id, rmw_l_id);
    *flag = RMW_ALREADY_COMMITTED;
    return true;
  }
  else return false;
}

// Returns true if the received log no is smaller than the committed. Iif the key has an allocated entry it assigns the "entry"
static inline bool is_log_smaller_or_has_rmw_committed(uint32_t log_no, struct cache_op *kv_ptr,
                                                       uint64_t rmw_l_id,
                                                       uint16_t glob_sess_id, uint16_t t_id,
                                                       uint32_t *entry, uint8_t *flag,
                                                       struct rmw_help_entry *rep_entry)
{
  if (kv_ptr->opcode == KEY_HAS_BEEN_RMWED) {
    (*entry) = *(uint32_t *) kv_ptr->value;
    struct rmw_entry *glob_entry = &rmw.entry[*entry];
    check_log_nos_of_glob_entry(glob_entry, "is_log_smaller_or_has_rmw_committed", t_id);
    if (ENABLE_ASSERTIONS) assert(*entry < RMW_ENTRIES_NUM);
    bool fill_the_rep = false;
    if (the_rmw_has_committed(glob_sess_id, rmw_l_id, t_id, flag))
      fill_the_rep = true;
    else if (glob_entry->last_committed_log_no >= log_no ||
             glob_entry->log_no > log_no) {
      if (DEBUG_RMW)
        yellow_printf("Wkrk %u Log number is too small %u/%u entry state %u, propose/accept with rmw_lid %u,"
                        " global_sess_id %u, entry %u\n", t_id, log_no, glob_entry->last_committed_log_no,
                      glob_entry->state, rmw_l_id, glob_sess_id, *entry);
      *flag = RMW_LOG_TOO_SMALL;
      fill_the_rep = true;
    }
    else if (DEBUG_RMW) { // remote log is higher than the locally stored!
      if (glob_entry->log_no < log_no )
        yellow_printf("Wkrk %u Log number is higher than expected %u/%u, entry state %u, "
                        "propose/accept with rmw_lid %u, global_sess_id %u, entry %u\n",
                      t_id, log_no, glob_entry->log_no,
                      glob_entry->state, rmw_l_id, glob_sess_id, *entry);
    }
    // If either the log is too smaller or the rmw_id has been committed,
    // store the committed value, TS & log_number to the reply
    if (fill_the_rep) {
      fill_reply_entry_with_committed_RMW (kv_ptr, *entry, rep_entry);
      return true;
    }
  }
  return false;
}

// When receiving a propose check that its ts is bigger than the KVS ts
static inline bool propose_ts_is_not_greater_than_kvs_ts(struct cache_op *kv_ptr, struct propose *prop,
                                                         uint8_t m_id, uint16_t t_id, uint8_t *flag,
                                                         struct rmw_help_entry *rep_entry)
{

  struct ts_tuple tmp_ts; // cant use the kvs version as we incremented it when locking.
  tmp_ts.version = kv_ptr->key.meta.version - 1;
  tmp_ts.m_id = kv_ptr->key.meta.m_id;
  if (compare_netw_ts_with_ts(&prop->ts, &tmp_ts) != GREATER) {
    if (DEBUG_RMW)
      yellow_printf("Wrkr %u: TS is NOT greater in a received propose: version %u/%u, m_id %u/%u"
                      " from machine %u with rmw_id % , glob_sess_id %u, \n",
                    t_id, *(uint32_t *) prop->ts.version, tmp_ts.version, prop->ts.m_id, tmp_ts.m_id,
                    m_id, *(uint64_t *) prop->t_rmw_id, *(uint16_t *)prop->glob_sess_id);
    *flag = RMW_TS_SMALLER_THAN_KVS;
    rep_entry->ts = tmp_ts;
    return true;
  }
  else return false;
}

// When receiving an accept check that its ts is bigger than the KVS ts
static inline bool accept_ts_is_not_greater_than_kvs_ts(struct cache_op *kv_ptr, struct accept *acc,
                                                         uint8_t m_id, uint16_t t_id, uint8_t *flag,
                                                         struct rmw_help_entry *rep_entry)
{
  struct ts_tuple tmp_ts; // cant use the kvs version as we incremented it when locking.
  tmp_ts.version = kv_ptr->key.meta.version - 1;
  tmp_ts.m_id = kv_ptr->key.meta.m_id;
  if (compare_netw_ts_with_ts(&acc->ts, &tmp_ts) != GREATER) {
    if (DEBUG_RMW)
      yellow_printf("Wrkr %u: TS is NOT greater in a received accept: version %u/%u, m_id %u/%u"
                      " from machine %u with rmw_id % , glob_sess_id %u, \n",
                    t_id, *(uint32_t *) acc->ts.version, tmp_ts.version, acc->ts.m_id, tmp_ts.m_id,
                    m_id, acc->t_rmw_id, *(uint16_t *)acc->glob_sess_id);
    *flag = RMW_TS_SMALLER_THAN_KVS;
    rep_entry->ts = tmp_ts;
    return true;
  }
  else return false;
}



// used when filling a propose/accept reply
static inline void copy_entry_on_rmw_reply_depending_on_opcode(struct rmw_rep_last_committed *rmw_rep,
                                                               struct r_rep_fifo *r_rep_fifo,
                                                               struct rmw_help_entry *rep_entry,
                                                               bool is_accept,
                                                               uint16_t t_id)
{
  switch (rmw_rep->opcode) {
    case RMW_ID_COMMITTED :
    case LOG_TOO_SMALL :
      *(uint32_t *) rmw_rep->log_no = rep_entry->log_no;
      r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] += LOG_NO_SIZE;
      if (DEBUG_RMW) yellow_printf("Wrkr %u adds log no %u to its reply \n",
                                   t_id, *(uint32_t *) rmw_rep->log_no);
    case RMW_ACCEPTED :
      *(uint64_t *) rmw_rep->rmw_id = rep_entry->rmw_id.id;
      *(uint16_t *) rmw_rep->glob_sess_id = rep_entry->rmw_id.glob_sess_id;
      if (DEBUG_RMW) yellow_printf("Wrkr %u adds rmw_id %u and glob_sess_id %u to its reply \n",
                                   t_id, *(uint64_t *) rmw_rep->rmw_id, *(uint16_t *) rmw_rep->glob_sess_id);
      memcpy(rmw_rep->value, rep_entry->value, (size_t) RMW_VALUE_SIZE);
      r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] += (RMW_VALUE_SIZE + RMW_ID_SIZE);
    case SEEN_HIGHER_PROP :
    case RMW_TS_STALE :
      MY_ASSERT(rep_entry->ts.version > 0 && rmw_rep->opcode != LOG_TOO_SMALL,
                "Creating an rmw reply: is accept %d,  version should be bigger "
                "than zero: %u, opcode: %u \n", is_accept, rep_entry->ts.version, rmw_rep->opcode);
      assign_ts_to_netw_ts(&rmw_rep->ts, &rep_entry->ts);
      r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] += TS_TUPLE_SIZE;
      if (DEBUG_RMW) yellow_printf("Wrkr %u adds ts with version %u and m_id %u to its reply \n",
                                   t_id, *(uint32_t *)rmw_rep->ts.version, rmw_rep->ts.m_id);
      if (ENABLE_ASSERTIONS) {
        assert((*(uint32_t *) rmw_rep->ts.version % 2 == 0));
        //assert((*(uint32_t *) rmw_rep->ts.version > 0));
      }
    case RMW_ACK:
      break;
    default:
      if (ENABLE_ASSERTIONS) assert(false);
  }

  if (ENABLE_ASSERTIONS) {
    if (DEBUG_RMW)
      yellow_printf("Wrkr %u, created a%s reply: opcode %u, size %u \n",
                    t_id, is_accept? "n Accept" : " Propose",
                    rmw_rep->opcode, r_rep_fifo->message_sizes[r_rep_fifo->push_ptr]);

    if (rmw_rep->opcode == RMW_ID_COMMITTED || rmw_rep->opcode == LOG_TOO_SMALL)
      assert(r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] == PROP_REP_MESSAGE_SIZE);
    else if (rmw_rep->opcode == RMW_ACCEPTED)
      assert(r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] == PROP_REP_MESSAGE_SIZE - LOG_NO_SIZE);
    else if (rmw_rep->opcode == RMW_TS_STALE || rmw_rep->opcode == SEEN_HIGHER_PROP)
      assert(r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] == PROP_REP_MES_HEADER + 8 + 1 + TS_TUPLE_SIZE);
    else if (rmw_rep->opcode == RMW_ACK)
      assert(r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] == PROP_REP_MES_HEADER + 8 + 1);
    else assert(false);
  }
}


// Post a quorum broadcast and post the appropriate receives for it
static inline void post_quorum_broadasts_and_recvs(struct recv_info *recv_info, uint32_t recvs_to_post_num,
                                                   struct quorum_info *q_info, uint16_t br_i, uint64_t br_tx,
                                                   struct ibv_send_wr *send_wr, struct ibv_qp *send_qp,
                                                   int enable_inlining)
{
  struct ibv_send_wr *bad_send_wr;
  if (recvs_to_post_num > 0) {
    // printf("Wrkr %d posting %d recvs\n", g_id,  recvs_to_post_num);
    if (recvs_to_post_num) post_recvs_with_recv_info(recv_info, recvs_to_post_num);
    recv_info->posted_recvs += recvs_to_post_num;
  }
  if (DEBUG_SS_BATCH)
    green_printf("Sending %u bcasts, total %lu \n", br_i, br_tx);

  send_wr[((br_i - 1) * MESSAGES_IN_BCAST) + q_info->last_active_rm_id].next = NULL;
  int ret = ibv_post_send(send_qp, &send_wr[q_info->first_active_rm_id], &bad_send_wr);
  if (ENABLE_ASSERTIONS) CPE(ret, "Broadcast ibv_post_send error", ret);
  if (!ENABLE_ADAPTIVE_INLINING)
    send_wr[q_info->first_active_rm_id].send_flags = enable_inlining == 1 ? IBV_SEND_INLINE : 0;
}


// Whe broadcasting writes, some of them may be accepts which trigger read replies instead of write acks
// For that reason we need to potentially also post receives for r_reps when broadcasting writes
static inline void post_receives_for_r_reps_for_accepts(struct recv_info *r_rep_recv_info,
                                                        uint16_t t_id)
{
  uint32_t recvs_to_post_num = MAX_RECV_R_REP_WRS - r_rep_recv_info->posted_recvs;
  if (recvs_to_post_num > 0) {
    // printf("Wrkr %d posting %d recvs\n", g_id,  recvs_to_post_num);
    if (recvs_to_post_num) post_recvs_with_recv_info(r_rep_recv_info, recvs_to_post_num);
    r_rep_recv_info->posted_recvs += recvs_to_post_num;
  }
}

// Search in the prepare entries for an lid (used when receiving a prep reply)
static inline int search_prop_entries_with_l_id(struct prop_info *prop_info, uint8_t state, uint64_t l_id)
{
  for (uint16_t i = 0; i < LOCAL_PROP_NUM; i++) {
    if (prop_info->entry[i].state == state &&
        prop_info->entry[i].l_id == l_id)
      return i;
  }
  return -1; // i.e. l_id not found!!

}

// If the prop/accept reply contains many replies move to the next message
// according to the current message
static inline void move_ptr_to_next_rmw_reply(uint16_t *byte_ptr, uint8_t opcode)
{
  if (opcode == RMW_ACK || (opcode == RMW_ACK + FALSE_POSITIVE_OFFSET))
    (*byte_ptr)+= PROP_REP_SMALL_SIZE; // l_id and opcode
  else if ((opcode == RMW_TS_STALE || (opcode == RMW_TS_STALE + FALSE_POSITIVE_OFFSET)) ||
           (opcode == SEEN_HIGHER_PROP || (opcode == SEEN_HIGHER_PROP + FALSE_POSITIVE_OFFSET)))
    (*byte_ptr) += PROP_REP_ONLY_TS_SIZE;
  else if ((opcode == RMW_ACCEPTED) || (opcode == RMW_ACCEPTED + FALSE_POSITIVE_OFFSET))
    (*byte_ptr) += PROP_REP_ACCEPTED_SIZE;
  else if((opcode == RMW_ID_COMMITTED || (opcode == RMW_ID_COMMITTED + FALSE_POSITIVE_OFFSET)) ||
          (opcode == LOG_TOO_SMALL || (opcode == LOG_TOO_SMALL + FALSE_POSITIVE_OFFSET)))
    (*byte_ptr) += PROP_REP_SIZE;
  else if (ENABLE_ASSERTIONS) assert(false);
}

// When forging a write wr
static inline uint32_t calculate_write_message_size(uint8_t opcode, uint8_t coalesce_num, uint16_t t_id)
{
  uint16_t header_size, size_of_struct;
  if (ENABLE_RMWS) {
    if (opcode == ACCEPT_OP) {
      header_size = ACCEPT_MES_HEADER;
      size_of_struct = ACCEPT_SIZE;
    } else if (opcode == COMMIT_OP) {
      header_size = COMMIT_MES_HEADER;
      size_of_struct = COMMIT_SIZE;
    } else {
      header_size = W_MES_HEADER;
      size_of_struct = W_SIZE;
    }

    return header_size + (coalesce_num * size_of_struct);
  }
  else return (uint32_t) (W_MES_HEADER + (coalesce_num * W_SIZE));
}

// If a local RMW managed to grab a global rmw entry, then it sets up its local entry
static inline void fill_loc_rmw_entry_on_grabbing_global(struct pending_ops* p_ops ,
                                                         struct rmw_local_entry* loc_entry,
                                                         uint32_t version, uint8_t state,
                                                         uint16_t sess_i, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    check_global_sess_id((uint8_t) machine_id, t_id,
                         (uint16_t) sess_i, loc_entry->rmw_id.glob_sess_id);
    check_version(version, "fill_loc_rmw_entry_on_grabbing_global");
  }
  loc_entry->help_loc_entry->state = INVALID_RMW;
  memset(&loc_entry->rmw_reps, 0, sizeof(struct rmw_rep_info));
  loc_entry->state = state;
  loc_entry->epoch_id = (uint16_t) epoch_id;
  loc_entry->new_ts.version = version;
  loc_entry->new_ts.m_id = (uint8_t) machine_id;
}

// Check if the global state that is blokcing a local RMW is persisting
static inline bool glob_state_has_not_changed(struct rmw_entry* glob_entry,
                                              struct rmw_local_entry* loc_entry)
{
  return glob_entry->state == loc_entry->help_rmw->state &&
         rmw_ids_are_equal(&loc_entry->help_rmw->rmw_id, &glob_entry->rmw_id);
}

// Check if the global state that is blokcing a local RMW is persisting
static inline bool glob_state_has_changed(struct rmw_entry* glob_entry,
                                          struct rmw_local_entry* loc_entry)
{
  return glob_entry->state != loc_entry->help_rmw->state ||
    (!rmw_ids_are_equal(&loc_entry->help_rmw->rmw_id, &glob_entry->rmw_id));
}

// Initialize a local  RMW entry on the first time it gets allocated
static inline void init_loc_entry(struct cache_resp* resp, struct pending_ops* p_ops,
                                  struct cache_op *prop, uint16_t session_id,
                                  uint16_t t_id, struct rmw_local_entry* loc_entry)
{
  loc_entry->opcode = PROPOSE_OP;
  memcpy(&loc_entry->key, &prop->key.bkt, TRUE_KEY_SIZE);
  memset(&loc_entry->rmw_reps, 0, sizeof(struct rmw_rep_info));
  loc_entry->index_to_rmw = resp->rmw_entry;
  loc_entry->ptr_to_kv_pair = resp->kv_pair_ptr;
  loc_entry->sess_id = (uint16_t) session_id;
  //loc_entry->accept_acks = 0;
  //loc_entry->accept_replies = 0;
  loc_entry->back_off_cntr = 0;
  loc_entry->helping_flag = NOT_HELPING;
  // Give it an RMW-id as soon as it has a local entry, because the RMW must happen eventually
  loc_entry->rmw_id.id = p_ops->prop_info->l_id;
  loc_entry->l_id = p_ops->prop_info->l_id;
  loc_entry->help_loc_entry->l_id = p_ops->prop_info->l_id;
  loc_entry->rmw_id.glob_sess_id = get_glob_sess_id((uint8_t) machine_id, t_id, (uint16_t) session_id);
  loc_entry->accepted_log_no = 0;
  //yellow_printf("Init  RMW-id %u glob_sess_id %u \n", loc_entry->rmw_id.id, loc_entry->rmw_id.glob_sess_id);
  //loc_entry->help_loc_entry->log_no = 0;
  loc_entry->help_loc_entry->state = INVALID_RMW;
  if (ENABLE_STAT_COUNTING) t_stats[t_id].rmws_completed++;
}

// The help_loc_entry is used when receiving an already committed reply or an already accepted
static inline void store_rmw_rep_to_help_loc_entry(struct rmw_local_entry* loc_entry,
                                                   struct rmw_rep_last_committed* prop_rep, uint16_t t_id)
{
  struct rmw_local_entry *help_loc_entry = loc_entry->help_loc_entry;
  bool already_accepted = prop_rep->opcode == RMW_ACCEPTED;
  uint32_t new_log_no = already_accepted ? loc_entry->log_no : *(uint32_t *) prop_rep->log_no;
  uint8_t state = help_loc_entry->state;
  bool overwrite_entry = false;
  // An accept can overwrite an invalid entry or an accepted entry with lower ts
  if (already_accepted) {
    if (ENABLE_ASSERTIONS) assert(state != COMMITTED);
    if (state == INVALID_RMW || (state == ACCEPTED &&
      compare_netw_ts_with_ts(&prop_rep->ts, &help_loc_entry->new_ts))) {
      overwrite_entry = true;
      help_loc_entry->state = ACCEPTED;
    }
  }
  else { // A commit overwrites the entry if it's not committed, or if its log is bigger
    if (help_loc_entry->state != COMMITTED || help_loc_entry->log_no < new_log_no) {
      overwrite_entry = true;
      help_loc_entry->state = COMMITTED;
    }
  }
  if (overwrite_entry) {
    help_loc_entry->sess_id = loc_entry->sess_id;
    help_loc_entry->log_no = new_log_no;
    assign_netw_ts_to_ts(&help_loc_entry->new_ts, &prop_rep->ts);
    help_loc_entry->rmw_id.id = *(uint64_t *) prop_rep->rmw_id;
    help_loc_entry->rmw_id.glob_sess_id = *(uint16_t *) prop_rep->glob_sess_id;
    memcpy(help_loc_entry->value_to_write, prop_rep->value, (size_t) RMW_VALUE_SIZE);
    memcpy(&help_loc_entry->key, &loc_entry->key, TRUE_KEY_SIZE);
  }
}

static inline void zero_out_the_rmw_reply_loc_entry_metadata(struct rmw_local_entry* loc_entry)
{
  if (ENABLE_ASSERTIONS) { // make sure the loc_entry is correctly set-up
    if (loc_entry->help_loc_entry == NULL) {
      red_printf("When Zeroing: The help_loc_ptr is NULL. The reason is typically that "
                   "help_loc_entry was passed to the function "
                   "instead of loc entry to check \n");
      assert(false);
    }
  }
  loc_entry->help_loc_entry->state = INVALID_RMW;
  memset(&loc_entry->rmw_reps, 0, sizeof(struct rmw_rep_info));
  loc_entry->back_off_cntr = 0;
}

// When a propose/accept has inspected the responses (after they have reached at least a quorum),
// advance the entry's l_id such that previous responses are disregarded
static inline void advance_loc_entry_l_id(struct pending_ops *p_ops, struct rmw_local_entry *loc_entry,
                                          uint16_t t_id)
{
  loc_entry->l_id = p_ops->prop_info->l_id;
  loc_entry->help_loc_entry->l_id = p_ops->prop_info->l_id;
  p_ops->prop_info->l_id++;
}

static inline void free_session(struct pending_ops *p_ops, uint16_t sess_id, uint16_t t_id)
{
  struct rmw_local_entry *loc_entry = &p_ops->prop_info->entry[sess_id];
  if (ENABLE_ASSERTIONS) {
    assert(sess_id < SESSIONS_PER_THREAD);
    assert(loc_entry->state == INVALID_RMW);
  }
  if (VERIFY_PAXOS) verify_paxos(loc_entry, t_id);
  p_ops->session_has_pending_op[sess_id] = false;
  p_ops->all_sessions_stalled = false;
}

static inline bool if_already_committed_free_session_invalidate_entry(struct pending_ops *p_ops,
                                                                      struct rmw_local_entry *loc_entry,
                                                                      uint16_t t_id)
{
  if (loc_entry->rmw_id.id <= committed_glob_sess_rmw_id[loc_entry->rmw_id.glob_sess_id]) {
    //red_printf("Wrkr %u Freeing session %u \n", t_id, loc_entry->sess_id);
    loc_entry->state = INVALID_RMW;
    free_session(p_ops, loc_entry->sess_id, t_id);
    return true;
  }
  return false;
}

// Write the committed log in the global entry and the new value in the KVS
static inline void take_actions_to_commit_rmw(struct rmw_entry *glob_entry,
                                              struct rmw_local_entry *loc_entry_to_commit,
                                              struct rmw_local_entry *loc_entry, // this may be different if helping
                                              uint16_t t_id)
{
  glob_entry->last_committed_log_no = loc_entry_to_commit->log_no;
  glob_entry->last_committed_rmw_id = loc_entry_to_commit->rmw_id;
  struct cache_op *kv_pair = (struct cache_op *) loc_entry->ptr_to_kv_pair;
  memcpy(&kv_pair->value[BYTES_OVERRIDEN_IN_KVS_VALUE], loc_entry_to_commit->value_to_write, (size_t) RMW_VALUE_SIZE);
  kv_pair->key.meta.m_id = loc_entry_to_commit->new_ts.m_id;
  kv_pair->key.meta.version = loc_entry_to_commit->new_ts.version + 1; // the unlock function will decrement 1
}

// After having helped another RMW, bring your own RMW back into the local entry
static inline void reinstate_loc_entry_after_helping(struct rmw_local_entry *loc_entry, uint16_t t_id)
{
  if (loc_entry->helping_flag == HELPING_NEED_STASHING) {
    loc_entry->opcode = loc_entry->help_rmw->opcode;
    assign_second_rmw_id_to_first(&loc_entry->rmw_id, &loc_entry->help_rmw->rmw_id);
  }
  loc_entry->state = NEEDS_GLOBAL;
  loc_entry->helping_flag = NOT_HELPING;
  zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);

  if (DEBUG_RMW)
    yellow_printf("Wrkr %u, sess %u reinstates its RMW id %u glob_sess-id %u after helping \n",
                  t_id, loc_entry->sess_id, loc_entry->rmw_id.id,
                  loc_entry->rmw_id.glob_sess_id);
  if (ENABLE_ASSERTIONS)
    assert(glob_ses_id_to_m_id(loc_entry->rmw_id.glob_sess_id) == (uint8_t) machine_id);

}

/* ---------------------------------------------------------------------------
//------------------------------ RMW------------------------------------------
//---------------------------------------------------------------------------*/

// RMWs hijack the read fifo, to send propose broadcasts to all
static inline void insert_prop_to_read_fifo(struct pending_ops *p_ops, struct rmw_local_entry *loc_entry,
                                            uint8_t flag, uint16_t t_id)
{

  check_loc_entry_metadata_is_reset(loc_entry);
  struct r_message *r_mes = p_ops->r_fifo->r_message;
  uint32_t r_mes_ptr = p_ops->r_fifo->push_ptr;
  uint8_t inside_r_ptr = r_mes[r_mes_ptr].coalesce_num;
  bool fresh_mess = inside_r_ptr == 0;
  //todo coalescing is disabled
  if (ENABLE_ASSERTIONS) assert(MAX_PROP_COALESCE == 1);

  // create a new message if the propose cannot be coalesced
  if (!fresh_mess) {
    MOD_ADD(p_ops->r_fifo->push_ptr, R_FIFO_SIZE);
    r_mes[p_ops->r_fifo->push_ptr].coalesce_num = 0;
    r_mes_ptr = p_ops->r_fifo->push_ptr;
    inside_r_ptr = r_mes[r_mes_ptr].coalesce_num;
  }

  if (DEBUG_RMW)
    green_printf("Worker: %u, inserting an rmw in r_mes_ptr %u and inside ptr %u \n",
                 t_id, r_mes_ptr, inside_r_ptr);
  struct prop_message *p_mes = (struct prop_message *) &r_mes[r_mes_ptr];
  struct propose *prop = &p_mes->prop[inside_r_ptr];
  assign_ts_to_netw_ts(&prop->ts, &loc_entry->new_ts);
  memcpy(&prop->key, (void *)&loc_entry->key, TRUE_KEY_SIZE);
  prop->opcode = loc_entry->opcode;
  *(uint64_t*) prop->l_id = loc_entry->l_id;
  *(uint64_t*) prop->t_rmw_id = loc_entry->rmw_id.id;
  *(uint16_t*) prop->glob_sess_id = loc_entry->rmw_id.glob_sess_id;
  *(uint32_t*) prop->log_no = loc_entry->log_no;

  // Query the conf to see if the machine has lost messages
  //on_starting_an_acquire_query_the_conf(t_id); // TODO specify failure semantics of propose
  p_ops->r_fifo->bcast_size++;
  p_mes->coalesce_num++;
  if (ENABLE_ASSERTIONS) {
    check_version(*(uint32_t *)prop->ts.version, "insert_prop_to_read_fifo");
    assert(r_mes->coalesce_num > 0);
    assert(p_mes->prop[inside_r_ptr].opcode == PROPOSE_OP);
    assert(p_mes->m_id == (uint8_t) machine_id);
  }
  if (p_mes->coalesce_num == MAX_PROP_COALESCE) {
    MOD_ADD(p_ops->r_fifo->push_ptr, R_FIFO_SIZE);
    r_mes[p_ops->r_fifo->push_ptr].coalesce_num = 0;
  }
}


// the first time a key gets RMWed, it grabs an RMW entry that lasts for life,
// the entry is protected by the KVS lock
static inline uint32_t grab_RMW_entry(uint8_t state, struct cache_op *kv_ptr,
                                      uint8_t opcode, uint8_t new_ts_m_id, uint32_t new_version,
                                      uint64_t l_id, uint32_t log_no,
                                      uint16_t glob_sess_id, uint16_t t_id)
{
  uint32_t next_entry =
    atomic_fetch_add_explicit(&next_rmw_entry_available, 1, memory_order_relaxed);
  my_assert(next_entry < RMW_ENTRIES_NUM, "Failed to grab an RMW entry");
  memcpy(kv_ptr->value, &next_entry, BYTES_OVERRIDEN_IN_KVS_VALUE);

  struct rmw_entry *glob_entry = &rmw.entry[next_entry];

  if (state == COMMITTED) {
    glob_entry->last_committed_rmw_id.glob_sess_id = glob_sess_id;
    glob_entry->last_committed_rmw_id.id = l_id;
    glob_entry->last_committed_log_no = log_no;
    glob_entry->state = INVALID_RMW;
  }
  else {
    glob_entry->opcode = opcode;
    glob_entry->new_ts.m_id = new_ts_m_id;
    glob_entry->new_ts.version = new_version;
    glob_entry->rmw_id.glob_sess_id = glob_sess_id;
    glob_entry->rmw_id.id = l_id;
    glob_entry->state = state;
    if (ENABLE_ASSERTIONS) {
      assert(glob_entry->new_ts.version % 2 == 0);
      assert(state == PROPOSED || state == ACCEPTED);
    }
  }
  glob_entry->key = *((struct key *) (((void *) kv_ptr) + sizeof(cache_meta)));
  //cyan_printf("Global Rmw entry %u/%u kv_ptr %lu got key: ", next_entry, *(uint32_t *) kv_ptr->value, kv_ptr->value);
  //print_true_key(&glob_entry->key);
  glob_entry->log_no = log_no; // not necessarily 1 if a remote machine is grabbing here
  if (ENABLE_ASSERTIONS) {
    true_keys_are_equal(&glob_entry->key, (struct key*) (((void*) kv_ptr) + sizeof(cache_meta)));
    assert(glob_sess_id < GLOBAL_SESSION_NUM);
  }
  return next_entry;
}

// Activate the entry that belongs to a given key to initiate an RMW (either a local or a remote)
static inline void activate_RMW_entry(uint8_t state, uint32_t new_version, struct rmw_entry *glob_entry,
                                      uint8_t opcode, uint8_t new_ts_m_id, uint64_t l_id, uint16_t glob_sess_id,
                                      uint32_t log_no, uint16_t t_id, char* message)
{
  // pass the new ts!
  glob_entry->opcode = opcode;
  glob_entry->new_ts.m_id = new_ts_m_id;
  glob_entry->new_ts.version = new_version;
  //cyan_printf("Glob_entry version %u, m_id %u \n", glob_entry->new_ts.version, glob_entry->new_ts.m_id);
  glob_entry->rmw_id.glob_sess_id = glob_sess_id;
  glob_entry->rmw_id.id = l_id;
  glob_entry->state = state;
  glob_entry->log_no = log_no;
  if (ENABLE_ASSERTIONS) {
    assert(glob_entry->rmw_id.glob_sess_id < GLOBAL_SESSION_NUM);
    if (committed_glob_sess_rmw_id[glob_entry->rmw_id.glob_sess_id] >= glob_entry->rmw_id.id) {
      red_printf("Wrkr %u, attempts to activate with already committed RMW id %u/%u glob_sess id %u, state %u: %s \n",
                 t_id, glob_entry->rmw_id.id, committed_glob_sess_rmw_id[glob_entry->rmw_id.glob_sess_id],
                 glob_entry->rmw_id.glob_sess_id, state, message);
      //assert(false);
    }
    assert(glob_sess_id < GLOBAL_SESSION_NUM);
    assert(glob_entry->new_ts.version % 2 == 0);
    assert(state == PROPOSED || state == ACCEPTED); // TODO accepted is allowed?
    assert(glob_entry->last_committed_log_no < glob_entry->log_no);
  }
}


// When inspecting an RMW that failed to grab a global entry in the past
static inline bool attempt_to_grab_global_entry_after_waiting(struct pending_ops *p_ops,
                                                              struct rmw_entry *glob_entry,
                                                              struct rmw_local_entry *loc_entry,
                                                              uint16_t sess_i, uint16_t t_id)
{
  bool global_entry_was_grabbed = false;
  struct cache_op *kv_pair = (struct cache_op *) loc_entry->ptr_to_kv_pair;
  uint32_t version = 0;
  optik_lock(loc_entry->ptr_to_kv_pair);
  if (if_already_committed_free_session_invalidate_entry(p_ops, loc_entry, t_id)) {
    optik_unlock_decrement_version(loc_entry->ptr_to_kv_pair);
    return false;
  }
  if (glob_entry->state == INVALID_RMW ||
    committed_glob_sess_rmw_id[glob_entry->rmw_id.glob_sess_id] >= glob_entry->rmw_id.id) {
    if (ENABLE_ASSERTIONS && glob_entry->state != INVALID_RMW &&
        committed_glob_sess_rmw_id[glob_entry->rmw_id.glob_sess_id] >= glob_entry->rmw_id.id) {
      //red_printf("Wrkr: %u waiting on an rmw id %u/%u glob_sess_id %u that has been committed, so we free it"
      //             "last committed rmw id %u , glob sess id %u, "
      //             "state %u, committed log/log %u/%u, version %u \n",
      //           t_id, glob_entry->rmw_id.id, committed_glob_sess_rmw_id[glob_entry->rmw_id.glob_sess_id],
      //           glob_entry->rmw_id.glob_sess_id, glob_entry->last_committed_rmw_id.id,
      //           glob_entry->last_committed_rmw_id.glob_sess_id,
      //           glob_entry->state, glob_entry->last_committed_log_no,
      //           glob_entry->log_no, glob_entry->new_ts.version);
    }
    loc_entry->log_no = glob_entry->last_committed_log_no + 1;
    //if (loc_entry->accepted_log_no > 0) yellow_printf("Accepted log no/ log no %u/%u \n",
    //                                                  loc_entry->accepted_log_no, loc_entry->log_no );
    version = kv_pair->key.meta.version + 1;
    activate_RMW_entry(PROPOSED, version, glob_entry, loc_entry->opcode,
                       (uint8_t) machine_id, loc_entry->rmw_id.id,
                       loc_entry->rmw_id.glob_sess_id, loc_entry->log_no, t_id,
                       ENABLE_ASSERTIONS ? "attempt_to_grab_global_entry_after_waiting" : NULL);

    global_entry_was_grabbed = true;
    if (DEBUG_RMW)
      yellow_printf("Wrkr %u, after waiting for %u cycles, session %u grabbed entry %u \n",
                    t_id, loc_entry->back_off_cntr, loc_entry->sess_id, loc_entry->index_to_rmw);
  }
  else if (glob_state_has_changed(glob_entry, loc_entry)) {
    if (ENABLE_ASSERTIONS) {
      if (committed_glob_sess_rmw_id[glob_entry->rmw_id.glob_sess_id] >= glob_entry->rmw_id.id) {
        red_printf("Wrkr: %u The saved rmw id %u/%u glob_sess_id %u has been committed, "
                     "last committed rmw id %u , glob sess id %u, "
                     "state %u, committed log/log %u/%u, version %u \n",
                   t_id, glob_entry->rmw_id.id, committed_glob_sess_rmw_id[glob_entry->rmw_id.glob_sess_id],
                   glob_entry->rmw_id.glob_sess_id, glob_entry->last_committed_rmw_id.id,
                   glob_entry->last_committed_rmw_id.glob_sess_id,
                   glob_entry->state, glob_entry->last_committed_log_no,
                   glob_entry->log_no, glob_entry->new_ts.version);
        //assert(false);
      }
    }
    if (DEBUG_RMW)
      yellow_printf("Wrkr %u, session %u changed who is waiting for in entry %u: waited for %u cycles on "
                      "state %u rmw_id %u glob_sess_id %u, now waiting on rmw_id %u glob_sess_id %u, state %u\n",
                    t_id, loc_entry->sess_id, loc_entry->index_to_rmw, loc_entry->back_off_cntr,
                    loc_entry->help_rmw->state, loc_entry->help_rmw->rmw_id.id, loc_entry->help_rmw->rmw_id.glob_sess_id,
                    glob_entry->rmw_id.id, glob_entry->rmw_id.glob_sess_id, glob_entry->state);
    loc_entry->help_rmw->state = glob_entry->state;
    assign_second_rmw_id_to_first(&loc_entry->help_rmw->rmw_id, &glob_entry->rmw_id);
    loc_entry->back_off_cntr = 0;
  }
  check_log_nos_of_glob_entry(glob_entry, "attempt_to_grab_global_entry_after_waiting", t_id);
  optik_unlock_decrement_version(loc_entry->ptr_to_kv_pair);
  if (global_entry_was_grabbed) {
    fill_loc_rmw_entry_on_grabbing_global(p_ops, loc_entry, version,
                                          PROPOSED, sess_i, t_id);
  }
  return global_entry_was_grabbed;
}

// Look at an RMW entry to answer to a propose message-- kv pair lock is held when calling this
static inline uint8_t propose_snoops_entry(struct propose *prop, uint32_t pos, uint8_t m_id,
                                           uint16_t t_id, struct rmw_help_entry *reply_rmw)
{
  uint8_t return_flag = RMW_ACK_PROPOSE;
  struct rmw_entry *glob_entry = &rmw.entry[pos];
  if (ENABLE_ASSERTIONS)  {
    assert(pos < RMW_ENTRIES_NUM);
    assert(prop->opcode == PROPOSE_OP);
    assert(*(uint32_t *) prop->log_no > glob_entry->last_committed_log_no);
    assert(*(uint32_t *) prop->log_no == glob_entry->log_no);
  }

  if (ENABLE_ASSERTIONS)
    assert(check_entry_validity_with_key((struct key *) prop->key, pos));

  // If entry is in Accepted state you typically NACK send back value and ts & RMW_id
  if (glob_entry->state == ACCEPTED) {
    // This is very likely: it typically means i have polled the accept before polling the propose
    // which can happen, because accepts are fired after a quorum of acks
    if (glob_entry->rmw_id.glob_sess_id == *(uint16_t *) prop->glob_sess_id &&
        glob_entry->rmw_id.id == *(uint64_t *) prop->t_rmw_id) {
      //red_printf("Wrkr %u Received a proposal with same RMW id as an already accepted proposal, "
        //           "glob sess %u/%u, rmw_id %u/%u,  \n", t_id, *(uint16_t *) prop->glob_sess_id,
          //       glob_entry->rmw_id.glob_sess_id, *(uint64_t *) prop->t_rmw_id, glob_entry->rmw_id.id);
      return_flag = ACCEPTED_SAME_RMW_ID;
    }
    else { // need to copy the value, ts and RMW-id here
      memcpy(reply_rmw->value, glob_entry->value, (size_t) RMW_VALUE_SIZE);
      reply_rmw->ts = glob_entry-> new_ts;
      reply_rmw->rmw_id = glob_entry->rmw_id;
      return_flag = RMW_ALREADY_ACCEPTED;
     // if (ENABLE_ASSERTIONS) {
      //  if (compare_netw_ts_with_ts(&prop->ts, &glob_entry->new_ts) == EQUAL)
      //    red_printf("Wrkr %u Received a proposal with same TS as an already accepted proposal, "
     //                  "glob sess %u/%u, rmw_id %u/%u,  \n", t_id, *(uint16_t *) prop->glob_sess_id,
      //               glob_entry->rmw_id.glob_sess_id, *(uint64_t *) prop->t_rmw_id, glob_entry->rmw_id.id);
     // }
    }

  }
  else if (glob_entry->state == PROPOSED) {
    enum ts_compare ts_comp = compare_netw_ts_with_ts(&prop->ts, &glob_entry->new_ts);
    switch(ts_comp) {
      case GREATER: // new prepare has higher TS
        return_flag = RMW_ACK_PROPOSE;
        break;
      case EQUAL: // this is valid, but should be rare
        //if (ENABLE_ASSERTIONS)
        // red_printf("Wrkr %u Received a proposal with same TS as an already acked proposal, "
         //            " prop log/glob log %u/%u, glob sess %u/%u, rmw_id %u/%u, version %u/%u, m_id %u/%u \n",
         //          t_id, *(uint32_t *) prop->log_no, glob_entry->log_no, *(uint16_t *) prop->glob_sess_id,
        //          glob_entry->rmw_id.glob_sess_id, *(uint64_t *) prop->t_rmw_id, glob_entry->rmw_id.id,
         //          *(uint32_t *) prop->ts.version, glob_entry->new_ts.version, prop->ts.m_id, glob_entry->new_ts.m_id);
        //assert(false);
        //break;
      case SMALLER:
        return_flag = RMW_SEEN_HIGHER_PROP_TS;
        reply_rmw->ts = glob_entry->new_ts;
        break;
      default : assert(false);
    }
  }
  assert(return_flag == RMW_ACK_PROPOSE || return_flag == ACCEPTED_SAME_RMW_ID ||
         reply_rmw->ts.version > 0);
  return return_flag;
}


// Look at an RMW entry to answer to a propose message-- kv pair lock is held when calling this
static inline uint8_t accept_snoops_entry(struct accept *acc, uint32_t pos, uint8_t sender_m_id,
                                           uint16_t t_id, struct rmw_help_entry *reply_rmw)
{
  uint8_t return_flag = RMW_ACK_ACCEPT;
  struct rmw_entry *glob_entry = &rmw.entry[pos];
  if (ENABLE_ASSERTIONS)  {
    assert(pos < RMW_ENTRIES_NUM);
    assert(acc->opcode == ACCEPT_OP);
    assert(*(uint32_t *) acc->log_no > glob_entry->last_committed_log_no);
    assert(*(uint32_t *) acc->log_no == glob_entry->log_no);
    assert(check_entry_validity_with_key((struct key *) acc->key, pos));
  }

  if (glob_entry->state != INVALID_RMW) {
    // Higher Ts  = Success,  Lower Ts  = Failure
    enum ts_compare ts_comp = compare_netw_ts_with_ts(&acc->ts, &glob_entry->new_ts);
    // Higher Ts  = Success
    if (ts_comp == EQUAL || ts_comp == GREATER) {
      return_flag = RMW_ACK_ACCEPT;
      if (ENABLE_ASSERTIONS) {
        if (ts_comp == EQUAL && glob_entry->state == ACCEPTED)
          red_printf("Wrkr %u Received Accept for the same TS as already accepted, "
                       "version %u/%u m_id %u/%u, rmw_id %u/%u, global_sess_id %u/%u \n",
                     t_id, *(uint32_t *)acc->ts.version, glob_entry->new_ts.version, acc->ts.m_id,
                     glob_entry->new_ts.m_id, acc->t_rmw_id, glob_entry->rmw_id.id,
                     *(uint16_t *) acc->glob_sess_id, glob_entry->rmw_id.glob_sess_id);
      }
    }
    else if (ts_comp == SMALLER) {
      if (glob_entry->state == PROPOSED) {
        reply_rmw->ts = glob_entry-> new_ts;
        return_flag = RMW_SEEN_HIGHER_PROP_TS;
      }
      else if (glob_entry->state == ACCEPTED) {
        memcpy(reply_rmw->value, glob_entry->value, (size_t) RMW_VALUE_SIZE);
        reply_rmw->ts = glob_entry->new_ts;
        assign_second_rmw_id_to_first(&reply_rmw->rmw_id, &glob_entry->rmw_id);
        return_flag = RMW_ACCEPTED_WITH_HIGHER_TS;
      }
    }
    else if (ENABLE_ASSERTIONS) assert(false);
  }

  if (DEBUG_RMW)
      yellow_printf("Wrkr %u: %s Accept with rmw_id %u, glob_sess_id %u, log_no: %u, ts.version: %u, ts_m_id %u,"
                        "locally stored state: %u, locally stored ts: version %u, m_id %u \n",
                    t_id, return_flag == RMW_ACK_ACCEPT ? "Acks" : "Nacks",
                    acc->t_rmw_id, *(uint16_t *) acc->glob_sess_id, *(uint32_t *) acc->log_no,
                      *(uint32_t *) acc->ts.version, acc->ts.m_id, glob_entry->state, glob_entry->new_ts.version,
                      glob_entry->new_ts.m_id);

  if (ENABLE_ASSERTIONS) assert(return_flag == RMW_ACK_ACCEPT || reply_rmw->ts.version > 0);
  return return_flag;
}

// Fill the propose/accept Reply
static inline void fill_rmw_reply(struct rmw_rep_last_committed *rmw_rep,
                                  struct r_rep_fifo *r_rep_fifo,
                                  uint8_t flag, struct rmw_help_entry *rep_entry,
                                  bool is_accept, uint16_t t_id)
{

  switch (flag) {
    case ACCEPTED_SAME_RMW_ID:
    case RMW_ACK_PROPOSE :
      if (DEBUG_RMW) green_printf("Worker %u: Remote propose gets Acked \n", t_id);
      rmw_rep->opcode = RMW_ACK;
      assert(!is_accept);
      if (ENABLE_ASSERTIONS) assert(!is_accept);
      break;
    case RMW_TS_SMALLER_THAN_KVS : // ts was stale, send TS and Value
      if (DEBUG_RMW) green_printf("Worker %u: %s TS is stale \n",
                                  t_id, is_accept ? "Accept": "Propose");
      rmw_rep->opcode = RMW_TS_STALE;
      break;
    case RMW_SEEN_HIGHER_PROP_TS :
      if (DEBUG_RMW) green_printf("Worker %u: %s TS gets nacked \n",
                                  t_id, is_accept ? "Accept": "Propose");
      rmw_rep->opcode = SEEN_HIGHER_PROP;
      break;
    case RMW_ALREADY_ACCEPTED :
      if (DEBUG_RMW) green_printf("Worker %u: %s gets nacked because a bigger Ts has been accepted \n",
                                  t_id, is_accept ? "Accept": "Propose");
      rmw_rep->opcode = RMW_ACCEPTED;
      break;
    case RMW_ALREADY_COMMITTED :
      if (DEBUG_RMW) green_printf("Worker %u: %s gets nacked because the RMW is Already committed! \n",
                                  t_id, is_accept ? "Accept": "Propose");
      rmw_rep->opcode = RMW_ID_COMMITTED;
      break;
    case RMW_LOG_TOO_SMALL:
      if (DEBUG_RMW) green_printf("Worker %u: %s gets nacked because its Log is too small! \n",
                                  t_id, is_accept ? "Accept": "Propose");
      rmw_rep->opcode = LOG_TOO_SMALL;
      break;
    case RMW_ACK_ACCEPT :
      if (DEBUG_RMW) green_printf("Worker %u: Remote accept gets Acked \n", t_id);
      rmw_rep->opcode = RMW_ACK;
      if (ENABLE_ASSERTIONS) assert(is_accept);
      break;
    case RMW_ACCEPTED_WITH_HIGHER_TS :
      if (DEBUG_RMW) green_printf("Worker %u: Remote Accept gets nacked because  bigger Ts has been accepted \n", t_id);
      rmw_rep->opcode = RMW_ACCEPTED;
      if (ENABLE_ASSERTIONS) assert(is_accept);
      break;
    default:
      if (ENABLE_ASSERTIONS) assert(false);
  }
  copy_entry_on_rmw_reply_depending_on_opcode(rmw_rep, r_rep_fifo, rep_entry, is_accept, t_id);
}

//------------------------------ ACCEPTS------------------------------------------

// Insert accepts to the write fifo
static inline void insert_accept_in_writes_message_fifo(struct pending_ops *p_ops,
                                                        struct rmw_local_entry *loc_entry,
                                                        uint16_t t_id)
{
  check_loc_entry_metadata_is_reset(loc_entry);
  struct w_message *w_mes = p_ops->w_fifo->w_message;
  uint32_t w_mes_ptr = p_ops->w_fifo->push_ptr;
  uint8_t inside_w_ptr = w_mes[w_mes_ptr].w_num;

  if (DEBUG_RMW) {
    yellow_printf("Wrkr %u Inserting an accept, bcast size %u, "
                    "rmw_id %lu, global_sess_id %u, fifo push_ptr %u, fifo pull ptr %u\n",
                  t_id, p_ops->w_fifo->bcast_size, loc_entry->rmw_id.id,
                  loc_entry->rmw_id.glob_sess_id,
                  p_ops->w_fifo->push_ptr, p_ops->w_fifo->bcast_pull_ptr);
  }
  // Put the accept on a new message because it will not be carrying a write l_id
  // and thus cannot be coalesced with other messages
  if (inside_w_ptr > 0) {
    MOD_ADD(p_ops->w_fifo->push_ptr, W_FIFO_SIZE);
    w_mes_ptr = p_ops->w_fifo->push_ptr;
    w_mes[w_mes_ptr].w_num = 0;
    inside_w_ptr = 0;
  }
  struct accept_message *acc_mes = (struct accept_message *) &w_mes[w_mes_ptr];
  struct accept *acc = &acc_mes->acc[inside_w_ptr];
  acc->l_id = loc_entry->l_id;
  acc->t_rmw_id = loc_entry->rmw_id.id;
  *(uint16_t *) acc->glob_sess_id = loc_entry->rmw_id.glob_sess_id;
  assign_rmw_id_to_net_rmw_id(&acc->last_committed_rmw_id, &loc_entry->last_committed_rmw_id);
  assign_ts_to_netw_ts(&acc->ts, &loc_entry->new_ts);
  memcpy(acc->key, &loc_entry->key, TRUE_KEY_SIZE);
  acc->opcode = ACCEPT_OP;
  memcpy(acc->value, loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
  *(uint32_t *)acc->log_no = loc_entry->log_no;

  p_ops->w_fifo->bcast_size++;
  w_mes[w_mes_ptr].w_num++;
  if (ENABLE_ASSERTIONS) {
    assert(w_mes[w_mes_ptr].w_num == 1);
    assert(acc->l_id < p_ops->prop_info->l_id);
  }
  if (w_mes[w_mes_ptr].w_num == MAX_ACC_COALESCE) {
    MOD_ADD(p_ops->w_fifo->push_ptr, W_FIFO_SIZE);
    if (ENABLE_ASSERTIONS) assert(p_ops->w_fifo->push_ptr != p_ops->w_fifo->bcast_pull_ptr);
    w_mes[p_ops->w_fifo->push_ptr].w_num = 0;
  }
}

// After gathering a quorum of proposal acks, check if you can accept locally-- THIS IS STRICTLY LOCAL RMWS -- no helps
static inline uint8_t attempt_local_accept(struct pending_ops *p_ops, struct rmw_local_entry *loc_entry,
                                           uint16_t t_id)
{
  uint8_t return_flag;
  struct rmw_entry *glob_entry = &rmw.entry[loc_entry->index_to_rmw];
  my_assert(true_keys_are_equal(&loc_entry->key, &glob_entry->key),
            "Local entry does not contain the same key as global entry");


  // we need to change the global rmw structure, which means we need to lock the kv-pair.
  optik_lock(loc_entry->ptr_to_kv_pair);
  if (loc_entry->rmw_id.id <= committed_glob_sess_rmw_id[loc_entry->rmw_id.glob_sess_id]) {
    optik_unlock_decrement_version(loc_entry->ptr_to_kv_pair);
   // if (!(glob_entry->last_committed_log_no == loc_entry->log_no &&
  //      rmw_ids_are_equal(&glob_entry->last_committed_rmw_id, &loc_entry->rmw_id)))
//      yellow_printf("%u: glob last committed log no %u/%u/%u, rmw_id g/l: %u/%u, glob sess id g/l %u/%u, committed glob session id %u\n",
//                    glob_entry->key.bkt, glob_entry->last_committed_log_no, loc_entry->log_no, loc_entry->accepted_log_no,
//                    glob_entry->last_committed_rmw_id.id,
//                    loc_entry->rmw_id.id, glob_entry->last_committed_rmw_id.glob_sess_id, loc_entry->rmw_id.glob_sess_id,
//                    committed_glob_sess_rmw_id[loc_entry->rmw_id.glob_sess_id]);
    return NACK_ALREADY_COMMITTED;
  }

  if (rmw_ids_are_equal(&loc_entry->rmw_id, &glob_entry->rmw_id) &&
      glob_entry->state != INVALID_RMW) {
    if (ENABLE_ASSERTIONS) {
      assert(glob_entry->log_no == loc_entry->log_no);
      assert(glob_entry->last_committed_log_no == loc_entry->log_no - 1);
      //if (glob_entry->state != PROPOSED){
        //red_printf("Wrkr %u attempting to accept locally, but the rmw is "
        //             "already accepted (from help) glob state %u \n", t_id, glob_entry->state);
      //}
    }
    //state would be typically proposed, but may also be accepted if someone has helped
    if (DEBUG_RMW)
      green_printf("Wrkr %u got rmw id %u, glob sess %u accepted locally \n",
                   t_id, loc_entry->rmw_id.id, loc_entry->rmw_id.glob_sess_id);
    glob_entry->state = ACCEPTED;
    // calculate the new value depending on the type of RMW
    struct cache_op *kv_pair = (struct cache_op *) loc_entry->ptr_to_kv_pair;
    memcpy(glob_entry->value, &kv_pair->value[BYTES_OVERRIDEN_IN_KVS_VALUE], (size_t) RMW_VALUE_SIZE);
    if (RMW_VALUE_SIZE >= 8) {
      *(uint32_t *) glob_entry->value = glob_entry->log_no;
      *(uint32_t *) loc_entry->value_to_write = glob_entry->log_no;
    }
    loc_entry->accepted_log_no = glob_entry->log_no;
    assign_second_rmw_id_to_first(&loc_entry->last_committed_rmw_id, &glob_entry->last_committed_rmw_id);
    //memcpy(loc_entry->value_to_write, glob_entry->value, (size_t) RMW_VALUE_SIZE);
    check_log_nos_of_glob_entry(glob_entry, "attempt_local_accept and succeed", t_id);
    optik_unlock_decrement_version(loc_entry->ptr_to_kv_pair);
    //loc_entry->accept_acks = 0; // the quorum-check always considers a local ack so we do not need to increment this
    return_flag = ACCEPT_ACK;
  }
  else { // the entry stores a different rmw_id and thus our proposal has been won by another
    // Some other RMW has won the RMW we are trying to get accepted
    // If the other RMW has been committed then the last_committed_log_no will be bigger/equal than the current log no
    if (glob_entry->last_committed_log_no >= loc_entry->log_no)
      return_flag = NACK_ACCEPT_LOG_OUT_OF_DATE;
      //Otherwise the RMW that won is still in progress
    else return_flag = NACK_ACCEPT_SEEN_HIGHER_TS;
    // --CHECKS--
    if (ENABLE_ASSERTIONS) {
      if (glob_entry->state == PROPOSED || glob_entry->state == ACCEPTED)
        assert (compare_ts(&glob_entry->new_ts, &loc_entry->new_ts) == GREATER ||
                glob_entry->log_no > loc_entry->log_no);
      else if (glob_entry->state == INVALID_RMW) // some other rmw committed
        assert(glob_entry->last_committed_log_no >= loc_entry->log_no);
      // The RMW can have been committed, because some may have helped it
      //assert (committed_glob_sess_rmw_id[loc_entry->rmw_id.glob_sess_id] < loc_entry->rmw_id.id);
    }
    if (DEBUG_RMW)
      green_printf("Wrkr %u failed to get rmw id %u, glob sess %u accepted locally opcode %u,"
                   "global entry rmw id %u, glob sess %u, state %u \n",
                   t_id, loc_entry->rmw_id.id, loc_entry->rmw_id.glob_sess_id, return_flag,
                   glob_entry->rmw_id.id, glob_entry->rmw_id.glob_sess_id, glob_entry->state);


    check_log_nos_of_glob_entry(glob_entry, "attempt_local_accept and fail", t_id);
    optik_unlock_decrement_version(loc_entry->ptr_to_kv_pair);
  }
  return return_flag;
}

// After gathering a quorum of proposal reps, one of them was a lower TS accept, try and help it
static inline uint8_t attempt_local_accept_to_help(struct pending_ops *p_ops, struct rmw_local_entry *loc_entry,
                                                   uint16_t t_id)
{
  uint8_t return_flag;
  struct rmw_entry *glob_entry = &rmw.entry[loc_entry->index_to_rmw];
  struct rmw_local_entry* help_loc_entry = loc_entry->help_loc_entry;
  help_loc_entry->new_ts = loc_entry->new_ts;
  my_assert(true_keys_are_equal(&help_loc_entry->key, &glob_entry->key),
            "Local entry does not contain the same key as global entry");
  my_assert(loc_entry->help_loc_entry->log_no == loc_entry->log_no,
            " the help entry and the regular have not the same log nos");

  optik_lock(loc_entry->ptr_to_kv_pair);

  if (help_loc_entry->rmw_id.id <= committed_glob_sess_rmw_id[help_loc_entry->rmw_id.glob_sess_id]) {
    optik_unlock_decrement_version(loc_entry->ptr_to_kv_pair);
    return ABORT_HELP;
  }

  // the global entry has seen a higher ts if
  // (its state is not invalid and its TS is higher) or  (it has committed the log)
  bool glob_entry_is_the_same = glob_entry->state == PROPOSED &&
                                help_loc_entry->log_no == glob_entry->log_no &&
                                compare_ts(&glob_entry->new_ts, &loc_entry->new_ts) == EQUAL &&
                                rmw_ids_are_equal(&glob_entry->rmw_id, &loc_entry->rmw_id);
  bool glob_entry_is_invalid_but_not_committed = glob_entry->state == INVALID_RMW &&
    glob_entry->last_committed_log_no < help_loc_entry->log_no;

//    (glob_entry->state != INVALID_RMW &&
//    compare_ts(&glob_entry->new_ts, &loc_entry->new_ts) == GREATER) ||
//    glob_entry->last_committed_log_no >= help_loc_entry->log_no;

  if (glob_entry_is_the_same || glob_entry_is_invalid_but_not_committed) {
    if (ENABLE_ASSERTIONS) {
      // if the TS are equal it better be that it is because it remembers the proposed request
      if (glob_entry->state != INVALID_RMW &&
          compare_ts(&glob_entry->new_ts, &loc_entry->new_ts) == EQUAL) {
        assert(rmw_ids_are_equal(&glob_entry->rmw_id, &loc_entry->rmw_id));
        if (glob_entry->state != PROPOSED) {
          red_printf("Wrkr: %u, state %u \n", t_id, glob_entry->state);
          assert(false);
        }
      }
    }

    if (DEBUG_RMW)
      green_printf("Wrkr %u on attempting to locally accept to help "
                   "got rmw id %u, glob sess %u accepted locally \n",
                   t_id, help_loc_entry->rmw_id.id, help_loc_entry->rmw_id.glob_sess_id);

    glob_entry->state = ACCEPTED;
    assign_second_rmw_id_to_first(&glob_entry->rmw_id, &help_loc_entry->rmw_id);
    assign_second_rmw_id_to_first(&help_loc_entry->last_committed_rmw_id, &glob_entry->last_committed_rmw_id);
    glob_entry->new_ts = help_loc_entry->new_ts;
    memcpy(glob_entry->value, help_loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
    check_log_nos_of_glob_entry(glob_entry, "attempt_local_accept_to_help and succeed", t_id);
    optik_unlock_decrement_version(loc_entry->ptr_to_kv_pair);
    //loc_entry->accept_acks = 0; // the quorum-check always considers a local ack so we do not need to increment this
    return_flag = ACCEPT_ACK;
  }
  else {
    //If the log entry is committed locally abort the help
    //if the global entry has accepted a higher TS then again abort help
   return_flag = ABORT_HELP;
    if (DEBUG_RMW)
      green_printf("Wrkr %u failed to get rmw id %u, glob sess %u accepted locally opcode %u,"
                     "global entry rmw id %u, glob sess %u, state %u \n",
                   t_id, loc_entry->rmw_id.id, loc_entry->rmw_id.glob_sess_id, return_flag,
                   glob_entry->rmw_id.id, glob_entry->rmw_id.glob_sess_id, glob_entry->state);


    check_log_nos_of_glob_entry(glob_entry, "attempt_local_accept_to_help and fail", t_id);
    optik_unlock_decrement_version(loc_entry->ptr_to_kv_pair);
  }
  return return_flag;
}

//------------------------------ COMMITS------------------------------------------

// Call holding the kv_lock to commit an RMW that is initiated locally (helped or from local session)
static inline void commit_helped_or_local_from_loc_entry(struct rmw_entry *glob_entry,
                                                         struct rmw_local_entry *loc_entry,
                                                         struct rmw_local_entry *loc_entry_to_commit, uint16_t t_id)
{
  // If the RMW has not been committed yet locally, commit it if it is not helping,
  // Otherwise, stay in 'accepted' state until a quorum of commit-acks come back, then commit
  if (glob_entry->last_committed_log_no < loc_entry_to_commit->log_no) {
    take_actions_to_commit_rmw(glob_entry, loc_entry_to_commit, loc_entry, t_id);
    if (DEBUG_RMW)
      green_printf("Wrkr %u got rmw id %u, glob sess %u, log %u committed locally,"
                     "glob entry stats: state %u, rmw_id &u, glob sess_id %u, log no %u  \n",
                   t_id, loc_entry_to_commit->rmw_id.id, loc_entry_to_commit->rmw_id.glob_sess_id, loc_entry_to_commit->log_no,
                   glob_entry->state, glob_entry->rmw_id.id, glob_entry->rmw_id.glob_sess_id, glob_entry->log_no);
  }
  // Check if the entry is still working on that log, or whether it has moved on
  // (because the current rmw is already committed)
  if (glob_entry->log_no == loc_entry_to_commit->log_no && glob_entry->state != INVALID_RMW) {
    //the log numbers should match
    if (ENABLE_ASSERTIONS) {
      if (!rmw_ids_are_equal(&loc_entry_to_commit->rmw_id, &glob_entry->rmw_id)) {
        red_printf("Wrkr %u glob entry is on same log as what is about to be committed but on different rmw-id:"
                     " committed rmw id %u, glob sess %u, "
                     "global entry rmw id %u, glob sess %u, state %u,"
                     " committed version %u/%u m_id %u/%u \n",
                   t_id, loc_entry_to_commit->rmw_id.id, loc_entry_to_commit->rmw_id.glob_sess_id,
                   glob_entry->rmw_id.id, glob_entry->rmw_id.glob_sess_id, glob_entry->state,
                   loc_entry_to_commit->new_ts.version, glob_entry->new_ts.version,
                   loc_entry_to_commit->new_ts.m_id, glob_entry->new_ts.m_id);
      }
      assert(rmw_ids_are_equal(&loc_entry_to_commit->rmw_id, &glob_entry->last_committed_rmw_id));
      assert(glob_entry->last_committed_log_no == glob_entry->log_no);
      assert(glob_entry->state == ACCEPTED);
    }
    glob_entry->state = INVALID_RMW;
  }
  else {
    // if the log has moved on then the RMW has been helped,
    // it has been committed in the other machines so there is no need to change its state
    check_log_nos_of_glob_entry(glob_entry, "commit_helped_or_local_from_loc_entry", t_id);
    if (ENABLE_ASSERTIONS) {
      if (glob_entry->state != INVALID_RMW)
        assert(!rmw_ids_are_equal(&glob_entry->rmw_id, &loc_entry_to_commit->rmw_id));
    }
  }
}


// The commitment of the rmw_id is certain here: it has either already been committed or it will be committed here
// Additionally we will always broadcast commits because we need to make sure that
// a quorum of machines have seen the RMW before, committing the RMW
static inline void attempt_local_commit(struct pending_ops *p_ops, struct rmw_local_entry *loc_entry,
                                        uint16_t t_id)
{
  // use only index_to_rmw and ptr_to_kv_pair from the loc_entry
  struct rmw_local_entry *loc_entry_to_commit =
    loc_entry->helping_flag == HELPING_NO_STASHING ? loc_entry->help_loc_entry : loc_entry;


  struct rmw_entry *glob_entry = &rmw.entry[loc_entry->index_to_rmw];
  my_assert(true_keys_are_equal(&loc_entry->key, &glob_entry->key),
            "Local entry does not contain the same key as global entry");
  // we need to change the global rmw structure, which means we need to lock the kv-pair.
  optik_lock(loc_entry->ptr_to_kv_pair);
  commit_helped_or_local_from_loc_entry(glob_entry, loc_entry, loc_entry_to_commit, t_id);
  // Register the RMW-id
  register_committed_global_sess_id (loc_entry_to_commit->rmw_id.glob_sess_id,
                                     loc_entry_to_commit->rmw_id.id, t_id);
  optik_unlock_decrement_version(loc_entry->ptr_to_kv_pair);
  if (DEBUG_RMW)
    green_printf("Wrkr %u will broadcast commits for rmw id %u, glob sess %u, "
                 "global entry rmw id %u, glob sess %u, state %u \n",
                 t_id, loc_entry_to_commit->rmw_id.id, loc_entry_to_commit->rmw_id.glob_sess_id,
                 glob_entry->rmw_id.id, glob_entry->rmw_id.glob_sess_id, glob_entry->state);

  if (DEBUG_LOG)
    green_printf("Log %u: RMW_id %u glob_sess %u, loc entry state %u, local commit\n",
                 loc_entry_to_commit->log_no, loc_entry_to_commit->rmw_id.id,
                 loc_entry_to_commit->rmw_id.glob_sess_id, loc_entry->state);
}

// A reply to a propose/accept may include an RMW to be committed,
static inline void attempt_local_commit_from_rep(struct pending_ops *p_ops, struct rmw_rep_last_committed *rmw_rep,
                                                 struct rmw_local_entry* loc_entry, uint16_t t_id)
{
  check_ptr_is_valid_rmw_rep(rmw_rep);
  struct rmw_entry *glob_entry = &rmw.entry[loc_entry->index_to_rmw];
  uint32_t new_log_no = *(uint32_t *) rmw_rep->log_no;
  uint64_t new_rmw_id = *(uint64_t *) rmw_rep->rmw_id;
  uint16_t new_glob_sess_id = *(uint16_t *) rmw_rep->glob_sess_id;
  if (glob_entry->last_committed_log_no >= new_log_no) return;
  my_assert(true_keys_are_equal(&loc_entry->key, &glob_entry->key),
            "Local entry does not contain the same key as global entry, when committing after rmw reply");

  // we need to change the global rmw structure, which means we need to lock the kv-pair.
  optik_lock(loc_entry->ptr_to_kv_pair);
  // If the RMW has not been committed yet locally, commit it
  if (glob_entry->last_committed_log_no < new_log_no) {
    glob_entry->last_committed_log_no = new_log_no;
    glob_entry->last_committed_rmw_id.id = new_rmw_id;
    glob_entry->last_committed_rmw_id.glob_sess_id = new_glob_sess_id;
    struct cache_op *kv_pair = (struct cache_op *) loc_entry->ptr_to_kv_pair;
    memcpy(&kv_pair->value[BYTES_OVERRIDEN_IN_KVS_VALUE], rmw_rep->value, (size_t) RMW_VALUE_SIZE);
    kv_pair->key.meta.m_id = rmw_rep->ts.m_id;
    kv_pair->key.meta.version = (* (uint32_t *)rmw_rep->ts.version) + 1; // the unlock function will decrement 1
    if (DEBUG_RMW)
      green_printf("Wrkr %u commits locally rmw id %u, glob sess %u after resp with opcode %u \n",
                   t_id, new_rmw_id, new_glob_sess_id, rmw_rep->opcode);
    // if the global entry was working on an already committed log, or
    // if it's not active advance its log no and in both cases transition to INVALID RMW
    if (glob_entry->log_no <= new_log_no ||
        rmw_id_is_equal_with_id_and_glob_sess_id(&glob_entry->rmw_id, new_rmw_id, new_glob_sess_id)) {
      glob_entry->log_no = new_log_no;
      glob_entry->state = INVALID_RMW;
    }
  }
  check_log_nos_of_glob_entry(glob_entry, "attempt_local_commit_from_rep", t_id);

  if (glob_entry->state != INVALID_RMW) {
    struct rmw_local_entry *working_entry = loc_entry->helping_flag == HELPING_NO_STASHING ?
                                           loc_entry->help_loc_entry : loc_entry;
    if (rmw_ids_are_equal(&glob_entry->rmw_id, &working_entry->rmw_id)) {
      assert(rmw_rep->opcode == RMW_ID_COMMITTED);
      red_printf("Wrkr: %u Received an already committed for rmw id id % glob_sess_id %u, "
                   "received highest committed log %u with rmw_id id %u, glob_sess id %u,"
                   "but glob_entry is in state %u, for rmw_id %u, glob_sess id %u \n",
                 t_id, working_entry->rmw_id.id, working_entry->rmw_id.glob_sess_id,
                 new_log_no, new_rmw_id, new_glob_sess_id,
                 glob_entry->state, glob_entry->rmw_id.id, glob_entry->rmw_id.glob_sess_id);
    }
  }
  if (ENABLE_ASSERTIONS) {
    if (glob_entry->state != INVALID_RMW)
      assert(!rmw_id_is_equal_with_id_and_glob_sess_id(&glob_entry->rmw_id, new_rmw_id, new_glob_sess_id));
  }
  register_committed_global_sess_id(new_glob_sess_id, new_rmw_id, t_id);
  optik_unlock_decrement_version(loc_entry->ptr_to_kv_pair);


  if (DEBUG_LOG)
    green_printf("Log %u: RMW_id %u glob_sess %u, loc entry state %u from reply \n",
                 new_log_no, new_rmw_id, new_glob_sess_id, loc_entry->state);

}

// Check if the commit must be applied to the KVS and
// transition the global entry to INVALID_RMW if it has been waiting for this commit
static inline bool handle_remote_commit(struct pending_ops *p_ops,
                                        struct rmw_entry* glob_entry, uint32_t log_no,
                                        uint64_t t_rmw_id, uint16_t glob_sess_id,
                                        struct commit* com, uint16_t t_id)
{
  // the function is called with the lock in hand
  bool overwrite_kv = false;

  // When receiving a commit for my own RMW, free the session
  if (is_global_ses_id_local(glob_sess_id, t_id)) {
    uint32_t sess_id = glob_ses_id_to_sess_id(glob_sess_id);
    struct rmw_local_entry *loc_entry = &p_ops->prop_info->entry[sess_id];
    if (loc_entry->state != INVALID_RMW &&
        loc_entry->rmw_id.id == t_rmw_id &&
        loc_entry->rmw_id.glob_sess_id == glob_sess_id &&
        loc_entry->state != COMMITTED) {
      if (ENABLE_ASSERTIONS) assert(loc_entry->key.bkt == glob_entry->key.bkt);
      if  (log_no != loc_entry->accepted_log_no) {
        cyan_printf("%u %u %u/%u %u \n", glob_entry->key.bkt, 0, log_no, loc_entry->log_no, loc_entry->state);
        loc_entry->accepted_log_no = log_no;
        // TODO also copy the value?
      }
      loc_entry->state = INVALID_RMW;
      free_session(p_ops, loc_entry->sess_id, t_id);
    }

  }

  // First check if that log no (or a higher) has been committed
  if (glob_entry->last_committed_log_no < log_no) {
    overwrite_kv = true;
    glob_entry->last_committed_log_no = log_no;
    glob_entry->last_committed_rmw_id.id = t_rmw_id;
    glob_entry->last_committed_rmw_id.glob_sess_id = glob_sess_id;
    if (DEBUG_LOG)
      green_printf("Log %u: RMW_id %u glob_sess %u ,from remote_commit \n",
                   log_no, t_rmw_id, glob_sess_id);
  }

  // now check if the entry was waiting for this message to get cleared
  if (glob_entry->state != INVALID_RMW && (glob_entry->log_no <= log_no ||
                                           rmw_id_is_equal_with_id_and_glob_sess_id(&glob_entry->rmw_id, t_rmw_id, glob_sess_id))) {
    //yellow_printf("Wrkr %u, when handling a remote commit: state %u, log_no %u/%u,"
    //               " rmw_id: %u/%u, glob_sess_id: %u/%u, version: %u/%u, m_id: %u/%u\n",
    //             t_id, glob_entry->state, glob_entry->log_no, log_no, glob_entry->rmw_id.id, t_rmw_id,
    //            glob_entry->rmw_id.glob_sess_id, glob_sess_id, glob_entry->new_ts.version,
    //            *(uint32_t*)com->ts.version, glob_entry->new_ts.m_id, com->ts.m_id);
    if (ENABLE_ASSERTIONS) {
      if (compare_netw_ts_with_ts(&com->ts, &glob_entry->new_ts) != EQUAL) {
        //assert(false);
      }
    }
    //glob_entry->log_no = glob_entry->last_committed_log_no;
    glob_entry->state = INVALID_RMW;
  }
  else if (glob_entry->log_no > log_no && glob_entry->state != INVALID_RMW) {
    if (glob_entry->rmw_id.id == t_rmw_id && glob_entry->rmw_id.glob_sess_id == glob_sess_id)
      red_printf("Wrkr %u, committed rmw_id %u and glob ses id %u on log no %u, but the global entry is working on "
                   "log %u, state %u rmw_id %u glob sess id %u \n", t_id, t_rmw_id, glob_sess_id, log_no,
                 glob_entry->log_no, glob_entry->state, glob_entry->rmw_id.id,  glob_entry->rmw_id.glob_sess_id);
  }
  return overwrite_kv;
}


// If a quorum of proposal acks have been gathered, try to broadcast accepts
static inline void act_on_quorum_of_prop_acks(struct pending_ops *p_ops, struct rmw_local_entry *loc_entry,
                                              uint16_t t_id)
{
  // first we need to accept locally,
  uint8_t local_state = attempt_local_accept(p_ops, loc_entry, t_id);
  if (ENABLE_ASSERTIONS)
    assert(local_state == ACCEPT_ACK || local_state == NACK_ACCEPT_SEEN_HIGHER_TS ||
           local_state == NACK_ACCEPT_LOG_OUT_OF_DATE || local_state == NACK_ALREADY_COMMITTED);

  zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
  if (local_state == ACCEPT_ACK) {
    check_loc_entry_metadata_is_reset(loc_entry);
    insert_accept_in_writes_message_fifo(p_ops, loc_entry, t_id);
    if (ENABLE_ASSERTIONS) assert(loc_entry->state == PROPOSED);
    loc_entry->state = ACCEPTED;
  }
  else if (local_state == NACK_ALREADY_COMMITTED) {
    // TODO read the (loc_entry->value_to_read)
    //if (is_global_ses_id_local(loc_entry->rmw_id.glob_sess_id, t_id))
     // green_printf("%u %u %u \n", loc_entry->key.bkt, 0, loc_entry->log_no);
    loc_entry->state = INVALID_RMW;
    free_session(p_ops, loc_entry->sess_id, t_id);
  }
  else loc_entry->state = NEEDS_GLOBAL;
}

//Handle a remote propose whose log number and TS are big enough
static inline uint8_t handle_remote_propose_in_cache(struct cache_op *kv_ptr, struct propose *prop,
                                                     uint8_t sender_m_id, uint16_t t_id,
                                                     struct rmw_help_entry *reply_rmw, uint32_t *entry)
{
  uint8_t flag  = RMW_ACK_PROPOSE;
  uint64_t rmw_l_id = *(uint64_t *) prop->t_rmw_id;
  // if there is no entry just ack and create entry
  if (kv_ptr->opcode == KEY_HAS_NEVER_BEEN_RMWED) {
    *entry = grab_RMW_entry(PROPOSED, kv_ptr, prop->opcode,
                            prop->ts.m_id, *(uint32_t *) prop->ts.version,
                            rmw_l_id, *(uint32_t *) prop->log_no,
                            *(uint16_t *)prop->glob_sess_id, t_id);
    if (ENABLE_ASSERTIONS) assert(*entry == *(uint32_t *)kv_ptr->value);
    kv_ptr->opcode = KEY_HAS_BEEN_RMWED;
    if (DEBUG_RMW)
      yellow_printf("Worker %u got entry %u for a remote propose RMW, new KVS-stored opcode %u \n",
                    t_id, *entry, kv_ptr->opcode);
    flag = RMW_ACK_PROPOSE;
  }
  else if (kv_ptr->opcode == KEY_HAS_BEEN_RMWED) { // Entry already exists
    // if the log number is higher than expected blindly ack
    if (*(uint32_t *) prop->log_no > rmw.entry[*entry].log_no) {
      check_that_log_is_too_high(&rmw.entry[*entry], *(uint32_t *) prop->log_no);
      flag = RMW_ACK_PROPOSE;
    }
    else
      flag = propose_snoops_entry(prop, *entry, sender_m_id, t_id, reply_rmw);
  }
  else my_assert(false, "KVS opcode is wrong!");
  return flag;
}

//Handle a remote accept whose log number and TS are big enough
static inline uint8_t handle_remote_accept_in_cache(struct cache_op *kv_ptr, struct accept *acc,
                                                     uint8_t sender_m_id, uint16_t t_id,
                                                     struct rmw_help_entry *reply_rmw, uint32_t *entry)
{
  uint8_t flag  = RMW_ACK_ACCEPT;
  // if there is no entry just ack and create entry
  if (kv_ptr->opcode == KEY_HAS_NEVER_BEEN_RMWED) {
    *entry = grab_RMW_entry(ACCEPTED, kv_ptr, acc->opcode,
                            acc->ts.m_id, *(uint32_t *) acc->ts.version,
                            acc->t_rmw_id, *(uint32_t *) acc->log_no,
                            *(uint16_t *)acc->glob_sess_id, t_id);
    if (ENABLE_ASSERTIONS) assert(*entry == *(uint32_t *)kv_ptr->value);
    kv_ptr->opcode = KEY_HAS_BEEN_RMWED;
    if (DEBUG_RMW)
      yellow_printf("Worker %u got entry %u for a remote accept RMW, new opcode %u \n",
                    t_id, *entry, kv_ptr->opcode);
    flag = RMW_ACK_ACCEPT;
  }
  else if (kv_ptr->opcode == KEY_HAS_BEEN_RMWED) { // Entry already exists
    // if the log number is higher than expected blindly ack
    if (*(uint32_t *) acc->log_no > rmw.entry[*entry].log_no) {
      check_that_log_is_too_high(&rmw.entry[*entry], *(uint32_t *) acc->log_no);
      flag = RMW_ACK_ACCEPT;
    }
    else
      flag = accept_snoops_entry(acc, *entry, sender_m_id, t_id, reply_rmw);
  }
  else my_assert(false, "KVS opcode is wrong!");
  return flag;
}

// Handle a proposal reply
static inline void handle_propose_reply(struct pending_ops *p_ops, struct rmw_rep_last_committed *prop_rep,
                                        struct rmw_local_entry *loc_entry,
                                        const uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->state == PROPOSED);
    assert(loc_entry->helping_flag == NOT_HELPING);
  }
  loc_entry->rmw_reps.tot_replies++;

  switch (prop_rep->opcode) {
    case RMW_ACK:
      loc_entry->rmw_reps.acks++;
      break;
    case RMW_ID_COMMITTED:
      loc_entry->rmw_reps.rmw_id_commited++;
      attempt_local_commit_from_rep(p_ops, prop_rep, loc_entry, t_id);
//      if (!(*(uint32_t*)prop_rep->log_no == loc_entry->log_no &&
//        (*(uint64_t*)prop_rep->rmw_id == loc_entry->rmw_id.id &&
//        *(uint16_t*)prop_rep->glob_sess_id == loc_entry->rmw_id.glob_sess_id)))
//        yellow_printf("%u: RMW_ID_COMMITTED-propose: committed log no %u/%u, rmw_id c/l: %u/%u, glob sess id c/l %u/%u, \n",
//                      loc_entry->key.bkt, *(uint32_t*)prop_rep->log_no, loc_entry->log_no, *(uint64_t*)prop_rep->rmw_id,
//                      loc_entry->rmw_id.id, *(uint16_t*)prop_rep->glob_sess_id, loc_entry->rmw_id.glob_sess_id);
      // store the reply in the help loc_entry
      store_rmw_rep_to_help_loc_entry(loc_entry, prop_rep, t_id);
      break;
    case LOG_TOO_SMALL:
      loc_entry->rmw_reps.log_too_small++;
      attempt_local_commit_from_rep(p_ops, prop_rep, loc_entry, t_id);
      break;
    case RMW_TS_STALE:
      loc_entry->rmw_reps.ts_stale++;
      if (loc_entry->rmw_reps.rmw_id_commited + loc_entry->rmw_reps.log_too_small == 0) {
        // if this is the first such response or if the ts is greater than the last such response
        if (loc_entry->rmw_reps.ts_stale == 1 ||
            compare_netw_ts_with_ts(&prop_rep->ts, &loc_entry->rmw_reps.kvs_higher_ts) == GREATER) {
          assign_netw_ts_to_ts(&loc_entry->rmw_reps.kvs_higher_ts, &prop_rep->ts);
          check_version(loc_entry->rmw_reps.kvs_higher_ts.version, "handle_propose_reply");
        }
      }
      break;
    case RMW_ACCEPTED:
      loc_entry->rmw_reps.already_accepted++;
      // Store the accepted rmw only if no higher priority reps have been seen
      if (loc_entry->rmw_reps.rmw_id_commited + loc_entry->rmw_reps.log_too_small +
          loc_entry->rmw_reps.ts_stale == 0) {
          store_rmw_rep_to_help_loc_entry(loc_entry, prop_rep, t_id);
      }
      break;
    case SEEN_HIGHER_PROP:
      loc_entry->rmw_reps.seen_higher_prop++;
      if (DEBUG_RMW)
        yellow_printf("Wrkr %u: the prop rep is SEEN_HIGHER_PROP %u sum of all other reps %u \n", t_id,
                       loc_entry->rmw_reps.seen_higher_prop,
                       loc_entry->rmw_reps.rmw_id_commited + loc_entry->rmw_reps.log_too_small +
                       loc_entry->rmw_reps.ts_stale + loc_entry->rmw_reps.already_accepted);
      if (loc_entry->rmw_reps.rmw_id_commited + loc_entry->rmw_reps.log_too_small +
          loc_entry->rmw_reps.ts_stale + loc_entry->rmw_reps.already_accepted == 0) {
        // if this is the first such response or if the ts is greater than the last such response
        if (loc_entry->rmw_reps.seen_higher_prop == 1 ||
            compare_netw_ts_with_ts(&prop_rep->ts, &loc_entry->rmw_reps.seen_higher_prop_ts) == GREATER) {
          assign_netw_ts_to_ts(&loc_entry->rmw_reps.seen_higher_prop_ts, &prop_rep->ts);
          if (DEBUG_RMW)
            yellow_printf("Wrkr %u: overwriting the TS version %u \n",
            t_id, loc_entry->rmw_reps.seen_higher_prop_ts.version);
        }
      }
      break;
    default:
      if (ENABLE_ASSERTIONS) assert(false);
  }

  if (ENABLE_ASSERTIONS)
    check_sum_of_reps(loc_entry);
}


// Handle an accept reply
static inline void handle_accept_reply(struct pending_ops *p_ops, struct rmw_rep_message *acc_rep_mes,
                                       struct rmw_rep_last_committed *acc_rep, struct rmw_local_entry *loc_entry,
                                       const uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) assert(loc_entry->state == ACCEPTED);
  loc_entry->rmw_reps.tot_replies++;
  switch (acc_rep->opcode) {
    case RMW_ACK:
      loc_entry->rmw_reps.acks++;
      if (DEBUG_RMW)
        green_printf("Wrkr %u, the received rep is an accept ack, "
                      "total acks %u \n", t_id, loc_entry->rmw_reps.acks);
      break;
    case RMW_ID_COMMITTED:
      loc_entry->rmw_reps.rmw_id_commited++;
      attempt_local_commit_from_rep(p_ops, acc_rep, loc_entry, t_id);
      if (!(*(uint32_t*)acc_rep->log_no == loc_entry->log_no &&
          (*(uint64_t*)acc_rep->rmw_id == loc_entry->rmw_id.id &&
          *(uint16_t*)acc_rep->glob_sess_id == loc_entry->rmw_id.glob_sess_id)) &&
          *(uint32_t*)acc_rep->log_no <= loc_entry->accepted_log_no &&
          loc_entry->helping_flag == NOT_HELPING) {
//        red_printf(
//          "%u: RMW_ID_COMMITTED-Accept: committed log no %u/%u/%u, rmw_id c/l: %u/%u, glob sess id c/l %u/%u, \n",
//          loc_entry->key.bkt, *(uint32_t *) acc_rep->log_no, loc_entry->log_no, loc_entry->accepted_log_no,
//          *(uint64_t *) acc_rep->rmw_id, loc_entry->rmw_id.id, *(uint16_t *) acc_rep->glob_sess_id,
//          loc_entry->rmw_id.glob_sess_id);
        //assert(false);
      }
      // store the reply in the help loc_entry
      store_rmw_rep_to_help_loc_entry(loc_entry, acc_rep, t_id);
      break;
    case LOG_TOO_SMALL:
      loc_entry->rmw_reps.log_too_small++;
      attempt_local_commit_from_rep(p_ops, acc_rep, loc_entry, t_id);
      break;
    case RMW_TS_STALE:
      loc_entry->rmw_reps.ts_stale++;
      if (loc_entry->rmw_reps.rmw_id_commited + loc_entry->rmw_reps.log_too_small == 0) {
        // if this is the first such response or if the ts is greater than the last such response
        if (loc_entry->rmw_reps.ts_stale == 1 ||
            compare_netw_ts_with_ts(&acc_rep->ts, &loc_entry->rmw_reps.kvs_higher_ts) == GREATER) {
          assign_netw_ts_to_ts(&loc_entry->rmw_reps.kvs_higher_ts, &acc_rep->ts);
          check_version(loc_entry->rmw_reps.kvs_higher_ts.version, "handle_accept_reply:  case RMW_TS_STALE");
        }
      }
      break;
    case RMW_ACCEPTED:
      loc_entry->rmw_reps.already_accepted++;
      // Store the accepted rmw only if no higher priority reps have been seen
      if (loc_entry->helping_flag == NOT_HELPING) {
        if (loc_entry->rmw_reps.rmw_id_commited + loc_entry->rmw_reps.log_too_small +
            loc_entry->rmw_reps.ts_stale == 0)
          store_rmw_rep_to_help_loc_entry(loc_entry, acc_rep, t_id);
      }
      break;
    case SEEN_HIGHER_PROP:
      loc_entry->rmw_reps.seen_higher_prop++;
      if (DEBUG_RMW)
        yellow_printf("Wrkr %u: the prop rep is SEEN_HIGHER_PROP %u sum of all other reps %u \n", t_id,
                      loc_entry->rmw_reps.seen_higher_prop,
                      loc_entry->rmw_reps.rmw_id_commited + loc_entry->rmw_reps.log_too_small +
                      loc_entry->rmw_reps.ts_stale + loc_entry->rmw_reps.already_accepted);
      if (loc_entry->rmw_reps.rmw_id_commited + loc_entry->rmw_reps.log_too_small +
          loc_entry->rmw_reps.ts_stale + loc_entry->rmw_reps.already_accepted == 0) {
        // if this is the first such response or if the ts is greater than the last such response
        if (loc_entry->rmw_reps.seen_higher_prop == 1 ||
            compare_netw_ts_with_ts(&acc_rep->ts, &loc_entry->rmw_reps.seen_higher_prop_ts) == GREATER) {
          assign_netw_ts_to_ts(&loc_entry->rmw_reps.seen_higher_prop_ts, &acc_rep->ts);
          if (DEBUG_RMW)
            yellow_printf("Wrkr %u: overwriting the TS version %u \n",
                          t_id, loc_entry->rmw_reps.seen_higher_prop_ts.version);
          check_version(loc_entry->rmw_reps.seen_higher_prop_ts.version, "handle_accept_reply: case SEEN_HIGHER_PROP:");
        }
      }
      break;
    default:
      if (ENABLE_ASSERTIONS) assert(false);
  }
}


// Handle read replies that refer to RMWs (either replies to accepts or proposes)
static inline void handle_rmw_rep_replies(struct pending_ops *p_ops, struct r_rep_message *r_rep_mes,
                                          bool is_accept, uint16_t t_id) {
  struct rmw_rep_message *rep_mes = (struct rmw_rep_message *) r_rep_mes;

  struct prop_info *prop_info = p_ops->prop_info;
  uint8_t rep_num = rep_mes->coalesce_num;

  if (ENABLE_ASSERTIONS) assert(rep_num == 1);

  uint16_t byte_ptr = PROP_REP_MES_HEADER; // same for both accepts and replies
  for (uint16_t i = 0; i < rep_num; i++) {
    // First attempt to find the entry the response is for
    struct rmw_rep_last_committed *rep = (struct rmw_rep_last_committed *) (((void *) rep_mes) + byte_ptr);
    if (ENABLE_ASSERTIONS) {
      if (prop_info->l_id <= *(uint64_t *) rep->l_id)
        red_printf("Wrkr %u, incoming rep l_id %u, max prop lid %u \n", t_id, *(uint64_t *) rep->l_id, prop_info->l_id);

        assert(prop_info->l_id > *(uint64_t *) rep->l_id);
    }
    int entry_i = search_prop_entries_with_l_id(prop_info, (uint8_t) (is_accept ? ACCEPTED : PROPOSED),
                                                *(uint64_t *) rep->l_id);
    if (entry_i == -1) continue;
    struct rmw_local_entry *loc_entry = &prop_info->entry[entry_i];
    // if there are more replies move the byte_ptr with respect to the current rep
    if (i < rep_num - 1) move_ptr_to_next_rmw_reply(&byte_ptr, rep->opcode);
    if (unlikely(rep->opcode) > LOG_TOO_SMALL) rep->opcode -= FALSE_POSITIVE_OFFSET;

    if (!is_accept) handle_propose_reply(p_ops, rep, loc_entry, t_id);
    else handle_accept_reply(p_ops, rep_mes, rep, loc_entry, t_id);
  }
  r_rep_mes->opcode = INVALID_OPCODE;
}


// Insert a helping accept in the write fifo after waiting on it
static inline void attempt_to_help_a_locally_accepted_value(struct pending_ops *p_ops,
                                                            struct rmw_local_entry *loc_entry,
                                                            struct rmw_entry *glob_entry, uint16_t t_id)
{
  bool help = false;
  // set_up the entry to be helped
  optik_lock(loc_entry->ptr_to_kv_pair);
  // check again with the lock in hand
  if ((glob_entry->state == ACCEPTED &&
       rmw_ids_are_equal(&loc_entry->help_rmw->rmw_id, &glob_entry->rmw_id))) {
    loc_entry->helping_flag = HELPING_NEED_STASHING;
    // Need only to stash RMW-id, opcode & the state as log, TS, value are unknown
    loc_entry->help_rmw->opcode = loc_entry->opcode;
    loc_entry->help_rmw->state = loc_entry->state;
    swap_rmw_ids(&loc_entry->rmw_id, &loc_entry->help_rmw->rmw_id);
    if (ENABLE_ASSERTIONS) {
      assert(rmw_ids_are_equal(&loc_entry->rmw_id, &glob_entry->rmw_id));
      //assert(committed_glob_sess_rmw_id[glob_entry->rmw_id.glob_sess_id] < glob_entry->rmw_id.id);
    }
    // Load the RMW to be helped (rmw-id is already there from swapping)
    loc_entry->new_ts = glob_entry->new_ts;
    loc_entry->log_no = glob_entry->log_no;
    assign_second_rmw_id_to_first(&loc_entry->last_committed_rmw_id, &glob_entry->last_committed_rmw_id);
    memcpy(loc_entry->value_to_write, glob_entry->value, (size_t) RMW_VALUE_SIZE);
    help = true;
  }
  check_log_nos_of_glob_entry(glob_entry, "attempt_to_help_a_locally_accepted_value", t_id);
  optik_unlock_decrement_version(loc_entry->ptr_to_kv_pair);

  loc_entry->back_off_cntr = 0;
  if (help) {
    //if (DEBUG_RMW)
      cyan_printf("Wrkr %u, session %u helps RMW id %u glob_sess_id %u with version %u, m_id %u,"
                  " glob log/help log %u/%u glob committed log %u , biggest committed rmw_id %u for glob sess %u"
                  " stashed rmw_id: %u, global_sess id %u, state %u \n",
                  t_id, loc_entry->sess_id, loc_entry->rmw_id.id, loc_entry->rmw_id.glob_sess_id,
                  loc_entry->new_ts.version, loc_entry->new_ts.m_id,
                  glob_entry->log_no, loc_entry->log_no, glob_entry->last_committed_log_no,
                  committed_glob_sess_rmw_id[glob_entry->rmw_id.glob_sess_id], glob_entry->rmw_id.glob_sess_id,
                  loc_entry->help_rmw->rmw_id.id, loc_entry->help_rmw->rmw_id.glob_sess_id, loc_entry->help_rmw->state);
    loc_entry->state = ACCEPTED;
    zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
    insert_accept_in_writes_message_fifo(p_ops, loc_entry, t_id);
  }
}

// After backing off waiting on a PROPOSED global entry try to steal it
static inline void attempt_to_steal_a_proposed_global_entry(struct pending_ops *p_ops,
                                                            struct rmw_local_entry *loc_entry,
                                                            struct rmw_entry *glob_entry,
                                                            uint16_t sess_i, uint16_t t_id) {
  bool global_entry_was_grabbed = false;
  struct cache_op *kv_pair = (struct cache_op *) loc_entry->ptr_to_kv_pair;
  optik_lock(loc_entry->ptr_to_kv_pair);
  if (if_already_committed_free_session_invalidate_entry(p_ops, loc_entry, t_id)) {
    optik_unlock_decrement_version(loc_entry->ptr_to_kv_pair);
    return;
  }
  //printf("glob rmw_id %lu, glob_sess id %u  state %u \n", glob_entry->rmw_id.id,
   //      glob_entry->rmw_id.glob_sess_id, glob_entry->state);
  uint32_t new_version = 0;
  if (glob_entry->state == INVALID_RMW || glob_state_has_not_changed(glob_entry, loc_entry)) {
    loc_entry->log_no = glob_entry->last_committed_log_no + 1;
    new_version = glob_entry->state == INVALID_RMW ?
                  kv_pair->key.meta.version + 1 : glob_entry->new_ts.version + 2;
    activate_RMW_entry(PROPOSED, new_version, glob_entry, loc_entry->opcode,
                       (uint8_t) machine_id, loc_entry->rmw_id.id,
                       loc_entry->rmw_id.glob_sess_id, loc_entry->log_no, t_id,
                       ENABLE_ASSERTIONS ? "attempt_to_steal_a_proposed_global_entry" : NULL);
    global_entry_was_grabbed = true;
  }
  else if (glob_state_has_changed(glob_entry, loc_entry)) {
    //if (DEBUG_RMW)
      yellow_printf("Wrkr %u, session %u on attempting to steal the propose, changed who is "
                      "waiting for in entry %u: waited for %u cycles for state %u "
                      "rmw_id %u glob_sess_id %u, state %u,  now waiting on rmw_id % glob_sess_id %u, state %u\n",
                    t_id, loc_entry->sess_id, loc_entry->index_to_rmw, loc_entry->back_off_cntr,
                    loc_entry->help_rmw->state, loc_entry->help_rmw->rmw_id.id, loc_entry->help_rmw->rmw_id.glob_sess_id,
                    glob_entry->rmw_id.id, glob_entry->rmw_id.glob_sess_id, glob_entry->state);
    loc_entry->help_rmw->state = glob_entry->state;
    assign_second_rmw_id_to_first(&loc_entry->help_rmw->rmw_id, &glob_entry->rmw_id);
  }
  else if (ENABLE_ASSERTIONS) assert(false);
  check_log_nos_of_glob_entry(glob_entry, "attempt_to_steal_a_proposed_global_entry", t_id);
  optik_unlock_decrement_version(loc_entry->ptr_to_kv_pair);
  loc_entry->back_off_cntr = 0;
  if (global_entry_was_grabbed) {
    //if (DEBUG_RMW)
      cyan_printf("Wrkr %u: session %u steals the global entry %u to do its propose \n",
                  t_id, loc_entry->sess_id, loc_entry->index_to_rmw);
    fill_loc_rmw_entry_on_grabbing_global(p_ops, loc_entry, new_version,
                                          PROPOSED, sess_i, t_id);
  }
}


// When receiving a response that says that a higher-TS RMW has been seen or TS was stale
static inline void take_global_entry_with_higher_TS(struct pending_ops *p_ops,
                                                    struct rmw_local_entry *loc_entry,
                                                    uint32_t new_version,
                                                    uint16_t t_id)
{
  bool global_entry_was_grabbed = false;
  struct rmw_entry* glob_entry = &rmw.entry[loc_entry->index_to_rmw];
  optik_lock(loc_entry->ptr_to_kv_pair);
  if (if_already_committed_free_session_invalidate_entry(p_ops, loc_entry, t_id)) {
    optik_unlock_decrement_version(loc_entry->ptr_to_kv_pair);
    return;
  }
  // if either state is invalid or we own it
  if (glob_entry->state == INVALID_RMW || rmw_ids_are_equal(&glob_entry->rmw_id, &loc_entry->rmw_id)) {
    if (glob_entry->state == INVALID_RMW) {
      glob_entry->log_no = glob_entry->last_committed_log_no + 1;
      glob_entry->opcode = loc_entry->opcode;
      assign_second_rmw_id_to_first(&glob_entry->rmw_id, &loc_entry->rmw_id);
    }
    else if (ENABLE_ASSERTIONS) {
      assert(loc_entry->log_no == glob_entry->last_committed_log_no + 1);
      assert(glob_entry->log_no == glob_entry->last_committed_log_no + 1);
    }
    loc_entry->log_no = glob_entry->last_committed_log_no + 1;
    loc_entry->new_ts.version = new_version + 2;
    loc_entry->new_ts.m_id = (uint8_t) machine_id;
    glob_entry->new_ts = loc_entry->new_ts;
    glob_entry->state = PROPOSED;
    global_entry_was_grabbed = true;
  }
  else  {
    if (DEBUG_RMW)
      yellow_printf("Wrkr %u, session %u  failed when attempting to get/regain the glob_entry, "
                      "waiting for in entry %u: waited for %u cycles for "
                      "now waiting on rmw_id % glob_sess_id %u, state %u\n",
                    t_id, loc_entry->sess_id, loc_entry->index_to_rmw,
                    glob_entry->rmw_id.id, glob_entry->rmw_id.glob_sess_id, glob_entry->state);
  }
  check_log_nos_of_glob_entry(glob_entry, "take_global_entry_with_higher_TS", t_id);
  optik_unlock_decrement_version(loc_entry->ptr_to_kv_pair);


  if (global_entry_was_grabbed) {
    if (DEBUG_RMW)
      cyan_printf("Wrkr %u: session %u gets/regains the global entry log %u to do its propose \n",
                  t_id, loc_entry->sess_id, glob_entry->log_no);
    loc_entry->state = PROPOSED;
  }
  else loc_entry->state = NEEDS_GLOBAL;

//  loc_entry->help_loc_entry->state = INVALID_RMW;
//  memset(&loc_entry->rmw_reps, 0, sizeof(struct rmw_rep_info));
//  loc_entry->back_off_cntr = 0;
}


// When a quorum of prop replies have been received, and one of the replies says it has accepted a different RMW
static inline void act_on_receiving_already_accepted_rep_to_prop(struct pending_ops *p_ops,
                                                                 struct rmw_local_entry* loc_entry,
                                                                 uint32_t* new_version,
                                                                 uint16_t t_id)
{
  struct rmw_local_entry* help_loc_entry = loc_entry->help_loc_entry;
  enum ts_compare ts_comp = compare_ts(&help_loc_entry->new_ts, &loc_entry->new_ts);
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->help_loc_entry->state == ACCEPTED);
    //assert(ts_comp != EQUAL);
  }
  bool accepted_ts_is_smaller = ts_comp == SMALLER;
  // If my ts is bigger then the accepted TS may need to be helped
  // if a quorum of acks could not be gathered
  if (accepted_ts_is_smaller) {
    if (loc_entry->rmw_reps.acks >= REMOTE_QUORUM) {
      act_on_quorum_of_prop_acks(p_ops, loc_entry, t_id);
    }
    else { // help the accepted
      uint8_t flag = attempt_local_accept_to_help(p_ops, loc_entry, t_id);
      if (flag == ACCEPT_ACK) {
        loc_entry->helping_flag = HELPING_NO_STASHING;
        loc_entry->state = ACCEPTED;
        zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
        insert_accept_in_writes_message_fifo(p_ops, help_loc_entry, t_id);
      }
      else { // abort the help, on failing to accept locally
        loc_entry->state = NEEDS_GLOBAL;
        help_loc_entry->state = INVALID_RMW;
      }
    }
  } else { // accepted ts is bigger
    // try and get the global entry (if it is not already held)
    // if successful then send proposals out again
    // else transition to needs global
    *new_version = help_loc_entry->new_ts.version;
    loc_entry->state = RETRY_WITH_BIGGER_TS;
  }
}


static inline void update_KVS_on_receiving_a_TS_stale_rep(struct pending_ops *p_ops,
                                                          struct rmw_local_entry *loc_entry,
                                                          uint16_t t_id)
{
  struct cache_op *kv_pair = (struct cache_op *) loc_entry->ptr_to_kv_pair;
  struct rmw_entry* glob_entry = &rmw.entry[loc_entry->index_to_rmw];

  optik_lock(loc_entry->ptr_to_kv_pair);
  // If the last committed_log_number is still the same then change the KV
  if (glob_entry->last_committed_log_no == loc_entry->log_no &&
      compare_meta_ts_with_ts(&kv_pair->key.meta, &loc_entry->rmw_reps.kvs_higher_ts) == SMALLER) {
    if (ENABLE_ASSERTIONS) {
      assert(kv_pair->key.meta.version <= loc_entry->rmw_reps.kvs_higher_ts.version);
      assert(loc_entry->rmw_reps.kvs_higher_ts.version % 2 == 0);
    }
    if (DEBUG_RMW)
      yellow_printf("Wrkr %u, on learning that the KVS TS was stale, update the TS version %u/%u m _id %u/%u "
                      "glob_entry rmw_id %u, glob sess id %u, state %u\n",
                    t_id, kv_pair->key.meta.version, loc_entry->rmw_reps.kvs_higher_ts.version,
                    kv_pair->key.meta.m_id, loc_entry->rmw_reps.kvs_higher_ts.m_id,
                    glob_entry->rmw_id.id, glob_entry->rmw_id.glob_sess_id, glob_entry->state);

    kv_pair->key.meta.version = loc_entry->rmw_reps.kvs_higher_ts.version + 1;
    kv_pair->key.meta.m_id = loc_entry->rmw_reps.kvs_higher_ts.m_id;
  }
  check_log_nos_of_glob_entry(glob_entry, "update_KVS_on_receiving_a_TS_stale_rep", t_id);
  optik_unlock_decrement_version(loc_entry->ptr_to_kv_pair);
}


static inline void free_glob_entry_if_help_failed(struct rmw_local_entry *loc_entry,
                                                 uint16_t t_id)
{
  struct rmw_entry *glob_entry = &rmw.entry[loc_entry->index_to_rmw];
  struct rmw_local_entry *working_loc_entry = loc_entry->helping_flag == HELPING_NO_STASHING ?
                                              loc_entry->help_loc_entry : loc_entry;
  assert(loc_entry->log_no == working_loc_entry->log_no);
  if (glob_entry->state == ACCEPTED &&
      glob_entry->log_no == working_loc_entry->log_no &&
      rmw_ids_are_equal(&glob_entry->rmw_id, &working_loc_entry->rmw_id) &&
      compare_ts(&glob_entry->new_ts, &working_loc_entry->new_ts) == EQUAL) {
//    cyan_printf("Wrkr %u, GLOB_ENTRY NEEDS TO BE FREED: session %u RMW id %u/%u glob_sess_id %u/%u with version %u/%u,"
//                  " m_id %u/%u,"
//                  " glob log/help log %u/%u glob committed log %u , biggest committed rmw_id %u for glob sess %u"
//                  " \n",
//                t_id, loc_entry->sess_id, working_loc_entry->rmw_id.id, glob_entry->rmw_id.id,
//                working_loc_entry->rmw_id.glob_sess_id, glob_entry->rmw_id.glob_sess_id,
//                working_loc_entry->new_ts.version, glob_entry->new_ts.version,
//                working_loc_entry->new_ts.m_id, glob_entry->new_ts.m_id,
//                glob_entry->log_no, working_loc_entry->log_no, glob_entry->last_committed_log_no,
//                committed_glob_sess_rmw_id[glob_entry->rmw_id.glob_sess_id], glob_entry->rmw_id.glob_sess_id);

    optik_lock(loc_entry->ptr_to_kv_pair);
    if (glob_entry->state == ACCEPTED &&
        glob_entry->log_no == working_loc_entry->log_no &&
        rmw_ids_are_equal(&glob_entry->rmw_id, &working_loc_entry->rmw_id) &&
        compare_ts(&glob_entry->new_ts, &working_loc_entry->new_ts) == EQUAL) {
      glob_entry->state = INVALID_RMW;
    }
    check_log_nos_of_glob_entry(glob_entry, "free_glob_entry_if_help_failed", t_id);
    optik_unlock_decrement_version(loc_entry->ptr_to_kv_pair);
  }

}

static inline void free_glob_entry_if_rmw_failed(struct rmw_local_entry *loc_entry,
                                                 uint8_t state, uint16_t t_id)
{
  struct rmw_entry *glob_entry = &rmw.entry[loc_entry->index_to_rmw];


  if (glob_entry->state == state &&
      glob_entry->log_no == loc_entry->log_no &&
      rmw_ids_are_equal(&glob_entry->rmw_id, &loc_entry->rmw_id) &&
      compare_ts(&glob_entry->new_ts, &loc_entry->new_ts) == EQUAL) {
//    cyan_printf("Wrkr %u, GLOB_ENTRY NEEDS TO BE FREED: session %u RMW id %u/%u glob_sess_id %u/%u with version %u/%u,"
//                  " m_id %u/%u,"
//                  " glob log/help log %u/%u glob committed log %u , biggest committed rmw_id %u for glob sess %u"
//                  " \n",
//                t_id, loc_entry->sess_id, working_loc_entry->rmw_id.id, glob_entry->rmw_id.id,
//                working_loc_entry->rmw_id.glob_sess_id, glob_entry->rmw_id.glob_sess_id,
//                working_loc_entry->new_ts.version, glob_entry->new_ts.version,
//                working_loc_entry->new_ts.m_id, glob_entry->new_ts.m_id,
//                glob_entry->log_no, working_loc_entry->log_no, glob_entry->last_committed_log_no,
//                committed_glob_sess_rmw_id[glob_entry->rmw_id.glob_sess_id], glob_entry->rmw_id.glob_sess_id);

    optik_lock(loc_entry->ptr_to_kv_pair);
    if (glob_entry->state == state &&
        glob_entry->log_no == loc_entry->log_no &&
        rmw_ids_are_equal(&glob_entry->rmw_id, &loc_entry->rmw_id) &&
        compare_ts(&glob_entry->new_ts, &loc_entry->new_ts) == EQUAL) {
      glob_entry->state = INVALID_RMW;
    }
    check_log_nos_of_glob_entry(glob_entry, "free_glob_entry_if_prop_failed", t_id);
    optik_unlock_decrement_version(loc_entry->ptr_to_kv_pair);
  }

}

// Inspect each propose that has gathered a quorum of replies
static inline void inspect_proposes(struct pending_ops *p_ops,
                                    struct rmw_local_entry *loc_entry,
                                    uint16_t t_id)
{
  uint32_t new_version = 0;
  struct rmw_local_entry tmp;
  struct rmw_local_entry *dbg_loc_entry = &tmp;
  memcpy(dbg_loc_entry, loc_entry, sizeof(struct rmw_local_entry));
  // RMW_ID COMMITTED
  if (loc_entry->rmw_reps.rmw_id_commited > 0) {
    if (loc_entry->rmw_reps.rmw_id_commited < REMOTE_QUORUM) {
      loc_entry->state = MUST_BCAST_COMMITS_FROM_HELP;
      if (MACHINE_NUM == 3) assert(false);
      // TODO: We assume that the value to be read and to be written must exist in the local_entry
      // TODO: Enforce that invariant when retrying accepts
      // TODO Read locally and broadcast commits
    }
    else {
      //free the session here as well
      loc_entry->state = INVALID_RMW;
      free_session(p_ops, loc_entry->sess_id, t_id);
    }
    check_state_with_allowed_flags(3, (int) loc_entry->state, INVALID_RMW, MUST_BCAST_COMMITS_FROM_HELP);
  }
  // LOG_NO TOO SMALL
  else if (loc_entry->rmw_reps.log_too_small > 0) {
  //It is impossible for this RMW to still hold the global entry
    loc_entry->state = NEEDS_GLOBAL;
  }
  // TS_STALE
  else if (loc_entry->rmw_reps.ts_stale > 0) {
    update_KVS_on_receiving_a_TS_stale_rep(p_ops, loc_entry, t_id);
    loc_entry->state = RETRY_WITH_BIGGER_TS;
    new_version = loc_entry->rmw_reps.kvs_higher_ts.version;
    check_version(new_version, "inspect_proposes: loc_entry->rmw_reps.ts_stale > 0");
  }
  // ALREADY ACCEPTED AN RMW
  else if (loc_entry->rmw_reps.already_accepted > 0) {
    act_on_receiving_already_accepted_rep_to_prop(p_ops, loc_entry, &new_version, t_id);
    check_state_with_allowed_flags(5, (int) loc_entry->state, INVALID_RMW,
                                      ACCEPTED, NEEDS_GLOBAL, RETRY_WITH_BIGGER_TS);
  }
  // SEEN HIGHER-TS PROPOSE
  else if (loc_entry->rmw_reps.seen_higher_prop > 0) {
  // retry by incrementing the highest ts seen
    loc_entry->state = RETRY_WITH_BIGGER_TS;
    new_version = loc_entry->rmw_reps.seen_higher_prop_ts.version;
    check_version(new_version, "inspect_proposes: loc_entry->rmw_reps.seen_higher_prop > 0");
  }
  // ACK QUORUM
  else if (loc_entry->rmw_reps.acks >= REMOTE_QUORUM) {
  // Quorum of prop acks gathered: send an accept
    act_on_quorum_of_prop_acks(p_ops, loc_entry, t_id);
    check_state_with_allowed_flags(5, (int) loc_entry->state, INVALID_RMW,
                                   ACCEPTED, NEEDS_GLOBAL, MUST_BCAST_COMMITS); // TODO MUST_BCAST_COMMITS should not be here
  }
  else if (ENABLE_ASSERTIONS) assert(false);

  // CLEAN_UP
  if (loc_entry->state == RETRY_WITH_BIGGER_TS) {
    check_version(new_version, "inspect_proposes: loc_entry->state == RETRY_WITH_BIGGER_TS");
    take_global_entry_with_higher_TS(p_ops, loc_entry, new_version, t_id);
    check_state_with_allowed_flags(4, (int) loc_entry->state, INVALID_RMW, PROPOSED, NEEDS_GLOBAL);
    zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
    if (loc_entry->state == PROPOSED) {
      insert_prop_to_read_fifo(p_ops, loc_entry, 0, t_id);
    }
  } else if (loc_entry->state != PROPOSED) {
    if (loc_entry->state != ACCEPTED) {
      assert(loc_entry->state == INVALID_RMW || loc_entry->state == NEEDS_GLOBAL ||
               loc_entry->state == MUST_BCAST_COMMITS_FROM_HELP);
      if (loc_entry->state != MUST_BCAST_COMMITS_FROM_HELP) {
        assert(dbg_loc_entry->log_no == loc_entry->log_no);
        assert(rmw_ids_are_equal(&dbg_loc_entry->rmw_id, &loc_entry->rmw_id));
        assert(compare_ts(&dbg_loc_entry->new_ts, &loc_entry->new_ts));
        free_glob_entry_if_rmw_failed(loc_entry, PROPOSED, t_id);
      }
    }
    zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
  }
/* The loc_entry can be in Proposed only when it retried with bigger TS */
}

// Inspect each propose that has gathered a quorum of replies
static inline void inspect_accepts(struct pending_ops *p_ops,
                                   struct rmw_local_entry *loc_entry,
                                   uint16_t t_id)
{
  struct rmw_local_entry tmp;
  struct rmw_local_entry *dbg_loc_entry = &tmp;
  if (ENABLE_ASSERTIONS)
    memcpy(dbg_loc_entry, loc_entry, sizeof(struct rmw_local_entry));

  struct rmw_local_entry *help_loc_entry = loc_entry->help_loc_entry;
  uint32_t new_version = 0;
  if (loc_entry->helping_flag != NOT_HELPING) {
    if (loc_entry->rmw_reps.rmw_id_commited + loc_entry->rmw_reps.log_too_small +
        loc_entry->rmw_reps.ts_stale + loc_entry->rmw_reps.already_accepted +
        loc_entry->rmw_reps.seen_higher_prop > 0) {
      if (loc_entry->rmw_reps.ts_stale + loc_entry->rmw_reps.already_accepted +
          loc_entry->rmw_reps.seen_higher_prop > 0) {
        free_glob_entry_if_help_failed(loc_entry, t_id);
      }
      reinstate_loc_entry_after_helping(loc_entry, t_id);
      return;
    }
  }
  // RMW_ID COMMITTED
  if (loc_entry->rmw_reps.rmw_id_commited > 0) {
    if (loc_entry->rmw_reps.rmw_id_commited < REMOTE_QUORUM)
      loc_entry->state = MUST_BCAST_COMMITS_FROM_HELP;
    else {
      loc_entry->state = INVALID_RMW;
      free_session(p_ops, loc_entry->sess_id, t_id);
    }
    if (ENABLE_ASSERTIONS) assert(loc_entry->helping_flag == NOT_HELPING);
    check_state_with_allowed_flags(3, (int) loc_entry->state,
                                   MUST_BCAST_COMMITS_FROM_HELP, INVALID_RMW);
  }
  // LOG_NO TOO SMALL
  else if (loc_entry->rmw_reps.log_too_small > 0) {
    //It is impossible for this RMW to still hold the global entry
    loc_entry->state = NEEDS_GLOBAL;
    if (ENABLE_ASSERTIONS) assert(loc_entry->helping_flag == NOT_HELPING);
  }
  // TS_STALE
  else if (loc_entry->rmw_reps.ts_stale > 0) {
    update_KVS_on_receiving_a_TS_stale_rep(p_ops, loc_entry, t_id);
    loc_entry->state = RETRY_WITH_BIGGER_TS;
    new_version = loc_entry->rmw_reps.kvs_higher_ts.version;
    check_version(new_version, "inspect_accepts: loc_entry->rmw_reps.ts_stale > 0");
    if (ENABLE_ASSERTIONS) assert(loc_entry->helping_flag == NOT_HELPING);
  }
  // ALREADY ACCEPTED AN RMW
  else if (loc_entry->rmw_reps.already_accepted > 0) {
    new_version = help_loc_entry->new_ts.version;
    loc_entry->state = RETRY_WITH_BIGGER_TS;
    if (ENABLE_ASSERTIONS) assert(loc_entry->helping_flag == NOT_HELPING);
  }
  // SEEN HIGHER-TS PROPOSE
  else if (loc_entry->rmw_reps.seen_higher_prop > 0) {
    // retry by incrementing the highest ts seen
    loc_entry->state = RETRY_WITH_BIGGER_TS;
    new_version = loc_entry->rmw_reps.seen_higher_prop_ts.version;
    check_version(new_version, "inspect_accepts: loc_entry->rmw_reps.seen_higher_prop > 0");
    if (ENABLE_ASSERTIONS) assert(loc_entry->helping_flag == NOT_HELPING);
  }
  // ACK QUORUM
  else if (loc_entry->rmw_reps.acks >= REMOTE_QUORUM) {
    attempt_local_commit(p_ops, loc_entry, t_id);
    loc_entry->state = (uint8_t) (loc_entry->helping_flag == HELPING_NO_STASHING ?
                       MUST_BCAST_COMMITS_FROM_HELP : MUST_BCAST_COMMITS);
  }
  else if (ENABLE_ASSERTIONS) assert(false);

  // CLEAN_UP
  if (loc_entry->state == RETRY_WITH_BIGGER_TS) {
    check_version(new_version, "inspect_accepts: loc_entry->state == RETRY_WITH_BIGGER_TS");
    take_global_entry_with_higher_TS(p_ops, loc_entry, new_version, t_id);
    check_state_with_allowed_flags(4, (int) loc_entry->state, INVALID_RMW, PROPOSED, NEEDS_GLOBAL);
    zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
    if (loc_entry->state == PROPOSED) {
      insert_prop_to_read_fifo(p_ops, loc_entry, 0, t_id);
    }
  }
  else if (loc_entry->state != PROPOSED)
    zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);

  if (loc_entry->state == INVALID_RMW || loc_entry->state == NEEDS_GLOBAL) {
    if (ENABLE_ASSERTIONS) {
      assert(dbg_loc_entry->log_no == loc_entry->log_no);
      assert(rmw_ids_are_equal(&dbg_loc_entry->rmw_id, &loc_entry->rmw_id));
      assert(compare_ts(&dbg_loc_entry->new_ts, &loc_entry->new_ts));
    }
    free_glob_entry_if_rmw_failed(loc_entry, ACCEPTED, t_id);
  }
  /* The loc_entry can be in Proposed only when it retried with bigger TS */
}

/* ---------------------------------------------------------------------------
//------------------------------ TRACE---------------------------------------
//---------------------------------------------------------------------------*/
// Worker inserts a new local read to the read fifo it maintains
static inline void insert_read(struct pending_ops *p_ops, struct cache_op *read, uint8_t source, uint16_t t_id)
{
  my_assert(source == FROM_TRACE || source == FROM_ACQUIRE, "The flag in inserting reads is wrong");
  // cache_op * read should only be accessed if flag == FROM TRACE
  struct r_message *r_mes = p_ops->r_fifo->r_message;
  uint32_t r_mes_ptr = p_ops->r_fifo->push_ptr;
  uint8_t inside_r_ptr = r_mes[r_mes_ptr].coalesce_num;
  uint32_t r_ptr = p_ops->r_push_ptr;

  if (inside_r_ptr > 0 && r_mes[r_mes_ptr].read[0].opcode == PROPOSE_OP) {
    MOD_ADD(p_ops->r_fifo->push_ptr, R_FIFO_SIZE);
    r_mes[p_ops->r_fifo->push_ptr].coalesce_num = 0;
    r_mes_ptr = p_ops->r_fifo->push_ptr;
    inside_r_ptr = r_mes[r_mes_ptr].coalesce_num;
  }
  if (DEBUG_READS)
    green_printf("Worker: %u, inserting a read in r_mes_ptr %u and inside ptr %u \n",
                 t_id, r_mes_ptr, inside_r_ptr);

  // this means that the purpose of the read is solely to flip remote bits
  if (source == FROM_ACQUIRE) {
    // overload the key with local_r_id
    memcpy(&r_mes[r_mes_ptr].read[inside_r_ptr].key, (void *) &p_ops->local_r_id, TRUE_KEY_SIZE);
    r_mes[r_mes_ptr].read[inside_r_ptr].opcode = OP_ACQUIRE_FLIP_BIT;
    p_ops->read_info[r_ptr].opcode = OP_ACQUIRE_FLIP_BIT;
    if (DEBUG_BIT_VECS)
      cyan_printf("Wrkr: %u Acquire generates a read with op %u and key %u \n",
                t_id, r_mes[r_mes_ptr].read[inside_r_ptr].opcode, *(uint64_t *)r_mes[r_mes_ptr].read[inside_r_ptr].key);
  }
  else { // FROM TRACE: out of epoch reads/writes, acquires and releases
    memcpy(&r_mes[r_mes_ptr].read[inside_r_ptr].ts, (void *) &read->key.meta.m_id, TS_TUPLE_SIZE + TRUE_KEY_SIZE);
    if (read->opcode != CACHE_OP_PUT) // if it's an out-of-epoch write, this copy has already happened in the cache
      memcpy(&p_ops->read_info[r_ptr].ts_to_read, (void *) &read->key.meta.m_id, TS_TUPLE_SIZE + TRUE_KEY_SIZE);
    p_ops->read_info[r_ptr].epoch_id = (uint16_t) atomic_load_explicit(&epoch_id, memory_order_seq_cst);
    r_mes[r_mes_ptr].read[inside_r_ptr].opcode = (read->opcode == OP_RELEASE || read->opcode == CACHE_OP_PUT) ?
                                                 (uint8_t) CACHE_OP_GET_TS : read->opcode;
  }

  if (inside_r_ptr == 0) {
    p_ops->r_fifo->backward_ptrs[r_mes_ptr] = r_ptr;
    uint64_t message_l_id = (uint64_t) (p_ops->local_r_id + p_ops->r_size);
    check_previous_read_lid(source, read, message_l_id, r_mes, r_mes_ptr, t_id);
    // printf("message_lid %lu, local_rid %lu, p_ops r_size %u \n", message_l_id, p_ops->local_r_id, p_ops->r_size);
    memcpy(r_mes[r_mes_ptr].l_id, &message_l_id, 8);
  }

  check_read_state_and_key(p_ops, r_ptr, source, r_mes, read, r_mes_ptr, inside_r_ptr, t_id);

  p_ops->r_state[r_ptr] = VALID;
  if (source == FROM_TRACE) {
    if (read->opcode == OP_ACQUIRE || read->opcode == OP_RELEASE) {
      memcpy(&p_ops->r_session_id[r_ptr], read, SESSION_BYTES); // session id has to fit in 3 bytes
      // Query the conf to see if the machine has lost messages
      if (read->opcode == OP_ACQUIRE) on_starting_an_acquire_query_the_conf(t_id);
    }
  }

  // Increase the virtual size by 2 if the req is an acquire
  p_ops->virt_r_size+= p_ops->read_info[p_ops->r_push_ptr].opcode == OP_ACQUIRE ? 2 : 1;
  p_ops->r_size++;
  p_ops->r_fifo->bcast_size++;
  r_mes[r_mes_ptr].coalesce_num++;
  if (ENABLE_ASSERTIONS)
    check_read_fifo_metadata(p_ops, &r_mes[r_mes_ptr], t_id);
  MOD_ADD(p_ops->r_push_ptr, PENDING_READS);
  if (r_mes[r_mes_ptr].coalesce_num == MAX_R_COALESCE) {
    MOD_ADD(p_ops->r_fifo->push_ptr, R_FIFO_SIZE);
    r_mes[p_ops->r_fifo->push_ptr].coalesce_num = 0;
  }
}

// Insert a new local or remote write to the pending writes
static inline void insert_write(struct pending_ops *p_ops, struct cache_op *write, const uint8_t source,
                                const uint32_t incoming_pull_ptr, uint16_t t_id)
{
  struct read_info *r_info = NULL;
  if (source == FROM_READ) r_info = &p_ops->read_info[incoming_pull_ptr];
  //  else if (source == RELEASE_SECOND) r_info = (struct read_info *) write;
  // TODO DO not coalesce with an accept...
  struct w_message *w_mes = p_ops->w_fifo->w_message;
  uint32_t w_mes_ptr = p_ops->w_fifo->push_ptr;
  uint8_t inside_w_ptr = w_mes[w_mes_ptr].w_num;
  uint32_t w_ptr = p_ops->w_push_ptr;
  uint64_t message_l_id;

  bool last_mes_is_rmw = inside_w_ptr > 0 &&
                           (w_mes[w_mes_ptr].write[0].opcode == ACCEPT_OP ||
                            w_mes[w_mes_ptr].write[0].opcode == COMMIT_OP);
  if (ENABLE_RMWS && last_mes_is_rmw) { // Do not coalesce a write with an accept or commit
    MOD_ADD(p_ops->w_fifo->push_ptr, W_FIFO_SIZE);
    w_mes_ptr = p_ops->w_fifo->push_ptr;
    w_mes[w_mes_ptr].w_num = 0;
    inside_w_ptr = 0;
  }
  //printf("Insert a write %u \n", *(uint32_t *)write);


  if (DEBUG_READS && source == FROM_READ) {
    yellow_printf("Wrkr %u Inserting a write as a second round of read/write w_size %u/%d, bcast size %u, "
                    " push_ptr %u, pull_ptr %u "
                    "l_id %lu, fifo push_ptr %u, fifo pull ptr %u\n", t_id,
                  p_ops->w_size, PENDING_WRITES, p_ops->w_fifo->bcast_size,
                  p_ops->w_push_ptr, p_ops->w_pull_ptr,
                  *(uint64_t *)w_mes->l_id, p_ops->w_fifo->push_ptr, p_ops->w_fifo->bcast_pull_ptr);
  }


  write_bookkeeping_in_insertion_based_on_source(p_ops, write, source, incoming_pull_ptr,
                                                 &inside_w_ptr, &w_mes_ptr, w_mes,  r_info, t_id);
  my_assert(inside_w_ptr < MAX_W_COALESCE, "After bookeeping: Inside pointer must not point to the last message");
  if (inside_w_ptr == 0) {
    p_ops->w_fifo->backward_ptrs[w_mes_ptr] = w_ptr;
    message_l_id = (uint64_t) (p_ops->local_w_id + p_ops->w_size);
    //printf("message_lid %lu, local_wid %lu, p_ops w_size %u \n", message_l_id, p_ops->local_w_id, p_ops->w_size);
    memcpy(w_mes[w_mes_ptr].l_id, &message_l_id, 8);
  }
  if (ENABLE_ASSERTIONS)
    debug_checks_when_inserting_a_write(source, inside_w_ptr, w_mes_ptr, w_mes,
                                        message_l_id, p_ops, w_ptr, t_id);
  p_ops->w_state[w_ptr] = VALID;
  if (source == FROM_COMMIT) {
    struct rmw_local_entry* loc_entry = (struct rmw_local_entry*) write;
    // we do not want to free the session if we are merely helping
    bool is_the_commit_helping = (bool) incoming_pull_ptr; // overloading this
    if (is_the_commit_helping)  p_ops->w_session_id[w_ptr] = SESSIONS_PER_THREAD;
    else p_ops->w_session_id[w_ptr] = loc_entry->sess_id;
  }
  else if (source != FROM_READ) { //source = FROM_WRITE || FROM_TRACE || LIN_WRITE
    if (write->opcode == OP_RELEASE_BIT_VECTOR) {
      memcpy(&p_ops->w_session_id[w_ptr], write, SESSION_BYTES);
      //if (DEBUG_SESSIONS)
      //  cyan_printf("Wrkr: %u third round of release by session %u \n", t_id, p_ops->w_session_id[w_ptr]);
    }
  }
  // source = FROM_READ: data reads/writes need not care about the session id as they are not blocking it
  else if (r_info->opcode == OP_ACQUIRE || r_info->opcode == OP_RELEASE)
    p_ops->w_session_id[w_ptr] = p_ops->r_session_id[incoming_pull_ptr];

  if (ENABLE_ASSERTIONS) {
    if (p_ops->w_size > 0) assert(p_ops->w_push_ptr != p_ops->w_pull_ptr);
    assert(w_mes[w_mes_ptr].write[0].opcode != ACCEPT_OP);
  }
  MOD_ADD(p_ops->w_push_ptr, PENDING_WRITES);
  p_ops->w_size++;
  p_ops->w_fifo->bcast_size++;
  w_mes[w_mes_ptr].w_num++;
  if (w_mes[w_mes_ptr].w_num == MAX_W_COALESCE) {
    MOD_ADD(p_ops->w_fifo->push_ptr, W_FIFO_SIZE);
    if (ENABLE_ASSERTIONS) assert(p_ops->w_fifo->push_ptr != p_ops->w_fifo->bcast_pull_ptr);
    w_mes[p_ops->w_fifo->push_ptr].w_num = 0;
  }
}

// setup a new r_rep entry
static inline void set_up_r_rep_entry(struct r_rep_fifo *r_rep_fifo, uint8_t rem_m_id, uint64_t l_id,
                                       struct r_rep_message *r_rep_mes, uint8_t read_opcode)
{
  r_rep_mes->coalesce_num = 0;
  r_rep_fifo->mes_size++;
  if (read_opcode == PROPOSE_OP) r_rep_mes->opcode = PROP_REPLY;
  else if (read_opcode == ACCEPT_OP) r_rep_mes->opcode = ACCEPT_REPLY;
  else r_rep_mes->opcode = READ_REPLY;
  r_rep_fifo->rem_m_id[r_rep_fifo->push_ptr] = rem_m_id;
  memcpy(r_rep_mes->l_id, &l_id, 8);
  r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] = R_REP_MES_HEADER; // ok for rmws
}

// Insert a new r_rep to the r_rep reply fifo
static inline void insert_r_rep(struct pending_ops *p_ops, struct network_ts_tuple *local_ts,
                                struct network_ts_tuple *remote_ts, uint64_t l_id, uint16_t t_id,
                                uint8_t rem_m_id, uint16_t op_i, void* value, uint8_t r_rep_flag,
                                uint8_t read_opcode) {
  if (ENABLE_ASSERTIONS) check_the_read_opcode(read_opcode);
  struct r_rep_fifo *r_rep_fifo = p_ops->r_rep_fifo;
  struct r_rep_message *r_rep_mes = r_rep_fifo->r_rep_message;
  bool is_propose = read_opcode == PROPOSE_OP, is_accept = read_opcode == ACCEPT_OP;
  bool is_rmw = (is_propose || is_accept);
  bool current_message_is_r_rep = r_rep_mes[r_rep_fifo->push_ptr].opcode == READ_REPLY;

  /* A reply message corresponds to exactly one read message
  * to avoid reasoning about l_ids, credits and so on */

  /* If the r_rep has a different recipient or different l_id then create a new message
   * Because the criterion to advance the push ptr is on creating a new message,
   * the pull ptr has to start from 1 (because we start by incrementing the push pointer).
   * Update: because rmw proposes have different lids, that can coincidentally match with read lids
   * the criterion is extended:
   * create a new on an rmw & create a new on a read if the current is not a read*/
  if ((rem_m_id != r_rep_fifo->rem_m_id[r_rep_fifo->push_ptr])   ||
     (l_id != *(uint64_t *)r_rep_mes[r_rep_fifo->push_ptr].l_id) ||
     (is_rmw) || ((!is_rmw) && (!current_message_is_r_rep))) {
    if (ENABLE_ASSERTIONS) assert(MAX_PROP_REP_COALESCE == 1); // TODO this will need multiple changes to support prop coalesicng
    MOD_ADD(r_rep_fifo->push_ptr, R_REP_FIFO_SIZE); // Keep this here: push ptr is used calling the function below
    set_up_r_rep_entry(r_rep_fifo, rem_m_id, l_id, &r_rep_mes[r_rep_fifo->push_ptr], read_opcode);
    //cyan_printf("Wrkr %u Creating a new read_reply message opcode: %u/%u at push_ptr %u\n",
     //           t_id, r_rep_mes[r_rep_fifo->push_ptr].opcode, read_opcode, r_rep_fifo->push_ptr);
  }
  uint32_t r_rep_mes_ptr = r_rep_fifo->push_ptr;
  uint32_t inside_r_rep_ptr = r_rep_fifo->message_sizes[r_rep_fifo->push_ptr]; // This pointer is in bytes
  r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] += R_REP_SMALL_SIZE;
  struct r_rep_big *r_rep = (struct r_rep_big *) (((void *)&r_rep_mes[r_rep_mes_ptr]) + inside_r_rep_ptr);

  if (unlikely(r_rep_flag == NO_OP_ACQ_FLIP_BIT)) {
    r_rep->opcode = TS_EQUAL;
  }
  else if (!is_rmw) {
    fill_read_reply(r_rep, r_rep_fifo, local_ts, remote_ts, value, r_rep_flag, t_id);
  }
  else {
    struct rmw_rep_last_committed *rmw_rep = (((void *)r_rep) - 8);
    fill_rmw_reply(rmw_rep, r_rep_fifo, r_rep_flag,
                   (struct rmw_help_entry *) value, is_accept, t_id);
    if (ENABLE_ASSERTIONS) assert(is_accept || is_propose);
  }

  if (read_opcode == OP_ACQUIRE_FP) {
    if (ENABLE_ASSERTIONS) assert(r_rep_flag != NO_OP_ACQ_FLIP_BIT);
    if (DEBUG_QUORUM)
      yellow_printf("Worker %u Letting machine %u know that I believed it failed \n", t_id, rem_m_id);
    r_rep->opcode += FALSE_POSITIVE_OFFSET;
  }
  p_ops->r_rep_fifo->total_size++;
  r_rep_mes[r_rep_mes_ptr].coalesce_num++;
  if (ENABLE_ASSERTIONS) {
    assert(r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] <= R_REP_SEND_SIZE);
    assert(r_rep_mes[r_rep_mes_ptr].coalesce_num <= MAX_R_REP_COALESCE);
  }
}



// Insert an RMW in the local RMW structs
static inline void insert_rmw(struct pending_ops *p_ops, struct cache_op *prop,
                              struct cache_resp *resp, uint16_t t_id)
{
  //struct cache_op *prop = &ops[op_i];
  uint32_t session_id = 0;
  memcpy(&session_id, prop, SESSION_BYTES);
  if (ENABLE_ASSERTIONS) assert(session_id < SESSIONS_PER_THREAD);
  struct rmw_local_entry *loc_entry = &p_ops->prop_info->entry[session_id];
  if (ENABLE_ASSERTIONS) {
    if (loc_entry->state != INVALID_RMW) {
      red_printf("Wrkr %u Expected an invalid loc entry for session %u, loc_entry state %u \n",
                 t_id, session_id, loc_entry->state);
      assert(false);
    }
  }
  init_loc_entry(resp, p_ops, prop, (uint16_t) session_id, t_id, loc_entry);
  p_ops->prop_info->l_id++;
  // if the global RMW entry was occupied, put in the next op to try next round
  if (resp->type == RETRY_RMW_KEY_EXISTS) {
    //if (DEBUG_RMW) green_printf("Worker %u failed to do its RMW and moved "
    //      "it from position %u to %u \n", t_id, op_i, *old_op_i);
    loc_entry->state = NEEDS_GLOBAL;
    // Set up the state that the RMW should wait on
    loc_entry->help_rmw->rmw_id = resp->glob_entry_rmw_id;
    loc_entry->help_rmw->state = resp->glob_entry_state;
  }
  else if (resp->type == RMW_SUCCESS) { // the RMW has gotten an entry and is to be sent
    fill_loc_rmw_entry_on_grabbing_global(p_ops, loc_entry, prop->key.meta.version,
                                          PROPOSED, (uint16_t) session_id, t_id);
    loc_entry->log_no = resp->log_no;
    insert_prop_to_read_fifo(p_ops, loc_entry, 0, t_id);
  }
  else my_assert(false, "Wrong resp type in RMW");
}


// Use this to r_rep the trace, propagate reqs to the cache and maintain their r_rep/write fifos
static inline uint32_t batch_from_trace_to_cache(uint32_t trace_iter, uint16_t t_id,
                                                 struct trace_command *trace, struct cache_op *ops,
                                                 struct pending_ops *p_ops, struct cache_resp *resp,
                                                 struct latency_flags *latency_info,
                                                 struct session_dbg *ses_dbg)
{
  //uint16_t i = 0;
  uint16_t writes_num = 0, reads_num = 0, op_i = 0;
  bool is_update;
  int working_session = -1;
  if (p_ops->all_sessions_stalled) {
    if (ENABLE_ASSERTIONS) debug_all_sessions(ses_dbg, p_ops, t_id);
    return trace_iter;
  }
  for (uint16_t i = 0; i < SESSIONS_PER_THREAD; i++) {
    if (!p_ops->session_has_pending_op[i]) {
      working_session = i;
      break;
    }
    else if (ENABLE_ASSERTIONS) debug_sessions(ses_dbg, p_ops, i, t_id);
  }
   //printf("working session = %d\n", working_session);
  if (ENABLE_ASSERTIONS) assert(working_session != -1);

  //green_printf("op_i %d , trace_iter %d, trace[trace_iter].opcode %d \n", op_i, trace_iter, trace[trace_iter].opcode);
  while (op_i < MAX_OP_BATCH && working_session < SESSIONS_PER_THREAD) {
    if (ENABLE_ASSERTIONS) {
      assert(trace[trace_iter].opcode != NOP);
      assert(p_ops->session_has_pending_op[working_session] == false);
      if (ENABLE_RMWS && p_ops->prop_info->entry[working_session].state != INVALID_RMW) {
        cyan_printf("wrk %u  Session %u has loc_entry state %u \n", t_id,
                    working_session, p_ops->prop_info->entry[working_session].state );
      }
      //assert(p_ops->prop_info->entry[working_session].state == INVALID_RMW);
    }
    is_update = (trace[trace_iter].opcode == (uint8_t) CACHE_OP_PUT ||
                 trace[trace_iter].opcode == (uint8_t) OP_RELEASE);
    // Create some back pressure from the buffers, since the sessions may never be stalled
    if (!EMULATE_ABD) {
      if (trace[trace_iter].opcode == (uint8_t) CACHE_OP_PUT) writes_num++;
      reads_num += trace[trace_iter].opcode == (uint8_t) OP_ACQUIRE ? 2
                                                                    : 1; // A write (relaxed or release) can first trigger a read
      if (p_ops->w_size + writes_num >= MAX_ALLOWED_W_SIZE || p_ops->virt_r_size + reads_num >= MAX_ALLOWED_R_SIZE)
        break;
    }
    increment_per_req_counters(trace[trace_iter].opcode, t_id);
    memcpy(((void *)&(ops[op_i].key)) + TRUE_KEY_SIZE, trace[trace_iter].key_hash, TRUE_KEY_SIZE);
    ops[op_i].opcode = trace[trace_iter].opcode;
    ops[op_i].val_len = is_update ? (uint8_t) (VALUE_SIZE >> SHIFT_BITS) : (uint8_t) 0;
    if (ops[op_i].opcode == OP_RELEASE || ops[op_i].opcode == OP_ACQUIRE
        || ops[op_i].opcode == PROPOSE_OP) {
      p_ops->session_has_pending_op[working_session] = true;
      memcpy(&ops[op_i], &working_session, SESSION_BYTES);// Overload this field to associate a session with an op
    }
    if (ENABLE_ASSERTIONS && DEBUG_SESSIONS) ses_dbg->dbg_cnt[working_session] = 0;
    if (MEASURE_LATENCY) start_measurement(latency_info, (uint32_t) working_session, t_id, ops[op_i].opcode);

    //if (trace_iter == 100000) yellow_printf("Working ses %u \n", working_session);
    //yellow_printf("BEFORE: OP_i %u -> session %u, opcode: %u \n", op_i, working_session, ops[op_i].opcode);
    while (p_ops->session_has_pending_op[working_session]) {
      if (ENABLE_ASSERTIONS) debug_sessions(ses_dbg, p_ops, (uint32_t) working_session, t_id);
      working_session++;
      if (working_session == SESSIONS_PER_THREAD) {
        p_ops->all_sessions_stalled = true;
        break;
      }
    }
    //cyan_printf("thread %d  next working session %d total ops %d\n", t_id, working_session, op_i);
    if (ENABLE_ASSERTIONS) {
      assert(WRITE_RATIO > 0 || is_update == 0);
      if (is_update) assert(ops[op_i].val_len > 0);
      ops[op_i].key.meta.version = 1;
    }
    resp[op_i].type = EMPTY;
    trace_iter++;
    if (trace[trace_iter].opcode == NOP) trace_iter = 0;
    op_i++;
  }

  t_stats[t_id].cache_hits_per_thread += op_i;
  cache_batch_op_trace(op_i, t_id, ops, resp, p_ops);
  //cyan_printf("thread %d  adds %d/%d ops\n", t_id, op_i, MAX_OP_BATCH);
  for (uint16_t i = 0; i < op_i; i++) {
    check_version_after_batching_trace_to_cache(&ops[i], &resp[i], t_id);
    // green_printf("After: OP_i %u -> session %u \n", i, *(uint32_t *) &ops[i]);
    if (resp[i].type == CACHE_MISS)  {
      //green_printf("Cache_miss: bkt %u, server %u, tag %u \n", ops[i].key.bkt, ops[i].key.server, ops[i].key.tag);
      clean_up_on_KVS_miss(&ops[i], p_ops, latency_info, t_id);
    }
    else if (resp[i].type == CACHE_LOCAL_GET_SUCCESS) ;
    else if (ENABLE_RMWS && ops[i].opcode == PROPOSE_OP) {
      insert_rmw(p_ops, &ops[i], &resp[i], t_id);
    }
    else if (resp[i].type == CACHE_PUT_SUCCESS)
      insert_write(p_ops, &ops[i], FROM_TRACE, 0, t_id);
    else {
      if (ENABLE_ASSERTIONS) assert(resp[i].type == CACHE_GET_SUCCESS ||
                                    resp[i].type == CACHE_GET_TS_SUCCESS);
      insert_read(p_ops, &ops[i], FROM_TRACE, t_id);
    }
  }
  return trace_iter;
}

/* ---------------------------------------------------------------------------
//------------------------------CONFIGURATION -----------------------------
//---------------------------------------------------------------------------*/
// Update the quorum info, use this one a timeout
static inline void update_q_info(struct quorum_info *q_info,  uint16_t credits[][MACHINE_NUM],
                                 uint16_t min_credits, uint8_t vc, uint16_t t_id)
{
  uint8_t i, rm_id;
  q_info->missing_num = 0;
  q_info->active_num = 0;
  for (i = 0; i < MACHINE_NUM; i++) {
    if (i == machine_id) continue;
    rm_id = mid_to_rmid(i);
    if (credits[vc][i] < min_credits) {
      q_info->missing_ids[q_info->missing_num] = i;
      q_info->missing_num++;
      q_info->send_vector[rm_id] = false;
      //Change the machine-wide configuration bit-vector and the bit vector to be sent
      set_send_and_conf_bit_after_detecting_failure(t_id, i); // this function changes both vectors
      //if (DEBUG_QUORUM) yellow_printf("Worker flips the vector bit_vec for machine %u, send vector bit_vec %u \n",
      //                               i, send_bit_vector.bit_vec[i].bit);
      if (!DEBUG_BIT_VECS)
        red_printf("Wrkr %u detects that machine %u has failed \n", t_id, i);
     // assert(false);
    }
    else {
      q_info->active_ids[q_info->active_num] = i;
      q_info->active_num++;
      q_info->send_vector[rm_id] = true;
    }
  }
  q_info->first_active_rm_id = mid_to_rmid(q_info->active_ids[0]);
  if (q_info->active_num > 0)
    q_info->last_active_rm_id = mid_to_rmid(q_info->active_ids[q_info->active_num - 1]);
  if (DEBUG_QUORUM) print_q_info(q_info);

  if (ENABLE_ASSERTIONS) {
    assert(q_info->missing_num <= REM_MACH_NUM);
    assert(q_info->active_num <= REM_MACH_NUM);
  }
}

// Bring back a machine
static inline void revive_machine(struct quorum_info *q_info,
                                  uint8_t revived_mach_id)
{

  uint8_t rm_id = mid_to_rmid(revived_mach_id);
  if (ENABLE_ASSERTIONS) {
    assert(revived_mach_id < MACHINE_NUM);
    assert(revived_mach_id != machine_id);
    assert(q_info->missing_num > 0);
    assert(q_info->send_vector[rm_id] == false);
  }
  // Fix the send vector and update the rest based on that,
  // because using the credits may not be reliable here
  q_info->send_vector[rm_id] = true;
  q_info->missing_num = 0;
  q_info->active_num = 0;
  for (uint8_t i = 0; i < REM_MACH_NUM; i++) {
    uint8_t m_id = rmid_to_mid(i);
    if (!q_info->send_vector[i]) {
      q_info->missing_ids[q_info->missing_num] = m_id;
      q_info->missing_num++;
    }
    else {
      q_info->active_ids[q_info->active_num] = m_id;
      q_info->active_num++;
    }
  }
  q_info->first_active_rm_id = mid_to_rmid(q_info->active_ids[0]);
  q_info->last_active_rm_id = mid_to_rmid(q_info->active_ids[q_info->active_num - 1]);
  if (DEBUG_QUORUM) print_q_info(q_info);
  for (uint16_t i = 0; i < q_info->missing_num; i++)
    if (DEBUG_QUORUM) green_printf("After: Missing position %u, missing id %u, id to revive\n",
                                   i, q_info->missing_ids[i], revived_mach_id);
}

// Update the links between the send Work Requests for broadcasts given the quorum information
static inline void update_bcast_wr_links(struct quorum_info *q_info, struct ibv_send_wr *wr, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) assert(MESSAGES_IN_BCAST == REM_MACH_NUM);
  uint8_t prev_i = 0, avail_mach = 0;
  if (DEBUG_QUORUM) green_printf("Worker %u fixing the links between the wrs \n", t_id);
  for (uint8_t i = 0; i < REM_MACH_NUM; i++) {
    wr[i].next = NULL;
    if (q_info->send_vector[i]) {
      if (avail_mach > 0) {
        for (uint16_t j = 0; j < MAX_BCAST_BATCH; j++) {
          if (DEBUG_QUORUM) yellow_printf("Worker %u, wr %d points to %d\n", t_id, (REM_MACH_NUM * j) + prev_i, (REM_MACH_NUM * j) + i);
          wr[(REM_MACH_NUM * j) + prev_i].next = &wr[(REM_MACH_NUM * j) + i];
        }
      }
      avail_mach++;
      prev_i = i;
    }
  }
}


/* ---------------------------------------------------------------------------
//------------------------------ BROADCASTS -----------------------------
//---------------------------------------------------------------------------*/


static inline void decrease_credits(uint16_t credits[][MACHINE_NUM], struct quorum_info *q_info,
                                    uint16_t mes_sent, uint8_t vc)
{
  for (uint8_t i = 0; i < q_info->active_num; i++) {
    if (ENABLE_ASSERTIONS) {
      assert(credits[vc][q_info->active_ids[i]] >= mes_sent);
      assert(q_info->active_ids[i] != machine_id && q_info->active_ids[i] < MACHINE_NUM);
      //assert(q_info->active_num == REM_MACH_NUM); // debug no-failure case
    }
    credits[vc][q_info->active_ids[i]] -= mes_sent;
  }
}

// Check credits, first see if there are credits from all active nodes, then if not and enough time has passed,
// transition to write_quorum broadcasts
static inline bool check_bcast_credits(uint16_t credits[][MACHINE_NUM], struct quorum_info *q_info,
                                       uint32_t *time_out_cnt, uint8_t vc,
                                       uint16_t *available_credits,
                                       struct ibv_send_wr *r_send_wr, struct ibv_send_wr *w_send_wr,
                                       uint16_t min_credits,
                                       uint32_t *credit_debug_cnt, uint16_t t_id)
{
  uint16_t i;
  // First check the active ids, to have a fast path when there are not enough credits
  for (i = 0; i < q_info->active_num; i++) {
    if (credits[vc][q_info->active_ids[i]] < min_credits) {
      time_out_cnt[vc]++;
      //if (DEBUG_BIT_VECS && t_id >= 0 && time_out_cnt[vc] % M_1 == 0)
        //red_printf("WKR %u: the timeout cnt is %u for vc %u machine %u, credits %u\n",
         //          t_id, time_out_cnt[vc], vc, q_info->active_ids[i], credits[vc][q_info->active_ids[i]]);
      if (time_out_cnt[vc] == CREDIT_TIMEOUT) {
        if (DEBUG_QUORUM)
          red_printf("Worker %u timed_out on machine %u  for vc % u, writes  done %lu \n",
                     t_id, q_info->active_ids[i], vc, t_stats[t_id].writes_sent);
        // assert(false);
        update_q_info(q_info, credits, min_credits, vc, t_id);
        update_bcast_wr_links(q_info, r_send_wr, t_id);
        update_bcast_wr_links(q_info, w_send_wr, t_id);
        time_out_cnt[vc] = 0;
      }
      return false;
    }
  }

  time_out_cnt[vc] = 0;

  // then check the missing credits to see if we need to change the configuration
  if (q_info->missing_num > 0) {
    for (i = 0; i < q_info->missing_num; i++) {
      if (credits[W_VC][q_info->missing_ids[i]] == W_CREDITS &&
          credits[R_VC][q_info->missing_ids[i]] == R_CREDITS ) {
        if (DEBUG_QUORUM) red_printf("Worker %u revives machine %u \n", t_id, q_info->missing_ids[i]);
        revive_machine(q_info, q_info->missing_ids[i]);
        update_bcast_wr_links(q_info, r_send_wr, t_id);
        update_bcast_wr_links(q_info, w_send_wr, t_id);
      }
    }
  }
  if (unlikely(q_info->active_num < REMOTE_QUORUM)) return false;
  //finally count credits
  uint16_t avail_cred = K_64_;
  //printf("avail cred %u\n", avail_cred);
  for (i = 0; i < q_info->active_num; i++) {
    if (ENABLE_ASSERTIONS) assert(q_info->active_ids[i] < MACHINE_NUM &&
                                  q_info->active_ids[i] != machine_id);
    if (credits[vc][q_info->active_ids[i]] < avail_cred)
      avail_cred = credits[vc][q_info->active_ids[i]];
  }
  *available_credits = avail_cred;
  return true;
}

// Form the Broadcast work request for the write
static inline void forge_w_wr(uint32_t w_mes_i, struct pending_ops *p_ops,
                              struct quorum_info *q_info,
                              struct hrd_ctrl_blk *cb, struct ibv_sge *send_sgl,
                              struct ibv_send_wr *send_wr, uint64_t *w_br_tx,
                              uint16_t br_i, uint16_t credits[][MACHINE_NUM],
                              uint8_t vc, uint16_t t_id) {
  struct ibv_wc signal_send_wc;
  struct w_message *w_mes = &p_ops->w_fifo->w_message[w_mes_i];
  uint32_t backward_ptr = p_ops->w_fifo->backward_ptrs[w_mes_i];
  uint8_t coalesce_num = w_mes->w_num;
  bool is_accept = w_mes->write[0].opcode == ACCEPT_OP;
  bool is_commit = w_mes->write[0].opcode == COMMIT_OP;
  send_sgl[br_i].length = calculate_write_message_size(w_mes->write[0].opcode, coalesce_num, t_id);
  send_sgl[br_i].addr = (uint64_t) (uintptr_t) w_mes;
  if (ENABLE_ADAPTIVE_INLINING)
    adaptive_inlining(send_sgl[br_i].length, &send_wr[br_i * MESSAGES_IN_BCAST], MESSAGES_IN_BCAST);

  if (ENABLE_ASSERTIONS) {
    if (is_accept) checks_when_forging_an_accept((struct accept_message *) w_mes, send_sgl, br_i,coalesce_num, t_id);
    else if (is_commit) checks_when_forging_a_commit((struct commit_message *) w_mes, send_sgl, br_i,coalesce_num, t_id);
    else checks_when_forging_a_write(w_mes, send_sgl, br_i, coalesce_num, t_id);
  }

  // Check if the release needs to send the vector bit_vec to get quoromized
  if (unlikely (!EMULATE_ABD && w_mes->write[0].opcode == OP_RELEASE &&
      send_bit_vector.state == DOWN_STABLE)) {
    add_failure_to_release(p_ops, w_mes, backward_ptr, t_id);
  }
  // else if the write is a release that does not need to quoromize failures go to cache
  else if (w_mes->write[0].opcode == OP_RELEASE){
    cache_isolated_op(t_id, &w_mes->write[0]);
  }

  // Set the w_state for each write
  if (!is_accept)
    set_w_state_for_each_write(p_ops, w_mes, backward_ptr, coalesce_num, t_id);

  if (DEBUG_WRITES && !is_accept)
    green_printf("Wrkr %d : I BROADCAST a write message %d of %u writes with mes_size %u, with credits: %d, lid: %u  \n",
                 t_id, w_mes->write[coalesce_num - 1].opcode, coalesce_num, send_sgl[br_i].length,
                 credits[vc][(machine_id + 1) % MACHINE_NUM], *(uint64_t*)w_mes->l_id);

  if (DEBUG_RMW && (is_accept || is_commit)) {
    struct accept_message *acc_mes = (struct accept_message *) w_mes;
    green_printf("Wrkr %d : I BROADCAST a%s message %d of %u accepts with mes_size %u, with credits: %d, lid: %u , "
                   "rmw_id %u, glob_sess id %u, log_no %u, version %u  \n",
                 t_id, is_accept ? "n accept" : " commit", acc_mes->acc[coalesce_num - 1].opcode, coalesce_num,
                 send_sgl[br_i].length,  credits[vc][(machine_id + 1) % MACHINE_NUM], acc_mes->acc[0].l_id,
                 acc_mes->acc[0].t_rmw_id, *(uint16_t *) acc_mes->acc[0].glob_sess_id,
                 *(uint32_t *) acc_mes->acc[0].log_no, *(uint32_t *) acc_mes->acc[0].ts.version);
  }

  // Do a Signaled Send every W_BCAST_SS_BATCH broadcasts (W_BCAST_SS_BATCH * (MACHINE_NUM - 1) messages)
  if ((*w_br_tx) % W_BCAST_SS_BATCH == 0) {
    if (DEBUG_SS_BATCH)
      printf("Wrkr %u Sending signaled the first message, total %lu, br_i %u \n", t_id, *w_br_tx, br_i);
    send_wr[q_info->first_active_rm_id].send_flags |= IBV_SEND_SIGNALED;
  }
  (*w_br_tx)++;
  if ((*w_br_tx) % W_BCAST_SS_BATCH == W_BCAST_SS_BATCH - 1) {
    if (DEBUG_SS_BATCH)
      printf("Wrkr %u POLLING for a send completion in writes, total %lu \n", t_id, *w_br_tx);
    poll_cq(cb->dgram_send_cq[W_QP_ID], 1, &signal_send_wc, POLL_CQ_W);
  }
  // Have the last message of each broadcast pointing to the first message of the next bcast
  if (br_i > 0) {
    send_wr[((br_i - 1) * MESSAGES_IN_BCAST) + q_info->last_active_rm_id].next =
      &send_wr[(br_i * MESSAGES_IN_BCAST) + q_info->first_active_rm_id];
  }
}


// Broadcast Writes
static inline void broadcast_writes(struct pending_ops *p_ops, struct quorum_info *q_info,
                                    uint16_t credits[][MACHINE_NUM], struct hrd_ctrl_blk *cb,
                                    uint32_t *credit_debug_cnt, uint32_t *time_out_cnt,
                                    struct ibv_sge *w_send_sgl, struct ibv_send_wr *r_send_wr,
                                    struct ibv_send_wr *w_send_wr,
                                    uint64_t *w_br_tx, struct recv_info *ack_recv_info,
                                    struct recv_info *r_rep_recv_info,
                                    uint16_t t_id, uint32_t *outstanding_writes, uint64_t *expected_next_l_id)
{
  //  printf("Worker %d bcasting writes \n", g_id);
  uint8_t vc = W_VC;
  uint16_t br_i = 0, mes_sent = 0, available_credits = 0;
  uint32_t bcast_pull_ptr = p_ops->w_fifo->bcast_pull_ptr;
  bool is_release;
  if (p_ops->w_fifo->bcast_size > 0) {
    is_release = p_ops->w_fifo->w_message[bcast_pull_ptr].write[0].opcode == OP_RELEASE && (!EMULATE_ABD);
    uint16_t min_credits = is_release ? (uint16_t) W_CREDITS : (uint16_t)1;
    if (!check_bcast_credits(credits, q_info, time_out_cnt, vc,
                             &available_credits, r_send_wr, w_send_wr, min_credits, credit_debug_cnt, t_id))
      return;
    if (ENABLE_ASSERTIONS && is_release) assert(available_credits == W_CREDITS);
  }
  else return;
  if (ENABLE_ASSERTIONS) assert(available_credits <= W_CREDITS);

  while (p_ops->w_fifo->bcast_size > 0 && mes_sent < available_credits) {
    is_release = p_ops->w_fifo->w_message[bcast_pull_ptr].write[0].opcode == OP_RELEASE && (!EMULATE_ABD);
    if (is_release) {
      if (mes_sent > 0 || available_credits < W_CREDITS) {
        break;
      }
    }
    if (DEBUG_WRITES)
      printf("Wrkr %d has %u write bcasts to send credits %d\n",t_id, p_ops->w_fifo->bcast_size, credits[W_VC][0]);
    // Create the broadcast messages
    forge_w_wr(bcast_pull_ptr, p_ops, q_info, cb,  w_send_sgl, w_send_wr, w_br_tx, br_i, credits, vc, t_id);
    br_i++;
    uint8_t coalesce_num = p_ops->w_fifo->w_message[bcast_pull_ptr].w_num;
    debug_and_count_stats_when_broadcasting_writes(p_ops, bcast_pull_ptr, coalesce_num,
                                                   t_id, expected_next_l_id, br_i, outstanding_writes);
    p_ops->w_fifo->bcast_size -= coalesce_num;
    // This message has been sent, do not add other writes to it!
    // this is tricky because releases leave half-filled messages, make sure this is the last message to bcast
    if (p_ops->w_fifo->bcast_size == 0) {
      uint8_t max_coalesce = (uint8_t) (p_ops->w_fifo->w_message[bcast_pull_ptr].write[0].opcode == ACCEPT_OP ?
                             MAX_ACC_COALESCE : MAX_W_COALESCE);
      if (coalesce_num < max_coalesce) {
        //yellow_printf("Broadcasting write with coalesce num %u \n", coalesce_num);
        MOD_ADD(p_ops->w_fifo->push_ptr, W_FIFO_SIZE);
        p_ops->w_fifo->w_message[p_ops->w_fifo->push_ptr].w_num = 0;
      }
    }
    mes_sent++;
    MOD_ADD(bcast_pull_ptr, W_FIFO_SIZE);
    if (br_i == MAX_BCAST_BATCH) {
      post_receives_for_r_reps_for_accepts(r_rep_recv_info, t_id);
      post_quorum_broadasts_and_recvs(ack_recv_info, MAX_RECV_ACK_WRS - ack_recv_info->posted_recvs,
                                      q_info, br_i, *w_br_tx, w_send_wr, cb->dgram_qp[W_QP_ID],
                                      W_ENABLE_INLINING);
      br_i = 0;
    }
  }
  if (br_i > 0) {
    if (ENABLE_ASSERTIONS) assert(MAX_BCAST_BATCH > 1);
    post_receives_for_r_reps_for_accepts(r_rep_recv_info, t_id);
    post_quorum_broadasts_and_recvs(ack_recv_info, MAX_RECV_ACK_WRS - ack_recv_info->posted_recvs,
                                    q_info, br_i, *w_br_tx, w_send_wr, cb->dgram_qp[W_QP_ID],
                                    W_ENABLE_INLINING);
  }

  p_ops->w_fifo->bcast_pull_ptr = bcast_pull_ptr;
  if (ENABLE_ASSERTIONS) assert(mes_sent <= available_credits && mes_sent <= W_CREDITS);
  if (mes_sent > 0) decrease_credits(credits, q_info, mes_sent, vc);
}

// Form the Broadcast work request for the red
static inline void forge_r_wr(uint32_t r_mes_i, struct pending_ops *p_ops,
                              struct quorum_info *q_info,
                              struct hrd_ctrl_blk *cb, struct ibv_sge *send_sgl,
                              struct ibv_send_wr *send_wr, uint64_t *r_br_tx,
                              uint16_t br_i, uint16_t credits[][MACHINE_NUM],
                              uint8_t vc, uint16_t t_id) {
  uint16_t i;
  struct ibv_wc signal_send_wc;
  struct r_message *r_mes = &p_ops->r_fifo->r_message[r_mes_i];
  uint32_t backward_ptr = p_ops->r_fifo->backward_ptrs[r_mes_i];
  uint16_t coalesce_num = r_mes->coalesce_num;
  bool is_propose = r_mes->read[0].opcode == PROPOSE_OP;
  uint16_t header_size = (uint16_t) (is_propose ? PROP_MES_HEADER : R_MES_HEADER);
  uint16_t size_of_struct = (uint16_t) (is_propose ? PROP_SIZE : R_SIZE);
  send_sgl[br_i].length = header_size + coalesce_num * size_of_struct;
  send_sgl[br_i].addr = (uint64_t) (uintptr_t) r_mes;
  if (ENABLE_ADAPTIVE_INLINING)
    adaptive_inlining(send_sgl[br_i].length, &send_wr[br_i * MESSAGES_IN_BCAST], MESSAGES_IN_BCAST);
  if (ENABLE_ASSERTIONS) {
    assert(coalesce_num > 0);
    assert(send_sgl[br_i].length <= R_MES_SIZE);
  }
  if (!is_propose) {
    for (i = 0; i < coalesce_num; i++) {
      p_ops->r_state[(backward_ptr + i) % PENDING_READS] = SENT;
      if (DEBUG_READS)
        yellow_printf("Read %d, message mes_size %d, version %u \n", i,
                      send_sgl[br_i].length, *(uint32_t *) r_mes->read[i].ts.version);
      if (ENABLE_ASSERTIONS) {
        assert(r_mes->read[i].opcode == CACHE_OP_GET || r_mes->read[i].opcode == CACHE_OP_GET_TS ||
               r_mes->read[i].opcode == OP_ACQUIRE ||
               r_mes->read[i].opcode == PROPOSE_OP || r_mes->read[i].opcode == OP_ACQUIRE_FLIP_BIT);
      }
    }
  }
  if (DEBUG_READS && !is_propose)
    green_printf("Wrkr %d : I BROADCAST a read message %d of %u reads with mes_size %u, with credits: %d, lid: %u  \n",
                 t_id, r_mes->read[coalesce_num - 1].opcode, coalesce_num, send_sgl[br_i].length,
                 credits[vc][(machine_id + 1) % MACHINE_NUM], *(uint64_t *) r_mes->l_id);
  else if (DEBUG_RMW && is_propose) {
    struct prop_message *prop_mes = (struct prop_message *) r_mes;
  green_printf("Wrkr %d : I BROADCAST a propose message %d of %u props with mes_size %u, with credits: %d, lid: %u, "
                 "rmw_id %u, glob_sess id %u, log_no %u, version %u \n",
               t_id, r_mes->read[coalesce_num - 1].opcode, coalesce_num, send_sgl[br_i].length,
               credits[vc][(machine_id + 1) % MACHINE_NUM], *(uint64_t *) r_mes->l_id,
               *(uint64_t *) prop_mes->prop[0].t_rmw_id, *(uint16_t *) prop_mes->prop[0].glob_sess_id,
               *(uint32_t *) prop_mes->prop[0].log_no, *(uint32_t *) prop_mes->prop[0].ts.version);
  }
  //send_wr[0].send_flags = R_ENABLE_INLINING == 1 ? IBV_SEND_INLINE : 0;
  // Do a Signaled Send every R_BCAST_SS_BATCH broadcasts (R_BCAST_SS_BATCH * (MACHINE_NUM - 1) messages)
  if ((*r_br_tx) % R_BCAST_SS_BATCH == 0)
    send_wr[q_info->first_active_rm_id].send_flags |= IBV_SEND_SIGNALED;
  (*r_br_tx)++;
  if ((*r_br_tx) % R_BCAST_SS_BATCH == R_BCAST_SS_BATCH - 1) {
    //printf("Wrkr %u POLLING for a send completion in reads \n", m_id);
    poll_cq(cb->dgram_send_cq[R_QP_ID], 1, &signal_send_wc, POLL_CQ_R);
  }
  // Have the last message of each broadcast pointing to the first message of the next bcast
  if (br_i > 0)
    send_wr[((br_i - 1) * MESSAGES_IN_BCAST) + q_info->last_active_rm_id].next =
      &send_wr[(br_i * MESSAGES_IN_BCAST) + q_info->first_active_rm_id];

}

// Broadcast Reads
static inline void broadcast_reads(struct pending_ops *p_ops,
                                   uint16_t credits[][MACHINE_NUM], struct hrd_ctrl_blk *cb,
                                   struct quorum_info *q_info, uint32_t *credit_debug_cnt,
                                   uint32_t *time_out_cnt,
                                   struct ibv_sge *r_send_sgl, struct ibv_send_wr *r_send_wr,
                                   struct ibv_send_wr *w_send_wr,
                                   uint64_t *r_br_tx, struct recv_info *r_rep_recv_info,
                                   uint16_t t_id, uint32_t *outstanding_reads)
{
  //  printf("Worker %d bcasting reads \n", g_id);
  uint8_t vc = R_VC;
  uint16_t reads_sent = 0, br_i = 0, mes_sent = 0, available_credits = 0;
  uint32_t bcast_pull_ptr = p_ops->r_fifo->bcast_pull_ptr;

  if (p_ops->r_fifo->bcast_size > 0) {
    if (!check_bcast_credits(credits, q_info, time_out_cnt, vc,
                             &available_credits, r_send_wr, w_send_wr, 1, credit_debug_cnt, t_id))
      return;
  }
  else return;
  if (ENABLE_ASSERTIONS) assert(available_credits <= R_CREDITS);

  while (p_ops->r_fifo->bcast_size > 0 &&  mes_sent < available_credits) {
    if (DEBUG_READS)
      printf("Wrkr %d has %u read bcasts to send credits %d\n",t_id, p_ops->r_fifo->bcast_size, credits[R_VC][0]);
    // Create the broadcast messages
    forge_r_wr(bcast_pull_ptr, p_ops, q_info, cb, r_send_sgl, r_send_wr, r_br_tx, br_i, credits, vc, t_id);
    br_i++;
    uint8_t coalesce_num = p_ops->r_fifo->r_message[bcast_pull_ptr].coalesce_num;
    bool is_propose = p_ops->r_fifo->r_message[bcast_pull_ptr].read[0].opcode == PROPOSE_OP;
    if (ENABLE_ASSERTIONS) {
      assert( p_ops->r_fifo->bcast_size >= coalesce_num);
      (*outstanding_reads) += coalesce_num;
    }
    if (ENABLE_STAT_COUNTING) {
      if (is_propose) t_stats[t_id].proposes_sent++;
      else {
        t_stats[t_id].reads_sent += coalesce_num;
        t_stats[t_id].reads_sent_mes_num++;
      }
    }
    p_ops->r_fifo->bcast_size -= coalesce_num;
    // This message has been sent, do not add other reads to it!
    // this is tricky because proposes leave half-filled messages, make sure this is the last message to bcast
    if (p_ops->r_fifo->bcast_size == 0) {
      uint8_t max_coalesce = (uint8_t) (is_propose ? MAX_PROP_COALESCE : MAX_R_COALESCE);
      if (coalesce_num < max_coalesce) {
        //yellow_printf("Broadcasting r_rep with coalesce num %u \n", coalesce_num);
        MOD_ADD(p_ops->r_fifo->push_ptr, R_FIFO_SIZE);
        p_ops->r_fifo->r_message[p_ops->r_fifo->push_ptr].coalesce_num = 0;
      }
    }
    reads_sent += coalesce_num;
    mes_sent++;
    MOD_ADD(bcast_pull_ptr, R_FIFO_SIZE);
    if (br_i == MAX_BCAST_BATCH) {
      post_quorum_broadasts_and_recvs(r_rep_recv_info, MAX_RECV_R_REP_WRS - r_rep_recv_info->posted_recvs,
                                      q_info, br_i, *r_br_tx, r_send_wr, cb->dgram_qp[R_QP_ID],
                                      R_ENABLE_INLINING);
      br_i = 0;
    }
  }
  if (br_i > 0)
    post_quorum_broadasts_and_recvs(r_rep_recv_info, MAX_RECV_R_REP_WRS - r_rep_recv_info->posted_recvs,
                                    q_info, br_i, *r_br_tx, r_send_wr, cb->dgram_qp[R_QP_ID],
                                    R_ENABLE_INLINING);
  p_ops->r_fifo->bcast_pull_ptr = bcast_pull_ptr;
  if (mes_sent > 0) decrease_credits(credits, q_info, mes_sent, vc);
}


/* ---------------------------------------------------------------------------
//------------------------------ POLLLING------- -----------------------------
//---------------------------------------------------------------------------*/

// Keep track of the write messages to send the appropriate acks
static inline void ack_bookkeeping(struct ack_message *ack, uint8_t w_num, uint64_t l_id,
                                   const uint8_t m_id, const uint16_t t_id)
{
  if (ENABLE_ASSERTIONS && DEBUG_QUORUM) {
    if(unlikely(*(uint64_t *) ack->local_id) + ack->ack_num != l_id) {
      red_printf("Wrkr %u: Adding to existing ack for machine %u  with l_id %lu, "
                   "ack_num %u with new l_id %lu and w_num %u\n", t_id, m_id,
                 *(uint64_t *) ack->local_id, ack->ack_num, l_id, w_num);
      //assert(false);
    }
  }
  if (ack->opcode == CACHE_OP_ACK) {// new ack
    //if (ENABLE_ASSERTIONS) assert((*(uint64_t *)ack->local_id) + ack->ack_num == l_id);
    memcpy(ack->local_id, &l_id, sizeof(uint64_t));
    ack->credits = 1;
    ack->ack_num = w_num;
    ack->opcode = ACK_NOT_YET_SENT;
    if (DEBUG_ACKS) yellow_printf("Create an ack with l_id  %lu \n", *(uint64_t *)ack->local_id);
  }
  else {
    if (ENABLE_ASSERTIONS) {
      assert((*(uint64_t *)ack->local_id) + ack->ack_num == l_id);
      assert(ack->ack_num < 63000);
      assert(W_CREDITS > 1);
      assert(ack->credits < W_CREDITS);
    }
    ack->credits++;
    ack->ack_num += w_num;
  }
}



//Handle the configuration bit_vec vector on receiving a release
static inline void handle_configuration_on_receiving_rel(struct write *write, uint16_t t_id)
{

  // On receiving the 1st round of a Release:
  // apply the change to the stable vector and set the bit_vec that gets changed to Stable state.
  // Do not change the sent vector
  if (unlikely(write->opcode == OP_RELEASE_BIT_VECTOR)) {
    uint16_t recv_conf_bit_vec = *(uint16_t *) write->value;
    for (uint16_t m_i = 0; m_i < MACHINE_NUM; m_i++) {
      if (recv_conf_bit_vec & machine_bit_id[m_i]) {
        set_conf_bit_to_new_state(t_id, m_i, DOWN_STABLE);
          if (DEBUG_BIT_VECS)
            green_printf("Worker %u updates the kept config bit_vec vector: received: %u, m_id %u \n",
                         t_id, recv_conf_bit_vec, m_i);
      }
    }
  } // we do not change the op back to OP_RELEASE, because we want to avoid making the actual write to the KVS
  // (because it only contains a bit vector)
}

// Poll for the write broadcasts
static inline void poll_for_writes(volatile struct w_message_ud_req *incoming_ws,
                                   uint32_t *pull_ptr, struct pending_ops *p_ops,
                                   struct ibv_cq *w_recv_cq, struct ibv_wc *w_recv_wc,
                                   struct recv_info *w_recv_info, struct ack_message *acks,
                                   uint16_t t_id)
{
  uint32_t polled_messages = 0, polled_writes = 0;
  int completed_messages =  ibv_poll_cq(w_recv_cq, W_BUF_SLOTS, w_recv_wc);
  if (DEBUG_RECEIVES) {
    w_recv_info->posted_recvs -= completed_messages;
    if (w_recv_info->posted_recvs < RECV_WR_SAFETY_MARGIN)
      red_printf("Wrkr %u some remote machine has created credits out of thin air \n", t_id);
  }
  if (completed_messages <= 0) return;
  uint32_t index = *pull_ptr;
  // Start polling
  while (polled_messages < completed_messages) {
    check_the_polled_write_message(&incoming_ws[index].w_mes, index, polled_writes, t_id);
    struct w_message *w_mes = (struct w_message*) &incoming_ws[index].w_mes;
    print_polled_write_message_info(w_mes, index, t_id);
    uint8_t w_num = w_mes->w_num;
    bool is_accept = w_mes->write[0].opcode == ACCEPT_OP;
    bool is_commit = w_mes->write[0].opcode == COMMIT_OP;

    for (uint16_t i = 0; i < w_num; i++) {
      struct write *write = &w_mes->write[i];
      check_a_polled_write(write, i, w_num, t_id);
      if (!EMULATE_ABD) handle_configuration_on_receiving_rel(write, t_id);
      p_ops->ptrs_to_w_ops[polled_writes] = (struct write *)(((void *) write) - 3); // align with cache_op
      polled_writes++;
    }
    if (!is_accept) ack_bookkeeping(&acks[w_mes->m_id], w_num, *(uint64_t *)w_mes->l_id, w_mes->m_id, t_id);
    count_stats_on_receiving_w_mes_reset_w_num(w_mes, w_num, t_id);
    MOD_ADD(index, W_BUF_SLOTS);
    polled_messages++;
  }
  (*pull_ptr) = index;

  if (polled_writes > 0) {
    if (DEBUG_WRITES) yellow_printf("Worker %u is going with %u writes to the cache \n", t_id, polled_writes);
    cache_batch_op_updates((uint32_t) polled_writes, 0, p_ops->ptrs_to_w_ops, p_ops, 0, MAX_INCOMING_W, ENABLE_ASSERTIONS == 1);
    if (DEBUG_WRITES) yellow_printf("Worker %u propagated %u writes to the cache \n", t_id, polled_writes);
  }
}

// Wait until the entire r_rep is there
static inline void wait_for_the_entire_read(volatile struct r_message *r_mes,
                                             uint16_t t_id, uint32_t index)
{
  uint32_t debug_cntr = 0;
  while (r_mes->read[r_mes->coalesce_num - 1].opcode != CACHE_OP_GET) {

    if ((r_mes->read[r_mes->coalesce_num - 1].opcode == CACHE_OP_GET_TS) ||
        (r_mes->read[r_mes->coalesce_num - 1].opcode == OP_ACQUIRE) ||
        (r_mes->read[r_mes->coalesce_num - 1].opcode == PROPOSE_OP) ||
          r_mes->read[r_mes->coalesce_num - 1].opcode == OP_ACQUIRE_FLIP_BIT) return;
    if (ENABLE_ASSERTIONS) {
      assert(false);
      debug_cntr++;
      if (debug_cntr == B_4_) {
        red_printf("Wrkr %d stuck waiting for a read to come index %u coalesce id %u\n",
                   t_id, index, r_mes->coalesce_num - 1);
        print_wrkr_stats(t_id);
        debug_cntr = 0;
      }
    }
  }
}

// Poll for the r_rep broadcasts
static inline void poll_for_reads(volatile struct r_message_ud_req *incoming_rs,
                                  uint32_t *pull_ptr, struct pending_ops *p_ops,
                                  struct ibv_cq *r_recv_cq, struct ibv_wc *r_recv_wc,
                                  uint16_t t_id, uint32_t *dbg_counter)
{
  if (p_ops->r_rep_fifo->mes_size == R_REP_FIFO_SIZE) return;
  int completed_messages =  ibv_poll_cq(r_recv_cq, R_BUF_SLOTS, r_recv_wc);
  if (completed_messages <= 0) return;
  uint32_t index = *pull_ptr;
  uint32_t polled_messages = 0, polled_reads = 0;
  // Start polling
  while (polled_messages < completed_messages) {
    if (ENABLE_ASSERTIONS) {
      assert(incoming_rs[index].r_mes.coalesce_num > 0);
      wait_for_the_entire_read(&incoming_rs[index].r_mes, t_id, index);
    }
    struct r_message *r_mes = (struct r_message*) &incoming_rs[index].r_mes;
    if (DEBUG_READS)
      printf("Worker %u sees a read Opcode %d at offset %d, l_id %lu  \n", t_id,
             incoming_rs[index].r_mes.read[0].opcode, index, *(uint64_t *)incoming_rs[index].r_mes.l_id);
    else if (DEBUG_RMW && r_mes->read[0].opcode == PROPOSE_OP) {
      struct prop_message *prop_mes = (struct prop_message *) r_mes;
      cyan_printf("Worker %u sees a Propose from m_id %u: opcode %d at offset %d, rmw_id %lu, "
                  "glob_sess_id %u, log_no %u, coalesce_num %u version %u \n",
                  t_id, prop_mes->m_id, prop_mes->prop[0].opcode, index, *(uint64_t *) prop_mes->prop[0].t_rmw_id,
                  *(uint16_t *) prop_mes->prop[0].glob_sess_id, *(uint32_t *) prop_mes->prop[0].log_no,
                  prop_mes->coalesce_num, *(uint32_t *) prop_mes->prop[0].ts.version);
    }

    uint8_t r_num = r_mes->coalesce_num;
    if (polled_reads + r_num > MAX_INCOMING_R && ENABLE_ASSERTIONS) assert(false);
    for (uint16_t i = 0; i < r_num; i++) {
      struct read *read = &r_mes->read[i];
      if (ENABLE_ASSERTIONS) {
        assert(MAX_PROP_COALESCE == 1); // this function won't work otherwise
        if (read->opcode != CACHE_OP_GET && read->opcode != OP_ACQUIRE &&
            read->opcode != CACHE_OP_GET_TS &&
            read->opcode != PROPOSE_OP && read->opcode != OP_ACQUIRE_FLIP_BIT)
          red_printf("Receiving read: Opcode %u, i %u/%u \n", read->opcode, i, r_num);
      }
      //printf("version %u \n", *(uint32_t*) read->ts.version);
      if (read->opcode == OP_ACQUIRE)
        read->opcode = take_ownership_of_a_conf_bit(t_id, (* (uint64_t *)r_mes->l_id) + i,
                                                    (uint16_t) r_mes->m_id);
      if (read->opcode == OP_ACQUIRE_FLIP_BIT)
        raise_conf_bit_iff_owned(t_id, *(uint64_t *) read->key, (uint16_t) r_mes->m_id);
      p_ops->ptrs_to_r_ops[polled_reads] = (struct read *)(((void *) read) - 3); //align with the cache op
      p_ops->ptrs_to_r_headers[polled_reads] = r_mes;
      polled_reads++;
    }
    if (ENABLE_STAT_COUNTING) {
      if (ENABLE_ASSERTIONS) t_stats[t_id].per_worker_reads_received[r_mes->m_id] += r_num;
      t_stats[t_id].received_reads += r_num;
      t_stats[t_id].received_reads_mes_num++;
    }
    if (ENABLE_ASSERTIONS) incoming_rs[index].r_mes.coalesce_num = 0;
    MOD_ADD(index, R_BUF_SLOTS);
    polled_messages++;
    // Back-pressure
    if (polled_messages + p_ops->r_rep_fifo->mes_size == R_REP_FIFO_SIZE) {
      assert(false);
      break;
    }
  }
  (*pull_ptr) = index;
  // Poll for the completion of the receives
  if (polled_messages > 0) {
    cache_batch_op_reads(polled_reads, t_id, p_ops, 0, MAX_INCOMING_R, ENABLE_ASSERTIONS == 1);
    if (ENABLE_ASSERTIONS) dbg_counter[R_QP_ID] = 0;
  }
  else if (ENABLE_ASSERTIONS && p_ops->r_rep_fifo->mes_size == 0) dbg_counter[R_QP_ID]++;


}


// Form the  work request for the read reply
static inline void forge_r_rep_wr(uint32_t r_rep_i, uint16_t mes_i, struct pending_ops *p_ops,
                                  struct hrd_ctrl_blk *cb, struct ibv_sge *send_sgl,
                                  struct ibv_send_wr *send_wr, uint64_t *r_rep_tx,
                                  uint16_t t_id) {

  struct ibv_wc signal_send_wc;
  struct r_rep_fifo *r_rep_fifo = p_ops->r_rep_fifo;
  struct r_rep_message *r_rep_mes = &r_rep_fifo->r_rep_message[r_rep_i];
  uint8_t coalesce_num = r_rep_mes->coalesce_num;

  send_sgl[mes_i].length = r_rep_fifo->message_sizes[r_rep_i];
  send_sgl[mes_i].addr = (uint64_t) (uintptr_t) r_rep_mes;

  checks_and_prints_when_forging_r_rep_wr(coalesce_num, mes_i, send_sgl, r_rep_i,
                                          r_rep_mes, r_rep_fifo, t_id);

  uint8_t rm_id = r_rep_fifo->rem_m_id[r_rep_i];
  if (ENABLE_ADAPTIVE_INLINING)
    adaptive_inlining(send_sgl[mes_i].length, &send_wr[mes_i], 1);
  else send_wr[mes_i].send_flags = R_REP_ENABLE_INLINING ? IBV_SEND_INLINE : 0;
  send_wr[mes_i].wr.ud.ah = remote_qp[rm_id][t_id][R_REP_QP_ID].ah;
  send_wr[mes_i].wr.ud.remote_qpn = (uint32) remote_qp[rm_id][t_id][R_REP_QP_ID].qpn;
  // Do a Signaled Send every R_SS_BATCH messages
  if ((*r_rep_tx) % R_REP_SS_BATCH == 0) send_wr[mes_i].send_flags |= IBV_SEND_SIGNALED;
  (*r_rep_tx)++;
  if ((*r_rep_tx) % R_REP_SS_BATCH == R_REP_SS_BATCH - 1) {
    //printf("Wrkr %u POLLING for a send completion in read replies \n", m_id);
    poll_cq(cb->dgram_send_cq[R_REP_QP_ID], 1, &signal_send_wc, POLL_CQ_R_REP);
  }
  if (mes_i > 0) send_wr[mes_i - 1].next = &send_wr[mes_i];

}

// Send Read Replies
static inline void send_r_reps(struct pending_ops *p_ops, struct hrd_ctrl_blk *cb,
                               struct ibv_send_wr *r_rep_send_wr, struct ibv_sge *r_rep_send_sgl,
                               struct recv_info *r_recv_info, struct recv_info *w_recv_info,
                               uint64_t *r_rep_tx,  uint16_t t_id)
{
  uint16_t mes_i = 0, r_reps_sent = 0, accept_recvs_to_post = 0, read_recvs_to_post = 0;
  uint32_t pull_ptr = p_ops->r_rep_fifo->pull_ptr;
  struct ibv_send_wr *bad_send_wr;

  struct r_rep_fifo *r_rep_fifo = p_ops->r_rep_fifo;
  while (r_rep_fifo->total_size > 0) {
    // Create the r_rep messages
    forge_r_rep_wr(pull_ptr, mes_i, p_ops, cb, r_rep_send_sgl, r_rep_send_wr, r_rep_tx, t_id);
    uint8_t coalesce_num = r_rep_fifo->r_rep_message[pull_ptr].coalesce_num;
    print_check_count_stats_when_sending_r_rep(r_rep_fifo, coalesce_num, mes_i, t_id);
    r_rep_fifo->total_size -= coalesce_num;
    r_rep_fifo->mes_size--;
    r_reps_sent += coalesce_num;
    if (r_rep_fifo->r_rep_message[pull_ptr].opcode == ACCEPT_REPLY)
      accept_recvs_to_post++;
    else read_recvs_to_post++;
    MOD_ADD(pull_ptr, R_REP_FIFO_SIZE);
    mes_i++;
  }
  if (mes_i > 0) {
    if (ENABLE_ASSERTIONS) assert(mes_i == accept_recvs_to_post + read_recvs_to_post);
    if (read_recvs_to_post > 0) {
      if (DEBUG_READS) printf("Wrkr %d posting %d read recvs\n", t_id,  read_recvs_to_post);
      post_recvs_with_recv_info(r_recv_info, read_recvs_to_post);
    }
    if (accept_recvs_to_post > 0) {
      if (DEBUG_RMW) printf("Wrkr %d posting %d accept recvs\n", t_id,  accept_recvs_to_post);
      post_recvs_with_recv_info(w_recv_info, accept_recvs_to_post);
    }
    r_rep_send_wr[mes_i - 1].next = NULL;
    int ret = ibv_post_send(cb->dgram_qp[R_REP_QP_ID], &r_rep_send_wr[0], &bad_send_wr);
    if (ENABLE_ASSERTIONS) CPE(ret, "R_REP ibv_post_send error", ret);
  }
  r_rep_fifo->pull_ptr = pull_ptr;

}




// Each read has an associated read_info structure that keeps track of the incoming replies, value, opcode etc.
static inline void read_info_bookkeeping(struct r_rep_big *r_rep, struct read_info *read_info)
{
  // Check for acquires that detected a false positive
  if (unlikely(r_rep->opcode > TS_GREATER)) {
    read_info->fp_detected = true;
    if (DEBUG_QUORUM) yellow_printf("Raising the fp flag after seeing read reply %u \n", r_rep->opcode);
    r_rep->opcode -= FALSE_POSITIVE_OFFSET;
    if (ENABLE_ASSERTIONS) {
      assert(r_rep->opcode >= TS_SMALLER && r_rep->opcode <= TS_GREATER);
      assert(read_info->opcode != OP_ACQUIRE_FLIP_BIT);
    }
  }

  if (r_rep->opcode == TS_GREATER || r_rep->opcode == TS_GREATER_TS_ONLY) {
    if (ENABLE_ASSERTIONS) {
      if (r_rep->opcode == TS_GREATER_TS_ONLY)
        assert(read_info->opcode == OP_RELEASE || read_info->opcode == CACHE_OP_PUT);
      else assert(read_info->opcode != OP_RELEASE && read_info->opcode != CACHE_OP_PUT);
    }
    if (!read_info->seen_larger_ts) { // If this is the first "Greater" ts
      read_info->ts_to_read = r_rep->ts;
      read_info->times_seen_ts = 1;
      memcpy(read_info->value, r_rep->value, VALUE_SIZE);
      read_info->seen_larger_ts = true;
    }
    else { // if the read has already received a "greater" ts
      enum ts_compare ts_comp = compare_netw_ts(&read_info->ts_to_read, &r_rep->ts);
      if (ts_comp == SMALLER) {
        read_info->ts_to_read = r_rep->ts;
        read_info->times_seen_ts = 1;
        memcpy(read_info->value, r_rep->value, VALUE_SIZE);
      }
      if (ts_comp == EQUAL) read_info->times_seen_ts++;
      // Nothing to do if the already stored ts is greater than the incoming
    }
  }
  else if (r_rep->opcode == TS_EQUAL) {
    if (!read_info->seen_larger_ts)  // If it has not seen a "greater ts"
      read_info->times_seen_ts++;
    // Nothing to do if the already stored ts is greater than the incoming
  }
  else if (r_rep->opcode == TS_SMALLER) { // Nothing to do if the already stored ts is greater than the incoming

  }
  // assert(read_info->rep_num == 0);
  read_info->rep_num++;
}



//Poll for read replies
static inline void poll_for_read_replies(volatile struct r_rep_message_ud_req *incoming_r_reps,
                                         uint32_t *pull_ptr, struct pending_ops *p_ops,
                                         uint16_t credits[][MACHINE_NUM],
                                         struct ibv_cq *r_rep_recv_cq, struct ibv_wc *r_rep_recv_wc,
                                         struct recv_info *r_rep_recv_info, uint16_t t_id,
                                         uint32_t *outstanding_reads, uint32_t *debug_cntr)
{
  if (p_ops->r_rep_fifo->mes_size == R_REP_FIFO_SIZE) return;
  int completed_messages =  ibv_poll_cq(r_rep_recv_cq, R_REP_BUF_SLOTS, r_rep_recv_wc);
  if (completed_messages <= 0) return;
  uint32_t index = *pull_ptr;
  uint32_t polled_messages = 0;
  // Start polling
  while (polled_messages < completed_messages) {
    print_and_check_mes_when_polling_r_reps((struct r_rep_message *) &incoming_r_reps[index].r_rep_mes, index, t_id);
    struct r_rep_message *r_rep_mes = (struct r_rep_message*) &incoming_r_reps[index].r_rep_mes;
    bool is_propose = r_rep_mes->opcode == PROP_REPLY;
    bool is_accept = r_rep_mes->opcode == ACCEPT_REPLY;
    increase_credits_when_polling_r_reps(credits, is_propose, is_accept, r_rep_mes->m_id, t_id);
    polled_messages++;
    MOD_ADD(index, R_REP_BUF_SLOTS);

    // If it is a reply to a prepare call a different handler
    if (is_propose || is_accept) {
      handle_rmw_rep_replies(p_ops, r_rep_mes, is_accept, t_id);
      continue;
    }

    r_rep_mes->opcode = INVALID_OPCODE; // a random meaningless opcode
    uint8_t r_rep_num = r_rep_mes->coalesce_num;
    // Find the request that the reply is referring to
    uint64_t l_id = *(uint64_t *) (r_rep_mes->l_id);
    uint64_t pull_lid = p_ops->local_r_id; // l_id at the pull pointer
    uint32_t r_ptr; // a pointer in the FIFO, from where r_rep refers to

    // if the pending read FIFO is empty it means the r_reps are for committed messages.
    if (p_ops->r_size == 0 ) {
      if (!USE_QUORUM) assert(false);
      continue;
    }
    if (ENABLE_ASSERTIONS) check_r_rep_l_id(l_id, r_rep_num, pull_lid, p_ops->r_size, t_id);

    if (pull_lid >= l_id) {
      if ((pull_lid - l_id) >= r_rep_num) continue;
      r_ptr = p_ops->r_pull_ptr;
    }
    else  // l_id > pull_lid
      r_ptr = (uint32_t) (p_ops->r_pull_ptr + (l_id - pull_lid)) % PENDING_READS;

    uint16_t byte_ptr = R_REP_MES_HEADER;
    for (uint16_t i = 0; i < r_rep_num; i++) {
      struct r_rep_big *r_rep = (struct r_rep_big *)(((void *)r_rep_mes) + byte_ptr);
      if (ENABLE_ASSERTIONS) {
        if ((r_rep->opcode < TS_SMALLER || r_rep->opcode > RMW_TS_STALE) &&
            (r_rep->opcode < TS_SMALLER + FALSE_POSITIVE_OFFSET  ||
              r_rep->opcode > RMW_TS_STALE + FALSE_POSITIVE_OFFSET)) {
          red_printf("Receiving r_rep: Opcode %u, i %u/%u \n", r_rep->opcode, i, r_rep_num);
          assert(false);
          wait_until_the_entire_r_rep((volatile struct r_rep_big *) r_rep, incoming_r_reps, i,
                                      index, p_ops, byte_ptr, t_id);
        }
      }
      if (r_rep->opcode == TS_GREATER || (r_rep->opcode == TS_GREATER + FALSE_POSITIVE_OFFSET) ||
          r_rep->opcode == RMW_ACCEPTED || (r_rep->opcode == RMW_ACCEPTED + FALSE_POSITIVE_OFFSET) ||
          r_rep->opcode == RMW_TS_STALE || (r_rep->opcode == RMW_TS_STALE + FALSE_POSITIVE_OFFSET))
        byte_ptr += R_REP_SIZE;
      else if (r_rep->opcode == TS_GREATER_TS_ONLY) byte_ptr += R_REP_ONLY_TS_SIZE;
      else byte_ptr++;
      if (pull_lid >= l_id) {
        if (l_id + i < pull_lid) continue;
      }
      struct read_info *read_info = &p_ops->read_info[r_ptr];
      read_info_bookkeeping(r_rep, read_info);
      if (DEBUG_READS)
        yellow_printf("Read reply %u, Received replies %u/%d at r_ptr %u \n",
                      i, read_info->rep_num, REMOTE_QUORUM, r_ptr);
      if (read_info->rep_num >= REMOTE_QUORUM) {
        // if (ENABLE_ASSERTIONS) assert(p_ops->r_state[r_ptr] >= SENT);
        p_ops->r_state[r_ptr] = READY;
        if (ENABLE_ASSERTIONS) {
          (*outstanding_reads)--;
          assert(read_info->rep_num <= REM_MACH_NUM);
        }
      }
      r_ptr = (r_ptr + 1) % PENDING_READS;
      r_rep->opcode = INVALID_OPCODE;
    }
    if (ENABLE_STAT_COUNTING) {
      if (ENABLE_ASSERTIONS) t_stats[t_id].per_worker_r_reps_received[r_rep_mes->m_id] += r_rep_num;
      t_stats[t_id].received_r_reps += r_rep_num;
      t_stats[t_id].received_r_reps_mes_num++;
    }
  }
  (*pull_ptr) = index;
  // Poll for the completion of the receives
  if (polled_messages > 0) {
    if (ENABLE_ASSERTIONS) assert(r_rep_recv_info->posted_recvs >= polled_messages);
    r_rep_recv_info->posted_recvs -= polled_messages;
    //poll_cq(r_rep_recv_cq, polled_messages, r_rep_recv_wc);
    if (ENABLE_ASSERTIONS) debug_cntr[R_REP_QP_ID] = 0;
  }
  else {
    if (ENABLE_ASSERTIONS && (*outstanding_reads) > 0) debug_cntr[R_REP_QP_ID]++;
    if (ENABLE_STAT_COUNTING && (*outstanding_reads) > 0) t_stats[t_id].stalled_r_rep++;
  }
}

// Handle acked reads: trigger second round if needed, update the KVS if needed
// Handle the first round of Lin Writes
// Increment the epoch_id after an acquire that learnt the node has missed messages
static inline void commit_reads(struct pending_ops *p_ops,
                                struct latency_flags * latency_info, uint16_t t_id)
{
  uint32_t pull_ptr = p_ops->r_pull_ptr;
  uint16_t writes_for_cache = 0;
  // Acquire needs to have a second READ round irrespective of the
  // timestamps if it is charged with flipping a bit
  bool acq_second_round_to_flip_bit;
  // A relaxed read need not do a second round ever, it does not need to quoromize a value before reading it
  // An acquire must have a second round trip if the timestamp has not been seen by a quorum
  // i.e. if it has been seen locally but not from a REMOTE_QUORUM
  // or has not been seen locally and has not been seen by a full QUORUM
  // or if it is the first round of a release or an out-of-epoch write
  bool insert_write_flag;
  // write_local_kvs : Write the local KVS if the ts has not been seen locally
  // or if it is an out-of-epoch write (but NOT a Release!!)
  bool write_local_kvs;

  /* Because it's possible for a read to insert another read i.e OP_ACQUIRE->OP_ACQUIRE_FLIP_BIT
   * we need to make sure that even if all requests do that the fifo will have enough space to:
   * 1 not deadlock and 2 not overwrite a read_info that will later get taken to the cache
   * That means that the fifo must have free slots equal to SESSION_PER_THREADS because
   * this many acquires can possibly exist in the fifo*/
  if (ENABLE_ASSERTIONS) assert(p_ops->virt_r_size < PENDING_READS);
  while(p_ops->r_state[pull_ptr] == READY) {
    //set the flags for each read
    set_flags_before_committing_a_read(p_ops, pull_ptr, &acq_second_round_to_flip_bit, &insert_write_flag,
                                       &write_local_kvs, t_id);
    if (ENABLE_ASSERTIONS)
      checks_when_commiting_a_read(p_ops, pull_ptr, acq_second_round_to_flip_bit, insert_write_flag,
                                   write_local_kvs, t_id);

    // Break condition: this read cannot be processed, and thus no subsequent read will be processed
    if ((insert_write_flag && (p_ops->w_size >= MAX_ALLOWED_W_SIZE)) ||
        (write_local_kvs && (writes_for_cache >= MAX_INCOMING_R)))// ||
       // (acq_second_round_to_flip_bit) && (p_ops->virt_r_size >= MAX_ALLOWED_R_SIZE))
      break;

    //CACHE: Reads that need to go to cache
    if (write_local_kvs) {
      // if a read did not see a larger ts it should only change the epoch
      if (p_ops->read_info[pull_ptr].opcode == CACHE_OP_GET &&
        (!p_ops->read_info[pull_ptr].seen_larger_ts))
        p_ops->read_info[pull_ptr].opcode = UPDATE_EPOCH_OP_GET;
      p_ops->ptrs_to_r_ops[writes_for_cache] = (struct read *) &p_ops->read_info[pull_ptr];
      writes_for_cache++;
      // An out-of-epoch write will get its TS set when inserting a write,
      // so there is no need to do it here
    }

    //INSERT WRITE: Reads that need to be converted to writes: second round of read/acquire
    if (insert_write_flag) {
      if (p_ops->read_info[pull_ptr].opcode == OP_RELEASE ||
          p_ops->read_info[pull_ptr].opcode == CACHE_OP_PUT) {
        p_ops->read_info[pull_ptr].ts_to_read.m_id = (uint8_t) machine_id;
        *(uint32_t *)p_ops->read_info[pull_ptr].ts_to_read.version += 2;
        if (p_ops->read_info[pull_ptr].opcode == OP_RELEASE)
          memcpy(&p_ops->read_info[pull_ptr], &p_ops->r_session_id[pull_ptr], SESSION_BYTES);
      }
      else if (ENABLE_STAT_COUNTING) t_stats[t_id].read_to_write++;

      insert_write(p_ops, NULL, FROM_READ, pull_ptr, t_id);
    }

    // FAULT_TOLERANCE: In the off chance that the acquire needs a second round for fault tolerance
    if (unlikely(acq_second_round_to_flip_bit)) {
      //if (p_ops->read_info[pull_ptr].epoch_id == epoch_id) {
      epoch_id++; // epoch_id should be incremented always even it has been incremented since the acquire fired
      if (DEBUG_QUORUM) printf("Worker %u increases the epoch id to %u \n", t_id, (uint16_t) epoch_id);
      //}
      // The read must have the struct key overloaded with the original acquire l_id
      if (DEBUG_BIT_VECS)
        cyan_printf("Wrkr, %u Opcode to be sent in the insert read %u, the local id to be sent %u, "
                    "read_info pull_ptr %u, read_info push_ptr %u read fifo size %u, virtual size: %u  \n",
                    t_id, p_ops->read_info[pull_ptr].opcode, p_ops->local_r_id, pull_ptr,
                    p_ops->r_push_ptr, p_ops->r_size, p_ops->virt_r_size);
      /* */
      insert_read(p_ops, NULL, FROM_ACQUIRE, t_id);
      p_ops->read_info[pull_ptr].fp_detected = false;
    }

    // SESSION: Acquires that wont have a second round and thus must free the session
    if (!insert_write_flag &&
        (p_ops->read_info[pull_ptr].opcode == OP_ACQUIRE)) {
      if (ENABLE_ASSERTIONS) assert(p_ops->r_session_id[pull_ptr] < SESSIONS_PER_THREAD);
      p_ops->session_has_pending_op[p_ops->r_session_id[pull_ptr]] = false;
      p_ops->all_sessions_stalled = false;
      if (MEASURE_LATENCY && t_id == LATENCY_THREAD && machine_id == LATENCY_MACHINE &&
          latency_info->measured_req_flag == ACQUIRE &&
          p_ops->r_session_id[pull_ptr] == latency_info->measured_sess_id)
        report_latency(latency_info);
    }

    // Clean-up code
    memset(&p_ops->read_info[pull_ptr], 0, 3); // a lin write uses these bytes for the session id but it's still fine to clear them
    p_ops->r_state[pull_ptr] = INVALID;
    p_ops->r_size--;
    p_ops->virt_r_size -= p_ops->read_info[pull_ptr].opcode == OP_ACQUIRE ? 2 : 1;
    if (ENABLE_ASSERTIONS) assert(p_ops->virt_r_size < PENDING_READS);
    MOD_ADD(pull_ptr, PENDING_READS);
    p_ops->local_r_id++;
  }
  p_ops->r_pull_ptr = pull_ptr;
  if (writes_for_cache > 0)
    cache_batch_op_first_read_round(writes_for_cache, t_id, (struct read_info **) p_ops->ptrs_to_r_ops,
                                    p_ops, 0, MAX_INCOMING_R, false);
}

// Send a batched ack that denotes the first local write id and the number of subsequent lids that are being acked
static inline void send_acks(struct ibv_send_wr *ack_send_wr,
                             uint64_t *sent_ack_tx,
                             struct hrd_ctrl_blk *cb, struct recv_info *w_recv_info,
                             struct ack_message *acks, uint16_t t_id)
{
  uint8_t ack_i = 0, prev_ack_i = 0, first_wr = 0;
  struct ibv_wc signal_send_wc;
  struct ibv_send_wr *bad_send_wr;
  uint32_t recvs_to_post_num = 0;

  for (uint8_t i = 0; i < MACHINE_NUM; i++) {
    if (acks[i].opcode == CACHE_OP_ACK) continue;
    if (ENABLE_STAT_COUNTING) {
      t_stats[t_id].per_worker_acks_sent[i] += acks[i].ack_num;
      t_stats[t_id].per_worker_acks_mes_sent[i]++;
      t_stats[t_id].acks_sent += acks[i].ack_num;
      t_stats[t_id].acks_sent_mes_num++;
    }
    if (DEBUG_ACKS)
      yellow_printf("Wrkr %d is sending an ack for lid %lu, credits %u and ack num %d and m id %d \n",
                    t_id, *(uint64_t *)acks[i].local_id, acks[i].credits, acks[i].ack_num, acks[i].m_id);

    acks[i].opcode = CACHE_OP_ACK;
    if (ENABLE_ASSERTIONS) {
      assert(acks[i].credits <= acks[i].ack_num);
      if (acks[i].ack_num > MAX_W_COALESCE) assert(acks[i].credits > 1);
      assert(acks[i].credits <= W_CREDITS);
      assert(acks[i].ack_num > 0);
    }
    if ((*sent_ack_tx) % ACK_SS_BATCH == 0) {
      ack_send_wr[i].send_flags |= IBV_SEND_SIGNALED;
      // if (g_id == 0) green_printf("Sending ack %llu signaled \n", *sent_ack_tx);
    } else ack_send_wr[i].send_flags = IBV_SEND_INLINE;
    if ((*sent_ack_tx) % ACK_SS_BATCH == ACK_SS_BATCH - 1) {
      // if (g_id == 0) green_printf("Polling for ack  %llu \n", *sent_ack_tx);
      poll_cq(cb->dgram_send_cq[ACK_QP_ID], 1, &signal_send_wc, POLL_CQ_ACK);
    }
    if (ack_i > 0) {
      if (DEBUG_ACKS) yellow_printf("Wrkr %u, ack %u points to ack %u \n", t_id, prev_ack_i, i);
      ack_send_wr[prev_ack_i].next = &ack_send_wr[i];
    }
    else first_wr = i;
    (*sent_ack_tx)++; // Selective signaling
    recvs_to_post_num += acks[i].credits;
    ack_i++;
    prev_ack_i = i;
  }
  // RECEIVES for writes
  if (recvs_to_post_num > 0) {
    post_recvs_with_recv_info(w_recv_info, recvs_to_post_num);
    if (DEBUG_RECEIVES) {
      w_recv_info->posted_recvs += recvs_to_post_num;
      assert(w_recv_info->posted_recvs == MAX_RECV_W_WRS);
    }
    //w_recv_info->posted_recvs += recvs_to_post_num;
       // printf("Wrkr %d posting %u recvs and has a total of %u recvs for writes \n",
        //       g_id, recvs_to_post_num,  w_recv_info->posted_recvs);
    if (ENABLE_ASSERTIONS) {

      assert(recvs_to_post_num <= MAX_RECV_W_WRS);
      if (ack_i > 0) assert(recvs_to_post_num >= ack_i);
      if (W_CREDITS == 1) assert(recvs_to_post_num == ack_i);
      //assert(w_recv_info->posted_recvs <= MAX_RECV_W_WRS);
    }
  }
  // SEND the acks
  if (ack_i > 0) {
    if (DEBUG_ACKS) printf("send %u acks, last recipient %u, first recipient %u \n", ack_i, prev_ack_i, first_wr);
    ack_send_wr[prev_ack_i].next = NULL;
    int ret = ibv_post_send(cb->dgram_qp[ACK_QP_ID], &ack_send_wr[first_wr], &bad_send_wr);
    if (ENABLE_ASSERTIONS) CPE(ret, "ACK ibv_post_send error", ret);
  }
}

// Release performs two writes when the first round must carry the send vector
static inline void commit_first_round_of_release_and_spawn_the_second (struct pending_ops *p_ops,
                                                                       uint16_t t_id)
{
  uint32_t w_pull_ptr = p_ops->w_pull_ptr;
  uint32_t w_size = p_ops->w_size;
  // take care to handle the case where the w_size == PENDING_WRITES
  p_ops->w_state[p_ops->w_pull_ptr] = INVALID;
  p_ops->acks_seen[p_ops->w_pull_ptr] = 0;
  p_ops->local_w_id++;
  MOD_ADD(p_ops->w_pull_ptr, PENDING_WRITES);
  p_ops->w_size--;
  if (ENABLE_ASSERTIONS && w_size == MAX_ALLOWED_W_SIZE) {
    red_printf("Wrkr %u p_ops is full sized -- it still works w_size %u, "
                 "Send failure state %u \n", t_id, w_size, send_bit_vector.state);
    bool found = false;
    for (uint8_t i = 0; i < MACHINE_NUM; i++) {
      if (send_bit_vector.bit_vec[i].bit != UP_STABLE) {
        yellow_printf("Bit %i = %u, owner t_id = %u, owner release = %u \n",
                      i, send_bit_vector.bit_vec[i].bit, send_bit_vector.bit_vec[i].owner_t_id,
                      send_bit_vector.bit_vec[i].owner_local_wr_id);
        found = true;
      }
    }
    if (!found && (send_bit_vector.state != UP_STABLE)) assert(false);
  }
  struct write *rel = p_ops->ptrs_to_local_w[w_pull_ptr];
  if (ENABLE_ASSERTIONS) assert (rel != NULL);
  // because we overwrite the value,
  memcpy(rel->value, &p_ops->overwritten_values[SEND_CONF_VEC_SIZE * w_pull_ptr], SEND_CONF_VEC_SIZE);
  struct cache_op op;
  memcpy((void *) &op, &p_ops->w_session_id[w_pull_ptr], SESSION_BYTES);
  memcpy((void *) &op.key.meta.m_id, rel, W_SIZE);
  if (DEBUG_SESSIONS)
    //cyan_printf("Wrkr: %u Inserting the write for the second round of the "
    //            "release opcode %u that carried a bit vector: session %u\n",
    //            t_id, op.opcode, p_ops->w_session_id[w_pull_ptr]);
  insert_write(p_ops, &op, RELEASE_THIRD, w_pull_ptr, t_id); // the push pointer is not needed because the session id is inside the op
  if (ENABLE_ASSERTIONS) {
    p_ops->ptrs_to_local_w[w_pull_ptr] =  NULL;
    memset(&p_ops->overwritten_values[SEND_CONF_VEC_SIZE * w_pull_ptr], 0, SEND_CONF_VEC_SIZE);
  }
}

// Remove writes that have seen all acks
static inline void remove_writes(struct pending_ops *p_ops, struct latency_flags *latency_info,
                                 uint16_t t_id)
{
  while(p_ops->w_state[p_ops->w_pull_ptr] >= READY_PUT) {
    uint32_t w_pull_ptr = p_ops->w_pull_ptr;
    uint8_t w_state = p_ops->w_state[p_ops->w_pull_ptr];
    if (ENABLE_ASSERTIONS && EMULATE_ABD)
      assert(w_state == READY_RELEASE);
    //if (DEBUG_ACKS)
    //  green_printf("Wkrk %u freeing write at pull_ptr %u, w_size %u, w_state %d, session %u, local_w_id %lu, acks seen %u \n",
    //               g_id, p_ops->w_pull_ptr, p_ops->w_size, p_ops->w_state[p_ops->w_pull_ptr],
    //               p_ops->w_session_id[p_ops->w_pull_ptr], p_ops->local_w_id, p_ops->acks_seen[p_ops->w_pull_ptr]);

    uint32_t sess_id = p_ops->w_session_id[w_pull_ptr];
      // Free the session if the commit was not helping:
     // Release or second round of acquire, or commit of non-helping rmw!!
    if ((w_state == READY_RELEASE || w_state == READY_COMMIT) && sess_id < SESSIONS_PER_THREAD)  {
      p_ops->session_has_pending_op[sess_id] = false;
      p_ops->all_sessions_stalled = false;
      if (w_state == READY_COMMIT) {
        struct rmw_local_entry *loc_entry = &p_ops->prop_info->entry[sess_id];
        if (ENABLE_ASSERTIONS ) assert(loc_entry->state != INVALID_RMW);
        loc_entry->state = INVALID_RMW;
        verify_paxos(loc_entry, t_id);
        if (DEBUG_RMW)
          yellow_printf("Worker %u freeing session %u after committing its RMW \n",
                        t_id, sess_id);
      }
    }
    // if the commit is helping another RMW, we give it an invalid sess_id --and we need to rectify it
    else if (sess_id >= SESSIONS_PER_THREAD) p_ops->w_session_id[w_pull_ptr] = 0;
    // This case is tricky because in order to remove the release we must add another release
    // but if the queue was full that would deadlock, therefore we must remove the write before inserting
    // the second round of the release
    if (unlikely(w_state == READY_BIT_VECTOR && (!EMULATE_ABD))) {
      commit_first_round_of_release_and_spawn_the_second (p_ops, t_id);
    }
    else {
      p_ops->w_state[p_ops->w_pull_ptr] = INVALID;
      p_ops->acks_seen[p_ops->w_pull_ptr] = 0;
      p_ops->local_w_id++;
      MOD_ADD(p_ops->w_pull_ptr, PENDING_WRITES);
      p_ops->w_size--;
    }
  }
  check_after_removing_writes(p_ops, t_id);
}

// Worker polls for acks
static inline void poll_acks(struct ack_message_ud_req *incoming_acks, uint32_t *pull_ptr,
                             struct pending_ops *p_ops,
                             uint16_t credits[][MACHINE_NUM],
                             struct ibv_cq * ack_recv_cq, struct ibv_wc *ack_recv_wc,
                             struct recv_info *ack_recv_info,
                             struct latency_flags *latency_info,
                             uint16_t t_id, uint32_t *dbg_counter,
                             uint32_t *outstanding_writes)
{
  uint32_t index = *pull_ptr;
  uint32_t polled_messages = 0;
  int completed_messages =  ibv_poll_cq(ack_recv_cq, ACK_BUF_SLOTS, ack_recv_wc);
  if (completed_messages <= 0) return;
  while (polled_messages < completed_messages) {
    struct ack_message *ack = &incoming_acks[index].ack;
    uint16_t ack_num = ack->ack_num;
    check_ack_message_count_stats(p_ops, ack, index, ack_num, t_id);

    MOD_ADD(index, ACK_BUF_SLOTS);
    polled_messages++;
    uint64_t l_id = *(uint64_t *) (ack->local_id);
    uint64_t pull_lid = p_ops->local_w_id; // l_id at the pull pointer
    uint32_t ack_ptr; // a pointer in the FIFO, from where ack should be added
    credits[W_VC][ack->m_id] += ack->credits;
    // if the pending write FIFO is empty it means the acks are for committed messages.
    if (p_ops->w_size == 0 ) {
      if (ENABLE_ASSERTIONS) assert(USE_QUORUM);
      ack->opcode = INVALID_OPCODE;
      ack->ack_num = 0; continue;
    }

    if (pull_lid >= l_id) {
      if ((pull_lid - l_id) >= ack_num) {ack->opcode = 5;
        ack->ack_num = 0; continue;}
      ack_num -= (pull_lid - l_id);
      ack_ptr = p_ops->w_pull_ptr;
    }
    else { // l_id > pull_lid
      ack_ptr = (uint32_t) (p_ops->w_pull_ptr + (l_id - pull_lid)) % PENDING_WRITES;
    }
    // Apply the acks that refer to stored writes
    for (uint16_t ack_i = 0; ack_i < ack_num; ack_i++) {
      check_ack_and_print(p_ops, ack_i, ack_ptr, ack_num, l_id, pull_lid, t_id);
      p_ops->acks_seen[ack_ptr]++;
      if (p_ops->acks_seen[ack_ptr] == REMOTE_QUORUM) {
        if (ENABLE_ASSERTIONS) (*outstanding_writes)--;
        //printf("Wrkr %d valid ack %u/%u, write at ptr %d is ready \n",
            //   g_id, ack_i, ack_num,  ack_ptr);
        if (unlikely(p_ops->w_state[ack_ptr] == SENT_BIT_VECTOR)) {
          p_ops->w_state[ack_ptr] = READY_BIT_VECTOR;
          raise_send_bit_iff_owned(t_id, l_id + ack_i);
        }
        else if (p_ops->w_state[ack_ptr] == SENT_PUT) p_ops->w_state[ack_ptr] = READY_PUT;
        else if (p_ops->w_state[ack_ptr] == SENT_COMMIT) p_ops->w_state[ack_ptr] = READY_COMMIT;
        else {
          p_ops->w_state[ack_ptr] = READY_RELEASE;
          if (MEASURE_LATENCY && t_id == LATENCY_THREAD && machine_id == LATENCY_MACHINE &&
              latency_info->measured_req_flag != NO_REQ &&
              p_ops->w_session_id[ack_ptr] == latency_info->measured_sess_id)
            report_latency(latency_info);
        }
      }
      MOD_ADD(ack_ptr, PENDING_WRITES);
    }
    if (ENABLE_ASSERTIONS) assert(credits[W_VC][ack->m_id] <= W_CREDITS);
    ack->opcode = INVALID_OPCODE;
    ack->ack_num = 0;
  } // while

  *pull_ptr = index;
  if (polled_messages > 0) {
    remove_writes(p_ops, latency_info, t_id);
    if (ENABLE_ASSERTIONS) dbg_counter[ACK_QP_ID] = 0;
  }
  else {
    if (ENABLE_ASSERTIONS && (*outstanding_writes) > 0) dbg_counter[ACK_QP_ID]++;
    if (ENABLE_STAT_COUNTING && (*outstanding_writes) > 0) t_stats[t_id].stalled_ack++;
  }
  if (ENABLE_ASSERTIONS) assert(ack_recv_info->posted_recvs >= polled_messages);
  ack_recv_info->posted_recvs -= polled_messages;
}


// Worker inspects its local RMW entries
static inline void inspect_rmws(struct pending_ops *p_ops, uint16_t t_id)
{

  for (uint16_t sess_i = 0; sess_i < SESSIONS_PER_THREAD; sess_i++) {
    struct rmw_local_entry* loc_entry = &p_ops->prop_info->entry[sess_i];
    uint8_t state = loc_entry->state;
    if (state == INVALID_RMW) {
      if (ENABLE_ASSERTIONS) assert(!p_ops->session_has_pending_op[sess_i]);
      continue;
    }
    if (ENABLE_ASSERTIONS) assert(p_ops->session_has_pending_op[sess_i]);

    /* =============== ACCEPTED ======================== */
    if (state == ACCEPTED) {
      check_sum_of_reps(loc_entry);
      if (loc_entry->rmw_reps.tot_replies >= REMOTE_QUORUM) {
        advance_loc_entry_l_id(p_ops, loc_entry, t_id);
        inspect_accepts(p_ops, loc_entry, t_id);
        check_state_with_allowed_flags(6, (int) loc_entry->state, INVALID_RMW, PROPOSED, NEEDS_GLOBAL,
                                       MUST_BCAST_COMMITS, MUST_BCAST_COMMITS_FROM_HELP);
      }
    }

    /* =============== BROADCAST COMMITS ======================== */
    if (state == MUST_BCAST_COMMITS || state == MUST_BCAST_COMMITS_FROM_HELP) {
      struct rmw_local_entry *entry_to_commit =
        state == MUST_BCAST_COMMITS ? loc_entry : loc_entry->help_loc_entry;
      bool is_commit_helping = loc_entry->helping_flag != NOT_HELPING;
      if (p_ops->w_size < MAX_ALLOWED_W_SIZE) {
        if (DEBUG_RMW && state == MUST_BCAST_COMMITS_FROM_HELP)
          green_printf("Wrkr %u will bcast commits for the latest committed RMW,"
                       " after learning its proposed RMW has already been committed \n", t_id);
        insert_write(p_ops, (struct cache_op *) entry_to_commit, FROM_COMMIT, (uint32_t) is_commit_helping, t_id);
        if (loc_entry->helping_flag != NOT_HELPING)
          reinstate_loc_entry_after_helping(loc_entry, t_id);
        else {
          loc_entry->state = COMMITTED;
          continue;
        }
      }
    }

    /* =============== NEEDS_GLOBAL ======================== */
    if (state == NEEDS_GLOBAL) {
      struct rmw_entry* glob_entry = &rmw.entry[loc_entry->index_to_rmw];
      // If this fails to grab a global entry it will try to update
      // the (rmw_id + state) that is being waited on.
      // If it updates it will zero the back-off counter
      if (!attempt_to_grab_global_entry_after_waiting(p_ops, glob_entry, loc_entry,
                                                      sess_i, t_id)) {
        loc_entry->back_off_cntr++;
        if (loc_entry->back_off_cntr == RMW_BACK_OFF_TIMEOUT) {
          red_printf("Wrkr %u  sess %u waiting for an rmw, back_of cntr %u waiting on rmw_id %u glob_sess id %u, state %u \n",
                     t_id, sess_i, loc_entry->back_off_cntr, loc_entry->help_rmw->rmw_id.id,
                     loc_entry->help_rmw->rmw_id.glob_sess_id, loc_entry->help_rmw->state);
          // This is failure-related help/stealing it should not be that we are being held up by the local machine
          // However we may wait on a "local" glob sess id, because it is being helped
          // if have accepted a value help it
          if (loc_entry->help_rmw->state == ACCEPTED)
            attempt_to_help_a_locally_accepted_value(p_ops, loc_entry, glob_entry, t_id); // zeroes the back-off counter
            // if have received a proposal, send your own proposal
          else if (loc_entry->help_rmw->state == PROPOSED) {
            attempt_to_steal_a_proposed_global_entry(p_ops, loc_entry, glob_entry, sess_i, t_id); // zeroes the back-off counter
          }
          else if (ENABLE_ASSERTIONS) assert(false);
        }
      }
      if (loc_entry->state == PROPOSED) {
        loc_entry->back_off_cntr = 0;
        insert_prop_to_read_fifo(p_ops, loc_entry, 0, t_id);
      }
      check_state_with_allowed_flags(5, (int) loc_entry->state, INVALID_RMW, PROPOSED, NEEDS_GLOBAL,
                                    ACCEPTED);
    }


    /* =============== PROPOSED ======================== */
    if (state == PROPOSED) {
      check_sum_of_reps(loc_entry);
      if (loc_entry->rmw_reps.tot_replies >= REMOTE_QUORUM) {
        // further responses for that broadcast of Propose must be disregarded
        advance_loc_entry_l_id(p_ops, loc_entry, t_id);
        inspect_proposes(p_ops, loc_entry, t_id);
        check_state_with_allowed_flags(6, (int) loc_entry->state, INVALID_RMW, PROPOSED, NEEDS_GLOBAL,
                                       ACCEPTED, MUST_BCAST_COMMITS_FROM_HELP);
      }
    }
  }
}


#endif /* INLINE_UTILS_H */

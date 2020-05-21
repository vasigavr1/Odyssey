#ifndef INLINE_UTILS_H
#define INLINE_UTILS_H

//#include "kvs.h"
#include "hrd.h"


#include "generic_util.h"
#include "rdma_util.h"
#include "kvs_util.h"
#include "debug_util.h"
#include "config_util.h"
#include "client_if_util.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <common_func.h>


//forward declarations
static inline void act_on_quorum_of_prop_acks(struct pending_ops *p_ops, struct rmw_local_entry *loc_entry,
                                              uint16_t t_id);

/* ---------------------------------------------------------------------------
------------------------------UTILITY --------------------------------------
---------------------------------------------------------------------------*/

// A condition to be used to trigger periodic (but rare) measurements
static inline bool trigger_measurement(uint16_t local_client_id)
{
  return t_stats[local_client_id].cache_hits_per_thread % K_32 > 0 &&
         t_stats[local_client_id].cache_hits_per_thread % K_32 <= 500 &&
         local_client_id == 0 && machine_id == MACHINE_NUM -1;
}



static inline struct key create_key(uint32_t key_id)
{
  uint64_t key_hash = CityHash128((char *) &(key_id), 4).second;
  struct key key;
  memcpy(&key, &key_hash, TRUE_KEY_SIZE);
  return key;
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
        //my_printf(green, "Found max acq latency: %u/%d \n",
        //             latency_count.max_acq_lat, useconds);
      }
      break;
    case RELEASE:
      latency_counter = &latency_count.releases;
      if (useconds > latency_count.max_rel_lat) {
        latency_count.max_rel_lat = (uint32_t) useconds;
        //my_printf(yellow, "Found max rel latency: %u/%d \n", latency_count.max_rel_lat, useconds);
      }
      break;
    case READ_REQ:
      latency_counter = &latency_count.hot_reads;
      if (useconds > latency_count.max_read_lat) latency_count.max_read_lat = (uint32_t) useconds;
      break;
    case WRITE_REQ:
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
      //my_printf(green, "Measuring a req %llu, opcode %d, flag %d op_i %d \n",
      //					 t_stats[t_id].cache_hits_per_thread, opcode, latency_info->measured_req_flag, latency_info->measured_sess_id);
      latency_info->measured_sess_id = sess_id;
      clock_gettime(CLOCK_MONOTONIC, &latency_info->start);
      if (ENABLE_ASSERTIONS) assert(latency_info->measured_req_flag != NO_REQ);
    }
  }
}


/* ---------------------------------------------------------------------------
//------------------------------ DRF-SPECIFIC UTILITY-------------------------
//---------------------------------------------------------------------------*/


static inline void local_rmw_ack(struct rmw_local_entry *loc_entry)
{
  loc_entry->rmw_reps.tot_replies = 1;
  loc_entry->rmw_reps.acks = 1;
}


// Returns true if it's valid to pull a request for that session
static inline bool pull_request_from_this_session(struct pending_ops *p_ops, uint16_t sess_id,
                                                  uint16_t t_id)
{
  uint32_t pull_ptr = interface[t_id].wrkr_pull_ptr[sess_id];
  if (ENABLE_ASSERTIONS) {
    assert(sess_id < SESSIONS_PER_THREAD);
    if (ENABLE_CLIENTS) {
      assert(pull_ptr < PER_SESSION_REQ_NUM);
    }
  }
  if (ENABLE_CLIENTS)
    return (!p_ops->sess_info[sess_id].stalled) && is_client_req_active(sess_id, pull_ptr, t_id);
  else
    return (!p_ops->sess_info[sess_id].stalled);
}

// Increment the per-request counters
static inline void increment_per_req_counters(uint8_t opcode, uint16_t t_id)
{
  if (ENABLE_STAT_COUNTING) {
    if (opcode == KVS_OP_PUT) t_stats[t_id].writes_per_thread++;
    else if (opcode == KVS_OP_GET) t_stats[t_id].reads_per_thread++;
    else if (opcode == OP_ACQUIRE) t_stats[t_id].acquires_per_thread++;
    else if (opcode == OP_RELEASE) t_stats[t_id].releases_per_thread++;
    else  t_stats[t_id].rmws_completed++;
  }
}

// In case of a miss in the KVS clean up the op, sessions and what not
static inline void clean_up_on_KVS_miss(struct trace_op *op, struct pending_ops *p_ops,
                                        struct latency_flags *latency_info, uint16_t t_id)
{
  if (op->opcode == OP_RELEASE || op->opcode == OP_ACQUIRE) {
    uint16_t session_id = op->session_id;
    my_printf(yellow, "Cache_miss, session %u \n", session_id);
    if (ENABLE_ASSERTIONS) assert(session_id < SESSIONS_PER_THREAD);
    p_ops->sess_info[session_id].stalled = false;
    p_ops->all_sessions_stalled = false;
    signal_completion_to_client(op->session_id, op->index_to_req_array, t_id);
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
  if (ENABLE_ASSERTIONS) assert(glob_sess_id < GLOBAL_SESSION_NUM);
  tmp_rmw_id = committed_glob_sess_rmw_id[glob_sess_id];
  do {
    if (ENABLE_ASSERTIONS) {
      debug_cntr++;
      if (debug_cntr > 100) {
        my_printf(red, "stuck on registering glob sess id %u \n", debug_cntr);
        debug_cntr = 0;
      }
    }
    if (rmw_id <= tmp_rmw_id) return;
  } while (!atomic_compare_exchange_strong(&committed_glob_sess_rmw_id[glob_sess_id], &tmp_rmw_id, rmw_id));
  MY_ASSERT(rmw_id <= committed_glob_sess_rmw_id[glob_sess_id], "After registering: rmw_id/registered %u/%u glob sess_id %u \n",
            rmw_id, committed_glob_sess_rmw_id[glob_sess_id], glob_sess_id);
}


// Fill a write message with a commit
static inline void fill_commit_message_from_l_entry(struct commit *com, struct rmw_local_entry *loc_entry,
                                                    uint8_t broadcast_state, uint16_t t_id)
{
  com->base_ts.m_id = loc_entry->base_ts.m_id;
  com->base_ts.version = loc_entry->base_ts.version;
  memcpy(&com->key, &loc_entry->key, TRUE_KEY_SIZE);
  com->opcode = COMMIT_OP;
  com->t_rmw_id = loc_entry->rmw_id.id;
  com->glob_sess_id = loc_entry->rmw_id.glob_sess_id;
  com->log_no = loc_entry->log_no;


  if (broadcast_state == MUST_BCAST_COMMITS && !loc_entry->rmw_is_successful) {
    memcpy(com->value, loc_entry->value_to_read, (size_t) RMW_VALUE_SIZE);
  }
  else {
    memcpy(com->value, loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
  }
  if (ENABLE_ASSERTIONS) {
    assert(com->t_rmw_id < B_4);
    assert(com->log_no > 0);
    assert(com->t_rmw_id > 0);
  }
}

// Fill a write message with a commit from read info, after an rmw acquire
static inline void fill_commit_message_from_r_info(struct commit *com,
                                                   struct read_info* r_info, uint16_t t_id)
{
  com->base_ts.m_id = r_info->ts_to_read.m_id;
  com->base_ts.version = r_info->ts_to_read.version;
  memcpy(&com->key, &r_info->key, TRUE_KEY_SIZE);
  com->opcode = RMW_ACQ_COMMIT_OP;
  memcpy(com->value, r_info->value, r_info->val_len);
  com->t_rmw_id = r_info->rmw_id.id;
  com->glob_sess_id = r_info->rmw_id.glob_sess_id;
  com->log_no = r_info->log_no;
  if (ENABLE_ASSERTIONS) {
    assert(com->log_no > 0);
    assert(com->t_rmw_id > 0);
  }
}

// calcualte how many reps an accept is waiting for
static inline uint8_t calculate_required_reps(struct rmw_local_entry *loc_entry)
{
  uint8_t remote_quorum = (uint8_t) (loc_entry->all_aboard ?
                                     MACHINE_NUM - 1 : REMOTE_QUORUM);
  return loc_entry->rmw_reps.nacks ? (uint8_t) 1 : remote_quorum;
}


/* --------------------SESSION INFO---------------------------------- */

static inline void add_request_to_sess_info(struct sess_info *sess_info, uint16_t t_id)
{
  sess_info->live_writes++;
  sess_info->ready_to_release = false;
}

static inline void check_sess_info_after_completing_release
  (struct sess_info *sess_info, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(sess_info->stalled);
    //assert(sess_info->ready_to_release);
    //assert(sess_info->live_writes == 0);
  }
}


//
static inline void update_sess_info_missing_ids_when_sending
  (struct pending_ops *p_ops, struct w_mes_info *info,
   struct quorum_info *q_info, uint8_t w_i, uint16_t t_id)
{
  if (q_info->missing_num == 0 ) return;
  struct sess_info *sess_info = &p_ops->sess_info[info->per_message_sess_id[w_i]];

  //printf("qinfo missing num %u, sess_info->missing_num %u \n");
  for (uint8_t i = 0; i < q_info->missing_num; i++) {
    bool found = false;
    for (uint8_t j = 0; j < sess_info->missing_num; j++) {
      if (q_info->missing_ids[i] == sess_info->missing_ids[j]) found = true;
    }
    if (!found) {
      sess_info->missing_ids[sess_info->missing_num] = q_info->missing_ids[i];
      sess_info->missing_num++;
    }
  }
  //printf("after update \n");
}


static inline void update_sess_info_with_fully_acked_write(struct pending_ops *p_ops,
                                                           uint32_t w_ptr, uint16_t t_id)
{
  struct sess_info *sess_info = &p_ops->sess_info[p_ops->w_meta[w_ptr].sess_id];
  // The write gathered all expected acks so it needs not update the missing num or ids of the sess info
  if (ENABLE_ASSERTIONS) assert(sess_info->live_writes > 0);
  sess_info->live_writes--;
//  printf("Removing a fully acked %u \n", sess_info->live_writes);

  //my_printf(green, "increasing live writes %u \n", sess_info->live_writes);
  if (sess_info->live_writes == 0) {
    sess_info->ready_to_release = true;
  }
}

static inline void update_sess_info_partially_acked_write(struct pending_ops *p_ops,
                                                          uint32_t w_ptr, uint16_t t_id)
{
  struct sess_info *sess_info = &p_ops->sess_info[p_ops->w_meta[w_ptr].sess_id];
  struct per_write_meta *w_meta = &p_ops->w_meta[w_ptr];

  // for each missing id
  if (ENABLE_ASSERTIONS) {
    check_state_with_allowed_flags(4, w_meta->w_state, READY_PUT, READY_RELEASE, READY_COMMIT);
    assert(w_meta->acks_seen >= REMOTE_QUORUM);
    assert(w_meta->acks_seen < w_meta->acks_expected);
    uint8_t dbg = 0;
    for (uint8_t j = 0; j < w_meta->acks_expected; j++) {
      if (!w_meta->seen_expected[j]) dbg++;
    }
    if (w_meta->acks_expected - w_meta->acks_seen != dbg){
      printf("Acks expected %u, acks_seen %u dbg %u \n",
             w_meta->acks_expected, w_meta->acks_seen, dbg);
      for (uint8_t j = 0; j < w_meta->acks_expected; j++) {
        printf("seen expected %u, %d \n", j, w_meta->seen_expected[j]);
      }
      //assert(false);
    }
  }
  uint8_t missing_id_num = w_meta->acks_expected - w_meta->acks_seen;
//  printf("Wrkr %u, write at ptr %u, state %u , expected acks %u seen acks %u \n",
//          t_id, w_ptr, w_meta->w_state, w_meta->acks_expected, w_meta->acks_seen);
  uint8_t expected_id_pos = 0;
  for (uint8_t i = 0; i < missing_id_num; i++) {
    // find the id
    uint8_t missing_id = MACHINE_NUM;
    for (uint8_t j = expected_id_pos; j < w_meta->acks_expected; j++) {
      if (!w_meta->seen_expected[j]) {
        missing_id = w_meta->expected_ids[j];
        //my_printf(yellow, "Write missed an ack from %u \n", missing_id);
        expected_id_pos = (uint8_t) (j + 1);
        break;
      }
    }
    if (ENABLE_ASSERTIONS) assert(missing_id < MACHINE_NUM);
    // having found which machine did not ack try to add it to the sess_info
    bool found_same_id = false;
    for (uint8_t j = 0; j < sess_info->missing_num; j++) {
      if (sess_info->missing_ids[j] == missing_id)
        found_same_id = true;
    }

    if (!found_same_id) {
      sess_info->missing_ids[sess_info->missing_num] = missing_id;
      sess_info->missing_num++;
    }
  }

  //
  if (ENABLE_ASSERTIONS) assert(sess_info->live_writes > 0);
  sess_info->live_writes--;
//  printf("Removing a partially acked %u \n", sess_info->live_writes);
  if (sess_info->live_writes == 0) {
    //printf("Wrkr %u last w_ptr %u , current w_ptr %u\n", t_id, sess_info->last_w_ptr, w_ptr);
    sess_info->ready_to_release = true;
  }
}


static inline void reset_sess_info_on_release(struct sess_info *sess_info,
                                              struct quorum_info *q_info, uint16_t t_id)
{
  sess_info->missing_num = q_info->missing_num;
  memcpy(sess_info->missing_ids, q_info->missing_ids, q_info->missing_num);
  if (ENABLE_ASSERTIONS) {
    assert(sess_info->stalled);
    assert(sess_info->live_writes == 0);
  }
  add_request_to_sess_info(sess_info, t_id);
}

static inline void reset_sess_info_on_accept(struct sess_info *sess_info,
                                             uint16_t t_id)
{
  sess_info->missing_num = 0;
  if (ENABLE_ASSERTIONS) {
    assert(sess_info->stalled);
    assert(sess_info->live_writes == 0);
    assert(sess_info->ready_to_release);
  }
}


/* ----------------------------------------------------------------- */

// returns the number of failures
static inline uint8_t create_bit_vec_of_failures(struct pending_ops *p_ops, struct w_message *w_mes,
                                                 struct w_mes_info *info, struct quorum_info *q_info,
                                                 uint8_t *bit_vector_to_send, uint16_t t_id)
{
  bool bit_vec[MACHINE_NUM] = {0};
  uint8_t failed_machine_num = 0 ;
  // Then look at each release in the message sess_info
  for (uint8_t w_i = 0; w_i < w_mes->coalesce_num; w_i++) {
    if (!info->per_message_release_flag[w_i]) continue;
    if (ENABLE_ASSERTIONS) assert(info->per_message_sess_id[w_i] <= SESSIONS_PER_THREAD);
    struct sess_info *sess_info = &p_ops->sess_info[info->per_message_sess_id[w_i]];
    for (uint8_t j = 0; j < sess_info->missing_num; j++) {
      if (!bit_vec[sess_info->missing_ids[j]]) {
        bit_vec[sess_info->missing_ids[j]] = true;
        set_conf_bit_after_detecting_failure(sess_info->missing_ids[j], t_id);
        failed_machine_num++;
      }
    }
  }
  if (ENABLE_ASSERTIONS) assert(failed_machine_num < MACHINE_NUM);
  if (failed_machine_num == 0) return failed_machine_num;

  {
    uint64_t bit_vect = 0;
    for (uint16_t i = 0; i < MACHINE_NUM; i++) {
      if (i == machine_id) continue;
      if (bit_vec[i])
        bit_vect = bit_vect | machine_bit_id[i];
    }
    if (ENABLE_ASSERTIONS) assert(bit_vect > 0);
    memcpy(bit_vector_to_send, (void *) &bit_vect, SEND_CONF_VEC_SIZE);
  }
  return failed_machine_num;
}


// When forging a write
static inline bool add_failure_to_release_from_sess_id
  (struct pending_ops *p_ops, struct w_message *w_mes,
   struct w_mes_info *info, struct quorum_info *q_info,
   uint32_t backward_ptr, uint16_t t_id)
{
  struct write *write = (struct write *) (((void *)w_mes) + info->first_release_byte_ptr);
  bool is_release = write->opcode == OP_RELEASE;
  bool is_accept = write->opcode == ACCEPT_OP;
  //printf("opcode %u \n", write->opcode);
  if (ENABLE_ASSERTIONS) assert(is_release || is_accept);

  uint8_t bit_vector_to_send[SEND_CONF_VEC_SIZE] = {0};
  // Find all machine ids that need to be included in the message
  // Do not include the machines the release will not be sent to
  uint8_t failed_machine_num = create_bit_vec_of_failures(p_ops, w_mes, info, q_info,
                                                          bit_vector_to_send, t_id);
  if (failed_machine_num == 0) return false;

  if (*(uint16_t *) bit_vector_to_send > 0) {
    uint8_t w_i = info->first_release_w_i;
    if (is_release) {
      backward_ptr = (backward_ptr + w_i) % PENDING_WRITES;
      // Save the overloaded bytes in some buffer, such that they can be used in the second round of the release
      memcpy(&p_ops->overwritten_values[SEND_CONF_VEC_SIZE * backward_ptr], write->value,
             SEND_CONF_VEC_SIZE);
      memcpy(write->value, bit_vector_to_send, SEND_CONF_VEC_SIZE);
      if (DEBUG_QUORUM)
        my_printf(green, "Wrkr %u Sending a release with a vector bit_vec %u \n", t_id,
                     *(uint16_t *) bit_vector_to_send);
      write->opcode = OP_RELEASE_BIT_VECTOR;
      p_ops->ptrs_to_local_w[backward_ptr] = write;
    }
    else if (is_accept) {
      assert(ACCEPT_IS_RELEASE);
      struct accept *acc = (struct accept *) write;
      // Overload the last 2 bytes of the rmw-id
      uint16_t *part_of_accept = (uint16_t *) (((void *)&acc->glob_sess_id) - SEND_CONF_VEC_SIZE);

      if (ENABLE_ASSERTIONS) {
        uint64_t rmw_id = *(uint64_t *) (((void *) &acc->glob_sess_id) - 8);
        assert(acc->t_rmw_id == rmw_id);
        if ((*part_of_accept) != 0) {
          printf("rmw_id %lu\n", acc->t_rmw_id);
          assert(false);
        }
        assert(info->per_message_release_flag[w_i]);
      }

      memcpy(part_of_accept, bit_vector_to_send, SEND_CONF_VEC_SIZE);
      if (ENABLE_ASSERTIONS) assert(*part_of_accept != 0);
      //if (t_id == 0)
      //printf("Wrkr %u sending an accept bit vector %u \n",
       //                     t_id, *part_of_accept);
      acc->opcode = ACCEPT_OP_BIT_VECTOR;
      //struct sess_info *sess_info = &p_ops->sess_info[info->per_message_sess_id[w_i]];
      //reset_sess_info_on_accept(sess_info, t_id);
    }
    else if (ENABLE_ASSERTIONS) assert(false);
    //if (DEBUG_SESSIONS)
    //  my_printf(cyan, "Wrkr %u release is from session %u, session has pending op: %u\n",
    //             t_id, p_ops->w_session_id[backward_ptr],
    //             p_ops->session_has_pending_op[p_ops->w_session_id[backward_ptr]]);
    return true;
  }
  if (ENABLE_ASSERTIONS) assert(false);
  return false;
}

// Returns the size of a write request given an opcode -- Accepts, commits, writes, releases
static inline uint16_t get_write_size_from_opcode(uint8_t opcode) {
  check_state_with_allowed_flags(11, opcode, OP_RELEASE, KVS_OP_PUT, ACCEPT_OP,
                                 ACCEPT_OP_BIT_VECTOR,
                                 COMMIT_OP, RMW_ACQ_COMMIT_OP, OP_RELEASE_BIT_VECTOR,
                                 OP_RELEASE_SECOND_ROUND, OP_ACQUIRE, NO_OP_RELEASE);
  switch(opcode) {
    case OP_RELEASE:
    case OP_ACQUIRE:
    case KVS_OP_PUT:
    case OP_RELEASE_BIT_VECTOR:
    case OP_RELEASE_SECOND_ROUND:
    case NO_OP_RELEASE:
      return W_SIZE;
    case ACCEPT_OP:
    case ACCEPT_OP_BIT_VECTOR:
      return ACCEPT_SIZE;
    case COMMIT_OP:
    case RMW_ACQ_COMMIT_OP:
      return COMMIT_SIZE;
    default: if (ENABLE_ASSERTIONS) assert(false);
  }
}


// When forging a write
static inline void set_w_state_for_each_write(struct pending_ops *p_ops, struct w_mes_info *info,
                                              struct w_message *w_mes, uint32_t backward_ptr,
                                              uint8_t coalesce_num, struct ibv_sge *send_sgl,
                                              uint16_t br_i, struct quorum_info *q_info, uint16_t t_id)
{
  uint16_t byte_ptr = W_MES_HEADER;
  bool failure = false;

  if (!EMULATE_ABD && info->is_release ) {//&& send_bit_vector.state == DOWN_STABLE)) {
    if (add_failure_to_release_from_sess_id(p_ops, w_mes, info, q_info, backward_ptr, t_id))
      failure = true;
  }
  for (uint8_t i = 0; i < coalesce_num; i++) {
    struct write *write = (struct write *)(((void *)w_mes) + byte_ptr);
    //printf("Write %u/%u opcode %u \n", i, coalesce_num, write->opcode);
    byte_ptr += get_write_size_from_opcode(write->opcode);

    struct per_write_meta *w_meta = &p_ops->w_meta[backward_ptr];
    uint8_t *w_state = &w_meta->w_state;

    struct sess_info *sess_info = &p_ops->sess_info[info->per_message_sess_id[i]];
    switch (write->opcode) {
      case ACCEPT_OP:
      case ACCEPT_OP_BIT_VECTOR:
        if (ACCEPT_IS_RELEASE) reset_sess_info_on_accept(sess_info, t_id);
        checks_when_forging_an_accept((struct accept *) write, send_sgl, br_i, i, coalesce_num, t_id);
        // accept gets a custom response from r_rep and need not set the w_state
        break;
      case KVS_OP_PUT:
        checks_when_forging_a_write(write, send_sgl, br_i, i, coalesce_num, t_id);
        update_sess_info_missing_ids_when_sending(p_ops, info, q_info, i, t_id);
        w_meta->acks_expected = q_info->active_num;
        *w_state = SENT_PUT;
        break;
      case COMMIT_OP:
        checks_when_forging_a_commit((struct commit*) write, send_sgl, br_i, i, coalesce_num, t_id);
        update_sess_info_missing_ids_when_sending(p_ops, info, q_info, i, t_id);
        w_meta->acks_expected = q_info->active_num;
        *w_state = SENT_COMMIT;
        break;
      case RMW_ACQ_COMMIT_OP:
        *w_state = SENT_RMW_ACQ_COMMIT;
        write->opcode = COMMIT_OP;
        w_meta->acks_expected = (uint8_t) REMOTE_QUORUM;
        break;
      case OP_RELEASE_BIT_VECTOR:
        w_meta->acks_expected = (uint8_t) REMOTE_QUORUM;
        checks_when_forging_a_write(write, send_sgl, br_i, i, coalesce_num, t_id);
        *w_state = SENT_BIT_VECTOR;
        break;
      case OP_RELEASE:
        if (failure) {
          write->opcode = NO_OP_RELEASE;
          //struct write *first_rel = (((write *)w_mes) + info->first_release_byte_ptr);
          //my_printf(yellow, "Wrkr %u Adding a no_op_release in position %u/%u, first opcode %u \n",
          //              t_id, i, coalesce_num, first_rel->opcode);
          *w_state = SENT_NO_OP_RELEASE;
          p_ops->ptrs_to_local_w[backward_ptr] = write;
          w_meta->acks_expected = (uint8_t) REMOTE_QUORUM;
          break;
        }
        // NO break here -- merge with actions of OP_RELEASE_SECOND_ROUND
      case OP_RELEASE_SECOND_ROUND:
        write->opcode = OP_RELEASE;
        reset_sess_info_on_release(sess_info, q_info, t_id);
        KVS_isolated_op(t_id, write);
        w_meta->acks_expected = q_info->active_num;
        checks_when_forging_a_write(write, send_sgl, br_i, i, coalesce_num, t_id);
        *w_state = SENT_RELEASE;
        break;
      case OP_ACQUIRE:
        checks_when_forging_a_write(write, send_sgl, br_i, i, coalesce_num, t_id);
        *w_state = SENT_ACQUIRE;
        w_meta->acks_expected = (uint8_t) REMOTE_QUORUM;
        break;
      default: if (ENABLE_ASSERTIONS) assert(false);
    }
    if (ENABLE_ASSERTIONS) (w_meta->acks_expected >= REMOTE_QUORUM);
//    if (write->opcode != ACCEPT_OP && t_id == 1)
//      my_printf(yellow, "Wrkr %u Setting state %u ptr %u/%d opcode %u message %u/%u \n",
//                  t_id, *w_state, backward_ptr, PENDING_WRITES, write->opcode,
//                  i, coalesce_num);
    if (write->opcode != ACCEPT_OP && write->opcode != ACCEPT_OP_BIT_VECTOR) {
      for (uint8_t m_i = 0; m_i < q_info->active_num; m_i++)
        w_meta->expected_ids[m_i] = q_info->active_ids[m_i];

      MOD_ADD(backward_ptr, PENDING_WRITES);
    }


  }
}

static inline uint16_t get_w_sess_id(struct pending_ops *p_ops, struct trace_op *op,
                                     const uint8_t source,
                                     const uint32_t incoming_pull_ptr,
                                     const uint16_t t_id)
{
  struct rmw_local_entry *loc_entry = (struct rmw_local_entry *) op;

  switch (source) {
    case FROM_COMMIT:
      return loc_entry->sess_id;
      // source = FROM_READ: 2nd round of Acquires/Releases, 2nd round of out-of-epoch Writes
      // This also includes Commits triggered by RMW-Acquires
    case FROM_READ:
      return (uint16_t) p_ops->r_session_id[incoming_pull_ptr];
    case FROM_TRACE:
    case RELEASE_THIRD: //source = FROM_WRITE || LIN_WRITE
      if (ENABLE_ASSERTIONS) {
        assert(op != NULL);
        uint16_t session_id = op->session_id;
        assert(session_id == *(uint16_t *) op);
        assert(session_id < SESSIONS_PER_THREAD);
        if (source == RELEASE_THIRD) {
          struct write *w = (struct write *) &op->ts;
          check_state_with_allowed_flags(3, w->opcode, OP_RELEASE_BIT_VECTOR, NO_OP_RELEASE);
        }
      }
      return op->session_id;
    default: if (ENABLE_ASSERTIONS) assert(false);
  }
}

// When inserting a write
static inline void
set_w_sess_info_and_index_to_req_array(struct pending_ops *p_ops, struct trace_op *write,
                                       const uint8_t source, uint32_t w_ptr,
                                       const uint32_t incoming_pull_ptr,
                                       uint8_t opcode, uint16_t sess_id, const uint16_t t_id)
{
  p_ops->w_meta[w_ptr].sess_id = sess_id;
  switch (source) {
    case FROM_TRACE:
      if (ENABLE_CLIENTS) {
        p_ops->w_index_to_req_array[w_ptr] = write->index_to_req_array;
      }
      return;
    case FROM_READ:
      if (ENABLE_CLIENTS) {
        p_ops->w_index_to_req_array[w_ptr] = p_ops->r_index_to_req_array[incoming_pull_ptr];
      }
      return;
    case FROM_COMMIT:
      add_request_to_sess_info(&p_ops->sess_info[sess_id], t_id);
      return;
    case RELEASE_THIRD: //source = FROM_WRITE || LIN_WRITE
      p_ops->w_index_to_req_array[w_ptr] = p_ops->w_index_to_req_array[incoming_pull_ptr];
      return;
    default:
      if (ENABLE_ASSERTIONS) assert(false);
  }
}


// Set up the message depending on where it comes from: trace, 2nd round of release, 2nd round of read etc.
static inline void write_bookkeeping_in_insertion_based_on_source
  (struct pending_ops *p_ops, struct write *write, struct trace_op *op,
   const uint8_t source, const uint32_t incoming_pull_ptr,
   struct read_info *r_info, const uint16_t t_id)
{
  my_assert(source <= FROM_COMMIT, "When inserting a write source is too high. Have you enabled lin writes?");

  if (source == FROM_TRACE) {
    write->version = op->ts.version;
    write->key = op->key;
    write->opcode = op->opcode;
    write->val_len = op->val_len;
    //memcpy(&write->version, (void *) &op->ts.version, 4 + TRUE_KEY_SIZE + 2);
    if (ENABLE_ASSERTIONS) assert(op->real_val_len <= VALUE_SIZE);
    memcpy(write->value, op->value_to_write, op->real_val_len);
    write->m_id = (uint8_t) machine_id;
  }
  else if (source == RELEASE_THIRD) { // Second round of a release
    struct write *tmp = (struct write *) &op->ts; // we have treated the rest as a struct write
    memcpy(&write->m_id, tmp, W_SIZE);
    write->opcode = OP_RELEASE_SECOND_ROUND;
    //if (DEBUG_SESSIONS)
    // my_printf(cyan, "Wrkr %u: Changing the opcode from %u to %u of write %u of w_mes %u \n",
    //             t_id, op->opcode, write->opcode, inside_w_ptr, w_mes_ptr);
    if (ENABLE_ASSERTIONS) assert (write->m_id == (uint8_t) machine_id);
    if (DEBUG_QUORUM) {
      printf("Thread %u: Second round release, from ptr: %u to ptr %u, key: ", t_id, incoming_pull_ptr, p_ops->w_push_ptr);
      print_true_key(&write->key);
    }
  }
  else if (source == FROM_COMMIT || (source == FROM_READ && r_info->is_rmw)) {

    if (source == FROM_READ)
      fill_commit_message_from_r_info((struct commit *) write, r_info, t_id);
    else {
      uint8_t broadcast_state = (uint8_t)incoming_pull_ptr;
      fill_commit_message_from_l_entry((struct commit *) write,
                                       (struct rmw_local_entry *) op, broadcast_state,  t_id);
    }
  }
  else { //source = FROM_READ: 2nd round of read/write/acquire/release
    write->m_id = r_info->ts_to_read.m_id;
    write->version = r_info->ts_to_read.version;
    write->key = r_info->key;
    memcpy(write->value, r_info->value, r_info->val_len);
    write->opcode = r_info->opcode;
    write->val_len = VALUE_SIZE >> SHIFT_BITS;
    if (ENABLE_ASSERTIONS) {
      assert(!r_info->is_rmw);
      assert(source == FROM_READ);
      check_state_with_allowed_flags(4, r_info->opcode, KVS_OP_PUT, OP_RELEASE, OP_ACQUIRE);
    }
  }
  // Make sure the pointed values are correct
}

static inline void increas_virt_w_size(struct pending_ops *p_ops, struct write *write,
                                       uint8_t source, uint16_t t_id) {
  if (write->opcode == OP_RELEASE) {
    if (ENABLE_ASSERTIONS) assert(source == FROM_READ);
    p_ops->virt_w_size += 2;
    //my_printf(yellow, "+2 %u at %u \n",  p_ops->virt_w_size, p_ops->w_push_ptr);
  } else {
    //my_printf(yellow, "Increasing virt_w_size %u at %u, source %u \n",
    //              p_ops->virt_w_size, p_ops->w_push_ptr, source);
    p_ops->virt_w_size++;
  }

  if (ENABLE_ASSERTIONS) {
    if (p_ops->virt_w_size > MAX_ALLOWED_W_SIZE + 1)
      my_printf(red, "Wrkr %u Virt_w_size %u/%d, source %u, write->opcode %u \n",
                 t_id, p_ops->virt_w_size, MAX_ALLOWED_W_SIZE, source, write->opcode);
    assert(p_ops->w_size <= MAX_ALLOWED_W_SIZE);
    assert(p_ops->w_size <= p_ops->virt_w_size);
  }
}
// When committing reads
static inline void set_flags_before_committing_a_read(struct read_info *read_info,
                                                      bool *acq_second_round_to_flip_bit, bool *insert_write_flag,
                                                      bool *write_local_kvs, bool *insert_commit_flag,
                                                      bool *signal_completion, bool *signal_completion_after_kvs_write,
                                                      uint16_t t_id)
{

  bool acq_needs_second_round = (!read_info->seen_larger_ts && read_info->times_seen_ts < REMOTE_QUORUM) ||
                               (read_info->seen_larger_ts && read_info->times_seen_ts <= REMOTE_QUORUM);

  (*insert_commit_flag) = read_info->is_rmw && acq_needs_second_round;

  (*insert_write_flag) = (read_info->opcode != KVS_OP_GET)  && !read_info->is_rmw &&
                         (read_info->opcode == OP_RELEASE ||
                          read_info->opcode == KVS_OP_PUT || acq_needs_second_round);


  (*write_local_kvs) = (read_info->opcode != OP_RELEASE) &&
                       (read_info->seen_larger_ts ||
                       (read_info->opcode == KVS_OP_GET) || // a simple read is quorum only if the kvs epoch is behind..
                       (read_info->opcode == KVS_OP_PUT));  // out-of-epoch write

  (*acq_second_round_to_flip_bit) = read_info->fp_detected;


  (*signal_completion) = (read_info->opcode == OP_ACQUIRE) && !(*insert_write_flag) &&
                         !(*insert_commit_flag) && !(*write_local_kvs);

  //all requests that will not be early signaled except: releases and acquires that actually have a second round
  //That leaves: out-of-epoch writes/reads & acquires that want to write the KVS
  (*signal_completion_after_kvs_write) = !(*signal_completion) &&
                                         !((read_info->opcode == OP_ACQUIRE && acq_needs_second_round) ||
                                           (read_info->opcode == OP_RELEASE) || (read_info->opcode == OP_ACQUIRE_FLIP_BIT) );

}


// In case of an out-of-epoch write that found a bigger TS --NOT NEEDED
static inline void rectify_version_of_w_mes(struct pending_ops *p_ops, struct read_info *r_info,
                                             uint32_t tmp_version, uint16_t t_id)
{
//  if (ENABLE_ASSERTIONS) {
//    my_printf(red, "Worker: %u, KVS has bigger version %u than read-info %u -> w_message must "
//               "be rectified in position w_mes: %u, inside_ptr: %u\n",
//               t_id, r_info->ts_to_read.version, tmp_version,
//               r_info->w_mes_ptr, r_info->inside_w_ptr);
//    assert(p_ops->w_fifo->w_message[r_info->w_mes_ptr].write[r_info->inside_w_ptr].version == tmp_version);
//  }
//  memcpy(p_ops->w_fifo->w_message[r_info->w_mes_ptr].write[r_info->inside_w_ptr].version, r_info->ts_to_read.version, 4);
}

// returns true if the key was found
static inline bool search_out_of_epoch_writes(struct pending_ops *p_ops,
                                              struct key *read_key,
                                              uint16_t t_id, void **val_ptr)
{
  struct pending_out_of_epoch_writes *writes = p_ops->p_ooe_writes;
  uint32_t w_i = writes->pull_ptr;
  for (uint32_t i = 0; i < writes->size; i++) {
    if (keys_are_equal(&p_ops->read_info[writes->r_info_ptrs[w_i]].key, read_key)) {
      *val_ptr = (void*) p_ops->read_info[writes->r_info_ptrs[w_i]].value;
      //my_printf(red, "Wrkr %u: Forwarding value from out-of-epoch write, read key: ", t_id);
      //print_true_key(read_key);
      //my_printf(red, "write key: "); print_true_key((struct key*)p_ops->read_info[writes->r_info_ptrs[w_i]].key);
      //my_printf(red, "size: %u, push_ptr %u, pull_ptr %u, r_info ptr %u \n",
      //          writes->size, writes->push_ptr, writes->pull_ptr, writes->r_info_ptrs[w_i]);
      return true;
    }
    MOD_ADD(w_i, PENDING_READS);
  }
  return false;
}

//fill the reply entry with last_committed RMW-id, TS, value and log number
static inline void fill_reply_entry_with_committed_RMW (mica_op_t *kv_ptr,
                                                        struct rmw_rep_last_committed *rep,
                                                        uint16_t t_id)
{
  rep->ts.m_id = kv_ptr->ts.m_id; // Here we reply with the base TS
  rep->ts.version = kv_ptr->ts.version;
  memcpy(rep->value, kv_ptr->value, (size_t) RMW_VALUE_SIZE);
  rep->log_no_or_base_version = kv_ptr->last_committed_log_no;
  rep->rmw_id = kv_ptr->last_committed_rmw_id.id;
  rep->glob_sess_id = kv_ptr->last_committed_rmw_id.glob_sess_id;
  //if (rep->ts.version == 0)
  //  my_printf(yellow, "Wrkr %u replies with flag %u Log_no %u, rmw_id %lu glob_sess id %u\n",
  //         t_id, rep->opcode, rep->log_no, rep->rmw_id, rep->glob_sess_id);
}


// Check the global RMW-id structure, to see if an RMW has already been committed
static inline bool the_rmw_has_committed(uint16_t glob_sess_id, uint64_t rmw_l_id,
                                         bool same_log, uint16_t t_id,
                                         struct rmw_rep_last_committed *rep)
{
  if (ENABLE_ASSERTIONS) assert(glob_sess_id < GLOBAL_SESSION_NUM);
  if (committed_glob_sess_rmw_id[glob_sess_id] >= rmw_l_id) {
    if (DEBUG_RMW)
      my_printf(green, "Worker %u: A Remote machine  is trying a propose with global sess_id %u, "
                     "rmw_id %lu, that has been already committed \n",
                   t_id, glob_sess_id, rmw_l_id);
    rep->opcode = (uint8_t) (same_log ? RMW_ID_COMMITTED_SAME_LOG : RMW_ID_COMMITTED);
    return true;
  }
  else return false;
}




// Returns true if the received log-no is smaller than the committed.
static inline bool is_log_smaller_or_has_rmw_committed(uint32_t log_no, mica_op_t *kv_ptr,
                                                       uint64_t rmw_l_id,
                                                       uint16_t glob_sess_id, uint16_t t_id,
                                                       struct rmw_rep_last_committed *rep)
{
  check_log_nos_of_kv_ptr(kv_ptr, "is_log_smaller_or_has_rmw_committed", t_id);
  // bool fill_the_rep = false;
  bool same_log = kv_ptr->last_committed_log_no == log_no;
  if (the_rmw_has_committed(glob_sess_id, rmw_l_id, same_log, t_id, rep))
    return true;
  else if (kv_ptr->last_committed_log_no >= log_no ||
           kv_ptr->log_no > log_no) {
    if (DEBUG_RMW)
      my_printf(yellow, "Wkrk %u Log number is too small %u/%u entry state %u, propose/accept with rmw_lid %u,"
                      " global_sess_id %u\n", t_id, log_no, kv_ptr->last_committed_log_no,
                    kv_ptr->state, rmw_l_id, glob_sess_id);
    rep->opcode = LOG_TOO_SMALL;
    fill_reply_entry_with_committed_RMW (kv_ptr, rep, t_id);
    return true;
    //fill_the_rep = true;
  }
  else if (DEBUG_RMW) { // remote log is higher than the locally stored!
    if (kv_ptr->log_no < log_no )
      my_printf(yellow, "Wkrk %u Log number is higher than expected %u/%u, entry state %u, "
                      "propose/accept with rmw_lid %u, global_sess_id %u \n",
                    t_id, log_no, kv_ptr->log_no,
                    kv_ptr->state, rmw_l_id, glob_sess_id);
  }
  // If either the log is too small or the rmw_id has been committed,
  // store the committed value, TS & log_number to the reply
//  if (fill_the_rep) {
//    fill_reply_entry_with_committed_RMW (kv_ptr, rep, t_id);
//    return true;
//  }
  return false;
}

// Returns true if the received log is higher than the last committed log no + 1
static inline bool is_log_too_high(uint32_t log_no, mica_op_t *kv_ptr,
                                   uint16_t t_id,
                                   struct rmw_rep_last_committed *rep)
{
  check_log_nos_of_kv_ptr(kv_ptr, "is_log_too_high", t_id);
  // If the request is for the working log_no, it does not have to equal committed + 1
  // because we may have received an accept for log 10, w/o having committed log 9,
  // then it's okay to process the propose for log 10
  if (log_no > kv_ptr->log_no &&
    log_no > kv_ptr->last_committed_log_no + 1) {
    if (DEBUG_RMW)
      my_printf(yellow, "Wkrk %u Log number is too high %u/%u entry state %u \n",
                    t_id, log_no, kv_ptr->last_committed_log_no,
                    kv_ptr->state);
    rep->opcode = LOG_TOO_HIGH;
    return true;
  }
  else if (log_no > kv_ptr->last_committed_log_no + 1) {
    if (ENABLE_ASSERTIONS) {
      assert(log_no == kv_ptr->log_no);
      if (log_no != kv_ptr->accepted_log_no)
        printf("log_no %u, kv_ptr accepted_log_no %u, kv_ptr log no %u, kv_ptr->state %u \n",
               log_no, kv_ptr->accepted_log_no, kv_ptr->log_no, kv_ptr->state);
      //assert(log_no == kv_ptr->accepted_log_no);
      //assert(kv_ptr->state == ACCEPTED);
    }
  }
  return false;
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
    my_printf(green, "Sending %u bcasts, total %lu \n", br_i, br_tx);

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


// If a local RMW managed to grab a kv_ptr, then it sets up its local entry
static inline void fill_loc_rmw_entry_on_grabbing_kv_ptr(struct pending_ops *p_ops,
                                                         struct rmw_local_entry *loc_entry,
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
  local_rmw_ack(loc_entry);
  loc_entry->state = state;
  loc_entry->epoch_id = (uint64_t) epoch_id;
  loc_entry->new_ts.version = version;
  loc_entry->new_ts.m_id = (uint8_t) machine_id;
}

// Check if the kv_ptr state that is blocking a local RMW is persisting
static inline bool kv_ptr_state_has_not_changed(mica_op_t *kv_ptr,
                                                struct rmw_help_entry *help_rmw)
{
  return kv_ptr->state == help_rmw->state &&
         rmw_ids_are_equal(&help_rmw->rmw_id, &kv_ptr->rmw_id) &&
         (compare_ts(&kv_ptr->prop_ts, &help_rmw->ts) == EQUAL);
}

// Check if the kv_ptr state that is blocking a local RMW is persisting
static inline bool kv_ptr_state_has_changed(mica_op_t *kv_ptr,
                                            struct rmw_help_entry *help_rmw)
{
  return kv_ptr->state != help_rmw->state ||
    (!rmw_ids_are_equal(&help_rmw->rmw_id, &kv_ptr->rmw_id)) ||
    (compare_ts(&kv_ptr->prop_ts, &help_rmw->ts) != EQUAL);
}

// Initialize a local  RMW entry on the first time it gets allocated
static inline void init_loc_entry(struct kvs_resp* resp, struct pending_ops* p_ops,
                                  struct trace_op *prop,
                                  uint16_t t_id, struct rmw_local_entry* loc_entry)
{
  loc_entry->opcode = prop->opcode;
  if (ENABLE_ASSERTIONS) assert(prop->real_val_len <= RMW_VALUE_SIZE);
  if (opcode_is_compare_rmw(prop->opcode) || prop->opcode == RMW_PLAIN_WRITE)
    memcpy(loc_entry->value_to_write, prop->value_to_write, prop->real_val_len);
  loc_entry->killable = prop->opcode == COMPARE_AND_SWAP_WEAK;
  if (opcode_is_compare_rmw(prop->opcode))
    loc_entry->compare_val = prop->value_to_read; //expected value
  else if (prop->opcode == FETCH_AND_ADD)
    loc_entry->compare_val = prop->value_to_write; // value to be added


  loc_entry->must_release = ACCEPT_IS_RELEASE != 0; // TODO That can be a programmer input
  loc_entry->fp_detected = false;
  loc_entry->rmw_val_len = prop->real_val_len;
  loc_entry->rmw_is_successful = false;
  loc_entry->all_aboard = false;
  loc_entry->all_aboard_time_out = 0;
  memcpy(&loc_entry->key, &prop->key, TRUE_KEY_SIZE);
  memset(&loc_entry->rmw_reps, 0, sizeof(struct rmw_rep_info));
  loc_entry->kv_ptr = resp->kv_ptr;
  if (ENABLE_ASSERTIONS) {
    loc_entry->help_loc_entry->kv_ptr = resp->kv_ptr;
  }
  // loc_entry->sess_id = prop->session_id;
  loc_entry->index_to_req_array = prop->index_to_req_array;
  //loc_entry->accept_acks = 0;
  //loc_entry->accept_replies = 0;
  loc_entry->back_off_cntr = 0;
  loc_entry->log_too_high_cntr = 0;
  loc_entry->helping_flag = NOT_HELPING;
  // Give it an RMW-id as soon as it has a local entry, because the RMW must happen eventually
  loc_entry->rmw_id.id = p_ops->prop_info->l_id;
  if (ENABLE_ASSERTIONS) assert(loc_entry->rmw_id.id < B_4);
  loc_entry->l_id = p_ops->prop_info->l_id;
  loc_entry->help_loc_entry->l_id = p_ops->prop_info->l_id;
  loc_entry->rmw_id.glob_sess_id = get_glob_sess_id((uint8_t) machine_id, t_id, prop->session_id);
  loc_entry->accepted_log_no = 0;
  //my_printf(yellow, "Init  RMW-id %u glob_sess_id %u \n", loc_entry->rmw_id.id, loc_entry->rmw_id.glob_sess_id);
  //loc_entry->help_loc_entry->log_no = 0;
  loc_entry->help_loc_entry->state = INVALID_RMW;
}

// The help_loc_entry is used when receiving an already committed reply or an already accepted
static inline void store_rmw_rep_to_help_loc_entry(struct rmw_local_entry* loc_entry,
                                                   struct rmw_rep_last_committed* prop_rep, uint16_t t_id)
{
  struct rmw_local_entry *help_loc_entry = loc_entry->help_loc_entry;
  if (ENABLE_ASSERTIONS) {
    if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED) {
      assert(help_loc_entry->new_ts.version > 0);
      assert(help_loc_entry->state == ACCEPTED);
    }
    assert(help_loc_entry->state == INVALID_RMW || help_loc_entry->state == ACCEPTED);
  }
  enum ts_compare ts_comp = compare_netw_ts_with_ts(&prop_rep->ts, &help_loc_entry->new_ts);
  // If i have locally accepted my own RMW for a higher TS,
  // then i can treat these replies as acks,
  // because their end result is asking me to broadcast accepts for me RMWs
//  if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED &&
//      help_loc_entry->state == INVALID_RMW &&
//      ts_comp != GREATER) {
//    //my_printf(green, "Locally accepted/incoming ts  %u,%u/%u,%u \n",
//    //       help_loc_entry->new_ts.version, help_loc_entry->new_ts.m_id,
//    //       prop_rep->ts.version, prop_rep->ts.m_id);
//    loc_entry->rmw_reps.already_accepted--;
//    loc_entry->rmw_reps.acks++;
//  }
//  else
  if (help_loc_entry->state == INVALID_RMW ||
      ts_comp == GREATER) {
    if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED)
      loc_entry->helping_flag = NOT_HELPING;
    assign_netw_ts_to_ts(&help_loc_entry->new_ts, &prop_rep->ts);
    help_loc_entry->base_ts.version = prop_rep->log_no_or_base_version;
    help_loc_entry->base_ts.m_id = prop_rep->base_m_id;
    help_loc_entry->log_no = loc_entry->log_no;
    help_loc_entry->state = ACCEPTED;
    help_loc_entry->rmw_id.id = prop_rep->rmw_id;
    if (ENABLE_ASSERTIONS) assert(help_loc_entry->rmw_id.id < B_4);
    help_loc_entry->rmw_id.glob_sess_id = prop_rep->glob_sess_id;
    memcpy(help_loc_entry->value_to_write, prop_rep->value, (size_t) RMW_VALUE_SIZE);
    help_loc_entry->key = loc_entry->key;
  }
}

static inline void zero_out_the_rmw_reply_loc_entry_metadata(struct rmw_local_entry* loc_entry)
{
  if (ENABLE_ASSERTIONS) { // make sure the loc_entry is correctly set-up
    if (loc_entry->help_loc_entry == NULL) {
      my_printf(red, "When Zeroing: The help_loc_ptr is NULL. The reason is typically that "
                   "help_loc_entry was passed to the function "
                   "instead of loc entry to check \n");
      assert(false);
    }
  }
  loc_entry->help_loc_entry->state = INVALID_RMW;
  memset(&loc_entry->rmw_reps, 0, sizeof(struct rmw_rep_info));
  loc_entry->back_off_cntr = 0;
  if (ENABLE_ALL_ABOARD) loc_entry->all_aboard_time_out = 0;
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

// free a session held by an RMW
static inline void free_session(struct pending_ops *p_ops, uint16_t sess_id, bool allow_paxos_log,
                                uint16_t t_id)
{
  struct rmw_local_entry *loc_entry = &p_ops->prop_info->entry[sess_id];
  if (ENABLE_ASSERTIONS) {
    assert(sess_id < SESSIONS_PER_THREAD);
    assert(loc_entry->state == INVALID_RMW);
    if(!p_ops->sess_info[sess_id].stalled) {
      my_printf(red, "Wrkr %u sess %u should be stalled \n", t_id, sess_id);
      assert(false);
    }
  }
  fill_req_array_when_after_rmw(loc_entry, t_id);
  if (VERIFY_PAXOS && allow_paxos_log) verify_paxos(loc_entry, t_id);
  signal_completion_to_client(sess_id, loc_entry->index_to_req_array, t_id);
  p_ops->sess_info[sess_id].stalled = false;
  p_ops->all_sessions_stalled = false;
}

//
static inline bool if_already_committed_bcast_commits(struct pending_ops *p_ops,
                                                      struct rmw_local_entry *loc_entry,
                                                      uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->rmw_id.glob_sess_id < GLOBAL_SESSION_NUM);
    assert(loc_entry->state != INVALID_RMW);
    assert(loc_entry == &p_ops->prop_info->entry[loc_entry->sess_id]);
    assert(loc_entry->helping_flag != HELPING);
  }
  if (loc_entry->rmw_id.id <= committed_glob_sess_rmw_id[loc_entry->rmw_id.glob_sess_id]) {
    //my_printf(yellow, "Wrkr %u, sess: %u Bcast rmws %u \n", t_id, loc_entry->sess_id);
    loc_entry->log_no = loc_entry->accepted_log_no;
    loc_entry->state = MUST_BCAST_COMMITS;
    return true;
  }
  return false;
}

// Write the committed log in the kv_ptr and the new value in the KVS
static inline void take_actions_to_commit_rmw(struct rmw_local_entry *loc_entry_to_commit,
                                              struct rmw_local_entry *loc_entry, // this may be different if helping
                                              uint16_t t_id)
{
  /*
  if (ENABLE_DEBUG_GLOBAL_ENTRY) {
      if (loc_entry->helping_flag == NOT_HELPING)
        kv_ptr->dbg->last_committed_flag = LOCAL_RMW;
      else kv_ptr->dbg->last_committed_flag = LOCAL_RMW_FROM_HELP;
    kv_ptr->dbg->last_committed_ts = loc_entry_to_commit->new_ts;
    kv_ptr->dbg->last_committed_log_no = loc_entry_to_commit->log_no;
    kv_ptr->dbg->last_committed_rmw_id = loc_entry_to_commit->rmw_id;
  }*/
  mica_op_t *kv_ptr = loc_entry->kv_ptr;

  kv_ptr->last_committed_log_no = loc_entry_to_commit->log_no;
  kv_ptr->last_committed_rmw_id = loc_entry_to_commit->rmw_id;
  bool overwrite_kv = compare_ts(&loc_entry_to_commit->base_ts, &kv_ptr->ts) != SMALLER;
  // Update the KVS if the base ts of the entry to be committed is equal or greater than the locally stored
  // Beyond that there are two cases:
  // 1. When not helping and the RMW is successful
  // 2  When helping
  if (loc_entry->rmw_is_successful && loc_entry->helping_flag != HELPING) {
    update_commit_logs(t_id, kv_ptr->key.bkt, loc_entry_to_commit->log_no,
                       kv_ptr->value, loc_entry_to_commit->value_to_write,
                       " local_commit", LOG_COMS);
    if (overwrite_kv) memcpy(kv_ptr->value, loc_entry_to_commit->value_to_write, loc_entry->rmw_val_len);
  }
  else if (loc_entry->helping_flag == HELPING) {
    update_commit_logs(t_id, kv_ptr->key.bkt, loc_entry_to_commit->log_no,
                       kv_ptr->value, loc_entry_to_commit->value_to_write,
                       " local_commit", LOG_COMS);
    if (overwrite_kv) memcpy(kv_ptr->value, loc_entry_to_commit->value_to_write, (size_t) RMW_VALUE_SIZE);
  }
  if (overwrite_kv) {
    kv_ptr->ts.m_id = loc_entry_to_commit->base_ts.m_id;
    kv_ptr->ts.version = loc_entry_to_commit->base_ts.version;
  }

}

// After having helped another RMW, bring your own RMW back into the local entry
static inline void reinstate_loc_entry_after_helping(struct rmw_local_entry *loc_entry, uint16_t t_id)
{
  //if (loc_entry->helping_flag == HELPING_NEED_STASHING) {
  //  loc_entry->opcode = loc_entry->help_rmw->opcode;
  //  assign_second_rmw_id_to_first(&loc_entry->rmw_id, &loc_entry->help_rmw->rmw_id);
 // }
  loc_entry->state = NEEDS_KV_PTR;
  loc_entry->helping_flag = NOT_HELPING;
  zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);

  if (DEBUG_RMW)
    my_printf(yellow, "Wrkr %u, sess %u reinstates its RMW id %u glob_sess-id %u after helping \n",
                  t_id, loc_entry->sess_id, loc_entry->rmw_id.id,
                  loc_entry->rmw_id.glob_sess_id);
  if (ENABLE_ASSERTIONS)
    assert(glob_ses_id_to_m_id(loc_entry->rmw_id.glob_sess_id) == (uint8_t) machine_id);

}

// Activate the entry that belongs to a given key to initiate an RMW (either a local or a remote)
static inline void activate_RMW_entry(uint8_t state, uint32_t new_version, mica_op_t  *kv_ptr,
                                      uint8_t opcode, uint8_t new_ts_m_id, uint64_t l_id, uint16_t glob_sess_id,
                                      uint32_t log_no, uint16_t t_id, const char* message)
{
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->log_no == log_no && kv_ptr->state == ACCEPTED && state != ACCEPTED) {
      printf("%s \n", message);
      assert(false);
    }
    assert(kv_ptr->log_no <= log_no);
  }
  // pass the new ts!
  kv_ptr->opcode = opcode;
  kv_ptr->prop_ts.m_id = new_ts_m_id;
  //if (ENABLE_ASSERTIONS) assert(new_version >= kv_ptr->prop_ts.version);
  kv_ptr->prop_ts.version = new_version;
  //my_printf(cyan, "Kv_ptr version %u, m_id %u \n", kv_ptr->prop_ts.version, kv_ptr->new_ts.m_id);
  kv_ptr->rmw_id.glob_sess_id = glob_sess_id;
  kv_ptr->rmw_id.id = l_id;
  kv_ptr->state = state;
  kv_ptr->log_no = log_no;

  if (state == ACCEPTED) {
    if (ENABLE_ASSERTIONS) {
      assert(kv_ptr->prop_ts.version == new_version);
      assert(kv_ptr->prop_ts.m_id == new_ts_m_id);
    }
    kv_ptr->accepted_ts = kv_ptr->prop_ts;
    kv_ptr->accepted_log_no = log_no;
    if (ENABLE_ASSERTIONS)  kv_ptr->accepted_rmw_id = kv_ptr->rmw_id;
  }
  if (ENABLE_ASSERTIONS) {
    assert(kv_ptr->rmw_id.glob_sess_id < GLOBAL_SESSION_NUM);
    if (committed_glob_sess_rmw_id[kv_ptr->rmw_id.glob_sess_id] >= kv_ptr->rmw_id.id) {
      //my_printf(red, "Wrkr %u, attempts to activate with already committed RMW id %u/%u glob_sess id %u, state %u: %s \n",
      //           t_id, kv_ptr->rmw_id.id, committed_glob_sess_rmw_id[kv_ptr->rmw_id.glob_sess_id],
      //           kv_ptr->rmw_id.glob_sess_id, state, message);
    }
    assert(glob_sess_id < GLOBAL_SESSION_NUM);
    //assert(kv_ptr->new_ts.version % 2 == 0);
    assert(state == PROPOSED || state == ACCEPTED); // TODO accepted is allowed?
    assert(kv_ptr->last_committed_log_no < kv_ptr->log_no);
  }
}

static inline bool opcode_is_rmw_rep(uint8_t opcode)
{
  return (opcode >= RMW_ACK && opcode <= NO_OP_PROP_REP) ||
         (opcode >= RMW_ACK + FALSE_POSITIVE_OFFSET &&
          opcode <= NO_OP_PROP_REP + FALSE_POSITIVE_OFFSET);
}

static inline bool r_rep_has_big_size(uint8_t opcode)
{
  return opcode == TS_GREATER || (opcode == TS_GREATER + FALSE_POSITIVE_OFFSET);
}

static inline bool r_rep_has_rmw_acq_size(uint8_t opcode)
{
  return opcode == ACQ_LOG_TOO_SMALL || (opcode == ACQ_LOG_TOO_SMALL + FALSE_POSITIVE_OFFSET);
}

static inline uint16_t r_rep_size_based_on_opcode(uint8_t opcode)
{
  if (r_rep_has_rmw_acq_size(opcode))
    return RMW_ACQ_REP_SIZE;
  else if (r_rep_has_big_size(opcode))
    return R_REP_SIZE;
  else if (opcode == TS_GREATER_TS_ONLY)
    return R_REP_ONLY_TS_SIZE;
  else return 1;
}

// Give an opcode to get the size of the read rep messages
static inline uint16_t get_size_from_opcode(uint8_t opcode)
{
  if (opcode > ACQ_LOG_EQUAL) opcode -= FALSE_POSITIVE_OFFSET;
  switch(opcode) {
    // ----RMWS-----
    case LOG_TOO_SMALL:
      return PROP_REP_LOG_TOO_LOW_SIZE;
    case SEEN_LOWER_ACC:
      return PROP_REP_ACCEPTED_SIZE;
    case SEEN_HIGHER_PROP:
    case SEEN_HIGHER_ACC:
      return PROP_REP_ONLY_TS_SIZE;
    case RMW_ID_COMMITTED:
    case RMW_ID_COMMITTED_SAME_LOG:
    case RMW_ACK:
    case RMW_ACK_ACC_SAME_RMW:
    case LOG_TOO_HIGH:
    case NO_OP_PROP_REP:
      return PROP_REP_SMALL_SIZE;
    //---- RMW ACQUIRES--------
    case ACQ_LOG_TOO_HIGH:
    case ACQ_LOG_EQUAL:
      return R_REP_SMALL_SIZE;
    case ACQ_LOG_TOO_SMALL:
      return RMW_ACQ_REP_SIZE;
    // -----REGULAR READS/ACQUIRES----
    case TS_SMALLER:
    case TS_EQUAL:
      return R_REP_SMALL_SIZE;
    case TS_GREATER:
      return R_REP_SIZE;
    case TS_GREATER_TS_ONLY:
      return R_REP_ONLY_TS_SIZE;
    default: if (ENABLE_ASSERTIONS) {
        my_printf(red, "Opcode %u \n", opcode);
        assert(false);
      }
  }
}


//
static inline void set_up_rmw_acq_rep_message_size(struct pending_ops *p_ops,
                                                   uint8_t opcode, uint16_t t_id)
{
  struct r_rep_fifo *r_rep_fifo = p_ops->r_rep_fifo;

  check_state_with_allowed_flags(7, opcode, ACQ_LOG_TOO_SMALL, ACQ_LOG_TOO_HIGH, ACQ_LOG_EQUAL,
                                 ACQ_LOG_TOO_SMALL + FALSE_POSITIVE_OFFSET,
                                 ACQ_LOG_TOO_HIGH + FALSE_POSITIVE_OFFSET,
                                 ACQ_LOG_EQUAL + FALSE_POSITIVE_OFFSET);

  if (opcode == ACQ_LOG_TOO_SMALL)
    r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] += (RMW_ACQ_REP_SIZE - R_REP_SMALL_SIZE);

  if (ENABLE_ASSERTIONS) assert(r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] <= R_REP_SEND_SIZE);
}

// This function sets the size and the opcode of a red reply, for reads/acquires and Read TS
// The locally stored TS is copied in the r_rep
static inline void set_up_r_rep_message_size(struct pending_ops *p_ops,
                                             struct r_rep_big *r_rep,
                                             struct network_ts_tuple *remote_ts,
                                             bool read_ts,
                                             uint16_t t_id)
{
  struct r_rep_fifo *r_rep_fifo = p_ops->r_rep_fifo;
  enum ts_compare ts_comp = compare_netw_ts(&r_rep->ts, remote_ts);
  if (machine_id == 0 && R_TO_W_DEBUG) {
    if (ts_comp == EQUAL)
      my_printf(green, "L/R:  m_id: %u/%u version %u/%u \n", r_rep->ts.m_id, remote_ts->m_id,
                   r_rep->ts.version, remote_ts->version);
    else
      my_printf(red, "L/R:  m_id: %u/%u version %u/%u \n", r_rep->ts.m_id, remote_ts->m_id,
                 r_rep->ts.version, remote_ts->version);
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
      if (read_ts) {
        //This does not need the value, as it is going to do a write eventually
        r_rep->opcode = TS_GREATER_TS_ONLY;
        r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] += (R_REP_ONLY_TS_SIZE - R_REP_SMALL_SIZE);
      } else {
        if (DEBUG_TS) printf("Read TS is greater \n");
        r_rep->opcode = TS_GREATER;
        r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] += (R_REP_SIZE - R_REP_SMALL_SIZE);
      }
      break;
    default:
      if (ENABLE_ASSERTIONS) assert(false);
  }

  if (ENABLE_ASSERTIONS) assert(r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] <= R_REP_SEND_SIZE);
}

// When time-out-ing on a stuck Accepted value, and try to help it, you need to first propose your own
static inline void set_up_a_proposed_but_not_locally_acked_entry(struct pending_ops *p_ops, mica_op_t  *kv_ptr,
                                                                 struct rmw_local_entry *loc_entry, uint16_t t_id)
{
  struct rmw_local_entry *help_loc_entry = loc_entry->help_loc_entry;
  if (DEBUG_RMW)
    my_printf(cyan, "Wrkr %u, session %u helps RMW id %u glob_sess_id %u with version %u, m_id %u,"
                  " kv_ptr log/help log %u/%u kv_ptr committed log %u , biggest committed rmw_id %u for kv_ptr sess %u"
                  " stashed rmw_id: %u, global_sess id %u, state %u \n",
                t_id, loc_entry->sess_id, loc_entry->rmw_id.id, loc_entry->rmw_id.glob_sess_id,
                loc_entry->new_ts.version, loc_entry->new_ts.m_id,
                kv_ptr->log_no, loc_entry->log_no, kv_ptr->last_committed_log_no,
                committed_glob_sess_rmw_id[kv_ptr->rmw_id.glob_sess_id], kv_ptr->rmw_id.glob_sess_id,
                loc_entry->help_rmw->rmw_id.id, loc_entry->help_rmw->rmw_id.glob_sess_id, loc_entry->help_rmw->state);
  loc_entry->state = PROPOSED;
  zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
  help_loc_entry->state = ACCEPTED;
  if (ENABLE_ASSERTIONS) assert(p_ops->sess_info[loc_entry->sess_id].stalled);
  loc_entry->helping_flag = PROPOSE_NOT_LOCALLY_ACKED;
  //  my_printf(cyan, "Wrkr %u Sess %u initiates prop help, key %u, log no %u \n", t_id,
  //             loc_entry->sess_id, loc_entry->key.bkt, loc_entry->log_no);
  // help_loc_entry->sess_id = loc_entry->sess_id;
  help_loc_entry->log_no = loc_entry->log_no;
  help_loc_entry->key = loc_entry->key;
  loc_entry->rmw_reps.tot_replies = 1;
  loc_entry->rmw_reps.already_accepted = 1;
  if (PRINT_LOGS && ENABLE_DEBUG_GLOBAL_ENTRY)
    fprintf(rmw_verify_fp[t_id], "Key: %u, log %u: Prop-not-locally accepted: helping rmw_id %lu, glob_sess id: %u, "
              "version %u, m_id: %u, From: rmw_id %lu, glob_sess id: %u with version %u, m_id: %u \n",
            loc_entry->key.bkt, loc_entry->log_no, help_loc_entry->rmw_id.id, help_loc_entry->rmw_id.glob_sess_id,
            help_loc_entry->new_ts.version, help_loc_entry->new_ts.m_id, loc_entry->rmw_id.id,
            loc_entry->rmw_id.glob_sess_id,
            loc_entry->new_ts.version, loc_entry->new_ts.m_id);
}


// Potentially useful (for performance only) when a propose receives already_committed
// responses and still is holding the kv_ptr
static inline void free_kv_ptr_if_rmw_failed(struct rmw_local_entry *loc_entry,
                                             uint8_t state, uint16_t t_id)
{
  mica_op_t *kv_ptr = loc_entry->kv_ptr;
  if (kv_ptr->state == state &&
      kv_ptr->log_no == loc_entry->log_no &&
      rmw_ids_are_equal(&kv_ptr->rmw_id, &loc_entry->rmw_id) &&
      compare_ts(&kv_ptr->prop_ts, &loc_entry->new_ts) == EQUAL) {
//    my_printf(cyan, "Wrkr %u, kv_ptr NEEDS TO BE FREED: session %u RMW id %u/%u glob_sess_id %u/%u with version %u/%u,"
//                  " m_id %u/%u,"
//                  " kv_ptr log/help log %u/%u kv_ptr committed log %u , biggest committed rmw_id %u for glob sess %u"
//                  " \n",
//                t_id, loc_entry->sess_id, loc_entry->rmw_id.id, kv_ptr->rmw_id.id,
//                loc_entry->rmw_id.glob_sess_id, kv_ptr->rmw_id.glob_sess_id,
//                loc_entry->new_ts.version, kv_ptr->new_ts.version,
//                loc_entry->new_ts.m_id, kv_ptr->new_ts.m_id,
//                kv_ptr->log_no, loc_entry->log_no, kv_ptr->last_committed_log_no,
//                committed_glob_sess_rmw_id[kv_ptr->rmw_id.glob_sess_id], kv_ptr->rmw_id.glob_sess_id);

    lock_seqlock(&loc_entry->kv_ptr->seqlock);
    if (kv_ptr->state == state &&
        kv_ptr->log_no == loc_entry->log_no &&
        rmw_ids_are_equal(&kv_ptr->rmw_id, &loc_entry->rmw_id)) {
      if (state == PROPOSED && compare_ts(&kv_ptr->prop_ts, &loc_entry->new_ts) == EQUAL) {
        printf("clearing\n");
        print_rmw_rep_info(loc_entry, t_id);
        //assert(false);
        kv_ptr->state = INVALID_RMW;
      }
      else if (state == ACCEPTED && compare_ts(&kv_ptr->accepted_ts, &loc_entry->new_ts) == EQUAL)
       if (ENABLE_ASSERTIONS) assert(false);
    }
    check_log_nos_of_kv_ptr(kv_ptr, "free_kv_ptr_if_prop_failed", t_id);
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
  }
}


//When inspecting an accept/propose and have received already-committed Response
static inline void handle_already_committed_rmw(struct pending_ops *p_ops,
                                                struct rmw_local_entry *loc_entry,
                                                uint16_t t_id)
{
  // Broadcast commits iff you got back you own RMW
  if (!loc_entry->rmw_reps.no_need_to_bcast &&
    (loc_entry->rmw_reps.rmw_id_commited < QUORUM_NUM)) {
    if (ENABLE_ASSERTIONS) {
//      my_printf(yellow, "%s: committed rmw received had too "
//                      "low a log, bcasting from loc_entry \n",
//                    loc_entry->state == PROPOSED ? "Propose" : "Accept");
    }
    // Here we know the correct value/log to broadcast: it's the locally accepted ones
    loc_entry->log_no = loc_entry->accepted_log_no;
    loc_entry->state = MUST_BCAST_COMMITS;
    if (MACHINE_NUM <= 3 && ENABLE_ASSERTIONS) assert(false);
  }
  else {
    //free the session here as well
    loc_entry->state = INVALID_RMW;
    free_session(p_ops, loc_entry->sess_id, true, t_id);
  }
  check_state_with_allowed_flags(4, (int) loc_entry->state, INVALID_RMW,
                                 MUST_BCAST_COMMITS, MUST_BCAST_COMMITS_FROM_HELP);
}

// Keep track of the write messages to send the appropriate acks
static inline bool ack_bookkeeping(struct ack_message *ack, uint8_t w_num, uint64_t l_id,
                                   const uint8_t m_id, const uint16_t t_id)
{
  if (ENABLE_ASSERTIONS && ack->opcode != CACHE_OP_ACK) {
    if(unlikely(ack->local_id) + ack->ack_num != l_id) {
      my_printf(red, "Wrkr %u: Adding to existing ack for machine %u  with l_id %lu, "
                   "ack_num %u with new l_id %lu, coalesce_num %u, opcode %u\n", t_id, m_id,
                 ack->local_id, ack->ack_num, l_id, w_num, ack->opcode);
      //assert(false);
      return false;
    }
  }
  if (ack->opcode == CACHE_OP_ACK) {// new ack
    //if (ENABLE_ASSERTIONS) assert((ack->local_id) + ack->ack_num == l_id);
    memcpy(&ack->local_id, &l_id, sizeof(uint64_t));
    ack->credits = 1;
    ack->ack_num = w_num;
    ack->opcode = ACK_NOT_YET_SENT;
    if (DEBUG_ACKS) my_printf(yellow, "Create an ack with l_id  %lu \n", ack->local_id);
  }
  else {
    if (ENABLE_ASSERTIONS) {
      assert(ack->local_id + ((uint64_t) ack->ack_num) == l_id);
      assert(ack->ack_num < 63000);
      assert(W_CREDITS > 1);
      assert(ack->credits < W_CREDITS);
    }
    ack->credits++;
    ack->ack_num += w_num;
  }
  return true;
}

//Handle the configuration bit_vec vector on receiving a release
static inline void handle_configuration_on_receiving_rel(struct write *write, uint16_t t_id)
{

  // On receiving the 1st round of a Release/ Accept:
  // apply the change to the stable vector and set the bit_vec that gets changed to Stable state.
  // Do not change the sent vector
  uint16_t recv_conf_bit_vec = 0;
  struct accept *acc;
  switch (write->opcode) {
    case OP_RELEASE_BIT_VECTOR :
      recv_conf_bit_vec = *(uint16_t *) write->value;
      if (ENABLE_ASSERTIONS) assert(recv_conf_bit_vec > 0);
      break;
    case ACCEPT_OP_BIT_VECTOR:
      acc = (struct accept *) write;
      uint16_t *part_of_acc = (uint16_t *) (((void*) &acc->glob_sess_id) - SEND_CONF_VEC_SIZE);
      recv_conf_bit_vec = *part_of_acc;
      //my_printf(yellow, "received %u bit vec \n", recv_conf_bit_vec);
      *part_of_acc = 0;
      write->opcode = ACCEPT_OP;
      if (ENABLE_ASSERTIONS) {
        assert(ACCEPT_IS_RELEASE);
        assert(recv_conf_bit_vec > 0);
        assert(acc->t_rmw_id < B_4);
      }
      break;
    default: return;
  }
  if (ENABLE_ASSERTIONS) assert(recv_conf_bit_vec > 0);
  for (uint16_t m_i = 0; m_i < MACHINE_NUM; m_i++) {
    if (recv_conf_bit_vec & machine_bit_id[m_i]) {
      set_conf_bit_to_new_state(t_id, m_i, DOWN_STABLE);
      if (DEBUG_BIT_VECS)
        my_printf(green, "Worker %u updates the kept config bit_vec vector: received: %u, m_id %u \n",
                     t_id, recv_conf_bit_vec, m_i);
    }
  }
   // we do not change the op back to OP_RELEASE, because we want to avoid making the actual write to the KVS
  // (because it only contains a bit vector)
}

// Remove the false positive offset from the opcode
static inline void detect_false_positives_on_read_info_bookkeeping(struct r_rep_big *r_rep,
                                                                   struct read_info *read_info,
                                                                   uint16_t t_id)
{
  // Check for acquires that detected a false positive
  if (unlikely(r_rep->opcode > ACQ_LOG_EQUAL)) {
    read_info->fp_detected = true;
    if (DEBUG_QUORUM)
      my_printf(yellow, "Raising the fp flag after seeing read reply %u \n", r_rep->opcode);
    r_rep->opcode -= FALSE_POSITIVE_OFFSET;
    check_state_with_allowed_flags(8, r_rep->opcode, TS_SMALLER, TS_EQUAL, TS_GREATER_TS_ONLY, TS_GREATER,
                                   ACQ_LOG_TOO_HIGH, ACQ_LOG_TOO_SMALL, ACQ_LOG_EQUAL);
    if (ENABLE_ASSERTIONS) {
      assert(read_info->opcode != OP_ACQUIRE_FLIP_BIT);
      assert(read_info->opcode == OP_ACQUIRE);
    }
  }
  if (ENABLE_ASSERTIONS) {
    if (r_rep->opcode > TS_GREATER) {
      check_state_with_allowed_flags(4, r_rep->opcode, ACQ_LOG_TOO_HIGH, ACQ_LOG_TOO_SMALL, ACQ_LOG_EQUAL);
      assert(read_info->is_rmw);
      assert(read_info->opcode == OP_ACQUIRE);
    }
    else {
      check_state_with_allowed_flags(5, r_rep->opcode, TS_SMALLER, TS_EQUAL,
                                     TS_GREATER_TS_ONLY, TS_GREATER);
    }
  }

}

// Returns true, if you should move to the next message
static inline bool find_the_r_ptr_rep_refers_to(uint32_t *r_ptr, uint64_t l_id, uint64_t pull_lid,
                                                struct pending_ops *p_ops,
                                                uint8_t mes_opcode, uint8_t r_rep_num, uint16_t  t_id)
{
  if (p_ops->r_size == 0 && mes_opcode == READ_REPLY) {
    if (!USE_QUORUM) assert(false);
    return true;
  }
  if (mes_opcode == READ_REPLY)
    check_r_rep_l_id(l_id, r_rep_num, pull_lid, p_ops->r_size, t_id);

  if (pull_lid >= l_id) {
    if ((pull_lid - l_id) >= r_rep_num && mes_opcode == READ_REPLY) return true;
    (*r_ptr) = p_ops->r_pull_ptr;
  } else  // l_id > pull_lid
    (*r_ptr) = (uint32_t) (p_ops->r_pull_ptr + (l_id - pull_lid)) % PENDING_READS;
  return false;
}

// Each read has an associated read_info structure that keeps track of the incoming replies, value, opcode etc.
static inline void read_info_bookkeeping(struct r_rep_big *r_rep, struct read_info *read_info,
                                         uint16_t t_id)
{
  // Check for acquires that detected a false positive
  detect_false_positives_on_read_info_bookkeeping(r_rep, read_info, t_id);
  if (r_rep->opcode == TS_GREATER || r_rep->opcode == TS_GREATER_TS_ONLY) {
    if (r_rep->opcode == TS_GREATER_TS_ONLY)
      check_state_with_allowed_flags(3, read_info->opcode, OP_RELEASE, KVS_OP_PUT);
    else check_state_with_disallowed_flags(3, read_info->opcode, OP_RELEASE, KVS_OP_PUT);
    // If this is the first "Greater" ts
    if (!read_info->seen_larger_ts) {
      assign_netw_ts_to_ts(&read_info->ts_to_read, &r_rep->ts);
      read_info->times_seen_ts = 1;
      if (r_rep->opcode == TS_GREATER) {
        if (ENABLE_ASSERTIONS) assert(read_info->val_len <= VALUE_SIZE);
        memcpy(read_info->value, r_rep->value, read_info->val_len);
      }
      read_info->seen_larger_ts = true;
    }
    else { // if the read has already received a "greater" ts
      enum ts_compare ts_comp = compare_netw_ts_with_ts(&r_rep->ts, &read_info->ts_to_read);
      if (ts_comp == GREATER) {
        assign_netw_ts_to_ts(&read_info->ts_to_read, &r_rep->ts);
        read_info->times_seen_ts = 1;
        if (r_rep->opcode == TS_GREATER) {
          if (ENABLE_ASSERTIONS) assert(read_info->val_len <= VALUE_SIZE);
          memcpy(read_info->value, r_rep->value, read_info->val_len);
        }
      }
      if (ts_comp == EQUAL) read_info->times_seen_ts++;
      // Nothing to do if the the incoming is smaller than the already stored
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

// Each read has an associated read_info structure that keeps track of the incoming replies, value, opcode etc.
static inline void rmw_acq_read_info_bookkeeping(struct rmw_acq_rep *acq_rep, struct read_info *read_info,
                                                 uint16_t t_id)
{
  detect_false_positives_on_read_info_bookkeeping((struct r_rep_big *) acq_rep, read_info, t_id);
  if (acq_rep->opcode == ACQ_LOG_TOO_SMALL) {
    if (!read_info->seen_larger_ts) { // If this is the first "Greater" ts
      if (ENABLE_ASSERTIONS) assert(read_info->log_no < acq_rep->log_no);
      read_info->log_no = acq_rep->log_no;
      read_info->rmw_id.id = acq_rep->rmw_id;
      read_info->rmw_id.glob_sess_id = acq_rep->glob_sess_id;
      assign_netw_ts_to_ts(&read_info->ts_to_read, &acq_rep->ts);
      check_version(read_info->ts_to_read.version, "rmw_Acquire ");
      memcpy(read_info->value, acq_rep->value, read_info->val_len);
      read_info->times_seen_ts = 1;
      read_info->seen_larger_ts = true;
    }
    else { // if the read has already received a "greater" ts
      //enum ts_compare ts_comp = compare_netw_ts_with_ts(&acq_rep->ts,&read_info->ts_to_read);
      if (acq_rep->log_no > read_info->log_no) {
        read_info->log_no = acq_rep->log_no;
        read_info->rmw_id.id = acq_rep->rmw_id;
        read_info->rmw_id.glob_sess_id = acq_rep->glob_sess_id;
        assign_netw_ts_to_ts(&read_info->ts_to_read, &acq_rep->ts);
        check_version(read_info->ts_to_read.version, "rmw_Acquire ");
        memcpy(read_info->value, acq_rep->value, read_info->val_len);
        read_info->times_seen_ts = 1;
      }
      else if (acq_rep->log_no == read_info->log_no) read_info->times_seen_ts++;
      // Nothing to do if the already stored ts is greater than the incoming
    }
  }
  else if (acq_rep->opcode == ACQ_LOG_EQUAL) {
    if (!read_info->seen_larger_ts)  // If it has not seen a "greater ts"
      read_info->times_seen_ts++;
    // Nothing to do if the already stored ts is greater than the incoming
  }
  else if (acq_rep->opcode == ACQ_LOG_TOO_HIGH) { // Nothing to do if the already stored ts is greater than the incoming

  }
  // assert(read_info->rep_num == 0);
  read_info->rep_num++;
}

//When polling read replies, handle a reply to read, acquire, readts, rmw acquire-- return true to continue to next rep
static inline bool handle_single_r_rep(struct r_rep_big *r_rep, uint32_t *r_ptr_, uint64_t l_id, uint64_t pull_lid,
                                       struct pending_ops *p_ops, int read_i, uint16_t r_rep_i,
                                       uint32_t *outstanding_reads, uint16_t t_id)
{
  uint32_t r_ptr = *r_ptr_;
  if (p_ops->r_size == 0) return true;
  check_r_rep_l_id(l_id, (uint8_t) read_i, pull_lid, p_ops->r_size, t_id);
  if (pull_lid >= l_id) {
    if (l_id + read_i < pull_lid) return true;
  }
  struct read_info *read_info = &p_ops->read_info[r_ptr];
  if (DEBUG_READ_REPS)
    my_printf(yellow, "Read reply %u, Received replies %u/%d at r_ptr %u \n",
                  r_rep_i, read_info->rep_num, REMOTE_QUORUM, r_ptr);
  if (read_info->is_rmw) {
    rmw_acq_read_info_bookkeeping((struct rmw_acq_rep *) r_rep, read_info, t_id);
  }
  else {
    read_info_bookkeeping(r_rep, read_info, t_id);
  }
  if (read_info->rep_num >= REMOTE_QUORUM) {
    //my_printf(yellow, "%u r_ptr becomes ready, l_id %u,   \n", r_ptr, l_id);
    p_ops->r_state[r_ptr] = READY;
    if (ENABLE_ASSERTIONS) {
      (*outstanding_reads)--;
      assert(read_info->rep_num <= REM_MACH_NUM);
    }
  }
  MOD_ADD(r_ptr, PENDING_READS);
  r_rep->opcode = INVALID_OPCODE;
  *r_ptr_ = r_ptr;
  return false;
}


// Perform the operation of the RMW and store the result in the local entry, call on locally accepting
static inline void perform_the_rmw_on_the_loc_entry(struct rmw_local_entry *loc_entry,
                                                    mica_op_t *kv_ptr,
                                                    uint16_t t_id)
{
  struct top *top =(struct top*) kv_ptr->value;
  struct top *comp_top =(struct top*) loc_entry->compare_val;
  struct top *new_top =(struct top*) loc_entry->value_to_write;
  // if (top->push_counter == top->pop_counter) assert(top->key_id == 0);
  if (ENABLE_ASSERTIONS) assert(loc_entry->log_no == kv_ptr->last_committed_log_no + 1);
  loc_entry->rmw_is_successful = true;
  loc_entry->base_ts = kv_ptr->ts;
  switch (loc_entry->opcode) {
   case RMW_PLAIN_WRITE:
     break;
   case FETCH_AND_ADD:
     memcpy(loc_entry->value_to_read, kv_ptr->value, loc_entry->rmw_val_len);
     *(uint64_t *)loc_entry->value_to_write = (*(uint64_t *)loc_entry->value_to_read) + (*(uint64_t *)loc_entry->compare_val);
     if (ENABLE_ASSERTIONS && !ENABLE_CLIENTS) assert((*(uint64_t *)loc_entry->compare_val == 1));
     //printf("%u %lu \n", loc_entry->log_no, *(uint64_t *)loc_entry->value_to_write);
     break;
   case COMPARE_AND_SWAP_WEAK:
   case COMPARE_AND_SWAP_STRONG:
     // if are equal
     loc_entry->rmw_is_successful = memcmp(loc_entry->compare_val,
                                          kv_ptr->value,
                                           loc_entry->rmw_val_len) == 0;
     if (!loc_entry->rmw_is_successful)
       memcpy(loc_entry->value_to_read, kv_ptr->value,
              loc_entry->rmw_val_len);
     break;
   default:
     if (ENABLE_ASSERTIONS) assert(false);
  }
}

// Returns true if the CAS has to be cut short
static inline bool rmw_compare_fails(uint8_t opcode, uint8_t *compare_val,
                                     uint8_t *kv_ptr_value, uint32_t val_len, uint16_t t_id)
{
  if (!opcode_is_compare_rmw(opcode) || (!ENABLE_CAS_CANCELLING)) return false; // there is nothing to fail
  if (ENABLE_ASSERTIONS) {
    assert(compare_val != NULL);
    assert(kv_ptr_value != NULL);
  }
  // memcmp() returns 0 if regions are equal. Thus the CAS fails if the result is not zero
  bool rmw_fails = memcmp(compare_val, kv_ptr_value, val_len) != 0;
  if (ENABLE_STAT_COUNTING && rmw_fails) {
    t_stats[t_id].cancelled_rmws++;
  }
  return rmw_fails;

}

// returns true if the RMW can be failed before allocating a local entry
static inline bool does_rmw_fail_early(struct trace_op *op, mica_op_t *kv_ptr,
                                       struct kvs_resp *resp, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) assert(op->real_val_len <= RMW_VALUE_SIZE);
  if (op->opcode == COMPARE_AND_SWAP_WEAK &&
      rmw_compare_fails(op->opcode, op->value_to_read,
                        kv_ptr->value, op->real_val_len, t_id)) {
    //my_printf(red, "CAS fails returns val %u/%u \n", kv_ptr->value[RMW_BYTE_OFFSET], op->value_to_read[0]);

    fill_req_array_on_rmw_early_fail(op->session_id, kv_ptr->value,
                                     op->index_to_req_array, t_id);
    resp->type = RMW_FAILURE;
    return true;
  }
  else return  false;
}

//
static inline bool rmw_fails_with_loc_entry(struct rmw_local_entry *loc_entry, mica_op_t *kv_ptr,
                                            bool *rmw_fails, uint16_t t_id)
{
  if (ENABLE_CAS_CANCELLING) {
    if (loc_entry->killable) {
      if (rmw_compare_fails(loc_entry->opcode, loc_entry->compare_val,
                            kv_ptr->value, loc_entry->rmw_val_len, t_id)) {
        (*rmw_fails) = true;
        if (ENABLE_ASSERTIONS) {
          assert(!loc_entry->rmw_is_successful);
          assert(loc_entry->rmw_val_len <= RMW_VALUE_SIZE);
          assert(loc_entry->helping_flag != HELPING);
        }
        memcpy(loc_entry->value_to_read, kv_ptr->value,
               loc_entry->rmw_val_len);
        return true;
      }
    }
  }
  return false;
}


// When creating the accept message have it try to flip the remote bits,
// if a false positive has been previously detected by a propose
static inline void signal_conf_bit_flip_in_accept(struct rmw_local_entry *loc_entry,
                                                  struct accept *acc,  uint16_t t_id)
{
  if (unlikely(loc_entry->fp_detected)) {
    if (loc_entry->helping_flag == NOT_HELPING) {
      uint8_t *ptr_to_reged_rmw_id = (uint8_t *)&acc->t_rmw_id;
      if (ENABLE_ASSERTIONS) assert(ptr_to_reged_rmw_id[7] == 0);
      ptr_to_reged_rmw_id[7] = ACCEPT_FLIPS_BIT_OP;
      loc_entry->fp_detected = false;
    }
  }

}

// When receiving an accept, check if it is trying to raise  its configuration bit
static inline void raise_conf_bit_if_accept_signals_it(struct accept *acc, uint8_t acc_m_id,
                                                       uint16_t t_id)
{
  if (unlikely(acc->t_rmw_id > B_512)) {
    uint8_t *ptr_to_reged_rmw_id = (uint8_t *)&acc->t_rmw_id;
    if (ptr_to_reged_rmw_id[7] == ACCEPT_FLIPS_BIT_OP) {
      raise_conf_bit_iff_owned(acc->t_rmw_id, (uint16_t) acc_m_id, true, t_id);
      ptr_to_reged_rmw_id[7] = 0;
    }
    else if (ENABLE_ASSERTIONS) assert(false);

  }
  if (ENABLE_ASSERTIONS) assert(acc->t_rmw_id < B_4);
}


/* ---------------------------------------------------------------------------
//------------------------------ TRACE---------------------------------------
//---------------------------------------------------------------------------*/

// Returns the size of a read request given an opcode -- Proposes, reads, acquires
static inline uint16_t get_read_size_from_opcode(uint8_t opcode) {
  check_state_with_allowed_flags(9, opcode, OP_RELEASE, OP_ACQUIRE, KVS_OP_PUT,
                                 KVS_OP_GET, OP_ACQUIRE_FLIP_BIT, CACHE_OP_GET_TS,
                                 PROPOSE_OP, OP_ACQUIRE_FP);
  switch(opcode) {
    case OP_RELEASE:
    case OP_ACQUIRE:
    case KVS_OP_PUT:
    case KVS_OP_GET:
    case OP_ACQUIRE_FLIP_BIT:
    case CACHE_OP_GET_TS:
    case OP_ACQUIRE_FP:
      return R_SIZE;
    case PROPOSE_OP:
      return PROP_SIZE;
    default: if (ENABLE_ASSERTIONS) assert(false);
  }
}

// Set up a fresh read message to coalesce requests -- Proposes, reads, acquires
static inline void reset_read_message(struct pending_ops *p_ops)
{
  MOD_ADD(p_ops->r_fifo->push_ptr, R_FIFO_SIZE);
  uint32_t r_mes_ptr = p_ops->r_fifo->push_ptr;
  struct r_message *r_mes = (struct r_message *) &p_ops->r_fifo->r_message[r_mes_ptr];
  struct r_mes_info * info = &p_ops->r_fifo->info[r_mes_ptr];

  r_mes->l_id = 0;
  r_mes->coalesce_num = 0;
  info->message_size = (uint16_t) R_MES_HEADER;
  info->max_rep_message_size = 0;
  info->reads_num = 0;
}

// Return a pointer, where the next request can be created -- Proposes, reads, acquires
static inline void* get_r_ptr(struct pending_ops *p_ops, uint8_t opcode,
                              bool is_rmw_acquire, uint16_t t_id)
{
  bool is_propose = opcode == PROPOSE_OP;
  uint32_t r_mes_ptr = p_ops->r_fifo->push_ptr;
  struct r_mes_info *info = &p_ops->r_fifo->info[r_mes_ptr];
  uint16_t new_size = get_read_size_from_opcode(opcode);
  if (is_propose) info->max_rep_message_size += PROP_REP_ACCEPTED_SIZE;
  else if (is_rmw_acquire) info->max_rep_message_size += RMW_ACQ_REP_SIZE;
  else info->max_rep_message_size += R_REP_SIZE;

  bool new_message_because_of_r_rep = info->max_rep_message_size > MTU;
  bool new_message = (info->message_size + new_size) > R_SEND_SIZE ||
            new_message_because_of_r_rep;

  if (new_message) {
    reset_read_message(p_ops);
  }

  r_mes_ptr = p_ops->r_fifo->push_ptr;
  info = &p_ops->r_fifo->info[r_mes_ptr];
  struct r_message *r_mes = (struct r_message *) &p_ops->r_fifo->r_message[r_mes_ptr];

  // Set up the backwards pointers to be able to change
  // the state of requests, after broadcasting
  if (!is_propose) {
   if (info->reads_num == 0) {
     info->backward_ptr = p_ops->r_push_ptr;
     r_mes->l_id = (uint64_t) (p_ops->local_r_id + p_ops->r_size);
   }
   info->reads_num++;
  }
  r_mes->coalesce_num++;

  uint32_t inside_r_ptr = info->message_size;
  info->message_size += new_size;
  if (ENABLE_ASSERTIONS) {
    assert(info->message_size <= R_SEND_SIZE);
    assert(info->max_rep_message_size <= MTU);
  }
  return (void *) (((void *)r_mes) + inside_r_ptr);
}


// RMWs hijack the read fifo, to send propose broadcasts to all
static inline void insert_prop_to_read_fifo(struct pending_ops *p_ops, struct rmw_local_entry *loc_entry,
                                            uint16_t t_id)
{
  if (loc_entry->helping_flag != PROPOSE_NOT_LOCALLY_ACKED &&
      loc_entry->helping_flag != PROPOSE_LOCALLY_ACCEPTED)
    check_loc_entry_metadata_is_reset(loc_entry, "insert_prop_to_read_fifo", t_id);
  struct propose *prop = (struct propose*) get_r_ptr(p_ops, PROPOSE_OP, false, t_id);
  uint32_t r_mes_ptr = p_ops->r_fifo->push_ptr;
  struct r_message *r_mes = (struct r_message *) &p_ops->r_fifo->r_message[r_mes_ptr];
  if (DEBUG_RMW)
    my_printf(green, "Worker: %u, inserting an rmw in r_mes_ptr %u and inside ptr %u \n",
                 t_id, r_mes_ptr, r_mes->coalesce_num);
//  struct propose *prop = &p_mes->prop[inside_r_ptr];
  assign_ts_to_netw_ts(&prop->ts, &loc_entry->new_ts);

  memcpy(&prop->key, (void *)&loc_entry->key, TRUE_KEY_SIZE);
  prop->opcode = PROPOSE_OP;
  prop->l_id = loc_entry->l_id;
  prop->t_rmw_id = loc_entry->rmw_id.id;
  prop->glob_sess_id = loc_entry->rmw_id.glob_sess_id;
  prop->log_no = loc_entry->log_no;

  // Query the conf to see if the machine has lost messages
  on_starting_an_acquire_query_the_conf(t_id, loc_entry->epoch_id);
  p_ops->r_fifo->bcast_size++;

  if (ENABLE_ASSERTIONS) {
    assert(prop->ts.version >= PAXOS_TS);
    //check_version(prop->ts.version, "insert_prop_to_read_fifo");
    assert(r_mes->coalesce_num > 0);
    assert(r_mes->m_id == (uint8_t) machine_id);
  }
  if (ENABLE_STAT_COUNTING) t_stats[t_id].proposes_sent++;
}


// Worker inserts a new local read to the read fifo it maintains -- Typically for Acquire
// but can also be the first round of an out-of-epoch write/release or an out-of-epoch read-- BUT NOT A PROPOSE!
static inline void insert_read(struct pending_ops *p_ops, struct trace_op *op,
                               uint8_t source, uint16_t t_id)
{
  check_state_with_allowed_flags(3, source, FROM_TRACE, FROM_ACQUIRE);
  const uint32_t r_ptr = p_ops->r_push_ptr;
  struct read_info *r_info = &p_ops->read_info[r_ptr];
  bool is_rmw_acquire = source == FROM_TRACE && r_info->opcode == OP_ACQUIRE && r_info->is_rmw;

  struct read *read = (struct read*) get_r_ptr(p_ops, r_info->opcode, is_rmw_acquire, t_id);

  // this means that the purpose of the read is solely to flip remote bits
  if (source == FROM_ACQUIRE) {
    // overload the key with local_r_id
    memcpy(&read->key, (void *) &p_ops->local_r_id, TRUE_KEY_SIZE);
    read->opcode = OP_ACQUIRE_FLIP_BIT;
    if (ENABLE_ASSERTIONS) assert(r_info->opcode == OP_ACQUIRE_FLIP_BIT);
    if (DEBUG_BIT_VECS)
      my_printf(cyan, "Wrkr: %u Acquire generates a read with op %u and key %u \n",
                  t_id, read->opcode, *(uint64_t *)&read->key);
  }
  else { // FROM TRACE: out of epoch reads/writes, acquires and releases
    if (is_rmw_acquire) read->ts.version = r_info->log_no;
    else assign_ts_to_netw_ts(&read->ts, &r_info->ts_to_read);
    read->key = r_info->key;
    r_info->epoch_id = (uint64_t) atomic_load_explicit(&epoch_id, memory_order_seq_cst);
    uint8_t opcode = r_info->opcode;
    read->opcode = (opcode == OP_RELEASE || opcode == KVS_OP_PUT) ?
                   (uint8_t) CACHE_OP_GET_TS : opcode;
  }

  uint32_t r_mes_ptr = p_ops->r_fifo->push_ptr;
  struct r_message *r_mes = (struct r_message *) &p_ops->r_fifo->r_message[r_mes_ptr];

  if (DEBUG_READS)
    my_printf(green, "Worker: %u, inserting a read in r_mes_ptr %u and inside ptr %u opcode %u \n",
                 t_id, r_mes_ptr, r_mes->coalesce_num, read->opcode);
  check_read_state_and_key(p_ops, r_ptr, source, r_mes, r_info, r_mes_ptr, read, t_id);

  //my_printf(green, "%u r_ptr becomes valid, size %u/%u \n", r_ptr, p_ops->r_size, p_ops->virt_r_size);
  p_ops->r_state[r_ptr] = VALID;
  if (source == FROM_TRACE) {
      p_ops->r_session_id[r_ptr] = op->session_id;
      if (ENABLE_CLIENTS) {
        p_ops->r_index_to_req_array[r_ptr] = op->index_to_req_array;
      }
      // Query the conf to see if the machine has lost messages
      if (r_info->opcode == OP_ACQUIRE)
        on_starting_an_acquire_query_the_conf(t_id, r_info->epoch_id);
  }

  // Increase the virtual size by 2 if the req is an acquire
  p_ops->virt_r_size+= r_info->opcode == OP_ACQUIRE ? 2 : 1;
  p_ops->r_size++;
  p_ops->r_fifo->bcast_size++;

  check_read_fifo_metadata(p_ops, r_mes, t_id);
  MOD_ADD(p_ops->r_push_ptr, PENDING_READS);

  if (ENABLE_STAT_COUNTING) {
    t_stats[t_id].reads_sent ++;
    if (r_mes->coalesce_num == 1) t_stats[t_id].reads_sent_mes_num++;
  }

}



// Set up a fresh write message to coalesce requests -- Accepts, commits, writes, releases
static inline void reset_write_message(struct pending_ops *p_ops)
{

  MOD_ADD(p_ops->w_fifo->push_ptr, W_FIFO_SIZE);
  uint32_t w_mes_ptr = p_ops->w_fifo->push_ptr;
  struct w_message *w_mes = (struct w_message *)
    &p_ops->w_fifo->w_message[w_mes_ptr];
  struct w_mes_info * info = &p_ops->w_fifo->info[w_mes_ptr];
  //my_printf(cyan, "resetting message %u \n", p_ops->w_fifo->push_ptr);
  w_mes->l_id = 0;
  w_mes->coalesce_num = 0;
  info->message_size = (uint16_t) W_MES_HEADER;
  info->max_rep_message_size = 0;
  info->writes_num = 0;
  info->is_release = false;
  info->valid_header_l_id = false;

}


// Find out if a release can be coalesced
static inline bool coalesce_release(struct w_mes_info *info, struct w_message *w_mes,
                                    uint16_t session_id, uint16_t t_id)
{
  /* release cannot be coalesced when
   * -- A write from the same session exists already in the message
   **/
  for (uint8_t i = 0; i < w_mes->coalesce_num; i++) {
    if (session_id == info->per_message_sess_id[i]) {
//      printf("Wrkr %u release is of session %u, which exists in write %u/%u \n",
//             t_id,session_id, i, w_mes->coalesce_num);
      return false;
    }
  }
  //my_printf(green, "Wrkr %u release is of session %u, and can be coalesced at %u \n",
  //       t_id, session_id, w_mes->coalesce_num);
  return true;

}

// Return a pointer, where the next request can be created -- Accepts, commits, writes, releases
static inline void* get_w_ptr(struct pending_ops *p_ops, uint8_t opcode,
                              uint16_t session_id, uint16_t t_id)
{
  check_state_with_allowed_flags(8, opcode, OP_RELEASE, KVS_OP_PUT, ACCEPT_OP,
                                 COMMIT_OP, RMW_ACQ_COMMIT_OP, OP_RELEASE_SECOND_ROUND,
                                 OP_ACQUIRE);
  if (ENABLE_ASSERTIONS) assert(session_id < SESSIONS_PER_THREAD);

  bool is_accept = opcode == ACCEPT_OP;
  bool is_release = opcode == OP_RELEASE;
  bool release_or_acc = is_release || (is_accept && ACCEPT_IS_RELEASE); //is_accept || is_release;

  uint32_t w_mes_ptr = p_ops->w_fifo->push_ptr;
  struct w_mes_info *info = &p_ops->w_fifo->info[w_mes_ptr];
  struct w_message *w_mes = (struct w_message *) &p_ops->w_fifo->w_message[w_mes_ptr];
  uint16_t new_size = get_write_size_from_opcode(opcode);
  bool new_message_because_of_release =
    release_or_acc ? (!coalesce_release(info, w_mes, session_id, t_id)) : false;

  if (is_accept) info->max_rep_message_size += ACC_REP_SIZE;
  bool new_message_because_of_r_rep = info->max_rep_message_size > MTU;
  bool new_message = ((info->message_size + new_size) > W_SEND_SIZE) ||
                      new_message_because_of_release ||
                      new_message_because_of_r_rep;

  if (ENABLE_ASSERTIONS && release_or_acc) {
    assert(p_ops->sess_info[session_id].writes_not_yet_inserted == 0);
  }
  if (new_message) {
    reset_write_message(p_ops);
    w_mes_ptr = p_ops->w_fifo->push_ptr;
    info = &p_ops->w_fifo->info[w_mes_ptr];
    w_mes = (struct w_message *) &p_ops->w_fifo->w_message[w_mes_ptr];
  }
  // Write opcode if it;s the first message
  if (w_mes->coalesce_num == 0)
    w_mes->opcode = (uint8_t) (is_accept ? ONLY_ACCEPTS : ONLY_WRITES);

  if (release_or_acc && !info->is_release) {
    info->is_release = true;
    info->first_release_byte_ptr = info->message_size;
    info->first_release_w_i = info->writes_num;
    if (ENABLE_ASSERTIONS && ACCEPT_IS_RELEASE)
      assert(info->writes_num == w_mes->coalesce_num);
  }
  info->per_message_release_flag[w_mes->coalesce_num] = release_or_acc;
  // Set up the backwards pointers to be able to change
  // the state of requests, after broadcasting
  if (!is_accept) {
    if (!info->valid_header_l_id) {
      info->valid_header_l_id = true;
      info->backward_ptr = p_ops->w_push_ptr;
      w_mes->l_id = (uint64_t) (p_ops->local_w_id + p_ops->w_size);
      //my_printf(yellow, "Setting l_id of ms %u to %lu \n", w_mes_ptr, w_mes->l_id);
    }
    info->writes_num++;
    if (w_mes->opcode == ONLY_ACCEPTS) w_mes->opcode = WRITES_AND_ACCEPTS;
  }
  else if (w_mes->opcode == ONLY_WRITES) w_mes->opcode = WRITES_AND_ACCEPTS;

  info->per_message_sess_id[w_mes->coalesce_num] = session_id;
  w_mes->coalesce_num++;
  uint32_t inside_w_ptr = info->message_size;
  info->message_size += new_size;
  if (DEBUG_WRITES)
     my_printf(green, "Wrkr %u, sess %u inserts write %u, new_message %d, coalesce num %u, "
                 "w_num %u, w_mes_ptr %u, mes_l_id %lu valid l_id %d,  message size %u \n",
               t_id, session_id, opcode, new_message, w_mes->coalesce_num,
               info->writes_num, w_mes_ptr, w_mes->l_id, info->valid_header_l_id, info->message_size);



  if (ENABLE_ASSERTIONS) assert(info->message_size <= W_SEND_SIZE);
  return (void *) (((void *)w_mes) + inside_w_ptr);
}


// Insert accepts to the write message fifo
static inline void insert_accept_in_writes_message_fifo(struct pending_ops *p_ops,
                                                        struct rmw_local_entry *loc_entry,
                                                        bool helping,
                                                        uint16_t t_id)
{
  check_loc_entry_metadata_is_reset(loc_entry, "insert_accept_in_writes_message_fifo", t_id);
  if (ENABLE_ASSERTIONS) assert(loc_entry->helping_flag != PROPOSE_NOT_LOCALLY_ACKED);
  if (DEBUG_RMW) {
    my_printf(yellow, "Wrkr %u Inserting an accept, bcast size %u, "
                    "rmw_id %lu, global_sess_id %u, fifo push_ptr %u, fifo pull ptr %u\n",
                  t_id, p_ops->w_fifo->bcast_size, loc_entry->rmw_id.id,
                  loc_entry->rmw_id.glob_sess_id,
                  p_ops->w_fifo->push_ptr, p_ops->w_fifo->bcast_pull_ptr);
  }
  struct accept *acc = (struct accept *)
    get_w_ptr(p_ops, ACCEPT_OP, loc_entry->sess_id, t_id);

  acc->l_id = loc_entry->l_id;
  acc->t_rmw_id = loc_entry->rmw_id.id;
  if (ENABLE_ASSERTIONS) assert(acc->t_rmw_id < B_4);
  acc->glob_sess_id = loc_entry->rmw_id.glob_sess_id;
  assign_ts_to_netw_ts(&acc->base_ts, &loc_entry->base_ts);
  assign_ts_to_netw_ts(&acc->ts, &loc_entry->new_ts);
  memcpy(&acc->key, &loc_entry->key, TRUE_KEY_SIZE);
  acc->opcode = ACCEPT_OP;
  if (!helping && !loc_entry->rmw_is_successful)
    memcpy(acc->value, loc_entry->value_to_read, (size_t) RMW_VALUE_SIZE);
  else memcpy(acc->value, loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
  acc->log_no = loc_entry->log_no;
  acc->val_len = (uint8_t) loc_entry->rmw_val_len;
  signal_conf_bit_flip_in_accept(loc_entry, acc, t_id);

  p_ops->w_fifo->bcast_size++;
  if (ENABLE_ASSERTIONS) {
    assert(acc->l_id < p_ops->prop_info->l_id);
  }
}

static inline uint8_t get_write_opcode(const uint8_t source, struct trace_op *op,
                                       struct read_info *r_info,
                                       struct rmw_local_entry *loc_entry)
{
  switch(source) {
    case FROM_TRACE:
      return op->opcode;
    case FROM_READ:
      if (r_info->is_rmw)
        return RMW_ACQ_COMMIT_OP;
      else return r_info->opcode;
    case FROM_COMMIT:
      return COMMIT_OP;
    case RELEASE_THIRD:
      return OP_RELEASE_SECOND_ROUND;
    default: assert(false);

  }
}

// Insert a new local or remote write to the pending writes
static inline void insert_write(struct pending_ops *p_ops, struct trace_op *op, const uint8_t source,
                                const uint32_t incoming_pull_ptr, uint16_t t_id)
{
  struct read_info *r_info = NULL;
  struct rmw_local_entry *loc_entry = (struct rmw_local_entry *) op;
  if (source == FROM_READ) r_info = &p_ops->read_info[incoming_pull_ptr];
  uint32_t w_ptr = p_ops->w_push_ptr;
  uint8_t opcode = get_write_opcode(source, op, r_info, loc_entry);
  uint16_t sess_id =  get_w_sess_id(p_ops, op, source, incoming_pull_ptr, t_id);
  set_w_sess_info_and_index_to_req_array(p_ops, op, source, w_ptr, incoming_pull_ptr,
                                         opcode, sess_id, t_id);

  if (ENABLE_ASSERTIONS && source == FROM_READ &&
    r_info->opcode == KVS_OP_PUT) {
    assert(p_ops->sess_info[sess_id].writes_not_yet_inserted > 0);
    p_ops->sess_info[sess_id].writes_not_yet_inserted--;
  }

  struct write *write = (struct write *)
    get_w_ptr(p_ops, opcode, (uint16_t)p_ops->w_meta[w_ptr].sess_id, t_id);

  uint32_t w_mes_ptr = p_ops->w_fifo->push_ptr;
  struct w_message *w_mes = (struct w_message *) &p_ops->w_fifo->w_message[w_mes_ptr];

  //printf("Insert a write %u \n", *(uint32_t *)write);
  if (DEBUG_READS && source == FROM_READ) {
    my_printf(yellow, "Wrkr %u Inserting a write as a second round of read/write w_size %u/%d, bcast size %u, "
                    " push_ptr %u, pull_ptr %u "
                    "l_id %lu, fifo push_ptr %u, fifo pull ptr %u\n", t_id,
                  p_ops->w_size, PENDING_WRITES, p_ops->w_fifo->bcast_size,
                  p_ops->w_push_ptr, p_ops->w_pull_ptr,
                  w_mes->l_id, p_ops->w_fifo->push_ptr, p_ops->w_fifo->bcast_pull_ptr);
  }

  write_bookkeeping_in_insertion_based_on_source(p_ops, write, op, source, incoming_pull_ptr,
                                                 r_info, t_id);

  if (ENABLE_ASSERTIONS) {
    debug_checks_when_inserting_a_write(source, write, w_mes_ptr,
                                        w_mes->l_id, p_ops, w_ptr, t_id);
    assert(p_ops->w_meta[w_ptr].w_state == INVALID);
  }
  //if (t_id == 1) printf("Wrkr %u Validating state at ptr %u \n", t_id, w_ptr);
  p_ops->w_meta[w_ptr].w_state = VALID;
  if (ENABLE_ASSERTIONS) {
    if (p_ops->w_size > 0) assert(p_ops->w_push_ptr != p_ops->w_pull_ptr);
  }
  p_ops->w_size++;
  p_ops->w_fifo->bcast_size++;
  increas_virt_w_size(p_ops, write, source, t_id);
  MOD_ADD(p_ops->w_push_ptr, PENDING_WRITES);
}

// setup a new r_rep entry
static inline void set_up_r_rep_entry(struct r_rep_fifo *r_rep_fifo, uint8_t rem_m_id, uint64_t l_id,
                                      uint8_t read_opcode, bool is_rmw)
{
  MOD_ADD(r_rep_fifo->push_ptr, R_REP_FIFO_SIZE);
  uint32_t r_rep_mes_ptr = r_rep_fifo->push_ptr;
  struct r_rep_message *r_rep_mes = (struct r_rep_message *) &r_rep_fifo->r_rep_message[r_rep_mes_ptr];
  r_rep_mes->coalesce_num = 0;
  r_rep_fifo->mes_size++;
  if (read_opcode == PROPOSE_OP) r_rep_mes->opcode = PROP_REPLY;
  else if (read_opcode == ACCEPT_OP) r_rep_mes->opcode = ACCEPT_REPLY;
  else if (read_opcode == ACCEPT_OP_NO_CREDITS) r_rep_mes->opcode = ACCEPT_REPLY_NO_CREDITS;
  else {
    r_rep_mes->opcode = READ_REPLY;
    r_rep_mes->l_id = l_id;
  }

  r_rep_fifo->rem_m_id[r_rep_mes_ptr] = rem_m_id;
  r_rep_fifo->message_sizes[r_rep_mes_ptr] = R_REP_MES_HEADER; // ok for rmws
}

// Get a pointer to the read reply that will be sent, typically before going to the kvs,
// such that the kvs value, can be copied directly to the reply
static inline struct r_rep_big* get_r_rep_ptr(struct pending_ops *p_ops, uint64_t l_id,
                                              uint8_t rem_m_id, uint8_t read_opcode, bool coalesce,
                                              uint16_t t_id)
{
  check_state_with_allowed_flags(9, read_opcode, KVS_OP_GET, OP_ACQUIRE, OP_ACQUIRE_FLIP_BIT,
                                 PROPOSE_OP, ACCEPT_OP, ACCEPT_OP_NO_CREDITS, OP_ACQUIRE_FP, CACHE_OP_GET_TS);
  struct r_rep_fifo *r_rep_fifo = p_ops->r_rep_fifo;
  //struct r_rep_message *r_rep_mes = r_rep_fifo->r_rep_message;
  bool is_propose = read_opcode == PROPOSE_OP,
       is_accept = read_opcode == ACCEPT_OP || read_opcode == ACCEPT_OP_NO_CREDITS;
  bool is_rmw = (is_propose || is_accept);
  bool is_read_rep = !is_rmw;
  //bool current_message_is_r_rep = r_rep_mes[r_rep_fifo->push_ptr].opcode == READ_REPLY;
  /* A reply message corresponds to exactly one read message
  * to avoid reasoning about l_ids, credits and so on */

  if (!coalesce){
    set_up_r_rep_entry(r_rep_fifo, rem_m_id, l_id, read_opcode, is_rmw);
    //my_printf(cyan, "Wrkr %u Creating a new read_reply message opcode: %u/%u at push_ptr %u\n",
    //           t_id, r_rep_mes[r_rep_fifo->push_ptr].opcode, read_opcode, r_rep_fifo->push_ptr);
  }
  uint32_t r_rep_mes_ptr = r_rep_fifo->push_ptr;
  struct r_rep_message *r_rep_mes = (struct r_rep_message *) &r_rep_fifo->r_rep_message[r_rep_mes_ptr];
  if (coalesce) {
    if (is_read_rep && r_rep_mes->opcode == PROP_REPLY) {
      r_rep_mes->opcode = READ_PROP_REPLY;
      r_rep_mes->l_id = l_id;
    }
    else if (is_propose && r_rep_mes->opcode == READ_REPLY)
      r_rep_mes->opcode = READ_PROP_REPLY;
    //if (ENABLE_ASSERTIONS) assert(!is_accept);
  }

  if (ENABLE_ASSERTIONS) {
    if (is_read_rep)
      check_state_with_allowed_flags(3, r_rep_mes->opcode, READ_REPLY, READ_PROP_REPLY);
  }

  uint32_t inside_r_rep_ptr = r_rep_fifo->message_sizes[r_rep_fifo->push_ptr]; // This pointer is in bytes

  if (!is_rmw) r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] += R_REP_SMALL_SIZE;
  if (ENABLE_ASSERTIONS) assert(r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] <= R_REP_SEND_SIZE);
  return (struct r_rep_big *) (((void *)r_rep_mes) + inside_r_rep_ptr);
}

//After filling the read reply do the final required bookkeeping
static inline void finish_r_rep_bookkeeping(struct pending_ops *p_ops, struct r_rep_big *rep,
                                            bool false_pos, uint8_t rem_m_id, uint16_t t_id)
{
  struct r_rep_fifo *r_rep_fifo = p_ops->r_rep_fifo;
  uint32_t r_rep_mes_ptr = r_rep_fifo->push_ptr;
  struct r_rep_message *r_rep_mes = (struct r_rep_message *) &r_rep_fifo->r_rep_message[r_rep_mes_ptr];

  if (false_pos) {
    if (DEBUG_QUORUM)
      my_printf(yellow, "Worker %u Letting machine %u know that I believed it failed \n", t_id, rem_m_id);
    rep->opcode += FALSE_POSITIVE_OFFSET;
  }
  p_ops->r_rep_fifo->total_size++;
  r_rep_mes->coalesce_num++;
  if (ENABLE_ASSERTIONS) {
    assert(r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] <= R_REP_SEND_SIZE);
    assert(r_rep_mes->coalesce_num <= MAX_REPS_IN_REP);
  }
}

// Insert a new r_rep to the r_rep reply fifo: used only for OP_ACQIUIRE_FLIP_BIT
// i.e. the message spawned by acquires that detected a false positive, meant to merely flip the owned bit
static inline void insert_r_rep(struct pending_ops *p_ops, uint64_t l_id, uint16_t t_id,
                                uint8_t rem_m_id, bool coalesce,  uint8_t read_opcode)
{
 check_state_with_allowed_flags(2, read_opcode, OP_ACQUIRE_FLIP_BIT);
  struct r_rep_big *r_rep = get_r_rep_ptr(p_ops, l_id, rem_m_id, read_opcode, coalesce, t_id);
  r_rep->opcode = TS_EQUAL;
  finish_r_rep_bookkeeping(p_ops, r_rep, false, rem_m_id, t_id);
}


// Insert an RMW in the local RMW structs
static inline void insert_rmw(struct pending_ops *p_ops, struct trace_op *prop,
                              struct kvs_resp *resp, uint16_t t_id)
{
  uint16_t session_id = prop->session_id;
  if (resp->type == RMW_FAILURE) {
    //printf("Wrkr%u, sess %u, entry %u rmw_failing \n", t_id, session_id, resp->rmw_entry);
    signal_completion_to_client(session_id, prop->index_to_req_array, t_id);
    p_ops->sess_info[session_id].stalled = false;
    p_ops->all_sessions_stalled = false;
    return;
  }
  if (ENABLE_ASSERTIONS) assert(session_id < SESSIONS_PER_THREAD);
  struct rmw_local_entry *loc_entry = &p_ops->prop_info->entry[session_id];
  if (ENABLE_ASSERTIONS) {
    if (loc_entry->state != INVALID_RMW) {
      my_printf(red, "Wrkr %u Expected an invalid loc entry for session %u, loc_entry state %u \n",
                 t_id, session_id, loc_entry->state);
      assert(false);
    }
  }
  init_loc_entry(resp, p_ops, prop, t_id, loc_entry);
  p_ops->prop_info->l_id++;
  // if the global RMW entry was occupied, put in the next op to try next round
  if (resp->type == RETRY_RMW_KEY_EXISTS) {
    //if (DEBUG_RMW) my_printf(green, "Worker %u failed to do its RMW and moved "
    //      "it from position %u to %u \n", t_id, op_i, *old_op_i);
    loc_entry->state = NEEDS_KV_PTR;
    // Set up the state that the RMW should wait on
    loc_entry->help_rmw->rmw_id = resp->kv_ptr_rmw_id;
    loc_entry->help_rmw->state = resp->kv_ptr_state;
    loc_entry->help_rmw->ts = resp->kv_ptr_ts;
    loc_entry->help_rmw->log_no = resp->log_no;
  }
  else if (resp->type == RMW_SUCCESS) { // the RMW has gotten an entry and is to be sent
    fill_loc_rmw_entry_on_grabbing_kv_ptr(p_ops, loc_entry, prop->ts.version,
                                          PROPOSED, session_id, t_id);
    loc_entry->log_no = resp->log_no;
    if (ENABLE_ALL_ABOARD && prop->attempt_all_aboard) {
      if (ENABLE_ASSERTIONS) assert(prop->ts.version == ALL_ABOARD_TS);
      act_on_quorum_of_prop_acks(p_ops, loc_entry, t_id);
      //if the flag is ACCEPTED, that means that accept messages
      // are already lined up to be broadcast, and thus you MUST do All aboard
      if (loc_entry->state == ACCEPTED) {
        loc_entry->all_aboard = true;
        loc_entry->all_aboard_time_out = 0;
      }
    }
    else {
      if (ENABLE_ASSERTIONS) assert(prop->ts.version == PAXOS_TS);
      local_rmw_ack(loc_entry);
      insert_prop_to_read_fifo(p_ops, loc_entry, t_id);
    }
  }
  else my_assert(false, "Wrong resp type in RMW");
}

// Fill the trace_op to be passed to the KVS. Returns whether no more requests can be processed
static inline bool fill_trace_op(struct pending_ops *p_ops, struct trace_op *op,
                                 uint32_t trace_iter, struct trace_command *trace,
                                 uint16_t op_i, int working_session, uint16_t *writes_num_, uint16_t *reads_num_,
                                 struct session_dbg *ses_dbg,struct latency_flags *latency_info,
                                 uint32_t *sizes_dbg_cntr,
                                 uint16_t t_id)
{
  uint8_t opcode;
  struct key *key;
  uint8_t *value_to_write, *value_to_read;
  uint32_t real_val_len;
  if (ENABLE_CLIENTS) {
    uint32_t pull_ptr = interface[t_id].wrkr_pull_ptr[working_session];
    struct client_op *if_cl_op = &interface[t_id].req_array[working_session][pull_ptr];
    opcode = if_cl_op->opcode;
    key = &if_cl_op->key;
    op->index_to_req_array = pull_ptr;
    value_to_write = if_cl_op->value_to_write;
    value_to_read = if_cl_op->value_to_read;
    real_val_len = if_cl_op->val_len;
    if (ENABLE_ASSERTIONS) {
      assert(is_client_req_active((uint32_t) working_session, pull_ptr, t_id));
      uint32_t next_pull_ptr = (pull_ptr + 1) % PER_SESSION_REQ_NUM;
      uint32_t prev_pull_ptr = (PER_SESSION_REQ_NUM + pull_ptr - 1) % PER_SESSION_REQ_NUM;
      // if the next req is not active, no request can be active, so check the previous
      if (!is_client_req_active((uint32_t) working_session, next_pull_ptr, t_id) &&
        next_pull_ptr != prev_pull_ptr && prev_pull_ptr != pull_ptr)
        assert(!is_client_req_active((uint32_t) working_session, prev_pull_ptr, t_id));
    }
    //printf("Wrkr %u sess %u saves poll ptr %u for req at state %u \n", t_id,
    //       working_session,
    //       op->index_to_req_array,
    //       interface[t_id].req_array[working_session][ op->index_to_req_array].state);
  }
  else {
    check_trace_req(p_ops, &trace[trace_iter], working_session, t_id);
    opcode = trace[trace_iter].opcode;
    key = (struct key *) &trace[trace_iter].key_hash;
    value_to_read = op->value;
    value_to_write = op->value;
    real_val_len = (uint32_t) VALUE_SIZE;
  }

  uint16_t writes_num = *writes_num_, reads_num = *reads_num_;
  // Create some back pressure from the buffers, since the sessions may never be stalled
  if (!EMULATE_ABD) {
    if (opcode == (uint8_t) KVS_OP_PUT) writes_num++;
    //if (opcode == (uint8_t) OP_RELEASE) writes_num+= 2;
    // A write (relaxed or release) may first trigger a read
    reads_num += opcode == (uint8_t) OP_ACQUIRE ? 2 : 1;
    if (p_ops->virt_w_size + writes_num >= MAX_ALLOWED_W_SIZE ||
        p_ops->virt_r_size + reads_num >= MAX_ALLOWED_R_SIZE) {
      if (ENABLE_ASSERTIONS) {
        (*sizes_dbg_cntr)++;
        if (*sizes_dbg_cntr == M_32) {
          *sizes_dbg_cntr = 0;
          printf("Wrkr %u breaking due to max allowed size r_size %u/%d w_size %u/%u \n",
                 t_id, p_ops->virt_r_size + reads_num, MAX_ALLOWED_R_SIZE,
                 p_ops->virt_w_size + writes_num, MAX_ALLOWED_W_SIZE);
        }
      }
      return true;
    } else if (ENABLE_ASSERTIONS) *sizes_dbg_cntr = 0;
  }
  memcpy(&op->key, key, TRUE_KEY_SIZE);
  bool is_update = (opcode == (uint8_t) KVS_OP_PUT ||
                    opcode == (uint8_t) OP_RELEASE);
  bool is_rmw = opcode_is_rmw(opcode);
  bool is_read = !is_update && !is_rmw;

  if (ENABLE_ASSERTIONS) assert(is_read || is_update || is_rmw);
  if (is_update || is_rmw) op->value_to_write = value_to_write;
  if (is_read || is_rmw ) {
    op->value_to_read = value_to_read;
  }
  op->real_val_len = real_val_len;

  if (is_rmw && ENABLE_ALL_ABOARD) {
      op->attempt_all_aboard = p_ops->q_info->missing_num == 0 ? true : false;
  }

  if (opcode == KVS_OP_PUT) {
    add_request_to_sess_info(&p_ops->sess_info[working_session], t_id);
  }
  increment_per_req_counters(opcode, t_id);

  op->opcode = opcode;
  op->val_len = is_update ? (uint8_t) (VALUE_SIZE >> SHIFT_BITS) : (uint8_t) 0;
  if (op->opcode == OP_RELEASE ||
      op->opcode == OP_ACQUIRE || is_rmw) {
    if (ENABLE_ASSERTIONS) assert(!p_ops->sess_info[working_session].stalled);
    p_ops->sess_info[working_session].stalled = true;
  }
  op->session_id = (uint16_t) working_session;

  if (ENABLE_ASSERTIONS && DEBUG_SESSIONS) ses_dbg->dbg_cnt[working_session] = 0;
  if (MEASURE_LATENCY) start_measurement(latency_info, (uint32_t) working_session, t_id, op->opcode);

  //if (pull_ptr[[working_session]] == 100000) my_printf(yellow, "Working ses %u \n", working_session);
  //my_printf(yellow, "BEFORE: OP_i %u -> session %u, opcode: %u \n", op_i, working_session, ops[op_i].opcode);
  //my_printf(yellow, "Wrkr %u, session %u, opcode %u \n", t_id, working_session, op->opcode);
  *writes_num_ = writes_num, *reads_num_ = reads_num;
  if (ENABLE_CLIENTS) {
    signal_in_progress_to_client(op->session_id, op->index_to_req_array, t_id);
    if (ENABLE_ASSERTIONS) assert(interface[t_id].wrkr_pull_ptr[working_session] == op->index_to_req_array);
    MOD_ADD(interface[t_id].wrkr_pull_ptr[working_session], PER_SESSION_REQ_NUM);
  }
  debug_set_version_of_op_to_one(op, opcode, t_id);
  return false;
}

//
static inline uint32_t batch_requests_to_KVS(uint16_t t_id,
                                             uint32_t trace_iter, struct trace_command *trace,
                                             struct trace_op *ops,
                                             struct pending_ops *p_ops, struct kvs_resp *resp,
                                             struct latency_flags *latency_info,
                                             struct session_dbg *ses_dbg, uint16_t *last_session_,
                                             uint32_t *sizes_dbg_cntr)
{
  uint16_t writes_num = 0, reads_num = 0, op_i = 0, last_session = *last_session_;
  int working_session = -1;
  if (!ENABLE_CLIENTS && p_ops->all_sessions_stalled) {
    if (ENABLE_ASSERTIONS) debug_all_sessions(ses_dbg, p_ops, t_id);
    return trace_iter;
  }
  for (uint16_t i = 0; i < SESSIONS_PER_THREAD; i++) {
    uint16_t sess_i = (uint16_t)((last_session + i) % SESSIONS_PER_THREAD);
    if (pull_request_from_this_session(p_ops, sess_i, t_id)) {
      working_session = sess_i;
      break;
    }
    else if (ENABLE_ASSERTIONS) {
      debug_sessions(ses_dbg, p_ops, sess_i, t_id);
    }
  }
  //printf("working session = %d\n", working_session);
  if (ENABLE_CLIENTS) {
    if (working_session == -1) return trace_iter;
  }
  else if (ENABLE_ASSERTIONS ) assert(working_session != -1);

  bool passed_over_all_sessions = false;
  //if (!passed_over_all_sessions)
  //  my_printf(green, "Pulling from working session %d \n", working_session);


  while (op_i < MAX_OP_BATCH && !passed_over_all_sessions) {
    if (fill_trace_op(p_ops, &ops[op_i], trace_iter, trace, op_i, working_session, &writes_num,
                      &reads_num, ses_dbg, latency_info, sizes_dbg_cntr, t_id))
      break;
    // Find out next session to work on
    while (!pull_request_from_this_session(p_ops, (uint16_t) working_session, t_id)) {
      debug_sessions(ses_dbg, p_ops, (uint32_t) working_session, t_id);
      MOD_ADD(working_session, SESSIONS_PER_THREAD);
      if (working_session == last_session) {
        passed_over_all_sessions = true;
        if (!ENABLE_CLIENTS) p_ops->all_sessions_stalled = true;
        break;
      }
    }
    resp[op_i].type = EMPTY;
    op_i++;
    if (!ENABLE_CLIENTS) {
      trace_iter++;
      if (trace[trace_iter].opcode == NOP) trace_iter = 0;
    }
  }


  *last_session_ = (uint16_t) working_session;

  t_stats[t_id].cache_hits_per_thread += op_i;
  KVS_batch_op_trace(op_i, t_id, ops, resp, p_ops);
  //my_printf(cyan, "thread %d  adds %d/%d ops\n", t_id, op_i, MAX_OP_BATCH);
  for (uint16_t i = 0; i < op_i; i++) {
//    signal_in_progress_to_client(ops[i].session_id, ops[i].index_to_req_array, t_id);
    //printf("%u %u \n", i, ops[i].opcode);

    // my_printf(green, "After: OP_i %u -> session %u \n", i, *(uint32_t *) &ops[i]);
    if (resp[i].type == KVS_MISS)  {
      my_printf(green, "Cache_miss %u: bkt %u, server %u, tag %u \n", i,
                   ops[i].key.bkt, ops[i].key.server, ops[i].key.tag);
      assert(false);
      clean_up_on_KVS_miss(&ops[i], p_ops, latency_info, t_id);
      continue;
    }
    // check_version_after_batching_trace_to_cache(&ops[i], &resp[i], t_id);
    // Local reads
    if (resp[i].type == KVS_LOCAL_GET_SUCCESS) {
      //check_state_with_allowed_flags(2, interface[t_id].req_array[ops[i].session_id][ops[i].index_to_req_array].state, IN_PROGRESS_REQ);
      //assert(interface[t_id].req_array[ops[i].session_id][ops[i].index_to_req_array].state == IN_PROGRESS_REQ);
      signal_completion_to_client(ops[i].session_id, ops[i].index_to_req_array, t_id);
    }
    // Writes
    else if (resp[i].type == KVS_PUT_SUCCESS) {
      insert_write(p_ops, &ops[i], FROM_TRACE, 0, t_id);
      signal_completion_to_client(ops[i].session_id, ops[i].index_to_req_array, t_id);
    }
    // RMWS
    else if (ENABLE_RMWS && opcode_is_rmw(ops[i].opcode)) {
      insert_rmw(p_ops, &ops[i], &resp[i], t_id);
    }
    // CACHE_GET_SUCCESS: Acquires, out-of-epoch reads, CACHE_GET_TS_SUCCESS: Releases, out-of-epoch Writes
    else {
      check_state_with_allowed_flags(3, resp[i].type, KVS_GET_SUCCESS, KVS_GET_TS_SUCCESS);
      insert_read(p_ops, &ops[i], FROM_TRACE, t_id);
      if (ENABLE_ASSERTIONS && ops[i].opcode == KVS_OP_PUT) {
        p_ops->sess_info[ops[i].session_id].writes_not_yet_inserted++;
      }
    }
  }
  return trace_iter;
}




/* ---------------------------------------------------------------------------
//------------------------------ RMW------------------------------------------
//---------------------------------------------------------------------------*/

//------------------------------LOCAL RMWS SNOOP GLOBAL------------------------------------------

// Look at the kv_ptr to answer to a propose message-- kv pair lock is held when calling this
static inline uint8_t propose_snoops_entry(struct propose *prop, mica_op_t *kv_ptr, uint8_t m_id,
                                           uint16_t t_id, struct rmw_rep_last_committed *rep)
{
  uint8_t return_flag;
  if (ENABLE_ASSERTIONS)  {
    assert(prop->opcode == PROPOSE_OP);
    assert(prop->log_no > kv_ptr->last_committed_log_no);
    assert(prop->log_no == kv_ptr->log_no);
  }

  if (ENABLE_ASSERTIONS)
    assert(check_entry_validity_with_key(&prop->key, kv_ptr));
  enum ts_compare prop_ts_comp = compare_netw_ts_with_ts(&prop->ts, &kv_ptr->prop_ts);

  if (prop_ts_comp == GREATER) {
    assign_netw_ts_to_ts(&kv_ptr->prop_ts, &prop->ts);
    enum ts_compare acc_ts_comp = compare_netw_ts_with_ts(&prop->ts, &kv_ptr->accepted_ts);
    if (kv_ptr->state == ACCEPTED && acc_ts_comp == GREATER) {
      if (kv_ptr->rmw_id.glob_sess_id == prop->glob_sess_id) {
        return_flag = RMW_ACK_ACC_SAME_RMW;
      }
      else {
        assign_ts_to_netw_ts(&rep->ts, &kv_ptr->accepted_ts);
        return_flag = SEEN_LOWER_ACC;
        rep->rmw_id = kv_ptr->rmw_id.id;
        rep->glob_sess_id = kv_ptr->rmw_id.glob_sess_id;
        memcpy(rep->value, kv_ptr->last_accepted_value, (size_t) RMW_VALUE_SIZE);
        rep->log_no_or_base_version = kv_ptr->base_acc_ts.version;
        rep->base_m_id = kv_ptr->base_acc_ts.m_id;
      }
    }
    else return_flag = RMW_ACK;
  }
  else {
    return_flag = SEEN_HIGHER_PROP;
    assign_ts_to_netw_ts(&rep->ts, &kv_ptr->prop_ts);
  }

  check_state_with_allowed_flags(5, return_flag, RMW_ACK, RMW_ACK_ACC_SAME_RMW,
                                 SEEN_HIGHER_PROP, SEEN_LOWER_ACC);
  return return_flag;
}


// Look at an RMW entry to answer to an accept message-- kv pair lock is held when calling this
static inline uint8_t accept_snoops_entry(struct accept *acc, mica_op_t *kv_ptr, uint8_t sender_m_id,
                                           uint16_t t_id, struct rmw_rep_last_committed *rep)
{
  uint8_t return_flag = RMW_ACK;

  if (ENABLE_ASSERTIONS)  {
    assert(acc->opcode == ACCEPT_OP);
    assert(acc->log_no > kv_ptr->last_committed_log_no);
    assert(acc->log_no == kv_ptr->log_no);
    assert(check_entry_validity_with_key(&acc->key, kv_ptr));
  }

  if (kv_ptr->state != INVALID_RMW) {
    // Higher Ts  = Success,  Lower Ts  = Failure
    enum ts_compare ts_comp = compare_netw_ts_with_ts(&acc->ts, &kv_ptr->prop_ts);
    // Higher Ts  = Success
    if (ts_comp == EQUAL || ts_comp == GREATER) {
      return_flag = RMW_ACK;
      if (ENABLE_ASSERTIONS) {
        if (DEBUG_RMW && ts_comp == EQUAL && kv_ptr->state == ACCEPTED)
          my_printf(red, "Wrkr %u Received Accept for the same TS as already accepted, "
                       "version %u/%u m_id %u/%u, rmw_id %u/%u, global_sess_id %u/%u \n",
                     t_id, acc->ts.version, kv_ptr->prop_ts.version, acc->ts.m_id,
                     kv_ptr->prop_ts.m_id, acc->t_rmw_id, kv_ptr->rmw_id.id,
                     acc->glob_sess_id, kv_ptr->rmw_id.glob_sess_id);
      }
    }
    else if (ts_comp == SMALLER) {
      if (kv_ptr->state == PROPOSED) {
        //reply_rmw->ts = kv_ptr-> new_ts;
        return_flag = SEEN_HIGHER_PROP;
      }
      else if (kv_ptr->state == ACCEPTED) {
        //memcpy(reply_rmw->value, kv_ptr->value, (size_t) RMW_VALUE_SIZE);
        //reply_rmw->ts = kv_ptr->new_ts; // Here you dont respond with Accepted-TS but with Proposed-TS
        //assign_second_rmw_id_to_first(&reply_rmw->rmw_id, &kv_ptr->rmw_id);
        return_flag = SEEN_HIGHER_ACC;
      }
      else if (ENABLE_ASSERTIONS) assert(false);
      assign_ts_to_netw_ts(&rep->ts, &kv_ptr->prop_ts);
    }
    else if (ENABLE_ASSERTIONS) assert(false);
  }

  if (DEBUG_RMW)
      my_printf(yellow, "Wrkr %u: %s Accept with rmw_id %u, glob_sess_id %u, log_no: %u, ts.version: %u, ts_m_id %u,"
                        "locally stored state: %u, locally stored ts: version %u, m_id %u \n",
                    t_id, return_flag == RMW_ACK ? "Acks" : "Nacks",
                    acc->t_rmw_id,  acc->glob_sess_id, acc->log_no,
                      acc->ts.version, acc->ts.m_id, kv_ptr->state, kv_ptr->prop_ts.version,
                      kv_ptr->prop_ts.m_id);

  if (ENABLE_ASSERTIONS) assert(return_flag == RMW_ACK || rep->ts.version > 0);
  return return_flag;
}

//------------------------------ACCEPTING------------------------------------------


// After gathering a quorum of proposal acks, check if you can accept locally-- THIS IS STRICTLY LOCAL RMWS -- no helps
// Every RMW that gets committed must pass through this function successfully (at least one time)
static inline uint8_t attempt_local_accept(struct pending_ops *p_ops, struct rmw_local_entry *loc_entry,
                                           uint16_t t_id)
{
  uint8_t return_flag;
  mica_op_t *kv_ptr = loc_entry->kv_ptr;
  my_assert(keys_are_equal(&loc_entry->key, &kv_ptr->key),
            "Attempt local accept: Local entry does not contain the same key as global entry");

  if (ENABLE_ASSERTIONS) assert(loc_entry->rmw_id.glob_sess_id < GLOBAL_SESSION_NUM);
  // we need to change the global rmw structure, which means we need to lock the kv-pair.
  lock_seqlock(&loc_entry->kv_ptr->seqlock);
  if (if_already_committed_bcast_commits(p_ops, loc_entry, t_id)) {
  //if (loc_entry->rmw_id.id <= committed_glob_sess_rmw_id[loc_entry->rmw_id.glob_sess_id]) {
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
    return NACK_ALREADY_COMMITTED;
  }

  if (rmw_ids_are_equal(&loc_entry->rmw_id, &kv_ptr->rmw_id) &&
      kv_ptr->state != INVALID_RMW &&
      compare_ts(&loc_entry->new_ts, &kv_ptr->prop_ts) == EQUAL) {
    if (ENABLE_ASSERTIONS) {
      //assert(compare_ts(&loc_entry->new_ts, &kv_ptr->prop_ts) == EQUAL);
      assert(kv_ptr->log_no == loc_entry->log_no);
      assert(kv_ptr->last_committed_log_no == loc_entry->log_no - 1);
    }
    //state would be typically proposed, but may also be accepted if someone has helped
    if (DEBUG_RMW)
      my_printf(green, "Wrkr %u got rmw id %u, glob sess %u accepted locally \n",
                   t_id, loc_entry->rmw_id.id, loc_entry->rmw_id.glob_sess_id);
    kv_ptr->state = ACCEPTED;
    // calculate the new value depending on the type of RMW
    perform_the_rmw_on_the_loc_entry(loc_entry, kv_ptr, t_id);
    // we need to remember the last accepted value
    if (loc_entry->rmw_is_successful) {
      memcpy(kv_ptr->last_accepted_value, loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
    }
    else {
      memcpy(kv_ptr->last_accepted_value, loc_entry->value_to_read, (size_t) RMW_VALUE_SIZE);
    }
    //when last_accepted_value is update also update the acc_base_ts
    kv_ptr->base_acc_ts = kv_ptr->ts;
    loc_entry->accepted_log_no = kv_ptr->log_no;
    kv_ptr->accepted_ts = loc_entry->new_ts;
    kv_ptr->accepted_log_no = kv_ptr->log_no;
    if (ENABLE_ASSERTIONS) {
      assert(loc_entry->accepted_log_no == loc_entry->log_no);
      assert(loc_entry->log_no == kv_ptr->last_committed_log_no + 1);
      assert(compare_ts(&kv_ptr->prop_ts, &kv_ptr->accepted_ts) != SMALLER);
      kv_ptr->accepted_rmw_id = kv_ptr->rmw_id;
    }
    if (ENABLE_DEBUG_GLOBAL_ENTRY) {
      //kv_ptr->dbg->proposed_ts = loc_entry->new_ts;
      //kv_ptr->dbg->proposed_log_no = loc_entry->log_no;
      //kv_ptr->dbg->proposed_rmw_id = loc_entry->rmw_id;
    }
    check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept and succeed", t_id);
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
    return_flag = ACCEPT_ACK;
  }
  else { // the entry stores a different rmw_id and thus our proposal has been won by another
    // Some other RMW has won the RMW we are trying to get accepted
    // If the other RMW has been committed then the last_committed_log_no will be bigger/equal than the current log no
    if (kv_ptr->last_committed_log_no >= loc_entry->log_no)
      return_flag = NACK_ACCEPT_LOG_OUT_OF_DATE;
      //Otherwise the RMW that won is still in progress
    else return_flag = NACK_ACCEPT_SEEN_HIGHER_TS;
    if (DEBUG_RMW)
      my_printf(green, "Wrkr %u failed to get rmw id %u, glob sess %u accepted locally opcode %u,"
                     "global entry rmw id %u, glob sess %u, state %u \n",
                   t_id, loc_entry->rmw_id.id, loc_entry->rmw_id.glob_sess_id, return_flag,
                   kv_ptr->rmw_id.id, kv_ptr->rmw_id.glob_sess_id, kv_ptr->state);
    // --CHECKS--
    if (ENABLE_ASSERTIONS) {
      if (kv_ptr->state == PROPOSED || kv_ptr->state == ACCEPTED) {
        if(!(compare_ts(&kv_ptr->prop_ts, &loc_entry->new_ts) == GREATER ||
               kv_ptr->log_no > loc_entry->log_no)) {
          my_printf(red, "State: %s,  loc-entry-helping %d, Kv prop/ts %u/%u -- loc-entry ts %u/%u, "
            "kv-log/loc-log %u/%u kv-rmw_id/loc-rmw-id %u/%u\n",
                    kv_ptr->state == ACCEPTED ? "ACCEPTED" : "PROPOSED",
                    loc_entry->helping_flag,
                    kv_ptr->prop_ts.version, kv_ptr->prop_ts.m_id,
                    loc_entry->new_ts.version, loc_entry->new_ts.m_id,
                    kv_ptr->log_no, loc_entry->log_no,
                    kv_ptr->rmw_id.id, loc_entry->rmw_id.id);
          assert(false);
        }
      }
      else if (kv_ptr->state == INVALID_RMW) // some other rmw committed
        assert(kv_ptr->last_committed_log_no >= loc_entry->log_no);
      // The RMW can have been committed, because some may have helped it
      //assert (committed_glob_sess_rmw_id[loc_entry->rmw_id.glob_sess_id] < loc_entry->rmw_id.id);
    }


    check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept and fail", t_id);
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
  }
  return return_flag;
}

// After gathering a quorum of proposal reps, one of them was a lower TS accept, try and help it
static inline uint8_t attempt_local_accept_to_help(struct pending_ops *p_ops, struct rmw_local_entry *loc_entry,
                                                   uint16_t t_id)
{
  uint8_t return_flag;
  mica_op_t *kv_ptr = loc_entry->kv_ptr;
  struct rmw_local_entry* help_loc_entry = loc_entry->help_loc_entry;
  help_loc_entry->new_ts = loc_entry->new_ts;
  my_assert(keys_are_equal(&help_loc_entry->key, &kv_ptr->key),
            "Attempt local accpet to help: Local entry does not contain the same key as global entry");
  my_assert(loc_entry->help_loc_entry->log_no == loc_entry->log_no,
            " the help entry and the regular have not the same log nos");
  if (ENABLE_ASSERTIONS) assert(help_loc_entry->rmw_id.glob_sess_id < GLOBAL_SESSION_NUM);
  lock_seqlock(&loc_entry->kv_ptr->seqlock);

//  if (help_loc_entry->rmw_id.id <= committed_glob_sess_rmw_id[help_loc_entry->rmw_id.glob_sess_id]) {
//    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
//    //if (loc_entry->helping_flag == PROPOSE_NOT_LOCALLY_ACKED && t_id == 0)
//     // my_printf(cyan, "Sess %u aborts because the rmw-id is already registered \n", loc_entry->sess_id);
//    return ABORT_HELP;
//  }
  //We don't need to check the registered RMW here -- it's not wrong to do so--
  // but if the RMW has been committed, it will be in the present log_no
  // and we will not be able to accept locally anyway.

  // the kv_ptr has seen a higher ts if
  // (its state is not invalid and its TS is higher) or  (it has committed the log)
  enum ts_compare comp = compare_ts(&kv_ptr->prop_ts, &loc_entry->new_ts);


  bool kv_ptr_is_the_same = kv_ptr->state == PROPOSED  &&
                                help_loc_entry->log_no == kv_ptr->log_no &&
                                comp == EQUAL &&
                                rmw_ids_are_equal(&kv_ptr->rmw_id, &loc_entry->rmw_id);

  bool kv_ptr_is_invalid_but_not_committed = kv_ptr->state == INVALID_RMW &&
    kv_ptr->last_committed_log_no < help_loc_entry->log_no;// && comp != GREATER;

  bool helping_stuck_accept = loc_entry->helping_flag == PROPOSE_NOT_LOCALLY_ACKED &&
                              help_loc_entry->log_no == kv_ptr->log_no &&
                              kv_ptr->state == ACCEPTED &&
                              rmw_ids_are_equal(&kv_ptr->rmw_id, &help_loc_entry->rmw_id) &&
                              comp != GREATER;
  // When retrying after accepts fail, i must first send proposes but if the local state is still accepted,
  // i can't downgrade it to proposed, so if i am deemed to help another RMW, i may come back to find
  // my original Accept still here
  bool propose_locally_accepted = kv_ptr->state == ACCEPTED  &&
                                  loc_entry->log_no == kv_ptr->log_no &&
                                  comp == EQUAL &&
                                  rmw_ids_are_equal(&kv_ptr->rmw_id, &loc_entry->rmw_id);

  if (kv_ptr_is_the_same   || kv_ptr_is_invalid_but_not_committed ||
      helping_stuck_accept || propose_locally_accepted) {
    checks_and_prints_local_accept_help(loc_entry, help_loc_entry, kv_ptr, kv_ptr_is_the_same,
                                        kv_ptr_is_invalid_but_not_committed,
                                        helping_stuck_accept, propose_locally_accepted, t_id);
    kv_ptr->state = ACCEPTED;
    assign_second_rmw_id_to_first(&kv_ptr->rmw_id, &help_loc_entry->rmw_id);
    kv_ptr->accepted_ts = help_loc_entry->new_ts;
    kv_ptr->accepted_log_no = kv_ptr->log_no;
    if (ENABLE_ASSERTIONS) {
      assert(compare_ts(&kv_ptr->prop_ts, &kv_ptr->accepted_ts) != SMALLER);
      kv_ptr->accepted_rmw_id = kv_ptr->rmw_id;
    }
    memcpy(kv_ptr->last_accepted_value, help_loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
    kv_ptr->base_acc_ts = help_loc_entry->base_ts;// the base ts of the RMW we are helping
    check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept_to_help and succeed", t_id);
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
    return_flag = ACCEPT_ACK;
  }
  else {
    //If the log entry is committed locally abort the help
    //if the global entry has accepted a higher TS then again abort help
   return_flag = ABORT_HELP;
    if (DEBUG_RMW)// || (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED))
      my_printf(green, "Wrkr %u sess %u failed to get rmw id %u, glob sess %u accepted locally opcode %u,"
                     "global entry rmw id %u, glob sess %u, state %u \n",
                   t_id, loc_entry->sess_id, loc_entry->rmw_id.id, loc_entry->rmw_id.glob_sess_id, return_flag,
                   kv_ptr->rmw_id.id, kv_ptr->rmw_id.glob_sess_id, kv_ptr->state);


    check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept_to_help and fail", t_id);
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
  }
  return return_flag;
}

//------------------------------ COMMITING------------------------------------------

// Call holding the kv_lock to commit an RMW that is initiated locally (helped or from local session)
static inline void commit_helped_or_local_from_loc_entry(mica_op_t *kv_ptr,
                                                         struct rmw_local_entry *loc_entry,
                                                         struct rmw_local_entry *loc_entry_to_commit, uint16_t t_id)
{
  // If the RMW has not been committed yet locally, commit it if it is not helping,
  // Otherwise, stay in 'accepted' state until a quorum of commit-acks come back, then commit
  if (kv_ptr->last_committed_log_no < loc_entry_to_commit->log_no) {
    take_actions_to_commit_rmw(loc_entry_to_commit, loc_entry, t_id);
    if (DEBUG_RMW)
      my_printf(green, "Wrkr %u got rmw id %u, kv_ptr sess %u, log %u committed locally,"
                     "kv_ptr stats: state %u, rmw_id &u, glob sess_id %u, log no %u  \n",
                   t_id, loc_entry_to_commit->rmw_id.id, loc_entry_to_commit->rmw_id.glob_sess_id, loc_entry_to_commit->log_no,
                   kv_ptr->state, kv_ptr->rmw_id.id, kv_ptr->rmw_id.glob_sess_id, kv_ptr->log_no);
  }
  // Check if the entry is still working on that log, or whether it has moved on
  // (because the current rmw is already committed)
  if (kv_ptr->log_no == loc_entry_to_commit->log_no && kv_ptr->state != INVALID_RMW) {
    //the log numbers should match
    if (ENABLE_ASSERTIONS) {
      if (!rmw_ids_are_equal(&loc_entry_to_commit->rmw_id, &kv_ptr->rmw_id)) {
        my_printf(red, "Wrkr %u kv_ptr is on same log as what is about to be committed but on different rmw-id:"
                     " committed rmw id %u, glob sess %u, "
                     "kv_ptr rmw id %u, glob sess %u, state %u,"
                     " committed version %u/%u m_id %u/%u \n",
                   t_id, loc_entry_to_commit->rmw_id.id, loc_entry_to_commit->rmw_id.glob_sess_id,
                   kv_ptr->rmw_id.id, kv_ptr->rmw_id.glob_sess_id, kv_ptr->state,
                   loc_entry_to_commit->new_ts.version, kv_ptr->prop_ts.version,
                   loc_entry_to_commit->new_ts.m_id, kv_ptr->prop_ts.m_id);// this is a hard error
      }
      assert(rmw_ids_are_equal(&loc_entry_to_commit->rmw_id, &kv_ptr->last_committed_rmw_id));
      assert(kv_ptr->last_committed_log_no == kv_ptr->log_no);
      if (kv_ptr->state != ACCEPTED) {
        my_printf(red, "Wrkr %u, sess %u  Logs are equal, rmw-ids are equal "
          "but state is not accepted %u \n", t_id, loc_entry->sess_id, kv_ptr->state);
        assert(false);
      }
      assert(kv_ptr->state == ACCEPTED);
    }
    kv_ptr->state = INVALID_RMW;
  }
  else {
    // if the log has moved on then the RMW has been helped,
    // it has been committed in the other machines so there is no need to change its state
    check_log_nos_of_kv_ptr(kv_ptr, "commit_helped_or_local_from_loc_entry", t_id);
    if (ENABLE_ASSERTIONS) {
      if (kv_ptr->state != INVALID_RMW)
        assert(!rmw_ids_are_equal(&kv_ptr->rmw_id, &loc_entry_to_commit->rmw_id));
    }
  }
}


// The commitment of the rmw_id is certain here: it has either already been committed or it will be committed here
// Additionally we will always broadcast commits because we need to make sure that
// a quorum of machines have seen the RMW before, committing the RMW
static inline void attempt_local_commit(struct pending_ops *p_ops, struct rmw_local_entry *loc_entry,
                                        uint16_t t_id)
{
  assert(loc_entry->helping_flag != HELP_PREV_COMMITTED_LOG_TOO_HIGH);
  // use only index_to_rmw and kv_ptr from the loc_entry
  struct rmw_local_entry *loc_entry_to_commit =
    loc_entry->helping_flag == HELPING ? loc_entry->help_loc_entry : loc_entry;


  mica_op_t *kv_ptr = loc_entry->kv_ptr;
  my_assert(keys_are_equal(&loc_entry->key, &kv_ptr->key),
            "Attempt local commit: Local entry does not contain the same key as global entry");
  // we need to change the global rmw structure, which means we need to lock the kv-pair.
  lock_seqlock(&loc_entry->kv_ptr->seqlock);
  if (loc_entry->state == COMMITTED)
    commit_helped_or_local_from_loc_entry(kv_ptr, loc_entry, loc_entry_to_commit, t_id);
  // Register the RMW-id
  register_committed_global_sess_id(loc_entry_to_commit->rmw_id.glob_sess_id,
                                    loc_entry_to_commit->rmw_id.id, t_id);
  check_registered_against_kv_ptr_last_committed(kv_ptr, loc_entry_to_commit->rmw_id.id,
                                                 loc_entry_to_commit->rmw_id.glob_sess_id,
                                                 "attempt_local_commit", t_id);

  unlock_seqlock(&loc_entry->kv_ptr->seqlock);
  if (DEBUG_RMW)
    my_printf(green, "Wrkr %u will broadcast commits for rmw id %u, glob sess %u, "
                 "global entry rmw id %u, glob sess %u, state %u \n",
                 t_id, loc_entry_to_commit->rmw_id.id, loc_entry_to_commit->rmw_id.glob_sess_id,
                 kv_ptr->rmw_id.id, kv_ptr->rmw_id.glob_sess_id, kv_ptr->state);

  if (DEBUG_LOG)
    my_printf(green, "Log %u: RMW_id %u glob_sess %u, loc entry state %u, local commit\n",
                 loc_entry_to_commit->log_no, loc_entry_to_commit->rmw_id.id,
                 loc_entry_to_commit->rmw_id.glob_sess_id, loc_entry->state);
}

// A reply to a propose/accept may include an RMW to be committed,
static inline void attempt_local_commit_from_rep(struct pending_ops *p_ops, struct rmw_rep_last_committed *rmw_rep,
                                                 struct rmw_local_entry* loc_entry, uint16_t t_id)
{
  check_ptr_is_valid_rmw_rep(rmw_rep);
  mica_op_t *kv_ptr = loc_entry->kv_ptr;
  uint32_t new_log_no = rmw_rep->log_no_or_base_version;
  uint64_t new_rmw_id = rmw_rep->rmw_id;
  uint16_t new_glob_sess_id = rmw_rep->glob_sess_id;
  if (kv_ptr->last_committed_log_no >= new_log_no) return;
  my_assert(keys_are_equal(&loc_entry->key, &kv_ptr->key),
            "Attempt local commit from rep: Local entry does not contain the same key as global entry");

  // we need to change the global rmw structure, which means we need to lock the kv-pair.
  lock_seqlock(&kv_ptr->seqlock);
  // If the RMW has not been committed yet locally, commit it
  if (kv_ptr->last_committed_log_no < new_log_no) {
    //if (ENABLE_DEBUG_GLOBAL_ENTRY) kv_ptr->dbg->last_committed_flag = REMOTE_RMW_FROM_REP;
    kv_ptr->last_committed_log_no = new_log_no;
    kv_ptr->last_committed_rmw_id.id = new_rmw_id;
    kv_ptr->last_committed_rmw_id.glob_sess_id = new_glob_sess_id;
    update_commit_logs(t_id, kv_ptr->key.bkt, new_log_no, kv_ptr->value,
                       rmw_rep->value, "From rep ", LOG_COMS);

    if (compare_ts_with_netw_ts(&kv_ptr->ts, &rmw_rep->ts) != GREATER) {
      memcpy(kv_ptr->value, rmw_rep->value, (size_t) RMW_VALUE_SIZE);
      kv_ptr->ts.m_id = rmw_rep->ts.m_id;
      kv_ptr->ts.version = rmw_rep->ts.version;
    }


    if (DEBUG_RMW)
      my_printf(green, "Wrkr %u commits locally rmw id %u, glob sess %u after resp with opcode %u \n",
                   t_id, new_rmw_id, new_glob_sess_id, rmw_rep->opcode);
    // if the kv_ptr was working on an already committed log, or
    // if it's not active advance its log no and in both cases transition to INVALID RMW
    if (kv_ptr->log_no <= new_log_no ||
        rmw_id_is_equal_with_id_and_glob_sess_id(&kv_ptr->rmw_id, new_rmw_id, new_glob_sess_id)) {
      kv_ptr->log_no = new_log_no;
      kv_ptr->state = INVALID_RMW;
    }
  }
  else if (kv_ptr->last_committed_log_no == new_log_no) {
    check_that_the_rmw_ids_match(kv_ptr,  new_rmw_id, new_glob_sess_id, new_log_no, rmw_rep->ts.version,
                                 rmw_rep->ts.m_id, "attempt_local_commit_from_rep", t_id);
  }
  check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_commit_from_rep", t_id);
  check_local_commit_from_rep(kv_ptr, loc_entry, rmw_rep, t_id);

  register_committed_global_sess_id(new_glob_sess_id, new_rmw_id, t_id);
  check_registered_against_kv_ptr_last_committed(kv_ptr, new_rmw_id, new_glob_sess_id,
                                                 "attempt_local_commit_from_rep", t_id);
  unlock_seqlock(&kv_ptr->seqlock);


  if (DEBUG_LOG)
    my_printf(green, "Log %u: RMW_id %u glob_sess %u, loc entry state %u from reply \n",
                 new_log_no, new_rmw_id, new_glob_sess_id, loc_entry->state);

}

// Check if the commit must be applied to the KVS and
// transition the kv_ptr to INVALID_RMW if it has been waiting for this commit
static inline bool attempt_remote_commit(mica_op_t *kv_ptr, struct commit *com,
                                         struct read_info *r_info, bool use_commit,
                                         uint16_t t_id)
{
  uint32_t new_log_no = use_commit ? com->log_no : r_info->log_no;
  uint64_t new_rmw_id = use_commit ? com->t_rmw_id : r_info->rmw_id.id;
  uint16_t glob_sess_id = use_commit ? com->glob_sess_id : r_info->rmw_id.glob_sess_id;
  uint32_t new_version = use_commit ? com->base_ts.version : r_info->ts_to_read.version;
  uint8_t new_m_id = use_commit ? com->base_ts.m_id : r_info->ts_to_read.m_id;
  // the function is called with the lock in hand
  bool is_log_higher = false;
  // First check if that log no (or a higher) has been committed
  if (kv_ptr->last_committed_log_no < new_log_no) {
    is_log_higher = true;
    //if (ENABLE_DEBUG_GLOBAL_ENTRY) kv_ptr->dbg->last_committed_flag = REMOTE_RMW;
    kv_ptr->last_committed_log_no = new_log_no;
    kv_ptr->last_committed_rmw_id.id = new_rmw_id;
    kv_ptr->last_committed_rmw_id.glob_sess_id = glob_sess_id;
    if (DEBUG_LOG)
      my_printf(green, "Log %u: RMW_id %u glob_sess %u ,from remote_commit \n",
                   new_log_no, new_rmw_id, glob_sess_id);
  }
  else if (kv_ptr->last_committed_log_no == new_log_no) {
    check_that_the_rmw_ids_match(kv_ptr,  new_rmw_id, glob_sess_id, new_log_no,
                                 new_version, new_m_id, "attempt_remote_commit", t_id);
  }

  // now check if the entry was waiting for this message to get cleared
  if (kv_ptr->state != INVALID_RMW && (kv_ptr->log_no <= new_log_no ||
     rmw_id_is_equal_with_id_and_glob_sess_id(&kv_ptr->rmw_id, new_rmw_id, glob_sess_id))) {
    kv_ptr->state = INVALID_RMW;
  }
  else if (kv_ptr->log_no > new_log_no && kv_ptr->state != INVALID_RMW) {
    if (kv_ptr->rmw_id.id == new_rmw_id && kv_ptr->rmw_id.glob_sess_id == glob_sess_id)
      my_printf(red, "Wrkr %u, committed rmw_id %u and glob ses id %u on log no %u, but the kv_ptr is working on "
                   "log %u, state %u rmw_id %u glob sess id %u \n", t_id, new_rmw_id, glob_sess_id, new_log_no,
                 kv_ptr->log_no, kv_ptr->state, kv_ptr->rmw_id.id,  kv_ptr->rmw_id.glob_sess_id);
  }
  return is_log_higher;
}

static inline uint64_t handle_remote_commit_message(mica_op_t *kv_ptr, void* op, bool use_commit, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) if (!use_commit) assert(ENABLE_RMW_ACQUIRES);
  bool is_log_higher;
  struct read_info * r_info = (struct read_info*) op;
  struct commit *com = (struct commit*) op;
  uint64_t rmw_l_id = !use_commit ? r_info->rmw_id.id : com->t_rmw_id;
  uint16_t glob_sess_id = !use_commit ? r_info->rmw_id.glob_sess_id : com->glob_sess_id;
  uint32_t log_no = !use_commit ? r_info->log_no : com->log_no;
  struct key *key = !use_commit ? &r_info->key : &com->key;
  uint8_t *value = !use_commit ? r_info->value : com->value;
  uint32_t version = !use_commit ? r_info->ts_to_read.version : com->base_ts.version;
  uint8_t m_id = !use_commit ? r_info->ts_to_read.m_id : com->base_ts.m_id;
  uint32_t entry;

  lock_seqlock(&kv_ptr->seqlock);
  check_keys_with_one_trace_op(key, kv_ptr);
  is_log_higher = attempt_remote_commit(kv_ptr, com, r_info, use_commit, t_id);

//  else if (ENABLE_ASSERTIONS) assert(false);
  // The commit must be applied to the KVS if
  //   1. the commit has a higher base-ts than the kv or
  //   2. the commit has the same base with the kv, but higher log
  // But if the kv-ptr has a greater base-ts, then its value should not be overwritten
  if (is_log_higher) {
    if (compare_ts_with_flat(&kv_ptr->ts, version, m_id) != GREATER) {
      kv_ptr->ts.m_id = m_id;
      kv_ptr->ts.version = version;
      memcpy(kv_ptr->value, value, (size_t) RMW_VALUE_SIZE);
    }
    update_commit_logs(t_id, kv_ptr->key.bkt, log_no, kv_ptr->value,
                       value, "From remote commit ", LOG_COMS);
  }
  check_log_nos_of_kv_ptr(kv_ptr, "Unlocking after received commit", t_id);
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->state != INVALID_RMW)
      assert(!rmw_id_is_equal_with_id_and_glob_sess_id(&kv_ptr->rmw_id, rmw_l_id, glob_sess_id));
  }
  uint64_t number_of_reqs = 0;
  if (ENABLE_DEBUG_GLOBAL_ENTRY) {
    //kv_ptr->dbg->prop_acc_num++;
    //number_of_reqs = kv_ptr->.dbg->prop_acc_num;
  }
  register_committed_global_sess_id (glob_sess_id, rmw_l_id, t_id);
  check_registered_against_kv_ptr_last_committed(kv_ptr, rmw_l_id, glob_sess_id,
                                                 "handle remote commit", t_id);

  unlock_seqlock(&kv_ptr->seqlock);
  return number_of_reqs;

}

// On gathering quorum of acks for commit, commit locally and signal that the session must be freed if not helping
static inline void act_on_quorum_of_commit_acks(struct pending_ops *p_ops, uint32_t ack_ptr, uint16_t t_id)
{
  struct rmw_local_entry *loc_entry = &p_ops->prop_info->entry[p_ops->w_meta[ack_ptr].sess_id];
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry != NULL);
    assert(loc_entry->state = COMMITTED);
  }

  if (loc_entry->helping_flag == HELPING &&
    rmw_ids_are_equal(&loc_entry->help_loc_entry->rmw_id, &loc_entry->rmw_id))
    loc_entry->helping_flag = HELPING_MYSELF;

  if (loc_entry->helping_flag != HELP_PREV_COMMITTED_LOG_TOO_HIGH)
    attempt_local_commit(p_ops, loc_entry, t_id);

  switch(loc_entry->helping_flag)
  {
    case NOT_HELPING:
    case PROPOSE_NOT_LOCALLY_ACKED:
    case HELPING_MYSELF:
    case PROPOSE_LOCALLY_ACCEPTED:
      loc_entry->state = INVALID_RMW;
      free_session(p_ops, loc_entry->sess_id, true, t_id);
      break;
    case HELPING:
      reinstate_loc_entry_after_helping(loc_entry, t_id);
      break;
    case HELP_PREV_COMMITTED_LOG_TOO_HIGH:
      //if (loc_entry->helping_flag == HELP_PREV_COMMITTED_LOG_TOO_HIGH)
      //  my_printf(yellow, "Wrkr %u, sess %u, rmw-id %u, sess stalled %d \n",
      //  t_id, loc_entry->sess_id, loc_entry->rmw_id.id, p_ops->sess_info[loc_entry->sess_id].stalled);
      loc_entry->state = PROPOSED;
      loc_entry->helping_flag = NOT_HELPING;

      break;
    default: if (ENABLE_ASSERTIONS) assert(false);
  }
}

//------------------------------REMOTE RMW REQUESTS------------------------------------------

//Handle a remote propose/accept whose log number is big enough
static inline uint8_t handle_remote_prop_or_acc_in_kvs(mica_op_t *kv_ptr, void *prop_or_acc,
                                                       uint8_t sender_m_id, uint16_t t_id,
                                                       struct rmw_rep_last_committed *rep, uint32_t log_no,
                                                       bool is_prop)
{
  uint8_t flag;
  // if the log number is higher than expected blindly ack
  if (log_no > kv_ptr->log_no) {
    check_that_log_is_high_enough(kv_ptr, log_no);
    flag = RMW_ACK;
  }
  else
    flag = is_prop ? propose_snoops_entry((struct propose *)prop_or_acc, kv_ptr, sender_m_id, t_id, rep) :
                     accept_snoops_entry((struct accept *) prop_or_acc, kv_ptr, sender_m_id, t_id, rep);
  return flag;
}


//------------------------------HANDLE REPLIES------------------------------------------
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
    local_rmw_ack(loc_entry);
    check_loc_entry_metadata_is_reset(loc_entry, "act_on_quorum_of_prop_acks", t_id);
    if (ENABLE_ASSERTIONS) assert(loc_entry->rmw_id.id < B_4);
    insert_accept_in_writes_message_fifo(p_ops, loc_entry, false, t_id);
    if (ENABLE_ASSERTIONS) {
      assert(glob_ses_id_to_t_id(loc_entry->rmw_id.glob_sess_id) == t_id &&
             glob_ses_id_to_m_id(loc_entry->rmw_id.glob_sess_id) == machine_id);
      assert(loc_entry->state == PROPOSED);
    }
    loc_entry->state = ACCEPTED;
    loc_entry->killable = false;
  }
  else if (local_state == NACK_ALREADY_COMMITTED) {
    //
    //printf("Already-committed when accepting locally--bcast commits \n");
    //loc_entry->state = INVALID_RMW;
    //free_session(p_ops, loc_entry->sess_id, true, t_id);
  }
  else loc_entry->state = NEEDS_KV_PTR;
}



// Handle a proposal/accept reply
static inline void handle_prop_or_acc_rep(struct pending_ops *p_ops, struct rmw_rep_message *rep_mes,
                                          struct rmw_rep_last_committed *rep,
                                          struct rmw_local_entry *loc_entry,
                                          bool is_accept,
                                          const uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (is_accept) assert(loc_entry->state == ACCEPTED);
    else {
      assert(loc_entry->rmw_reps.tot_replies > 0);
      assert(loc_entry->state == PROPOSED);
      // this checks that the performance optimization of NO-op reps is valid
      assert(rep->opcode != NO_OP_PROP_REP);
      check_state_with_allowed_flags(4, loc_entry->helping_flag, NOT_HELPING,
                                     PROPOSE_NOT_LOCALLY_ACKED, PROPOSE_LOCALLY_ACCEPTED);
    }
  }
  loc_entry->rmw_reps.tot_replies++;
  if (rep->opcode > RMW_ACK_ACC_SAME_RMW) loc_entry->rmw_reps.nacks++;
  switch (rep->opcode) {
    case RMW_ACK:
    case RMW_ACK_ACC_SAME_RMW:
      loc_entry->rmw_reps.acks++;
      if (ENABLE_ASSERTIONS)
        assert(rep_mes->m_id < MACHINE_NUM && rep_mes->m_id != machine_id);
      if (DEBUG_RMW)
        my_printf(green, "Wrkr %u, the received rep is an %s ack, "
          "total acks %u \n", t_id, is_accept ? "acc" : "prop",
                  loc_entry->rmw_reps.acks);
      break;
    case LOG_TOO_HIGH:
      loc_entry->rmw_reps.log_too_high++;
      break;
    case RMW_ID_COMMITTED:
      loc_entry->rmw_reps.no_need_to_bcast = true;
    case RMW_ID_COMMITTED_SAME_LOG:
      loc_entry->rmw_reps.rmw_id_commited++;
      if (loc_entry->helping_flag != HELPING)
        loc_entry->log_no = loc_entry->accepted_log_no;
      attempt_local_commit(p_ops, loc_entry, t_id);
      //attempt_local_commit_from_rep(p_ops, rep, loc_entry, t_id);
      // store the reply in the help loc_entry
      //store_rmw_rep_to_help_loc_entry(loc_entry, rep, t_id);
      break;
    case LOG_TOO_SMALL:
      loc_entry->rmw_reps.log_too_small++;
      attempt_local_commit_from_rep(p_ops, rep, loc_entry, t_id);
      break;
    case SEEN_LOWER_ACC:
      loc_entry->rmw_reps.already_accepted++;
      if (ENABLE_ASSERTIONS) {
        assert(compare_netw_ts_with_ts(&rep->ts, &loc_entry->new_ts) == SMALLER);
        assert(rep->rmw_id < B_4);
        assert(!is_accept);
      }
      // Store the accepted rmw only if no higher priority reps have been seen
      if (!loc_entry->rmw_reps.seen_higher_prop_acc +
          (loc_entry->rmw_reps.rmw_id_commited + loc_entry->rmw_reps.log_too_small == 0)) {
          store_rmw_rep_to_help_loc_entry(loc_entry, rep, t_id);
      }
      break;
    case SEEN_HIGHER_ACC:
    case SEEN_HIGHER_PROP:
      // make sure no lower accepts will be helped, useful only for proposes
      //loc_entry->rmw_reps.seen_higher_acc = true;

      loc_entry->rmw_reps.seen_higher_prop_acc++;
      if (DEBUG_RMW)
        my_printf(yellow, "Wrkr %u: the %s rep is %u, %u sum of all other reps %u \n", t_id,
                  is_accept ? "acc" : "prop",rep->opcode,
                  loc_entry->rmw_reps.seen_higher_prop_acc,
                  loc_entry->rmw_reps.rmw_id_commited + loc_entry->rmw_reps.log_too_small +
                  loc_entry->rmw_reps.already_accepted);
       if (rep->ts.version > loc_entry->rmw_reps.seen_higher_prop_version) {
        loc_entry->rmw_reps.seen_higher_prop_version = rep->ts.version;
        if (DEBUG_RMW)
          my_printf(yellow, "Wrkr %u: overwriting the TS version %u \n",
          t_id, loc_entry->rmw_reps.seen_higher_prop_version);
      }
      break;
    default:
      if (ENABLE_ASSERTIONS) assert(false);
  }

  if (ENABLE_ASSERTIONS)
    check_sum_of_reps(loc_entry);
}


// Handle one accept or propose reply
static inline void handle_single_rmw_rep(struct pending_ops *p_ops, struct rmw_rep_last_committed *rep,
                                         struct rmw_rep_message *rep_mes, uint16_t byte_ptr,
                                         bool is_accept, uint16_t r_rep_i, uint16_t t_id)
{
  struct prop_info *prop_info = p_ops->prop_info;
  if (ENABLE_ASSERTIONS) {
    if (!opcode_is_rmw_rep(rep->opcode)) {
      printf("Rep_i %u, current opcode %u first opcode: %u, byte_ptr %u \n",
             r_rep_i, rep->opcode, rep_mes->rmw_rep[0].opcode, byte_ptr);
    }
    assert(opcode_is_rmw_rep(rep->opcode));
    if (prop_info->l_id <= rep->l_id)
      my_printf(red, "Wrkr %u, rep_i %u, opcode %u, is_accept %d, incoming rep l_id %u, max prop lid %u \n",
                 t_id, r_rep_i, rep->opcode, is_accept, rep->l_id, prop_info->l_id);

    assert(prop_info->l_id > rep->l_id);
  }
  //my_printf(cyan, "RMW rep opcode %u, l_id %u \n", rep->opcode, rep->l_id);
  int entry_i = search_prop_entries_with_l_id(prop_info, (uint8_t) (is_accept ? ACCEPTED : PROPOSED),
                                              rep->l_id);
  if (entry_i == -1) return;
  struct rmw_local_entry *loc_entry = &prop_info->entry[entry_i];
  if (unlikely(rep->opcode) > NO_OP_PROP_REP) {
    increment_epoch_id(loc_entry->epoch_id, t_id);
    rep->opcode -= FALSE_POSITIVE_OFFSET;
    loc_entry->fp_detected = true;
  }
  handle_prop_or_acc_rep(p_ops, rep_mes, rep, loc_entry, is_accept, t_id);
//  if (!is_accept) handle_prop_or_acc_rep(p_ops, rep_mes, rep, loc_entry, t_id);
//  else handle_accept_reply(p_ops, rep_mes, rep, loc_entry, t_id);
}

// Handle read replies that refer to RMWs (either replies to accepts or proposes)
static inline void handle_rmw_rep_replies(struct pending_ops *p_ops, struct r_rep_message *r_rep_mes,
                                          bool is_accept, uint16_t t_id)
{
  struct rmw_rep_message *rep_mes = (struct rmw_rep_message *) r_rep_mes;
  check_state_with_allowed_flags(4, r_rep_mes->opcode, ACCEPT_REPLY,
                                 PROP_REPLY, ACCEPT_REPLY_NO_CREDITS);
  uint8_t rep_num = rep_mes->coalesce_num;
  //my_printf(yellow, "Received opcode %u, prop_rep num %u \n", r_rep_mes->opcode, rep_num);
  uint16_t byte_ptr = R_REP_MES_HEADER; // same for both accepts and replies
  for (uint16_t r_rep_i = 0; r_rep_i < rep_num; r_rep_i++) {
    struct rmw_rep_last_committed *rep = (struct rmw_rep_last_committed *) (((void *) rep_mes) + byte_ptr);
    handle_single_rmw_rep(p_ops, rep, rep_mes, byte_ptr, is_accept, r_rep_i, t_id);
    byte_ptr += get_size_from_opcode(rep->opcode);
  }
  r_rep_mes->opcode = INVALID_OPCODE;
}

// When a quorum of prop replies have been received, and one of the replies says it has accepted an RMW with lower TS
static inline void act_on_receiving_already_accepted_rep_to_prop(struct pending_ops *p_ops,
                                                                 struct rmw_local_entry* loc_entry,
                                                                 uint32_t* new_version,
                                                                 uint16_t t_id)
{
  struct rmw_local_entry* help_loc_entry = loc_entry->help_loc_entry;
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->log_no == help_loc_entry->log_no);
    assert(loc_entry->help_loc_entry->state == ACCEPTED);
    assert(compare_ts(&help_loc_entry->new_ts, &loc_entry->new_ts) == SMALLER);
    //assert(loc_entry->rmw_reps.acks < REMOTE_QUORUM);
    //assert(ts_comp != EQUAL);
  }
  // help the accepted
  uint8_t flag = attempt_local_accept_to_help(p_ops, loc_entry, t_id);
  if (flag == ACCEPT_ACK) {
    loc_entry->helping_flag = HELPING;
    loc_entry->state = ACCEPTED;
    zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
    local_rmw_ack(loc_entry);
    if (ENABLE_ASSERTIONS) assert(help_loc_entry->rmw_id.id < B_4);
    insert_accept_in_writes_message_fifo(p_ops, help_loc_entry, true, t_id);
  }
  else { // abort the help, on failing to accept locally
    loc_entry->state = NEEDS_KV_PTR;
    help_loc_entry->state = INVALID_RMW;
  }

}


//------------------------------HELP STUCK RMW------------------------------------------
// When inspecting an RMW that failed to grab a kv_ptr in the past
static inline bool attempt_to_grab_kv_ptr_after_waiting(struct pending_ops *p_ops,
                                                        mica_op_t *kv_ptr,
                                                        struct rmw_local_entry *loc_entry,
                                                        uint16_t sess_i, uint16_t t_id)
{
  bool kv_ptr_was_grabbed = false;
  bool rmw_fails = false;
  uint32_t version = PAXOS_TS;
  if (ENABLE_ASSERTIONS) assert(loc_entry->rmw_id.glob_sess_id < GLOBAL_SESSION_NUM);
  lock_seqlock(&kv_ptr->seqlock);
  if (if_already_committed_bcast_commits(p_ops, loc_entry, t_id)) {
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
    return true;
  }
  if (kv_ptr->state == INVALID_RMW) {
    if (!rmw_fails_with_loc_entry(loc_entry, kv_ptr, &rmw_fails, t_id)) {
      if (ENABLE_ASSERTIONS) assert(kv_ptr->rmw_id.glob_sess_id < GLOBAL_SESSION_NUM);
      if (ENABLE_ASSERTIONS && kv_ptr->state != INVALID_RMW &&
          committed_glob_sess_rmw_id[kv_ptr->rmw_id.glob_sess_id] >= kv_ptr->rmw_id.id) {
        //my_printf(red, "Wrkr: %u waiting on an rmw id %u/%u glob_sess_id %u that has been committed, so we free it"
        //             "last committed rmw id %u , glob sess id %u, "
        //             "state %u, committed log/log %u/%u, version %u \n",
        //           t_id, kv_ptr->rmw_id.id, committed_glob_sess_rmw_id[kv_ptr->rmw_id.glob_sess_id],
        //           kv_ptr->rmw_id.glob_sess_id, kv_ptr->last_committed_rmw_id.id,
        //           kv_ptr->last_committed_rmw_id.glob_sess_id,
        //           kv_ptr->state, kv_ptr->last_committed_log_no,
        //           kv_ptr->log_no, kv_ptr->new_ts.version);
      }
      loc_entry->log_no = kv_ptr->last_committed_log_no + 1;
      activate_RMW_entry(PROPOSED, PAXOS_TS, kv_ptr, loc_entry->opcode,
                         (uint8_t) machine_id, loc_entry->rmw_id.id,
                         loc_entry->rmw_id.glob_sess_id, loc_entry->log_no, t_id,
                         ENABLE_ASSERTIONS ? "attempt_to_grab_kv_ptr_after_waiting" : NULL);

      kv_ptr_was_grabbed = true;
      if (DEBUG_RMW)
        my_printf(yellow, "Wrkr %u, after waiting for %u cycles, session %u  \n",
                      t_id, loc_entry->back_off_cntr, loc_entry->sess_id);
    }
  }
  else if (kv_ptr_state_has_changed(kv_ptr, loc_entry->help_rmw)) {
    if (ENABLE_ASSERTIONS) {
      if (committed_glob_sess_rmw_id[kv_ptr->rmw_id.glob_sess_id] >= kv_ptr->rmw_id.id) {
        //my_printf(red, "Wrkr: %u The saved rmw id %u/%u glob_sess_id %u has been committed, "
        //             "last committed rmw id %u , glob sess id %u, "
        //             "state %u, committed log/log %u/%u, version %u \n",
        //           t_id, kv_ptr->rmw_id.id, committed_glob_sess_rmw_id[kv_ptr->rmw_id.glob_sess_id],
        //           kv_ptr->rmw_id.glob_sess_id, kv_ptr->last_committed_rmw_id.id,
        //           kv_ptr->last_committed_rmw_id.glob_sess_id,
        //           kv_ptr->state, kv_ptr->last_committed_log_no,
        //           kv_ptr->log_no, kv_ptr->new_ts.version);
        //assert(false);
      }
    }
    if (DEBUG_RMW)
      my_printf(yellow, "Wrkr %u, session %u changed who is waiting: waited for %u cycles on "
                      "state %u rmw_id %u glob_sess_id %u, now waiting on rmw_id %u glob_sess_id %u, state %u\n",
                    t_id, loc_entry->sess_id, loc_entry->back_off_cntr,
                    loc_entry->help_rmw->state, loc_entry->help_rmw->rmw_id.id, loc_entry->help_rmw->rmw_id.glob_sess_id,
                    kv_ptr->rmw_id.id, kv_ptr->rmw_id.glob_sess_id, kv_ptr->state);
    loc_entry->help_rmw->state = kv_ptr->state;
    assign_second_rmw_id_to_first(&loc_entry->help_rmw->rmw_id, &kv_ptr->rmw_id);
    loc_entry->help_rmw->ts = kv_ptr->prop_ts;
    loc_entry->help_rmw->log_no = kv_ptr->log_no;
    loc_entry->back_off_cntr = 0;
  }
  check_log_nos_of_kv_ptr(kv_ptr, "attempt_to_grab_kv_ptr_after_waiting", t_id);
  unlock_seqlock(&loc_entry->kv_ptr->seqlock);
  if (kv_ptr_was_grabbed) {
    fill_loc_rmw_entry_on_grabbing_kv_ptr(p_ops, loc_entry, PAXOS_TS,
                                          PROPOSED, sess_i, t_id);
  }
  else if (rmw_fails) {
    if (ENABLE_ASSERTIONS) {
      assert(loc_entry->accepted_log_no == 0);
      assert(loc_entry->killable);
    }
    loc_entry->state = INVALID_RMW;
    //printf("Cancelling on needing kv_ptr Wrkr%u, sess %u, entry %u rmw_failing \n",
      //     t_id, loc_entry->sess_id, loc_entry->index_to_rmw);
    free_session(p_ops, loc_entry->sess_id, false, t_id);
    return true;
  }
  return kv_ptr_was_grabbed;
}

// Insert a helping accept in the write fifo after waiting on it
static inline void attempt_to_help_a_locally_accepted_value(struct pending_ops *p_ops,
                                                            struct rmw_local_entry *loc_entry,
                                                            mica_op_t *kv_ptr, uint16_t t_id)
{
  bool help = false;
  struct rmw_local_entry *help_loc_entry = loc_entry->help_loc_entry;
  // The stat of the kv_ptr must not be downgraded from ACCEPTED
  lock_seqlock(&loc_entry->kv_ptr->seqlock);
  // check again with the lock in hand
  if (kv_ptr_state_has_not_changed(kv_ptr, loc_entry->help_rmw)) {
    loc_entry->log_no = kv_ptr->accepted_log_no;
    help_loc_entry->new_ts = kv_ptr->accepted_ts;
    help_loc_entry->rmw_id = kv_ptr->rmw_id;
    memcpy(help_loc_entry->value_to_write, kv_ptr->last_accepted_value, (size_t) RMW_VALUE_SIZE);
    help_loc_entry->base_ts = kv_ptr->base_acc_ts;

    // we must make it appear as if the kv_ptr has seen our propose
    // and has replied with a lower-ts-accept
    loc_entry->new_ts.version = kv_ptr->prop_ts.version + 1;
    loc_entry->new_ts.m_id = (uint8_t) machine_id;
    kv_ptr->prop_ts = loc_entry->new_ts;


    if (ENABLE_ASSERTIONS) {
      assert(kv_ptr->accepted_log_no == kv_ptr->log_no);
      assert(kv_ptr->prop_ts.version > kv_ptr->accepted_ts.version);
      assert(rmw_ids_are_equal(&kv_ptr->rmw_id, &kv_ptr->accepted_rmw_id));
      assert(loc_entry->key.bkt == kv_ptr->key.bkt);
      assert(kv_ptr->state == ACCEPTED);
    }
    help = true;
  }
  check_log_nos_of_kv_ptr(kv_ptr, "attempt_to_help_a_locally_accepted_value", t_id);
  unlock_seqlock(&loc_entry->kv_ptr->seqlock);

  loc_entry->back_off_cntr = 0;
  if (help) {
    // Helping means we are proposing, but we are not locally acked:
    // We store a reply from the local machine that says already ACCEPTED
    set_up_a_proposed_but_not_locally_acked_entry(p_ops, kv_ptr, loc_entry, t_id);
  }
}

// After backing off waiting on a PROPOSED kv_ptr try to steal it
static inline void attempt_to_steal_a_proposed_kv_ptr(struct pending_ops *p_ops,
                                                      struct rmw_local_entry *loc_entry,
                                                      mica_op_t *kv_ptr,
                                                      uint16_t sess_i, uint16_t t_id)
{
  bool kv_ptr_was_grabbed = false;
  lock_seqlock(&loc_entry->kv_ptr->seqlock);
  if (if_already_committed_bcast_commits(p_ops, loc_entry, t_id)) {
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
    return ;
  }
  uint32_t new_version = 0;

  if (kv_ptr->state == INVALID_RMW || kv_ptr_state_has_not_changed(kv_ptr, loc_entry->help_rmw)) {
    check_the_proposed_log_no(kv_ptr, loc_entry, t_id);
    loc_entry->log_no = kv_ptr->last_committed_log_no + 1;
    new_version = kv_ptr->prop_ts.version + 1;
    activate_RMW_entry(PROPOSED, new_version, kv_ptr, loc_entry->opcode,
                       (uint8_t) machine_id, loc_entry->rmw_id.id,
                       loc_entry->rmw_id.glob_sess_id, loc_entry->log_no, t_id,
                       ENABLE_ASSERTIONS ? "attempt_to_steal_a_proposed_kv_ptr" : NULL);

    kv_ptr_was_grabbed = true;
  }
  else if (kv_ptr_state_has_changed(kv_ptr, loc_entry->help_rmw)) {
    if (DEBUG_RMW)
      my_printf(yellow, "Wrkr %u, session %u on attempting to steal the propose, changed who is "
                      "waiting: waited for %u cycles for state %u "
                      "rmw_id %u glob_sess_id %u, state %u,  now waiting on rmw_id % glob_sess_id %u, state %u\n",
                    t_id, loc_entry->sess_id, loc_entry->back_off_cntr,
                    loc_entry->help_rmw->state, loc_entry->help_rmw->rmw_id.id, loc_entry->help_rmw->rmw_id.glob_sess_id,
                    kv_ptr->rmw_id.id, kv_ptr->rmw_id.glob_sess_id, kv_ptr->state);
    loc_entry->help_rmw->log_no = kv_ptr->log_no;
    loc_entry->help_rmw->state = kv_ptr->state;
    loc_entry->help_rmw->ts = kv_ptr->prop_ts;
    assign_second_rmw_id_to_first(&loc_entry->help_rmw->rmw_id, &kv_ptr->rmw_id);
  }
  else if (ENABLE_ASSERTIONS) assert(false);
  check_log_nos_of_kv_ptr(kv_ptr, "attempt_to_steal_a_proposed_kv_ptr", t_id);
  unlock_seqlock(&loc_entry->kv_ptr->seqlock);
  loc_entry->back_off_cntr = 0;
  if (kv_ptr_was_grabbed) {
    if (DEBUG_RMW)
      my_printf(cyan, "Wrkr %u: session %u steals kv_ptr to do its propose \n",
                  t_id, loc_entry->sess_id);
    fill_loc_rmw_entry_on_grabbing_kv_ptr(p_ops, loc_entry, new_version,
                                          PROPOSED, sess_i, t_id);
  }
}


// When receiving a response that says that a higher-TS RMW has been seen or TS was stale
static inline void take_kv_ptr_with_higher_TS(struct pending_ops *p_ops,
                                              struct rmw_local_entry *loc_entry,
                                              uint32_t new_version, bool from_propose,
                                              uint16_t t_id) {
  bool kv_ptr_was_grabbed  = false,
    is_still_proposed, is_still_accepted, kv_ptr_can_be_taken_with_higher_TS;
  bool rmw_fails = false;
  bool help = false;
  mica_op_t *kv_ptr = loc_entry->kv_ptr;
  lock_seqlock(&kv_ptr->seqlock);
  {
    if (if_already_committed_bcast_commits(p_ops, loc_entry, t_id)) {
      unlock_seqlock(&loc_entry->kv_ptr->seqlock);
      return;
    }
    is_still_proposed = rmw_ids_are_equal(&kv_ptr->rmw_id, &loc_entry->rmw_id) &&
                             kv_ptr->state == PROPOSED;

    is_still_accepted = rmw_ids_are_equal(&kv_ptr->rmw_id, &loc_entry->rmw_id) &&
                             kv_ptr->state == ACCEPTED &&
                             compare_ts(&kv_ptr->accepted_ts, &loc_entry->new_ts) == EQUAL;
    kv_ptr_can_be_taken_with_higher_TS =
      kv_ptr->state == INVALID_RMW || is_still_proposed || is_still_accepted;

    // if either state is invalid or we own it
    if (kv_ptr_can_be_taken_with_higher_TS) {
      if (!rmw_fails_with_loc_entry(loc_entry, kv_ptr, &rmw_fails, t_id)) {
        if (kv_ptr->state == INVALID_RMW) {
          kv_ptr->log_no = kv_ptr->last_committed_log_no + 1;
          kv_ptr->opcode = loc_entry->opcode;
          assign_second_rmw_id_to_first(&kv_ptr->rmw_id, &loc_entry->rmw_id);
        } else if (ENABLE_ASSERTIONS) {
          assert(loc_entry->log_no == kv_ptr->last_committed_log_no + 1);
          assert(kv_ptr->log_no == kv_ptr->last_committed_log_no + 1);
          if (kv_ptr->state == ACCEPTED) {
            assert(!from_propose);
            assert(compare_ts(&kv_ptr->accepted_ts, &loc_entry->new_ts) == EQUAL);
          }
        }
        loc_entry->log_no = kv_ptr->last_committed_log_no + 1;

        loc_entry->new_ts.version = MAX(new_version, kv_ptr->prop_ts.version) + 1;
        if (ENABLE_ASSERTIONS) {
          assert(loc_entry->new_ts.version > kv_ptr->prop_ts.version);
        }
        loc_entry->new_ts.m_id = (uint8_t) machine_id;
        kv_ptr->prop_ts = loc_entry->new_ts;
        if (!is_still_accepted) {
          if (ENABLE_ASSERTIONS) assert(kv_ptr->state != ACCEPTED);
          kv_ptr->state = PROPOSED;
        } else {
          // Attention: when retrying an RMW that has been locally accepted,
          // you need to start from Proposes, but the kv_ptr can NOT be downgraded to proposed
          help = true;
          //loc_entry->helping_flag = PROPOSE_LOCALLY_ACCEPTED;
          loc_entry->help_loc_entry->new_ts = kv_ptr->accepted_ts;
        }
        kv_ptr_was_grabbed = true;
      } else kv_ptr->state = INVALID_RMW;
    } else {
      if (DEBUG_RMW)
        my_printf(yellow, "Wrkr %u, session %u  failed when attempting to get/regain the kv_ptr, "
                        "waiting: waited for %u cycles for "
                        "now waiting on rmw_id % glob_sess_id %u, state %u\n",
                      t_id, loc_entry->sess_id,
                      kv_ptr->rmw_id.id, kv_ptr->rmw_id.glob_sess_id, kv_ptr->state);
    }
    check_log_nos_of_kv_ptr(kv_ptr, "take_kv_ptr_with_higher_TS", t_id);
  }
  unlock_seqlock(&loc_entry->kv_ptr->seqlock);

  zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);

  if (kv_ptr_was_grabbed) {
    if (DEBUG_RMW)
      my_printf(cyan, "Wrkr %u: session %u gets/regains the kv_ptr log %u to do its propose \n",
                  t_id, loc_entry->sess_id, kv_ptr->log_no);
    loc_entry->state = PROPOSED;
    if (help) {
      struct rmw_local_entry *help_loc_entry = loc_entry->help_loc_entry;
      //help_loc_entry->log_no = loc_entry->accepted_log_no;
      if (ENABLE_ASSERTIONS) assert(loc_entry->accepted_log_no == loc_entry->log_no);
      //help_loc_entry->rmw_id = loc_entry->rmw_id;
      //memcpy(help_loc_entry->value_to_write, loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
      //help_loc_entry->base_ts = loc_entry->base_ts;
//      set_up_a_proposed_but_not_locally_acked_entry(p_ops, kv_ptr, loc_entry, t_id);
      loc_entry->rmw_reps.tot_replies = 1;
      loc_entry->rmw_reps.already_accepted = 1;
      help_loc_entry->state = ACCEPTED;
      loc_entry->helping_flag = PROPOSE_LOCALLY_ACCEPTED;
    }
    else local_rmw_ack(loc_entry);
  }
  else if (rmw_fails) {
    if (ENABLE_ASSERTIONS) {
      assert(loc_entry->accepted_log_no == 0);
      assert(loc_entry->killable);
      assert(!is_still_accepted);
    }
    loc_entry->state = INVALID_RMW;
    //printf("Cancelling on needing kv_ptr Wrkr%u, sess %u, entry %u rmw_failing \n",
    //     t_id, loc_entry->sess_id, loc_entry->index_to_rmw);
    free_session(p_ops, loc_entry->sess_id, false, t_id);
  }
  else loc_entry->state = NEEDS_KV_PTR;
}

static inline void react_on_log_too_high(struct rmw_local_entry *loc_entry, bool is_propose,
                                         uint16_t t_id)
{
  loc_entry->state = RETRY_WITH_BIGGER_TS;
  loc_entry->log_too_high_cntr++;
  if (loc_entry->log_too_high_cntr == LOG_TOO_HIGH_TIME_OUT) {
    my_printf(red, "Worker: %u session %u, %s for rmw-id %u Timed out on log_too-high \n",
              t_id, loc_entry->sess_id,
              is_propose? "Prop" : "Acc",
               loc_entry->rmw_id.id);
    mica_op_t *kv_ptr = loc_entry->kv_ptr;
    lock_seqlock(&kv_ptr->seqlock);
    if (kv_ptr->last_committed_log_no + 1 == loc_entry->log_no) {
      loc_entry->state = MUST_BCAST_COMMITS_FROM_HELP;
      struct rmw_local_entry *help_loc_entry = loc_entry->help_loc_entry;
      memcpy(help_loc_entry->value_to_write, kv_ptr->value, RMW_VALUE_SIZE);
      assign_second_rmw_id_to_first(&help_loc_entry->rmw_id, &kv_ptr->last_committed_rmw_id);
      help_loc_entry->base_ts = kv_ptr->ts;
    }
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);


    if (loc_entry->state == MUST_BCAST_COMMITS_FROM_HELP) {
      loc_entry->helping_flag = HELP_PREV_COMMITTED_LOG_TOO_HIGH;
      loc_entry->help_loc_entry->log_no = loc_entry->log_no - 1;
      loc_entry->help_loc_entry->key = loc_entry->key;
      // loc_entry->help_loc_entry->sess_id = loc_entry->sess_id;
    }

    loc_entry->log_too_high_cntr = 0;
  }
}

//------------------------------REGULAR INSPECTIONS------------------------------------------

// Inspect each propose that has gathered a quorum of replies
static inline void inspect_proposes(struct pending_ops *p_ops,
                                    struct rmw_local_entry *loc_entry,
                                    uint16_t t_id)
{
  uint32_t new_version = 0;
  struct rmw_local_entry tmp;
  struct rmw_local_entry *dbg_loc_entry = &tmp;
  bool zero_out_log_too_high_cntr = true;
  memcpy(dbg_loc_entry, loc_entry, sizeof(struct rmw_local_entry));
  // RMW_ID COMMITTED
  if (loc_entry->rmw_reps.rmw_id_commited > 0) {
    debug_fail_help(loc_entry, " rmw id committed", t_id);
    // as an optimization clear the kv_ptr entry if it is still in proposed state
    if (loc_entry->accepted_log_no != loc_entry->log_no)
      free_kv_ptr_if_rmw_failed(loc_entry, PROPOSED, t_id);
    handle_already_committed_rmw(p_ops, loc_entry, t_id);
  }
  // LOG_NO TOO SMALL
  else if (loc_entry->rmw_reps.log_too_small > 0) {
    debug_fail_help(loc_entry, " log too small", t_id);
    //It is impossible for this RMW to still hold the kv_ptr
    loc_entry->state = NEEDS_KV_PTR;
  }
  // SEEN HIGHER-TS PROPOSE OR ACCEPT
  else if (loc_entry->rmw_reps.seen_higher_prop_acc > 0) {
    debug_fail_help(loc_entry, " seen higher prop", t_id);
    // retry by incrementing the highest ts seen
    loc_entry->state = RETRY_WITH_BIGGER_TS;
    new_version = loc_entry->rmw_reps.seen_higher_prop_version;
    check_version(new_version, "inspect_proposes: loc_entry->rmw_reps.seen_higher_prop > 0");
  }
  // ACK QUORUM
  else if (loc_entry->rmw_reps.acks >= QUORUM_NUM &&
           loc_entry->helping_flag != PROPOSE_NOT_LOCALLY_ACKED) {
    debug_fail_help(loc_entry, " quorum", t_id);
    // Quorum of prop acks gathered: send an accept
    act_on_quorum_of_prop_acks(p_ops, loc_entry, t_id);
    check_state_with_allowed_flags(5, (int) loc_entry->state, INVALID_RMW,
                                   ACCEPTED, NEEDS_KV_PTR, MUST_BCAST_COMMITS);
  }
  // ALREADY ACCEPTED AN RMW WITH LOWER_TS
  else if (loc_entry->rmw_reps.already_accepted > 0) {
    debug_fail_help(loc_entry, " already accepted", t_id);
    if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED)
      act_on_quorum_of_prop_acks(p_ops, loc_entry, t_id);
    else act_on_receiving_already_accepted_rep_to_prop(p_ops, loc_entry, &new_version, t_id);
    check_state_with_allowed_flags(5, (int) loc_entry->state, INVALID_RMW, ACCEPTED,
                                   NEEDS_KV_PTR, MUST_BCAST_COMMITS);
  }
  // LOG TOO HIGH
  else if (loc_entry->rmw_reps.log_too_high > 0) {
    react_on_log_too_high(loc_entry, true, t_id);
    new_version = loc_entry->new_ts.version;
    zero_out_log_too_high_cntr = false;

  }
  else if (ENABLE_ASSERTIONS) assert(false);

  if (zero_out_log_too_high_cntr) loc_entry->log_too_high_cntr = 0;

  // DECIDE WHETHER THE RMW IS KILLABLE
  if (ENABLE_CAS_CANCELLING) {
    loc_entry->killable = (loc_entry->state == RETRY_WITH_BIGGER_TS ||
                           loc_entry->state == NEEDS_KV_PTR) &&
                           loc_entry->accepted_log_no == 0 &&
                           loc_entry->opcode == COMPARE_AND_SWAP_WEAK;

  }
  // CLEAN_UP
  if (loc_entry->state == RETRY_WITH_BIGGER_TS) {
    check_version(new_version, "inspect_proposes: loc_entry->state == RETRY_WITH_BIGGER_TS");
    take_kv_ptr_with_higher_TS(p_ops, loc_entry, (new_version + 2), true, t_id);
    check_state_with_allowed_flags(5, (int) loc_entry->state, INVALID_RMW, PROPOSED,
                                   NEEDS_KV_PTR, MUST_BCAST_COMMITS);
    if (loc_entry->state == PROPOSED) {
      insert_prop_to_read_fifo(p_ops, loc_entry, t_id);
    }
  }
  else if (loc_entry->state != PROPOSED && loc_entry->helping_flag != HELP_PREV_COMMITTED_LOG_TOO_HIGH) {
    if (loc_entry->state != ACCEPTED) {
      check_state_with_allowed_flags(5, (int) loc_entry->state, INVALID_RMW, NEEDS_KV_PTR,
                                     MUST_BCAST_COMMITS, MUST_BCAST_COMMITS_FROM_HELP);
      if (ENABLE_ASSERTIONS && loc_entry->state != MUST_BCAST_COMMITS_FROM_HELP) {
        assert(dbg_loc_entry->log_no == loc_entry->log_no);
        assert(rmw_ids_are_equal(&dbg_loc_entry->rmw_id, &loc_entry->rmw_id));
        assert(compare_ts(&dbg_loc_entry->new_ts, &loc_entry->new_ts));
        //free_kv_ptr_if_rmw_failed(loc_entry, PROPOSED, t_id);
      }
    }
    zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
  }
/* The loc_entry can be in Proposed only when it retried with bigger TS */
}

static inline void clean_up_after_inspecting_accept(struct pending_ops *p_ops,
                                                    struct rmw_local_entry *loc_entry,
                                                    uint32_t new_version,
                                                    struct rmw_local_entry *dbg_loc_entry,
                                                    uint16_t t_id)
{
  //advance the entry's l_id such that subsequent responses are disregarded
  advance_loc_entry_l_id(p_ops, loc_entry, t_id);
  // CLEAN_UP
  if (ENABLE_ALL_ABOARD && loc_entry->all_aboard) {
    if (ENABLE_STAT_COUNTING) {
      t_stats[t_id].all_aboard_rmws++;
    }
    loc_entry->all_aboard = false;
  }
  if (loc_entry->state == RETRY_WITH_BIGGER_TS) {
    check_version(new_version, "inspect_accepts: loc_entry->state == RETRY_WITH_BIGGER_TS");
    take_kv_ptr_with_higher_TS(p_ops, loc_entry, (new_version + 2), false, t_id);
    check_state_with_allowed_flags(5, (int) loc_entry->state, INVALID_RMW, PROPOSED,
                                   NEEDS_KV_PTR, MUST_BCAST_COMMITS);
    //zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
    if (loc_entry->state == PROPOSED) {
      insert_prop_to_read_fifo(p_ops, loc_entry, t_id);
    }
  }
  else if (loc_entry->state != PROPOSED)
    zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);

  if (loc_entry->state == INVALID_RMW || loc_entry->state == NEEDS_KV_PTR) {
    if (ENABLE_ASSERTIONS) {
      assert(dbg_loc_entry->log_no == loc_entry->log_no);
      assert(rmw_ids_are_equal(&dbg_loc_entry->rmw_id, &loc_entry->rmw_id));
      assert(compare_ts(&dbg_loc_entry->new_ts, &loc_entry->new_ts));
    }
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

  uint8_t remote_quorum = (uint8_t) (loc_entry->all_aboard ?
                          MACHINE_NUM : QUORUM_NUM);

  uint32_t new_version = 0;
  if (loc_entry->helping_flag != NOT_HELPING) {
    if (ENABLE_ASSERTIONS) assert(loc_entry->helping_flag == HELPING);
    if (loc_entry->rmw_reps.rmw_id_commited  + loc_entry->rmw_reps.log_too_small +
        loc_entry->rmw_reps.already_accepted + loc_entry->rmw_reps.seen_higher_prop_acc +
          loc_entry->rmw_reps.log_too_high > 0) {
      reinstate_loc_entry_after_helping(loc_entry, t_id);
      return;
    }
  }
  // RMW_ID COMMITTED
  if (loc_entry->rmw_reps.rmw_id_commited > 0) {
    handle_already_committed_rmw(p_ops, loc_entry, t_id);
    if (ENABLE_ASSERTIONS) assert(loc_entry->helping_flag == NOT_HELPING);
  }
  // LOG_NO TOO SMALL
  else if (loc_entry->rmw_reps.log_too_small > 0) {
    //It is impossible for this RMW to still hold the kv_ptr
    loc_entry->state = NEEDS_KV_PTR;
    if (ENABLE_ASSERTIONS) assert(loc_entry->helping_flag == NOT_HELPING);
  }
  // ACK QUORUM
  else if (loc_entry->rmw_reps.acks >= remote_quorum) {
    if (ENABLE_ASSERTIONS) {
      assert(loc_entry->state != COMMITTED);
      if (loc_entry->helping_flag == HELPING) assert(!loc_entry->all_aboard);
    }
    loc_entry->state = (uint8_t) (loc_entry->helping_flag == HELPING ?
                                  MUST_BCAST_COMMITS_FROM_HELP : MUST_BCAST_COMMITS);
  }
  // SEEN HIGHER-TS PROPOSE
  else if (loc_entry->rmw_reps.seen_higher_prop_acc > 0) {
    // retry by incrementing the highest ts seen
    loc_entry->state = RETRY_WITH_BIGGER_TS;
    new_version = loc_entry->rmw_reps.seen_higher_prop_version;
    check_version(new_version, "inspect_accepts: loc_entry->rmw_reps.seen_higher_prop > 0");
    if (ENABLE_ASSERTIONS) assert(loc_entry->helping_flag == NOT_HELPING);
  }
  // LOG TOO HIGH
  else if (loc_entry->rmw_reps.log_too_high > 0) {
    loc_entry->state = RETRY_WITH_BIGGER_TS;
    new_version = loc_entry->new_ts.version;
  }

  // if a quorum of messages have been received but
  // we are waiting for more, then we are doing all aboard
  else if (ENABLE_ALL_ABOARD) {
    if (ENABLE_ASSERTIONS) assert(loc_entry->all_aboard);
    loc_entry->all_aboard_time_out++;
    if (ENABLE_ASSERTIONS) assert(loc_entry->new_ts.version == 2);
    if (loc_entry->all_aboard_time_out > ALL_ABOARD_TIMEOUT_CNT) {
      // printf("Wrkr %u, Timing out on key %u \n", t_id, loc_entry->key.bkt);
      loc_entry->state = RETRY_WITH_BIGGER_TS;
      loc_entry->all_aboard_time_out = 0;
      new_version = 2;
    }
    else return; // avoid zeroing out the responses
  }
  else if (ENABLE_ASSERTIONS) assert(false);


  clean_up_after_inspecting_accept(p_ops, loc_entry, new_version, dbg_loc_entry, t_id);

}

// Worker inspects its local RMW entries
static inline void inspect_rmws(struct pending_ops *p_ops, uint16_t t_id)
{
  for (uint16_t sess_i = 0; sess_i < SESSIONS_PER_THREAD; sess_i++) {
    struct rmw_local_entry* loc_entry = &p_ops->prop_info->entry[sess_i];
    uint8_t state = loc_entry->state;
    if (state == INVALID_RMW) continue;
    if (ENABLE_ASSERTIONS) {
      assert(loc_entry->sess_id == sess_i);
      assert(p_ops->sess_info[sess_i].stalled);
    }

    /* =============== ACCEPTED ======================== */
    if (state == ACCEPTED) {
      check_sum_of_reps(loc_entry);
      //printf("reps %u \n", loc_entry->rmw_reps.tot_replies);
      if (loc_entry->rmw_reps.tot_replies >= QUORUM_NUM) {
        inspect_accepts(p_ops, loc_entry, t_id);
        check_state_with_allowed_flags(7, (int) loc_entry->state, INVALID_RMW, PROPOSED, NEEDS_KV_PTR,
                                       MUST_BCAST_COMMITS, MUST_BCAST_COMMITS_FROM_HELP, ACCEPTED);
      }
    }
    /* =============== BROADCAST COMMITS ======================== */
    if (state == MUST_BCAST_COMMITS || state == MUST_BCAST_COMMITS_FROM_HELP) {
      struct rmw_local_entry *entry_to_commit =
        state == MUST_BCAST_COMMITS ? loc_entry : loc_entry->help_loc_entry;
      //bool is_commit_helping = loc_entry->helping_flag != NOT_HELPING;
      if (p_ops->virt_w_size < MAX_ALLOWED_W_SIZE) {
        if (state == MUST_BCAST_COMMITS_FROM_HELP && loc_entry->helping_flag == PROPOSE_NOT_LOCALLY_ACKED) {
          my_printf(green, "Wrkr %u sess %u will bcast commits for the latest committed RMW,"
                         " after learning its proposed RMW has already been committed \n", t_id, loc_entry->sess_id);
        }
        insert_write(p_ops, (struct trace_op*) entry_to_commit, FROM_COMMIT, state, t_id);
        loc_entry->state = COMMITTED;
        continue;
      }
    }
    /* =============== NEEDS_KV_PTR ======================== */
    if (state == NEEDS_KV_PTR) {
      mica_op_t *kv_ptr = loc_entry->kv_ptr;
      // If this fails to grab a kv_ptr it will try to update
      // the (rmw_id + state) that is being waited on.
      // If it updates it will zero the back-off counter
      if (!attempt_to_grab_kv_ptr_after_waiting(p_ops, kv_ptr, loc_entry,
                                                sess_i, t_id)) {
        if (ENABLE_ASSERTIONS) assert(p_ops->sess_info[sess_i].stalled);
        loc_entry->back_off_cntr++;
        if (loc_entry->back_off_cntr == RMW_BACK_OFF_TIMEOUT) {
//          my_printf(yellow, "Wrkr %u  sess %u waiting for an rmw on key %u on log %u, back_of cntr %u waiting on rmw_id %u glob_sess id %u, state %u \n",
//                        t_id, sess_i,loc_entry->key.bkt, loc_entry->help_rmw->log_no, loc_entry->back_off_cntr,
//                        loc_entry->help_rmw->rmw_id.id, loc_entry->help_rmw->rmw_id.glob_sess_id,
//                        loc_entry->help_rmw->state);

          // This is failure-related help/stealing it should not be that we are being held up by the local machine
          // However we may wait on a "local" glob sess id, because it is being helped
          // if have accepted a value help it
          if (loc_entry->help_rmw->state == ACCEPTED)
            attempt_to_help_a_locally_accepted_value(p_ops, loc_entry, kv_ptr, t_id); // zeroes the back-off counter
          // if have received a proposal, send your own proposal
          else  if (loc_entry->help_rmw->state == PROPOSED) {
            attempt_to_steal_a_proposed_kv_ptr(p_ops, loc_entry, kv_ptr, sess_i, t_id); // zeroes the back-off counter
          }
        }
      }
      if (loc_entry->state == PROPOSED) {
        loc_entry->back_off_cntr = 0;
        insert_prop_to_read_fifo(p_ops, loc_entry, t_id);
      }
      check_state_with_allowed_flags(6, (int) loc_entry->state, INVALID_RMW, PROPOSED, NEEDS_KV_PTR,
                                     ACCEPTED, MUST_BCAST_COMMITS);
    }
    /* =============== PROPOSED ======================== */
    if (state == PROPOSED) {
      if (loc_entry->must_release && !p_ops->sess_info[sess_i].ready_to_release) {
        continue;
      }
      else if (loc_entry->must_release) loc_entry->must_release = false;

      uint8_t quorum = QUORUM_NUM;
//      if (loc_entry->helping_flag != PROPOSE_NOT_LOCALLY_ACKED)
//        check_sum_of_reps(loc_entry);
//      else quorum++;
      if (loc_entry->rmw_reps.tot_replies >= quorum) {
        // further responses for that broadcast of Propose must be disregarded
        advance_loc_entry_l_id(p_ops, loc_entry, t_id);
        inspect_proposes(p_ops, loc_entry, t_id);
        if (loc_entry->helping_flag == PROPOSE_NOT_LOCALLY_ACKED ||
            loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED)
          loc_entry->helping_flag = NOT_HELPING;
        check_state_with_allowed_flags(7, (int) loc_entry->state, INVALID_RMW, PROPOSED, NEEDS_KV_PTR,
                                       ACCEPTED, MUST_BCAST_COMMITS, MUST_BCAST_COMMITS_FROM_HELP);
      }
    }
  }
}



/* ---------------------------------------------------------------------------
//------------------------------CONFIGURATION -----------------------------
//---------------------------------------------------------------------------*/
// Update the quorum info, use this one a timeout
// On a timeout it goes through all machines
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
      //set_conf_bit_after_detecting_failure(t_id, i); // this function changes both vectors
      //if (DEBUG_QUORUM) my_printf(yellow, "Worker flips the vector bit_vec for machine %u, send vector bit_vec %u \n",
      //                               i, send_bit_vector.bit_vec[i].bit);
      if (!DEBUG_BIT_VECS) {
        if (t_id == 0)
         my_printf(cyan, "Wrkr %u detects that machine %u has failed \n", t_id, i);
      }
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
    assert(!q_info->send_vector[rm_id]);
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
    if (DEBUG_QUORUM) my_printf(green, "After: Missing position %u, missing id %u, id to revive\n",
                                   i, q_info->missing_ids[i], revived_mach_id);
}

// Update the links between the send Work Requests for broadcasts given the quorum information
static inline void update_bcast_wr_links(struct quorum_info *q_info, struct ibv_send_wr *wr, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) assert(MESSAGES_IN_BCAST == REM_MACH_NUM);
  uint8_t prev_i = 0, avail_mach = 0;
  if (DEBUG_QUORUM) my_printf(green, "Worker %u fixing the links between the wrs \n", t_id);
  for (uint8_t i = 0; i < REM_MACH_NUM; i++) {
    wr[i].next = NULL;
    if (q_info->send_vector[i]) {
      if (avail_mach > 0) {
        for (uint16_t j = 0; j < MAX_BCAST_BATCH; j++) {
          if (DEBUG_QUORUM) my_printf(yellow, "Worker %u, wr %d points to %d\n", t_id, (REM_MACH_NUM * j) + prev_i, (REM_MACH_NUM * j) + i);
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
                                       uint16_t min_credits, uint16_t t_id)
{
  uint16_t i;
  // First check the active ids, to have a fast path when there are not enough credits
  for (i = 0; i < q_info->active_num; i++) {
    if (credits[vc][q_info->active_ids[i]] < min_credits) {
      time_out_cnt[vc]++;
      //if (DEBUG_BIT_VECS && t_id >= 0 && time_out_cnt[vc] % M_1 == 0)
        //my_printf(red, "WKR %u: the timeout cnt is %u for vc %u machine %u, credits %u\n",
         //          t_id, time_out_cnt[vc], vc, q_info->active_ids[i], credits[vc][q_info->active_ids[i]]);
      if (time_out_cnt[vc] == CREDIT_TIMEOUT) {
        if (DEBUG_QUORUM)
          my_printf(red, "Worker %u timed_out on machine %u  for vc % u, writes  done %lu \n",
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
        if (DEBUG_QUORUM)
          my_printf(red, "Worker %u revives machine %u \n", t_id, q_info->missing_ids[i]);
        revive_machine(q_info, q_info->missing_ids[i]);
        // printf("Wrkr %u, after reviving, active num %u, active_id %u, %u, %u, %u \n",
        //t_id, q_info->active_num, q_info->active_ids[0], q_info->active_ids[1],
        //       q_info->active_ids[2],q_info->active_ids[3]);
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
                              struct hrd_ctrl_blk *cb, struct ibv_sge *send_sgl,
                              struct ibv_send_wr *send_wr, uint64_t *w_br_tx,
                              uint16_t br_i, uint16_t credits[][MACHINE_NUM],
                              uint8_t vc, uint16_t t_id) {
  struct ibv_wc signal_send_wc;
  struct w_message *w_mes = (struct w_message *) &p_ops->w_fifo->w_message[w_mes_i];
  struct w_mes_info *info = &p_ops->w_fifo->info[w_mes_i];
  uint8_t coalesce_num = w_mes->coalesce_num;
  bool has_writes = info->writes_num > 0;
  bool all_writes = info->writes_num == w_mes->coalesce_num;
  uint32_t backward_ptr = info->backward_ptr;
  send_sgl[br_i].length = info->message_size;
  send_sgl[br_i].addr = (uint64_t) (uintptr_t) w_mes;
  if (ENABLE_ADAPTIVE_INLINING)
    adaptive_inlining(send_sgl[br_i].length, &send_wr[br_i * MESSAGES_IN_BCAST], MESSAGES_IN_BCAST);
  // Set the w_state for each write and perform checks

  set_w_state_for_each_write(p_ops, info, w_mes, backward_ptr, coalesce_num,
                             send_sgl, br_i, p_ops->q_info, t_id);

  if (DEBUG_WRITES)
    my_printf(green, "Wrkr %d : I BROADCAST a write message %d of %u writes with mes_size %u,"
                   " with credits: %d, lid: %u  \n",
                 t_id, w_mes->write[0].opcode, coalesce_num, send_sgl[br_i].length,
                 credits[vc][(machine_id + 1) % MACHINE_NUM], w_mes->l_id);

  if (DEBUG_RMW) {
    struct accept *acc = (struct accept *) &w_mes->write[0];
    my_printf(green, "Wrkr %d : I BROADCAST a message %d of %u accepts with mes_size %u, "
                   "with credits: %d, lid: %u , "
                   "rmw_id %u, glob_sess id %u, log_no %u, version %u  \n",
                 t_id, acc->opcode, coalesce_num,
                 send_sgl[br_i].length,  credits[vc][(machine_id + 1) % MACHINE_NUM], acc->l_id,
                 acc->t_rmw_id, acc->glob_sess_id,
                 acc->log_no, acc->ts.version);
  }

  // Do a Signaled Send every W_BCAST_SS_BATCH broadcasts (W_BCAST_SS_BATCH * (MACHINE_NUM - 1) messages)
  if ((*w_br_tx) % W_BCAST_SS_BATCH == 0) {
    if (DEBUG_SS_BATCH)
      printf("Wrkr %u Sending signaled the first message, total %lu, br_i %u \n", t_id, *w_br_tx, br_i);
    send_wr[p_ops->q_info->first_active_rm_id].send_flags |= IBV_SEND_SIGNALED;
  }
  (*w_br_tx)++;
  if ((*w_br_tx) % W_BCAST_SS_BATCH == W_BCAST_SS_BATCH - 1) {
    if (DEBUG_SS_BATCH)
      printf("Wrkr %u POLLING for a send completion in writes, total %lu \n", t_id, *w_br_tx);
    poll_cq(cb->dgram_send_cq[W_QP_ID], 1, &signal_send_wc, POLL_CQ_W);
  }
  // Have the last message of each broadcast pointing to the first message of the next bcast
  if (br_i > 0) {
    send_wr[((br_i - 1) * MESSAGES_IN_BCAST) + p_ops->q_info->last_active_rm_id].next =
      &send_wr[(br_i * MESSAGES_IN_BCAST) + p_ops->q_info->first_active_rm_id];
  }
}



static inline bool release_not_ready(struct pending_ops *p_ops,
                                     struct w_mes_info *info, struct w_message *w_mes,
                                     uint32_t *release_rdy_dbg_cnt, uint16_t t_id) {
  if (!info->is_release)
    return false; // not even a release

  //struct sess_info *sess_info = p_ops->sess_info;
  // We know the message contains releases. let's check their sessions!
  for (uint8_t i = 0; i < w_mes->coalesce_num; i++) {
    if (info->per_message_release_flag[i]) {
      struct sess_info *sess_info = &p_ops->sess_info[info->per_message_sess_id[i]];
      if (!sess_info->ready_to_release) {
        if (ENABLE_ASSERTIONS) {
          assert(sess_info->live_writes > 0);
          (*release_rdy_dbg_cnt)++;
          if (*release_rdy_dbg_cnt == M_4) {
            if (t_id == 0) printf("Wrkr %u stuck. Release cannot fire \n", t_id);
            (*release_rdy_dbg_cnt) = 0;
          }
        }
        return true; // release is not ready yet
      }
    }
  }
  if (ENABLE_ASSERTIONS) (*release_rdy_dbg_cnt) = 0;
  return false; // release is ready

}

// Broadcast Writes
static inline void broadcast_writes(struct pending_ops *p_ops,
                                    uint16_t credits[][MACHINE_NUM], struct hrd_ctrl_blk *cb,
                                    uint32_t *release_rdy_dbg_cnt, uint32_t *time_out_cnt,
                                    struct ibv_sge *w_send_sgl, struct ibv_send_wr *r_send_wr,
                                    struct ibv_send_wr *w_send_wr,
                                    uint64_t *w_br_tx, struct recv_info *ack_recv_info,
                                    struct recv_info *r_rep_recv_info,
                                    uint16_t t_id, uint32_t *outstanding_writes, uint64_t *expected_next_l_id)
{
  //printf("Worker %d bcasting writes \n", t_id);
  uint8_t vc = W_VC;
  uint16_t br_i = 0, mes_sent = 0, available_credits = 0;
  uint32_t bcast_pull_ptr = p_ops->w_fifo->bcast_pull_ptr;
  if (p_ops->w_fifo->bcast_size == 0) return;
  if (release_not_ready(p_ops, &p_ops->w_fifo->info[bcast_pull_ptr], (struct w_message *)
    &p_ops->w_fifo->w_message[bcast_pull_ptr], release_rdy_dbg_cnt, t_id))
    return;
  if (!check_bcast_credits(credits, p_ops->q_info, time_out_cnt, vc,
                           &available_credits, r_send_wr, w_send_wr,
                           1, t_id))
    return;
  if (ENABLE_ASSERTIONS) assert(available_credits <= W_CREDITS);

  while (p_ops->w_fifo->bcast_size > 0 && mes_sent < available_credits) {
    if (mes_sent >  0 &&
      release_not_ready(p_ops, &p_ops->w_fifo->info[bcast_pull_ptr], (struct w_message *)
        &p_ops->w_fifo->w_message[bcast_pull_ptr], release_rdy_dbg_cnt, t_id)) {
      break;
    }
    if (DEBUG_WRITES)
      printf("Wrkr %d has %u write bcasts to send credits %d\n",t_id, p_ops->w_fifo->bcast_size, available_credits);
    // Create the broadcast messages
    forge_w_wr(bcast_pull_ptr, p_ops, cb,  w_send_sgl, w_send_wr, w_br_tx, br_i, credits, vc, t_id);
    br_i++;
    struct w_message *w_mes = (struct w_message *) &p_ops->w_fifo->w_message[bcast_pull_ptr];
      uint8_t coalesce_num = w_mes->coalesce_num;
    debug_and_count_stats_when_broadcasting_writes(p_ops, bcast_pull_ptr, coalesce_num,
                                                   t_id, expected_next_l_id, br_i, outstanding_writes);
    p_ops->w_fifo->bcast_size -= coalesce_num;
    // This message has been sent, do not add other writes to it!
    if (p_ops->w_fifo->bcast_size == 0) reset_write_message(p_ops);
    mes_sent++;
    MOD_ADD(bcast_pull_ptr, W_FIFO_SIZE);
    if (br_i == MAX_BCAST_BATCH) {
      post_receives_for_r_reps_for_accepts(r_rep_recv_info, t_id);
      post_quorum_broadasts_and_recvs(ack_recv_info, MAX_RECV_ACK_WRS - ack_recv_info->posted_recvs,
                                      p_ops->q_info, br_i, *w_br_tx, w_send_wr, cb->dgram_qp[W_QP_ID],
                                      W_ENABLE_INLINING);
      br_i = 0;
    }
  }
  if (br_i > 0) {
    if (ENABLE_ASSERTIONS) assert(MAX_BCAST_BATCH > 1);
    post_receives_for_r_reps_for_accepts(r_rep_recv_info, t_id);
    post_quorum_broadasts_and_recvs(ack_recv_info, MAX_RECV_ACK_WRS - ack_recv_info->posted_recvs,
                                    p_ops->q_info, br_i, *w_br_tx, w_send_wr, cb->dgram_qp[W_QP_ID],
                                    W_ENABLE_INLINING);
  }

  p_ops->w_fifo->bcast_pull_ptr = bcast_pull_ptr;
  if (ENABLE_ASSERTIONS) assert(mes_sent <= available_credits && mes_sent <= W_CREDITS);
  if (mes_sent > 0) decrease_credits(credits, p_ops->q_info, mes_sent, vc);
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
  struct r_message *r_mes = (struct r_message *) &p_ops->r_fifo->r_message[r_mes_i];
  struct r_mes_info *info = &p_ops->r_fifo->info[r_mes_i];
  uint16_t coalesce_num = r_mes->coalesce_num;
  bool has_reads = info->reads_num > 0;
  bool all_reads = info->reads_num == r_mes->coalesce_num;
  send_sgl[br_i].length = info->message_size;
  send_sgl[br_i].addr = (uint64_t) (uintptr_t) r_mes;
  if (ENABLE_ADAPTIVE_INLINING)
    adaptive_inlining(send_sgl[br_i].length, &send_wr[br_i * MESSAGES_IN_BCAST], MESSAGES_IN_BCAST);
  if (ENABLE_ASSERTIONS) {
    assert(coalesce_num > 0);
    assert(send_sgl[br_i].length <= R_SEND_SIZE);
  }

  if (DEBUG_READS && all_reads)
    my_printf(green, "Wrkr %d : I BROADCAST a read message %d of %u reads with mes_size %u, with credits: %d, lid: %u  \n",
                 t_id, r_mes->read[coalesce_num - 1].opcode, coalesce_num, send_sgl[br_i].length,
                 credits[vc][(machine_id + 1) % MACHINE_NUM], r_mes->l_id);
  else if (DEBUG_RMW) {
    //struct prop_message *prop_mes = (struct prop_message *) r_mes;
    struct propose *prop = (struct propose *) &r_mes->read[0];
  my_printf(green, "Wrkr %d : I BROADCAST a propose message %d of %u props with mes_size %u, with credits: %d, lid: %u, "
                 "rmw_id %u, glob_sess id %u, log_no %u, version %u \n",
               t_id, prop->opcode, coalesce_num, send_sgl[br_i].length,
               credits[vc][(machine_id + 1) % MACHINE_NUM], r_mes->l_id,
               prop->t_rmw_id, prop->glob_sess_id,
               prop->log_no, prop->ts.version);
  }
  if (has_reads) {
    for (i = 0; i < info->reads_num; i++) {
      p_ops->r_state[(info->backward_ptr + i) % PENDING_READS] = SENT;
      if (DEBUG_READS)
        my_printf(yellow, "Read %d/%u, message mes_size %d, version %u \n", i, coalesce_num,
                      send_sgl[br_i].length, r_mes->read[i].ts.version);
      if (ENABLE_ASSERTIONS && all_reads) {
        check_state_with_allowed_flags(5, r_mes->read[i].opcode, KVS_OP_GET, CACHE_OP_GET_TS,
                                       OP_ACQUIRE, OP_ACQUIRE_FLIP_BIT);
      }
    }
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
                                   uint32_t *credit_debug_cnt,
                                   uint32_t *time_out_cnt,
                                   struct ibv_sge *r_send_sgl, struct ibv_send_wr *r_send_wr,
                                   struct ibv_send_wr *w_send_wr,
                                   uint64_t *r_br_tx, struct recv_info *r_rep_recv_info,
                                   uint16_t t_id, uint32_t *outstanding_reads)
{
  //  printf("Worker %d bcasting reads \n", g_id);
  uint8_t vc = R_VC;
  //uint16_t reads_sent = 0,
  uint16_t br_i = 0, mes_sent = 0, available_credits = 0;
  uint32_t bcast_pull_ptr = p_ops->r_fifo->bcast_pull_ptr;

  if (p_ops->r_fifo->bcast_size > 0) {
    if (!check_bcast_credits(credits, p_ops->q_info, time_out_cnt, vc,
                             &available_credits, r_send_wr, w_send_wr, 1,  t_id))
      return;
  }
  else return;
  if (ENABLE_ASSERTIONS) assert(available_credits <= R_CREDITS);

  while (p_ops->r_fifo->bcast_size > 0 &&  mes_sent < available_credits) {
    if (DEBUG_READS)
      printf("Wrkr %d has %u read bcasts to send credits %d\n",t_id, p_ops->r_fifo->bcast_size, credits[R_VC][0]);
    // Create the broadcast messages
    forge_r_wr(bcast_pull_ptr, p_ops, p_ops->q_info, cb, r_send_sgl, r_send_wr, r_br_tx, br_i, credits, vc, t_id);
    br_i++;
    struct r_message * r_mes = (struct r_message *) &p_ops->r_fifo->r_message[bcast_pull_ptr];
      uint8_t coalesce_num = r_mes->coalesce_num;
    if (ENABLE_ASSERTIONS) {
      assert( p_ops->r_fifo->bcast_size >= coalesce_num);
      (*outstanding_reads) += coalesce_num;
    }
    p_ops->r_fifo->bcast_size -= coalesce_num;
    if (p_ops->r_fifo->bcast_size == 0) reset_read_message(p_ops);
    //reads_sent += coalesce_num;
    mes_sent++;
    MOD_ADD(bcast_pull_ptr, R_FIFO_SIZE);
    if (br_i == MAX_BCAST_BATCH) {
      post_quorum_broadasts_and_recvs(r_rep_recv_info, MAX_RECV_R_REP_WRS - r_rep_recv_info->posted_recvs,
                                      p_ops->q_info, br_i, *r_br_tx, r_send_wr, cb->dgram_qp[R_QP_ID],
                                      R_ENABLE_INLINING);
      br_i = 0;
    }
  }
  if (br_i > 0)
    post_quorum_broadasts_and_recvs(r_rep_recv_info, MAX_RECV_R_REP_WRS - r_rep_recv_info->posted_recvs,
                                    p_ops->q_info, br_i, *r_br_tx, r_send_wr, cb->dgram_qp[R_QP_ID],
                                    R_ENABLE_INLINING);
  p_ops->r_fifo->bcast_pull_ptr = bcast_pull_ptr;
  if (mes_sent > 0) decrease_credits(credits, p_ops->q_info, mes_sent, vc);
}


/* ---------------------------------------------------------------------------
//------------------------------ POLLLING------- -----------------------------
//---------------------------------------------------------------------------*/

static inline int find_how_many_write_messages_can_be_polled(struct ibv_cq *w_recv_cq, struct ibv_wc *w_recv_wc,
                                                             struct recv_info *w_recv_info, struct ack_message *acks,
                                                             uint32_t *completed_but_not_polled_writes,
                                                             uint16_t t_id)
{
  int completed_messages = ibv_poll_cq(w_recv_cq, W_BUF_SLOTS, w_recv_wc);
  if (DEBUG_RECEIVES) {
    w_recv_info->posted_recvs -= completed_messages;
    if (w_recv_info->posted_recvs < RECV_WR_SAFETY_MARGIN)
      my_printf(red, "Wrkr %u some remote machine has created credits out of thin air \n", t_id);
  }
  // There is a chance that you wont be able to poll all completed writes,
  // because you wont be able to create acks for them, in which case you
  // pass the number of completed (i.e. from the completion queue) but not polled messages to the next round
  if (unlikely(*completed_but_not_polled_writes > 0)) {
    if (DEBUG_QUORUM)
      my_printf(yellow, "Wrkr %u adds %u messages to the %u completed messages \n",
                    t_id, *completed_but_not_polled_writes, completed_messages);
    completed_messages += (*completed_but_not_polled_writes);
    (*completed_but_not_polled_writes) = 0;
  }
  if (ENABLE_ASSERTIONS && completed_messages > 0) {
    for (int i = 0; i < MACHINE_NUM; i++)
      assert(acks[i].opcode == CACHE_OP_ACK);
  }
  return completed_messages;
}

// Poll for the write broadcasts
static inline void poll_for_writes(volatile struct w_message_ud_req *incoming_ws,
                                   uint32_t *pull_ptr, struct pending_ops *p_ops,
                                   struct ibv_cq *w_recv_cq, struct ibv_wc *w_recv_wc,
                                   struct recv_info *w_recv_info, struct ack_message *acks,
                                   uint32_t *completed_but_not_polled_writes,
                                   uint16_t t_id)
{

  uint32_t polled_messages = 0, writes_for_kvs = 0;
  int completed_messages =
    find_how_many_write_messages_can_be_polled(w_recv_cq, w_recv_wc, w_recv_info,
                                               acks, completed_but_not_polled_writes, t_id);
  if (completed_messages <= 0) return;
  uint32_t index = *pull_ptr;
  // Start polling
  while (polled_messages < completed_messages) {
    struct w_message *w_mes = (struct w_message*) &incoming_ws[index].w_mes;
    check_the_polled_write_message(w_mes, index, writes_for_kvs, t_id);
    print_polled_write_message_info(w_mes, index, t_id);
    uint8_t w_num = w_mes->coalesce_num;
    check_state_with_allowed_flags(4, w_mes->opcode, ONLY_WRITES, ONLY_ACCEPTS, WRITES_AND_ACCEPTS);
    bool is_only_accepts = w_mes->opcode == ONLY_ACCEPTS;


    uint8_t writes_to_be_acked = 0, accepts = 0;
    uint32_t running_writes_for_kvs = writes_for_kvs;
    uint16_t byte_ptr = W_MES_HEADER;
    for (uint16_t i = 0; i < w_num; i++) {
      struct write *write = (struct write *)(((void *)w_mes) + byte_ptr);
      byte_ptr += get_write_size_from_opcode(write->opcode);
      check_a_polled_write(write, i, w_num, w_mes->opcode, t_id);
      if (!EMULATE_ABD) handle_configuration_on_receiving_rel(write, t_id);
      if (ENABLE_ASSERTIONS) assert(write->opcode != ACCEPT_OP_BIT_VECTOR);

      if (write->opcode != NO_OP_RELEASE) {
        p_ops->ptrs_to_mes_ops[running_writes_for_kvs] = (void *) write; //(((void *) write) - 3); // align with trace_op
        if (write->opcode == ACCEPT_OP) {
          p_ops->ptrs_to_mes_headers[running_writes_for_kvs] = (struct r_message *) w_mes;
          p_ops->coalesce_r_rep[running_writes_for_kvs] = accepts > 0;
          raise_conf_bit_if_accept_signals_it((struct accept *) write, w_mes->m_id, t_id);
        }
        if (PRINT_LOGS && write->opcode == COMMIT_OP) {
          p_ops->ptrs_to_mes_headers[running_writes_for_kvs] = (struct r_message *) w_mes;
        }
        running_writes_for_kvs++;
      }
      if (write->opcode != ACCEPT_OP) writes_to_be_acked++;
      else accepts++;
    }

    if (ENABLE_ASSERTIONS) assert(accepts + writes_to_be_acked == w_num);
    // Make sure the writes of the message can be processed
    if (!is_only_accepts) {
      if (ENABLE_ASSERTIONS) assert(writes_to_be_acked > 0);
      if (!ack_bookkeeping(&acks[w_mes->m_id], writes_to_be_acked, w_mes->l_id, w_mes->m_id, t_id)) {
        (*completed_but_not_polled_writes) = completed_messages - polled_messages;
        //if (DEBUG_QUORUM)
          my_printf(yellow, "Wrkr %u leaves %u messages for the next polling round \n",
                        t_id, *completed_but_not_polled_writes);
        break;
      }
    }
    else if (ENABLE_ASSERTIONS) assert(w_mes->l_id == 0);

    writes_for_kvs = running_writes_for_kvs;
    count_stats_on_receiving_w_mes_reset_w_num(w_mes, w_num, t_id);
    MOD_ADD(index, W_BUF_SLOTS);
    polled_messages++;
  }
  (*pull_ptr) = index;

  if (writes_for_kvs > 0) {
    if (DEBUG_WRITES) my_printf(yellow, "Worker %u is going with %u writes to the kvs \n", t_id, writes_for_kvs);
    KVS_batch_op_updates((uint16_t) writes_for_kvs, t_id, (struct write **) p_ops->ptrs_to_mes_ops,
                         p_ops, 0, (uint32_t) MAX_INCOMING_W, ENABLE_ASSERTIONS == 1);
    if (DEBUG_WRITES) my_printf(yellow, "Worker %u propagated %u writes to the kvs \n", t_id, writes_for_kvs);
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
    struct r_message *r_mes = (struct r_message*) &incoming_rs[index].r_mes;
    check_when_polling_for_reads(r_mes, index, polled_reads, t_id);
    uint8_t r_num = r_mes->coalesce_num;
    uint16_t byte_ptr = R_MES_HEADER;
    for (uint16_t i = 0; i < r_num; i++) {
      struct read *read = (struct read*)(((void *) r_mes) + byte_ptr);
      //printf("Receiving read opcode %u \n", read->opcode);
      bool is_propose = read->opcode == PROPOSE_OP;
      if (is_propose) {
        struct propose *prop = (struct propose *) read;
        check_state_with_allowed_flags(2, prop->opcode, PROPOSE_OP);
        p_ops->ptrs_to_mes_ops[polled_reads] = (void *) prop; //(((void *) prop) -3); //align with the kvs op
      }
      else {
        check_read_opcode_when_polling_for_reads(read, i, r_num, t_id);
        if (read->opcode == OP_ACQUIRE) {
          read->opcode =
            take_ownership_of_a_conf_bit(r_mes->l_id + i, (uint16_t) r_mes->m_id, false, t_id) ?
            (uint8_t) OP_ACQUIRE_FP : (uint8_t) OP_ACQUIRE;
        }
        if (read->opcode == OP_ACQUIRE_FLIP_BIT)
          raise_conf_bit_iff_owned(*(uint64_t *) &read->key, (uint16_t) r_mes->m_id, false, t_id);

        p_ops->ptrs_to_mes_ops[polled_reads] = (void *) read; //(((void *) read) - 3); //align with the kvs op

      }
      p_ops->ptrs_to_mes_headers[polled_reads] = r_mes;
      p_ops->coalesce_r_rep[polled_reads] = i > 0;
      polled_reads++;
      byte_ptr += get_read_size_from_opcode(read->opcode);
    }
    if (ENABLE_ASSERTIONS) r_mes->coalesce_num = 0;
    MOD_ADD(index, R_BUF_SLOTS);
    polled_messages++;
    if (ENABLE_ASSERTIONS)
      assert(polled_messages + p_ops->r_rep_fifo->mes_size < R_REP_FIFO_SIZE);
  }
  (*pull_ptr) = index;
  // Poll for the completion of the receives
  if (polled_messages > 0) {
    KVS_batch_op_reads(polled_reads, t_id, p_ops, 0, MAX_INCOMING_R, ENABLE_ASSERTIONS == 1);
    if (ENABLE_ASSERTIONS) dbg_counter[R_QP_ID] = 0;
  }
  else if (ENABLE_ASSERTIONS && p_ops->r_rep_fifo->mes_size == 0) dbg_counter[R_QP_ID]++;
}


// Form the  work request for the read reply
static inline void forge_r_rep_wr(uint32_t r_rep_pull_ptr, uint16_t mes_i, struct pending_ops *p_ops,
                                  struct hrd_ctrl_blk *cb, struct ibv_sge *send_sgl,
                                  struct ibv_send_wr *send_wr, uint64_t *r_rep_tx,
                                  uint16_t t_id) {

  struct ibv_wc signal_send_wc;
  struct r_rep_fifo *r_rep_fifo = p_ops->r_rep_fifo;
  struct r_rep_message *r_rep_mes = (struct r_rep_message *) &r_rep_fifo->r_rep_message[r_rep_pull_ptr];
  uint8_t coalesce_num = r_rep_mes->coalesce_num;
  //struct rmw_rep_message *rmw_rep_mes = (struct rmw_rep_message *)r_rep_mes;
  //printf("%u\n", rmw_rep_mes->rmw_rep[0].opcode);

  send_sgl[mes_i].length = r_rep_fifo->message_sizes[r_rep_pull_ptr];
  if (ENABLE_ASSERTIONS) assert(send_sgl[mes_i].length <= R_REP_SEND_SIZE);
  //printf("Forging a r_resp with size %u \n", send_sgl[mes_i].length);
  send_sgl[mes_i].addr = (uint64_t) (uintptr_t) r_rep_mes;

  checks_and_prints_when_forging_r_rep_wr(coalesce_num, mes_i, send_sgl, r_rep_pull_ptr,
                                          r_rep_mes, r_rep_fifo, t_id);

  uint8_t rm_id = r_rep_fifo->rem_m_id[r_rep_pull_ptr];
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



// called when sending read replies
static inline void print_check_count_stats_when_sending_r_rep(struct r_rep_fifo *r_rep_fifo,
                                                              uint8_t coalesce_num,
                                                              uint16_t mes_i, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    uint32_t pull_ptr = r_rep_fifo->pull_ptr;
    struct r_rep_message *r_rep_mes = (struct r_rep_message *) &r_rep_fifo->r_rep_message[pull_ptr];
    check_state_with_allowed_flags(6, r_rep_mes->opcode, ACCEPT_REPLY_NO_CREDITS, ACCEPT_REPLY,
                                   PROP_REPLY, READ_REPLY, READ_PROP_REPLY);
    uint16_t byte_ptr = R_REP_MES_HEADER;
    struct r_rep_big *r_rep;
    struct rmw_rep_last_committed *rmw_rep;
    assert(r_rep_mes->coalesce_num > 0 && r_rep_mes->coalesce_num <= MAX_R_REP_COALESCE);
    for (uint8_t i = 0; i < r_rep_mes->coalesce_num; i++) {
      r_rep = (struct r_rep_big *)(((void *) r_rep_mes) + byte_ptr);
      uint8_t opcode = r_rep->opcode;
      //if (byte_ptr > 505)
        //printf("%u/%u \n", byte_ptr, r_rep_fifo->message_sizes[pull_ptr]);
      if (opcode > ACQ_LOG_EQUAL) opcode -= FALSE_POSITIVE_OFFSET;
      if (opcode < TS_SMALLER || opcode > ACQ_LOG_EQUAL)
        printf("R_rep %u/%u, byte ptr %u/%u opcode %u/%u \n",
               i, r_rep_mes->coalesce_num, byte_ptr, r_rep_fifo->message_sizes[pull_ptr],
               opcode, r_rep_mes->opcode);


      assert(opcode >= TS_SMALLER && opcode <= ACQ_LOG_EQUAL);
      bool is_rmw = false, is_rmw_acquire = false;
      if (opcode >= RMW_ACK && opcode <= NO_OP_PROP_REP)
        is_rmw = true;
      else if (opcode > NO_OP_PROP_REP)
        is_rmw_acquire = true;

      if (is_rmw) {
        check_state_with_allowed_flags(6, r_rep_mes->opcode, ACCEPT_REPLY_NO_CREDITS, ACCEPT_REPLY,
                                       PROP_REPLY, READ_PROP_REPLY);
        rmw_rep = (struct rmw_rep_last_committed *) r_rep;
        assert(opcode_is_rmw_rep(rmw_rep->opcode));
      }
      byte_ptr += get_size_from_opcode(r_rep->opcode);

    }
    //if (r_rep->opcode > ACQ_LOG_EQUAL) printf("big opcode comes \n");
    //check_a_polled_r_rep(r_rep, r_rep_mes, i, r_rep_num, t_id);
    if (DEBUG_READ_REPS)
      printf("Wrkr %d has %u read replies to send \n", t_id, r_rep_fifo->total_size);
    if (ENABLE_ASSERTIONS) {
      assert(r_rep_fifo->total_size >= coalesce_num);
      assert(mes_i < MAX_R_REP_WRS);
    }
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[t_id].r_reps_sent += coalesce_num;
    t_stats[t_id].r_reps_sent_mes_num++;
  }
}

// Send Read Replies
static inline void send_r_reps(struct pending_ops *p_ops, struct hrd_ctrl_blk *cb,
                               struct ibv_send_wr *r_rep_send_wr, struct ibv_sge *r_rep_send_sgl,
                               struct recv_info *r_recv_info, struct recv_info *w_recv_info,
                               uint64_t *r_rep_tx,  uint16_t t_id)
{
  uint16_t mes_i = 0, accept_recvs_to_post = 0, read_recvs_to_post = 0;
  uint32_t pull_ptr = p_ops->r_rep_fifo->pull_ptr;
  struct ibv_send_wr *bad_send_wr;

  struct r_rep_fifo *r_rep_fifo = p_ops->r_rep_fifo;
  while (r_rep_fifo->total_size > 0) {
    struct r_rep_message *r_rep_mes = (struct r_rep_message *) &r_rep_fifo->r_rep_message[pull_ptr];
    // Create the r_rep messages
    forge_r_rep_wr(pull_ptr, mes_i, p_ops, cb, r_rep_send_sgl, r_rep_send_wr, r_rep_tx, t_id);
    uint8_t coalesce_num = r_rep_mes->coalesce_num;
    print_check_count_stats_when_sending_r_rep(r_rep_fifo, coalesce_num, mes_i, t_id);
    r_rep_fifo->total_size -= coalesce_num;
    r_rep_fifo->mes_size--;
    //r_reps_sent += coalesce_num;
    if (r_rep_mes->opcode == ACCEPT_REPLY)
      accept_recvs_to_post++;
    else if (r_rep_mes->opcode != ACCEPT_REPLY_NO_CREDITS)
      read_recvs_to_post++;
    MOD_ADD(pull_ptr, R_REP_FIFO_SIZE);
    mes_i++;
  }
  if (mes_i > 0) {
    //if (ENABLE_ASSERTIONS) assert(mes_i == accept_recvs_to_post + read_recvs_to_post);
    if (read_recvs_to_post > 0) {
      if (DEBUG_READ_REPS) printf("Wrkr %d posting %d read recvs\n", t_id,  read_recvs_to_post);
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
    struct r_rep_message *r_rep_mes = (struct r_rep_message*) &incoming_r_reps[index].r_rep_mes;
    print_and_check_mes_when_polling_r_reps(r_rep_mes, index, t_id);
    bool is_propose = r_rep_mes->opcode == PROP_REPLY;
    bool is_accept = r_rep_mes->opcode == ACCEPT_REPLY ||
                     r_rep_mes->opcode == ACCEPT_REPLY_NO_CREDITS;
    if (r_rep_mes->opcode != ACCEPT_REPLY_NO_CREDITS)
      increase_credits_when_polling_r_reps(credits, is_accept, r_rep_mes->m_id, t_id);

    polled_messages++;
    MOD_ADD(index, R_REP_BUF_SLOTS);
    // If it is a reply to a propose/accept only call a different handler
    if (is_propose || is_accept) {
      handle_rmw_rep_replies(p_ops, r_rep_mes, is_accept, t_id);
      continue;
    }
    check_state_with_allowed_flags(3, r_rep_mes->opcode, READ_REPLY, READ_PROP_REPLY);
    r_rep_mes->opcode = INVALID_OPCODE; // a random meaningless opcode
    uint8_t r_rep_num = r_rep_mes->coalesce_num;
    // Find the request that the reply is referring to
    uint64_t l_id = r_rep_mes->l_id;
    uint64_t pull_lid = p_ops->local_r_id; // l_id at the pull pointer
    uint32_t r_ptr; // a pointer in the FIFO, from where r_rep refers to

    // if the pending read FIFO is empty it means the r_reps are for committed messages.
    if (find_the_r_ptr_rep_refers_to(&r_ptr, l_id, pull_lid, p_ops,
                                     r_rep_mes->opcode, r_rep_num,  t_id)) {
      if (ENABLE_ASSERTIONS) assert(r_rep_mes->opcode == READ_REPLY); // there are no rmw reps
      continue;
    }

    uint16_t byte_ptr = R_REP_MES_HEADER;
    int read_i = -1; // count non-rmw read replies
    for (uint16_t i = 0; i < r_rep_num; i++) {
      struct r_rep_big *r_rep = (struct r_rep_big *)(((void *) r_rep_mes) + byte_ptr);
      //if (r_rep->opcode > ACQ_LOG_EQUAL) printf("big opcode comes \n");
      check_a_polled_r_rep(r_rep, r_rep_mes, i, r_rep_num, t_id);
      byte_ptr += get_size_from_opcode(r_rep->opcode);
      bool is_rmw_rep = opcode_is_rmw_rep(r_rep->opcode);
      //printf("Wrkr %u, polling read %u/%u opcode %u irs_rmw %u\n",
      //       t_id, i, r_rep_num, r_rep->opcode, is_rmw_rep);
      if (!is_rmw_rep) {
        read_i++;
        if (handle_single_r_rep(r_rep, &r_ptr, l_id, pull_lid, p_ops, read_i, i, outstanding_reads, t_id))
          continue;
      }
      else handle_single_rmw_rep(p_ops, (struct rmw_rep_last_committed *) r_rep,
        (struct rmw_rep_message *) r_rep_mes, byte_ptr, is_accept, i, t_id);
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

  // Acquires on RMWs: In the same spirit we need a flag to denote whether we should broadcast commits
  // while the flag 'write_local_kvs' denotes whether we should commit to the local KVS
  bool insert_commit_flag;

  // Signal completion before going to the KVS on an Acquire that needs not go to the KVS
  bool signal_completion;
  // Signal completion after going to the KVS on an Acquire that needs to go to the KVS but does not need to be sent out (!!),
  // on any out-of epoch write, and an out-of-epoch read that needs to go to the KVS
  bool signal_completion_after_kvs_write;


  /* Because it's possible for a read to insert another read i.e OP_ACQUIRE->OP_ACQUIRE_FLIP_BIT
   * we need to make sure that even if all requests do that, the fifo will have enough space to:
   * 1) not deadlock and 2) not overwrite a read_info that will later get taken to the kvs
   * That means that the fifo must have free slots equal to SESSION_PER_THREADS because
   * this many acquires can possibly exist in the fifo*/
  if (ENABLE_ASSERTIONS) assert(p_ops->virt_r_size < PENDING_READS);
  while(p_ops->r_state[pull_ptr] == READY) {
    struct read_info *read_info = &p_ops->read_info[pull_ptr];
    //set the flags for each read
    set_flags_before_committing_a_read(read_info, &acq_second_round_to_flip_bit, &insert_write_flag,
                                       &write_local_kvs, &insert_commit_flag,
                                       &signal_completion, &signal_completion_after_kvs_write, t_id);
    checks_when_committing_a_read(p_ops, pull_ptr, acq_second_round_to_flip_bit, insert_write_flag,
                                  write_local_kvs, insert_commit_flag,
                                  signal_completion, signal_completion_after_kvs_write, t_id);

    // Break condition: this read cannot be processed, and thus no subsequent read will be processed
    if (((insert_write_flag || insert_commit_flag) && ((p_ops->virt_w_size + 1) >= MAX_ALLOWED_W_SIZE)) ||
        (write_local_kvs && (writes_for_cache >= MAX_INCOMING_R)))// ||
       // (acq_second_round_to_flip_bit) && (p_ops->virt_r_size >= MAX_ALLOWED_R_SIZE))
      break;

    //CACHE: Reads that need to go to kvs
    if (write_local_kvs) {
      // if a read did not see a larger ts it should only change the epoch
      if (read_info->opcode == KVS_OP_GET &&
        (!read_info->seen_larger_ts)) {
        read_info->opcode = UPDATE_EPOCH_OP_GET;
      }
      //check_state_with_allowed_flags(3, read_info->opcode, OP_ACQUIRE, UPDATE_EPOCH_OP_GET);
      if (read_info->seen_larger_ts) {
        if (ENABLE_ASSERTIONS) {
          //assert(read_info->value_to_read != NULL);
          //assert(read_info->val_len == VALUE_SIZE);
        }
        if (ENABLE_CLIENTS)
          memcpy(read_info->value_to_read, read_info->value, read_info->val_len);
      }
      p_ops->ptrs_to_mes_ops[writes_for_cache] = (void *) &p_ops->read_info[pull_ptr];
      writes_for_cache++;
      // An out-of-epoch write will get its TS set when inserting a write,
      // so there is no need to do it here
    }

    //INSERT WRITE: Reads that need to be converted to writes: second round of read/acquire or
    // Writes whose first round is a read: out-of-epoch writes/releases
    if (insert_write_flag) {
      if (read_info->opcode == OP_RELEASE ||
          read_info->opcode == KVS_OP_PUT) {
        read_info->ts_to_read.m_id = (uint8_t) machine_id;
        read_info->ts_to_read.version++;
        if (read_info->opcode == OP_RELEASE)
          memcpy(&p_ops->read_info[pull_ptr], &p_ops->r_session_id[pull_ptr], SESSION_BYTES);
      }
      else if (ENABLE_STAT_COUNTING) t_stats[t_id].read_to_write++;
      insert_write(p_ops, NULL, FROM_READ, pull_ptr, t_id);
    }
    // insert commit after rmw acquire if not a quorum of people have seen the last committed value
    else if (insert_commit_flag) {
      insert_write(p_ops, NULL, FROM_READ, pull_ptr, t_id);
    }


    // FAULT_TOLERANCE: In the off chance that the acquire needs a second round for fault tolerance
    if (unlikely(acq_second_round_to_flip_bit)) {
      increment_epoch_id(read_info->epoch_id, t_id); // epoch_id should be incremented always even it has been incremented since the acquire fired
      //printf("epoch_id is wrapping around %u ", epoch_id);
      if (DEBUG_QUORUM) printf("Worker %u increases the epoch id to %lu \n", t_id, (uint64_t) epoch_id);

      // The read must have the struct key overloaded with the original acquire l_id
      if (DEBUG_BIT_VECS)
        my_printf(cyan, "Wrkr, %u Opcode to be sent in the insert read %u, the local id to be sent %u, "
                    "read_info pull_ptr %u, read_info push_ptr %u read fifo size %u, virtual size: %u  \n",
                    t_id, read_info->opcode, p_ops->local_r_id, pull_ptr,
                    p_ops->r_push_ptr, p_ops->r_size, p_ops->virt_r_size);
      /* */
      p_ops->read_info[p_ops->r_push_ptr].opcode = OP_ACQUIRE_FLIP_BIT;
      insert_read(p_ops, NULL, FROM_ACQUIRE, t_id);
      read_info->fp_detected = false;
    }

    // SESSION: Acquires that wont have a second round and thus must free the session
    if (!insert_write_flag && !insert_commit_flag && (read_info->opcode == OP_ACQUIRE)) {
      if (ENABLE_ASSERTIONS) {
        assert(p_ops->r_session_id[pull_ptr] < SESSIONS_PER_THREAD);
        assert(p_ops->sess_info[p_ops->r_session_id[pull_ptr]].stalled);
      }
      p_ops->sess_info[p_ops->r_session_id[pull_ptr]].stalled = false;
      p_ops->all_sessions_stalled = false;
      if (MEASURE_LATENCY && t_id == LATENCY_THREAD && machine_id == LATENCY_MACHINE &&
          latency_info->measured_req_flag == ACQUIRE &&
          p_ops->r_session_id[pull_ptr] == latency_info->measured_sess_id)
        report_latency(latency_info);
    }

    // COMPLETION: Signal completion for reads/acquires that need not write the local KVS or
    // have a second write round (applicable only for acquires)
    if (signal_completion || read_info->opcode == UPDATE_EPOCH_OP_GET) {
      //printf("Completing opcode %u read_info val %u, copied over val %u \n",
      //read_info->opcode, read_info->value[0], read_info->value_to_read[0]);
      signal_completion_to_client(p_ops->r_session_id[pull_ptr],
                                  p_ops->r_index_to_req_array[pull_ptr], t_id);
    }
    else if (signal_completion_after_kvs_write) {
      if (ENABLE_ASSERTIONS) assert(!read_info->complete_flag);
        read_info->complete_flag = true;
    }
    //my_printf(cyan, "%u ptr freed, size %u/%u \n", pull_ptr, p_ops->r_size, p_ops->virt_r_size);
    // Clean-up code
    memset(&p_ops->read_info[pull_ptr], 0, 3); // a lin write uses these bytes for the session id but it's still fine to clear them
    p_ops->r_state[pull_ptr] = INVALID;
    p_ops->r_size--;
    p_ops->virt_r_size -= read_info->opcode == OP_ACQUIRE ? 2 : 1;

    if (read_info->is_rmw) read_info->is_rmw = false;
    if (ENABLE_ASSERTIONS) {
      assert(p_ops->virt_r_size < PENDING_READS);
      if (p_ops->r_size == 0) assert(p_ops->virt_r_size == 0);
    }
    MOD_ADD(pull_ptr, PENDING_READS);
    p_ops->local_r_id++;
  }
  p_ops->r_pull_ptr = pull_ptr;
  if (writes_for_cache > 0)
    KVS_batch_op_first_read_round(writes_for_cache, t_id, (struct read_info **) p_ops->ptrs_to_mes_ops,
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
      my_printf(yellow, "Wrkr %d is sending an ack for lid %lu, credits %u and ack num %d and m id %d \n",
                    t_id, acks[i].local_id, acks[i].credits, acks[i].ack_num, acks[i].m_id);

    acks[i].opcode = CACHE_OP_ACK;
    if (ENABLE_ASSERTIONS) {
      assert(acks[i].credits <= acks[i].ack_num);
      if (acks[i].ack_num > MAX_MES_IN_WRITE) assert(acks[i].credits > 1);
      assert(acks[i].credits <= W_CREDITS);
      assert(acks[i].ack_num > 0);
    }
    if ((*sent_ack_tx) % ACK_SS_BATCH == 0) {
      ack_send_wr[i].send_flags |= IBV_SEND_SIGNALED;
      // if (g_id == 0) my_printf(green, "Sending ack %llu signaled \n", *sent_ack_tx);
    } else ack_send_wr[i].send_flags = IBV_SEND_INLINE;
    if ((*sent_ack_tx) % ACK_SS_BATCH == ACK_SS_BATCH - 1) {
      // if (g_id == 0) my_printf(green, "Polling for ack  %llu \n", *sent_ack_tx);
      poll_cq(cb->dgram_send_cq[ACK_QP_ID], 1, &signal_send_wc, POLL_CQ_ACK);
    }
    if (ack_i > 0) {
      if (DEBUG_ACKS) my_printf(yellow, "Wrkr %u, ack %u points to ack %u \n", t_id, prev_ack_i, i);
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
  bool is_no_op = p_ops->w_meta[p_ops->w_pull_ptr].w_state == READY_NO_OP_RELEASE;
  struct write *rel = p_ops->ptrs_to_local_w[w_pull_ptr];
  if (ENABLE_ASSERTIONS) {
    assert (rel != NULL);
    if (is_no_op) assert(rel->opcode == NO_OP_RELEASE);
    else assert(rel->opcode == OP_RELEASE_BIT_VECTOR);
  }
  // because we overwrite the value,
  if (!is_no_op)
    memcpy(rel->value, &p_ops->overwritten_values[SEND_CONF_VEC_SIZE * w_pull_ptr], SEND_CONF_VEC_SIZE);
  struct trace_op op;
  op.session_id = (uint16_t) p_ops->w_meta[w_pull_ptr].sess_id;
  memcpy((void *) &op.ts, rel, W_SIZE); // We are treating the trace op as a sess_id + struct write
  //if (DEBUG_SESSIONS)
  //my_printf(cyan, "Wrkr: %u Inserting the write for the second round of the "
  //            "release opcode %u that carried a bit vector: session %u\n",
  //            t_id, op.opcode, p_ops->w_session_id[w_pull_ptr]);
  insert_write(p_ops, &op, RELEASE_THIRD, w_pull_ptr, t_id); // the push pointer is not needed because the session id is inside the op
  if (ENABLE_ASSERTIONS) {
    p_ops->ptrs_to_local_w[w_pull_ptr] =  NULL;
    memset(&p_ops->overwritten_values[SEND_CONF_VEC_SIZE * w_pull_ptr], 0, SEND_CONF_VEC_SIZE);
  }
}

// When a write has not gathered all acks but time-out expires
static inline bool complete_requests_that_wait_all_acks(uint8_t *w_state,
                                                        uint32_t w_ptr, uint16_t t_id)
{
  switch(*w_state) {
    case SENT_PUT:
    case SENT_RELEASE:
    case SENT_COMMIT:
      (*w_state) += W_STATE_OFFSET;
      return true;
    default:
      if (ENABLE_ASSERTIONS) {
        if (*w_state >= READY_PUT && *w_state <= READY_NO_OP_RELEASE)
          break;
        my_printf(red, "Wrkr %u state %u, ptr %u \n", t_id, w_state, w_ptr);
        assert(false);
      }
  }
  return false;
}

//
static inline void attempt_to_free_partially_acked_write(struct pending_ops *p_ops, uint16_t t_id)
{
  struct per_write_meta *w_meta = &p_ops->w_meta[p_ops->w_pull_ptr];

  if (w_meta->w_state >= SENT_PUT && w_meta->acks_seen >= REMOTE_QUORUM) {
    p_ops->full_w_q_fifo++;
    if (p_ops->full_w_q_fifo == WRITE_FIFO_TIMEOUT) {
      //printf("Wrkr %u expires write fifo timeout and "
      //         "releases partially acked writes \n", t_id);
      p_ops->full_w_q_fifo = 0;
      uint32_t w_pull_ptr = p_ops->w_pull_ptr;
      for (uint32_t i = 0; i < p_ops->w_size; i++) {
        w_meta = &p_ops->w_meta[w_pull_ptr];
        if (w_meta->w_state >= SENT_PUT && w_meta->acks_seen >= REMOTE_QUORUM) {
          if (complete_requests_that_wait_all_acks(&w_meta->w_state, w_pull_ptr, t_id))
              update_sess_info_partially_acked_write(p_ops, w_pull_ptr, t_id);
        }
        else if (p_ops->w_meta[w_pull_ptr].w_state < SENT_PUT) { break; }
        MOD_ADD(w_pull_ptr, PENDING_WRITES);
      }
    }
  }
}

//
static inline void clear_after_release_quorum(struct pending_ops *p_ops,
                                              uint32_t w_ptr, uint16_t t_id)
{
  uint32_t sess_id = p_ops->w_meta[w_ptr].sess_id;
  if (ENABLE_ASSERTIONS) assert( sess_id < SESSIONS_PER_THREAD);
  struct sess_info *sess_info = &p_ops->sess_info[sess_id];
  if (ENABLE_ASSERTIONS && !sess_info->stalled)
    printf("state %u ptr %u \n", p_ops->w_meta[w_ptr].w_state, w_ptr);
  // Releases, and Acquires/RMW-Acquires that needed a "write" round complete here
  signal_completion_to_client(sess_id, p_ops->w_index_to_req_array[w_ptr], t_id);
  check_sess_info_after_completing_release(sess_info, t_id);
  sess_info->stalled = false;
  p_ops->all_sessions_stalled = false;
}

// Remove writes that have seen all acks
static inline void remove_writes(struct pending_ops *p_ops, struct latency_flags *latency_info,
                                 uint16_t t_id)
{
  while(p_ops->w_meta[p_ops->w_pull_ptr].w_state >= READY_PUT) {
    p_ops->full_w_q_fifo = 0;
    uint32_t w_pull_ptr = p_ops->w_pull_ptr;
    struct per_write_meta *w_meta = &p_ops->w_meta[p_ops->w_pull_ptr];
    uint8_t w_state = w_meta->w_state;
    if (ENABLE_ASSERTIONS && EMULATE_ABD)
      assert(w_state == READY_RELEASE || w_state == READY_ACQUIRE);
    //if (DEBUG_ACKS)
    //  my_printf(green, "Wkrk %u freeing write at pull_ptr %u, w_size %u, w_state %d, session %u, local_w_id %lu, acks seen %u \n",
    //               g_id, p_ops->w_pull_ptr, p_ops->w_size, p_ops->w_state[p_ops->w_pull_ptr],
    //               p_ops->w_session_id[p_ops->w_pull_ptr], p_ops->local_w_id, p_ops->acks_seen[p_ops->w_pull_ptr]);
    //if (t_id == 1) printf("Wrkr %u Clearing state %u ptr %u \n", t_id, w_state, p_ops->w_pull_ptr);
    uint32_t sess_id = w_meta->sess_id;
    if (ENABLE_ASSERTIONS) assert(sess_id < SESSIONS_PER_THREAD);
    struct sess_info *sess_info = &p_ops->sess_info[sess_id];

    //if (w_state == READY_RELEASE ||
    if(w_state == READY_RMW_ACQ_COMMIT || w_state == READY_ACQUIRE) {
      if (!sess_info->stalled)
        printf("state %u ptr %u \n", w_state, p_ops->w_pull_ptr);
      // Releases, and Acquires/RMW-Acquires that needed a "write" round complete here
      signal_completion_to_client(sess_id, p_ops->w_index_to_req_array[w_pull_ptr], t_id);
      sess_info->stalled = false;
      p_ops->all_sessions_stalled = false;
    }

    // This case is tricky because in order to remove the release we must add another release
    // but if the queue was full that would deadlock, therefore we must remove the write before inserting
    // the second round of the release
    if (unlikely((w_state == READY_BIT_VECTOR || w_state == READY_NO_OP_RELEASE))) {
      commit_first_round_of_release_and_spawn_the_second (p_ops, t_id);
    }

    //  Free the write fifo entry
    if (w_state == READY_RELEASE) {
      if (ENABLE_ASSERTIONS) assert(p_ops->virt_w_size >= 2);
      p_ops->virt_w_size -= 2;
    }
    else {
      p_ops->virt_w_size--;
      //my_printf(yellow, "Decreasing virt_w_size %u at %u, state %u \n",
       //             p_ops->virt_w_size, w_pull_ptr, w_state);
    }
    p_ops->w_size--;
    w_meta->w_state = INVALID;
    w_meta->acks_seen = 0;
    p_ops->local_w_id++;
    memset(w_meta->seen_expected, 0, REM_MACH_NUM);
    MOD_ADD(p_ops->w_pull_ptr, PENDING_WRITES);
  } // while loop

  attempt_to_free_partially_acked_write(p_ops, t_id);
  // check_after_removing_writes(p_ops, t_id);
}

// Apply the acks that refer to stored writes
static inline void apply_acks(struct pending_ops *p_ops, uint16_t ack_num, uint32_t ack_ptr,
                              uint8_t ack_m_id, uint32_t *outstanding_writes,
                              uint64_t l_id, uint64_t pull_lid,
                              struct quorum_info *q_info,
                              struct latency_flags *latency_info, uint16_t t_id)
{
  for (uint16_t ack_i = 0; ack_i < ack_num; ack_i++) {
    //printf("Checking my acks \n");
    check_ack_and_print(p_ops, ack_i, ack_ptr, ack_num, l_id, pull_lid, t_id);
    struct per_write_meta *w_meta = &p_ops->w_meta[ack_ptr];
    w_meta->acks_seen++;
    bool ack_m_id_found = false;
    if (ENABLE_ASSERTIONS) assert(w_meta->acks_expected >= REMOTE_QUORUM);

//    if (machine_id == 0 && w_meta->acks_expected == 3) assert(w_meta->expected_ids[0] != 1);
//    else if (w_meta->acks_expected == 3 && (w_meta->expected_ids[1] == 1))
//      my_printf(red, "Wrkr %u ack_ptr %u/%u Expected %d/%d, expected_id[1] = %d/%d \n", t_id,
//                 ack_ptr, PENDING_WRITES,
//                 w_meta->acks_expected, q_info->active_num,
//                 w_meta->expected_ids[1], q_info->active_ids[1]);

    for (uint8_t i = 0; i < w_meta->acks_expected; i++) {
      if (ack_m_id == w_meta->expected_ids[i]) {
        ack_m_id_found = true;
        w_meta->seen_expected[i] = true;
        break;
      }
    }
    if (w_meta->w_state == SENT_PUT || w_meta->w_state == SENT_COMMIT ||
        w_meta->w_state == SENT_RELEASE) {
      if (!ack_m_id_found) {
        my_printf(red, "Wrkr %u, ack_ptr %u/%u received ack from m_i %u, state %u, received/expected %u/%u "
                     "active-machines/acks-seen: \n",
                   t_id, ack_ptr, PENDING_WRITES, ack_m_id,
                   w_meta->w_state, w_meta->acks_seen, w_meta->acks_expected);
        for (uint8_t i = 0; i < w_meta->acks_expected; i++) {
          my_printf(red, "%u/%u \n", w_meta->expected_ids[i], w_meta->seen_expected[i]);
        }
        assert(ack_m_id_found);
      }
    }


    uint8_t w_state = w_meta->w_state;
//    printf("Wrkr %d valid ack %u/%u, from %u write at ptr %d is %u/%u \n",
//           t_id, ack_i, ack_num, ack_m_id, ack_ptr,
//           w_meta->acks_seen, w_meta->acks_expected);

    // If it's a quorum, the request has been completed -- but releases/writes/commits will
    // still hold a slot in the write FIFO until they see expected acks (or timeout)
    if (w_meta->acks_seen == REMOTE_QUORUM) {
      if (ENABLE_ASSERTIONS) (*outstanding_writes)--;
//      printf("Wrkr %d valid ack %u/%u, write at ptr %d is ready \n",
//         t_id, ack_i, ack_num,  ack_ptr);
      switch(w_state) {
        case SENT_PUT : break;
        case SENT_RELEASE:
          clear_after_release_quorum(p_ops, ack_ptr, t_id);
          if (MEASURE_LATENCY && t_id == LATENCY_THREAD && machine_id == LATENCY_MACHINE &&
              latency_info->measured_req_flag != NO_REQ &&
              p_ops->w_meta[ack_ptr].sess_id == latency_info->measured_sess_id)
            report_latency(latency_info);
          break;
        case SENT_COMMIT:
          act_on_quorum_of_commit_acks(p_ops, ack_ptr, t_id);
          break;
        // THE FOLLOWING ARE WAITING FOR A QUORUM
        case SENT_ACQUIRE:
          if (MEASURE_LATENCY && t_id == LATENCY_THREAD && machine_id == LATENCY_MACHINE &&
              latency_info->measured_req_flag != NO_REQ &&
              p_ops->w_meta[ack_ptr].sess_id == latency_info->measured_sess_id)
            report_latency(latency_info);
        case SENT_RMW_ACQ_COMMIT:
        case SENT_BIT_VECTOR:
        case SENT_NO_OP_RELEASE:
          p_ops->w_meta[ack_ptr].w_state += W_STATE_OFFSET;
          break;
        default:
          if (w_state >= READY_PUT && w_state <= READY_NO_OP_RELEASE)
            break;
          my_printf(red, "Wrkr %u state %u, ptr %u \n", t_id, w_state, ack_ptr);
          assert(false);
      }
    }

    // Free writes/releases/commits
    if (w_meta->acks_seen == w_meta->acks_expected) {
      //assert(w_meta->acks_seen == REM_MACH_NUM);
      if (complete_requests_that_wait_all_acks(&w_meta->w_state, ack_ptr, t_id))
        update_sess_info_with_fully_acked_write(p_ops, ack_ptr, t_id);
    }
    MOD_ADD(ack_ptr, PENDING_WRITES);
  }
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
  //printf("Wrkr %u first time %d\n", t_id, completed_messages);
  if (completed_messages <= 0) return;
  while (polled_messages < completed_messages) {
    struct ack_message *ack = &incoming_acks[index].ack;
    uint16_t ack_num = ack->ack_num;
    check_ack_message_count_stats(p_ops, ack, index, ack_num, t_id);

    MOD_ADD(index, ACK_BUF_SLOTS);
    polled_messages++;
    uint64_t l_id = ack->local_id;
    uint64_t pull_lid = p_ops->local_w_id; // l_id at the pull pointer
    uint32_t ack_ptr; // a pointer in the FIFO, from where ack should be added
    credits[W_VC][ack->m_id] += ack->credits;
    //if (t_id == 1) printf("Credits %u, %u, \n", credits[W_VC][ack->m_id], ack->credits);
    assert(credits[W_VC][ack->m_id] <= W_CREDITS);
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
    apply_acks(p_ops, ack_num, ack_ptr, ack->m_id,  outstanding_writes, l_id,
               pull_lid, p_ops->q_info,  latency_info, t_id);
    if (ENABLE_ASSERTIONS) assert(credits[W_VC][ack->m_id] <= W_CREDITS);
    ack->opcode = INVALID_OPCODE;
    ack->ack_num = 0;
  } // while

  *pull_ptr = index;
  if (polled_messages > 0) {
    if (ENABLE_ASSERTIONS) dbg_counter[ACK_QP_ID] = 0;
  }
  else {
    if (ENABLE_ASSERTIONS && (*outstanding_writes) > 0) dbg_counter[ACK_QP_ID]++;
    if (ENABLE_STAT_COUNTING && (*outstanding_writes) > 0) t_stats[t_id].stalled_ack++;
  }
  if (ENABLE_ASSERTIONS) assert(ack_recv_info->posted_recvs >= polled_messages);
  ack_recv_info->posted_recvs -= polled_messages;
}



#endif /* INLINE_UTILS_H */

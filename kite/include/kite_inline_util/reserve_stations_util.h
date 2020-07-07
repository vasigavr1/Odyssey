//
// Created by vasilis on 22/05/20.
//

#ifndef KITE_RESERVE_STATIONS_UTIL_H
#define KITE_RESERVE_STATIONS_UTIL_H


#include <inline_util.h>
#include "main.h"
#include "latency_util.h"
#include "kite_debug_util.h"
#include "kite_config_util.h"
#include "client_if_util.h"
#include "paxos_util.h"
#include "paxos_generic_util.h"

//-------------------------------------------------------------------------------------
// -------------------------------FORWARD DECLARATIONS--------------------------------
//-------------------------------------------------------------------------------------
static inline void fill_commit_message_from_l_entry(struct commit *com, loc_entry_t *loc_entry,
                                                    uint8_t broadcast_state, uint16_t t_id);
static inline void fill_commit_message_from_r_info(struct commit *com,
                                                   r_info_t* r_info, uint16_t t_id);

static inline void KVS_isolated_op(int t_id, write_t *write);



/*--------------------------------------------------------------------------
 * -----------------Trace-pulling utility------------------------------
 * --------------------------------------------------------------------------*/




// In case of a miss in the KVS clean up the op, sessions and what not
static inline void clean_up_on_KVS_miss(trace_op_t *op, p_ops_t *p_ops,
                                        latency_info_t *latency_info, uint16_t t_id)
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


/* -----------------------------------------------------
 * --------------------SESSION INFO----------------------
 * -----------------------------------------------------*/

static inline void add_request_to_sess_info(sess_info_t *sess_info, uint16_t t_id)
{
  sess_info->live_writes++;
  sess_info->ready_to_release = false;
}

static inline void check_sess_info_after_completing_release
  (sess_info_t *sess_info, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(sess_info->stalled);
    //assert(sess_info->ready_to_release);
    //assert(sess_info->live_writes == 0);
  }
}


// If we are not bothering sending a write to a machine, we note this machine
// so that we know this amchine has not acked the write
static inline void update_sess_info_missing_ids_when_sending
  (p_ops_t *p_ops, w_mes_info_t *info,
   quorum_info_t *q_info, uint8_t w_i, uint16_t t_id)
{
  if (q_info->missing_num == 0 ) return;
  sess_info_t *sess_info = &p_ops->sess_info[info->per_message_sess_id[w_i]];

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


static inline void update_sess_info_with_fully_acked_write(p_ops_t *p_ops,
                                                           uint32_t w_ptr, uint16_t t_id)
{
  if (TURN_OFF_KITE) return;
  sess_info_t *sess_info = &p_ops->sess_info[p_ops->w_meta[w_ptr].sess_id];
  // The write gathered all expected acks so it needs not update the missing num or ids of the sess info
  if (ENABLE_ASSERTIONS) assert(sess_info->live_writes > 0);
  sess_info->live_writes--;
//  printf("Removing a fully acked %u \n", sess_info->live_writes);

  //my_printf(green, "increasing live writes %u \n", sess_info->live_writes);
  if (sess_info->live_writes == 0) {
    sess_info->ready_to_release = true;
  }
}

static inline void update_sess_info_partially_acked_write(p_ops_t *p_ops,
                                                          uint32_t w_ptr, uint16_t t_id)
{
  if (TURN_OFF_KITE) return;
  sess_info_t *sess_info = &p_ops->sess_info[p_ops->w_meta[w_ptr].sess_id];
  per_write_meta_t *w_meta = &p_ops->w_meta[w_ptr];

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


static inline void reset_sess_info_on_release(sess_info_t *sess_info,
                                              quorum_info_t *q_info, uint16_t t_id)
{
  if (TURN_OFF_KITE) return;
  sess_info->missing_num = q_info->missing_num;
  memcpy(sess_info->missing_ids, q_info->missing_ids, q_info->missing_num);
  if (ENABLE_ASSERTIONS) {
    assert(sess_info->stalled);
    assert(sess_info->live_writes == 0);
  }
  add_request_to_sess_info(sess_info, t_id);
}

static inline void reset_sess_info_on_accept(sess_info_t *sess_info,
                                             uint16_t t_id)
{
  if (!ACCEPT_IS_RELEASE || TURN_OFF_KITE) return;
  sess_info->missing_num = 0;
  if (ENABLE_ASSERTIONS) {
    assert(sess_info->stalled);
    assert(sess_info->live_writes == 0);
    assert(sess_info->ready_to_release);
  }
}


/* ---------------------------------------------------------------------------
//------------------------------ Inserting-utility----------------------------
//---------------------------------------------------------------------------*/


/*-------------READS/PROPOSES------------- */

// Returns the size of a read request given an opcode -- Proposes, reads, acquires
static inline uint16_t get_read_size_from_opcode(uint8_t opcode) {
  check_state_with_allowed_flags(9, opcode, OP_RELEASE, OP_ACQUIRE, KVS_OP_PUT,
                                 KVS_OP_GET, OP_ACQUIRE_FLIP_BIT, OP_GET_TS,
                                 PROPOSE_OP, OP_ACQUIRE_FP);
  switch(opcode) {
    case OP_RELEASE:
    case OP_ACQUIRE:
    case KVS_OP_PUT:
    case KVS_OP_GET:
    case OP_ACQUIRE_FLIP_BIT:
    case OP_GET_TS:
    case OP_ACQUIRE_FP:
      return R_SIZE;
    case PROPOSE_OP:
      return PROP_SIZE;
    default: if (ENABLE_ASSERTIONS) assert(false);
  }
}


// Set up a fresh read message to coalesce requests -- Proposes, reads, acquires
static inline void reset_read_message(p_ops_t *p_ops)
{
  MOD_INCR(p_ops->r_fifo->push_ptr, R_FIFO_SIZE);
  uint32_t r_mes_ptr = p_ops->r_fifo->push_ptr;
  struct r_message *r_mes = (struct r_message *) &p_ops->r_fifo->r_message[r_mes_ptr];
  r_mes_info_t * info = &p_ops->r_fifo->info[r_mes_ptr];

  r_mes->l_id = 0;
  r_mes->coalesce_num = 0;
  info->message_size = (uint16_t) R_MES_HEADER;
  info->max_rep_message_size = 0;
  info->reads_num = 0;
}


// Returns a pointer, where the next request can be created -- Proposes, reads, acquires
static inline void* get_r_ptr(p_ops_t *p_ops, uint8_t opcode,
                              bool is_get_ts, uint16_t t_id)
{
  bool is_propose = opcode == PROPOSE_OP;
  uint32_t r_mes_ptr = p_ops->r_fifo->push_ptr;
  r_mes_info_t *info = &p_ops->r_fifo->info[r_mes_ptr];
  uint16_t new_size = get_read_size_from_opcode(opcode);
  if (is_propose) info->max_rep_message_size += PROP_REP_ACCEPTED_SIZE;
  else if (is_get_ts) info->max_rep_message_size += R_REP_ONLY_TS_SIZE;
  else info->max_rep_message_size += ACQ_REP_SIZE;


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


/*-------------WRITES/ACCEPTS/COMMITS------------- */

// Set up a fresh write message to coalesce requests -- Accepts, commits, writes, releases
static inline void reset_write_message(p_ops_t *p_ops)
{

  MOD_INCR(p_ops->w_fifo->push_ptr, W_FIFO_SIZE);
  uint32_t w_mes_ptr = p_ops->w_fifo->push_ptr;
  struct w_message *w_mes = (struct w_message *)
    &p_ops->w_fifo->w_message[w_mes_ptr];
  w_mes_info_t * info = &p_ops->w_fifo->info[w_mes_ptr];
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
static inline bool coalesce_release(w_mes_info_t *info, struct w_message *w_mes,
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
static inline void* get_w_ptr(p_ops_t *p_ops, uint8_t opcode,
                              uint16_t session_id, uint16_t t_id)
{
  check_state_with_allowed_flags(9, opcode, OP_RELEASE, KVS_OP_PUT, ACCEPT_OP,
                                 COMMIT_OP, RMW_ACQ_COMMIT_OP, OP_RELEASE_SECOND_ROUND,
                                 OP_ACQUIRE, COMMIT_OP_NO_VAL);
  if (ENABLE_ASSERTIONS) assert(session_id < SESSIONS_PER_THREAD);

  bool is_accept = opcode == ACCEPT_OP;
  bool is_release = opcode == OP_RELEASE;
  bool release_or_acc = (!TURN_OFF_KITE) &&
    (is_release || (is_accept && ACCEPT_IS_RELEASE)); //is_accept || is_release;

  uint32_t w_mes_ptr = p_ops->w_fifo->push_ptr;
  w_mes_info_t *info = &p_ops->w_fifo->info[w_mes_ptr];
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

//
static inline uint8_t get_write_opcode(const uint8_t source, trace_op_t *op,
                                       r_info_t *r_info,
                                       loc_entry_t *loc_entry)
{
  switch(source) {
    case FROM_TRACE:
      return op->opcode;
    case FROM_READ:
      check_state_with_allowed_flags(4, r_info->opcode, OP_ACQUIRE, OP_RELEASE, KVS_OP_PUT);
      if (r_info->opcode == OP_ACQUIRE)
        return RMW_ACQ_COMMIT_OP;
      else return r_info->opcode;
    case FROM_COMMIT:
      if (loc_entry->avoid_val_in_com) {
        //loc_entry->avoid_val_in_com = false;
        return COMMIT_OP_NO_VAL;
      }
      return COMMIT_OP;
    case RELEASE_THIRD:
      return OP_RELEASE_SECOND_ROUND;
    default: assert(false);

  }
}

//
static inline void increase_virt_w_size(p_ops_t *p_ops, write_t *write,
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

//
static inline uint16_t get_w_sess_id(p_ops_t *p_ops, trace_op_t *op,
                                     const uint8_t source,
                                     const uint32_t incoming_pull_ptr,
                                     const uint16_t t_id)
{
  loc_entry_t *loc_entry = (loc_entry_t *) op;

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
          write_t *w = (write_t *) &op->ts;
          check_state_with_allowed_flags(3, w->opcode, OP_RELEASE_BIT_VECTOR, NO_OP_RELEASE);
        }
      }
      return op->session_id;
    default: if (ENABLE_ASSERTIONS) assert(false);
  }
}

// When inserting a write
static inline void
set_w_sess_info_and_index_to_req_array(p_ops_t *p_ops, trace_op_t *write,
                                       const uint8_t source, uint32_t w_ptr,
                                       const uint32_t incoming_pull_ptr,
                                       uint16_t sess_id, const uint16_t t_id)
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
  (p_ops_t *p_ops, write_t *write, trace_op_t *op,
   const uint8_t source, const uint32_t incoming_pull_ptr,
   r_info_t *r_info, const uint16_t t_id)
{
  my_assert(source <= FROM_COMMIT, "When inserting a write source is too high. Have you enabled lin writes?");

  if (source == FROM_TRACE) {
    write->version = op->ts.version;
    write->key = op->key;
    write->opcode = op->opcode;
    write->val_len = op->val_len;
    //memcpy(&write->version, (void *) &op->base_ts.version, 4 + KEY_SIZE + 2);
    if (ENABLE_ASSERTIONS) assert(op->real_val_len <= VALUE_SIZE);
    memcpy(write->value, op->value_to_write, op->real_val_len);
    write->m_id = (uint8_t) machine_id;
  }
  else if (source == RELEASE_THIRD) { // Second round of a release
    write_t *tmp = (write_t *) &op->ts; // we have treated the rest as a write_t
    memcpy(&write->m_id, tmp, W_SIZE);
    write->opcode = OP_RELEASE_SECOND_ROUND;
    //if (DEBUG_SESSIONS)
    // my_printf(cyan, "Wrkr %u: Changing the opcode from %u to %u of write %u of w_mes %u \n",
    //             t_id, op->opcode, write->opcode, inside_w_ptr, w_mes_ptr);
    if (ENABLE_ASSERTIONS) assert (write->m_id == (uint8_t) machine_id);
    if (DEBUG_QUORUM) {
      printf("Thread %u: Second round release, from ptr: %u to ptr %u, key: ", t_id, incoming_pull_ptr, p_ops->w_push_ptr);
      print_key(&write->key);
    }
  }
  else if (source == FROM_COMMIT || (source == FROM_READ && r_info->is_read)) {
    if (source == FROM_READ){
      if (ENABLE_ASSERTIONS) assert(r_info->opcode == OP_ACQUIRE);
      fill_commit_message_from_r_info((struct commit *) write, r_info, t_id);}
    else {
      uint8_t broadcast_state = (uint8_t) incoming_pull_ptr;
      fill_commit_message_from_l_entry((struct commit *) write,
                                       (loc_entry_t *) op, broadcast_state,  t_id);
    }
  }
  else { //source = FROM_READ: 2nd round of ooe-write/release
    write->m_id = r_info->ts_to_read.m_id;
    write->version = r_info->ts_to_read.version;
    write->key = r_info->key;
    memcpy(write->value, r_info->value, r_info->val_len);
    write->opcode = r_info->opcode;
    write->val_len = VALUE_SIZE >> SHIFT_BITS;
    if (ENABLE_ASSERTIONS) {
      assert(!r_info->is_read);
      assert(source == FROM_READ);
      check_state_with_allowed_flags(4, r_info->opcode, KVS_OP_PUT, OP_RELEASE);
    }
  }
  // Make sure the pointed values are correct
}




/*-------------R_REPS------------- */

// setup a new r_rep entry
static inline void set_up_r_rep_entry(struct r_rep_fifo *r_rep_fifo, uint8_t rem_m_id, uint64_t l_id,
                                      uint8_t read_opcode, bool is_rmw)
{
  MOD_INCR(r_rep_fifo->push_ptr, R_REP_FIFO_SIZE);
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
static inline struct r_rep_big* get_r_rep_ptr(p_ops_t *p_ops, uint64_t l_id,
                                              uint8_t rem_m_id, uint8_t read_opcode, bool coalesce,
                                              uint16_t t_id)
{
  check_state_with_allowed_flags(9, read_opcode, KVS_OP_GET, OP_ACQUIRE, OP_ACQUIRE_FLIP_BIT,
                                 PROPOSE_OP, ACCEPT_OP, ACCEPT_OP_NO_CREDITS, OP_ACQUIRE_FP, OP_GET_TS);
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
static inline void finish_r_rep_bookkeeping(p_ops_t *p_ops, struct r_rep_big *rep,
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

// This function sets the size and the opcode of a red reply, for reads/acquires and Read TS
// The locally stored TS is copied in the r_rep
static inline void set_up_r_rep_message_size(p_ops_t *p_ops,
                                             struct r_rep_big *r_rep,
                                             struct network_ts_tuple *remote_ts,
                                             bool read_ts,
                                             uint16_t t_id)
{
  struct r_rep_fifo *r_rep_fifo = p_ops->r_rep_fifo;
  compare_t ts_comp = compare_netw_ts(&r_rep->base_ts, remote_ts);
  if (machine_id == 0 && R_TO_W_DEBUG) {
    if (ts_comp == EQUAL)
      my_printf(green, "L/R:  m_id: %u/%u version %u/%u \n", r_rep->base_ts.m_id, remote_ts->m_id,
                r_rep->base_ts.version, remote_ts->version);
    else
      my_printf(red, "L/R:  m_id: %u/%u version %u/%u \n", r_rep->base_ts.m_id, remote_ts->m_id,
                r_rep->base_ts.version, remote_ts->version);
  }
  switch (ts_comp) {
    case SMALLER: // local is smaller than remote
      //if (DEBUG_TS) printf("Read TS is smaller \n");
      r_rep->opcode = TS_TOO_HIGH;
      break;
    case EQUAL:
      //if (DEBUG_TS) /printf("Read TS are equal \n");
      r_rep->opcode = TS_EQUAL;
      break;
    case GREATER: // local is greater than remote
      //This does not need the value, as it is going to do a write eventually
      r_rep->opcode = TS_TOO_SMALL;
      r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] += (R_REP_ONLY_TS_SIZE - R_REP_SMALL_SIZE);
      break;
    default:
      if (ENABLE_ASSERTIONS) assert(false);
  }

  if (ENABLE_ASSERTIONS) assert(r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] <= R_REP_SEND_SIZE);
}

//
static inline void set_up_rmw_acq_rep_message_size(p_ops_t *p_ops,
                                                   uint8_t opcode, uint16_t t_id)
{
  struct r_rep_fifo *r_rep_fifo = p_ops->r_rep_fifo;

  check_state_with_allowed_flags(7, opcode, CARTS_TOO_SMALL, CARTS_TOO_HIGH, CARTS_EQUAL,
                                 CARTS_TOO_SMALL + FALSE_POSITIVE_OFFSET,
                                 CARTS_TOO_HIGH + FALSE_POSITIVE_OFFSET,
                                 CARTS_EQUAL + FALSE_POSITIVE_OFFSET);

  if (opcode == CARTS_TOO_SMALL) {
    r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] += (ACQ_REP_SIZE - R_REP_SMALL_SIZE);
  }


  if (ENABLE_ASSERTIONS) assert(r_rep_fifo->message_sizes[r_rep_fifo->push_ptr] <= R_REP_SEND_SIZE);
}

/* ---------------------------------------------------------------------------
//------------------------------INSERTS-TO-MESSAGE_FIFOS----------------------
//---------------------------------------------------------------------------*/

// RMWs hijack the read fifo, to send propose broadcasts to all
static inline void insert_prop_to_read_fifo(p_ops_t *p_ops, loc_entry_t *loc_entry,
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

  memcpy(&prop->key, (void *)&loc_entry->key, KEY_SIZE);
  prop->opcode = PROPOSE_OP;
  prop->l_id = loc_entry->l_id;
  prop->t_rmw_id = loc_entry->rmw_id.id;
  prop->log_no = loc_entry->log_no;

  if (!loc_entry->base_ts_found)
    prop->base_ts = loc_entry->base_ts;
  else prop->base_ts.version = DO_NOT_CHECK_BASE_TS;

  // Query the conf to see if the machine has lost messages
  on_starting_an_acquire_query_the_conf(t_id, loc_entry->epoch_id);
  p_ops->r_fifo->bcast_size++;

  if (ENABLE_ASSERTIONS) {
    assert(prop->ts.version >= PAXOS_TS);
    //check_version(prop->base_ts.version, "insert_prop_to_read_fifo");
    assert(r_mes->coalesce_num > 0);
    assert(r_mes->m_id == (uint8_t) machine_id);
  }
  if (ENABLE_STAT_COUNTING) t_stats[t_id].proposes_sent++;
}


// Worker inserts a new local read to the read fifo it maintains -- Typically for Acquire
// but can also be the first round of an out-of-epoch write/release or an out-of-epoch read-- BUT NOT A PROPOSE!
static inline void insert_read(p_ops_t *p_ops, trace_op_t *op,
                               uint8_t source, uint16_t t_id)
{
  check_state_with_allowed_flags(3, source, FROM_TRACE, FROM_ACQUIRE);
  const uint32_t r_ptr = p_ops->r_push_ptr;
  r_info_t *r_info = &p_ops->read_info[r_ptr];
  uint8_t opcode = r_info->opcode;
  bool is_get_ts = source == FROM_TRACE && (opcode == OP_RELEASE || opcode == KVS_OP_PUT);

  struct read *read = (struct read*) get_r_ptr(p_ops, opcode, is_get_ts, t_id);

  // this means that the purpose of the read is solely to flip remote bits
  if (source == FROM_ACQUIRE) {
    // overload the key with local_r_id
    memcpy(&read->key, (void *) &p_ops->local_r_id, KEY_SIZE);
    read->opcode = OP_ACQUIRE_FLIP_BIT;
    if (ENABLE_ASSERTIONS) assert(opcode == OP_ACQUIRE_FLIP_BIT);
    if (DEBUG_BIT_VECS)
      my_printf(cyan, "Wrkr: %u Acquire generates a read with op %u and key %u \n",
                t_id, read->opcode, *(uint64_t *)&read->key);
  }
  else { // FROM TRACE: out of epoch reads/writes, acquires and releases
    assign_ts_to_netw_ts(&read->ts, &r_info->ts_to_read);
    read->log_no = r_info->log_no;
    read->key = r_info->key;
    if (!TURN_OFF_KITE)
      r_info->epoch_id = (uint64_t) atomic_load_explicit(&epoch_id, memory_order_seq_cst);
    read->opcode = is_get_ts ? (uint8_t) OP_GET_TS : opcode;
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
    if (opcode == OP_ACQUIRE)
      on_starting_an_acquire_query_the_conf(t_id, r_info->epoch_id);
  }

  // Increase the virtual size by 2 if the req is an acquire
  p_ops->virt_r_size+= opcode == OP_ACQUIRE ? 2 : 1;
  p_ops->r_size++;
  p_ops->r_fifo->bcast_size++;

  check_read_fifo_metadata(p_ops, r_mes, t_id);
  MOD_INCR(p_ops->r_push_ptr, PENDING_READS);

  if (ENABLE_STAT_COUNTING) {
    t_stats[t_id].reads_sent ++;
    if (r_mes->coalesce_num == 1) t_stats[t_id].reads_sent_mes_num++;
  }

}


// Insert accepts to the write message fifo
static inline void insert_accept_in_writes_message_fifo(p_ops_t *p_ops,
                                                        loc_entry_t *loc_entry,
                                                        bool helping,
                                                        uint16_t t_id)
{
  check_loc_entry_metadata_is_reset(loc_entry, "insert_accept_in_writes_message_fifo", t_id);
  if (ENABLE_ASSERTIONS) assert(loc_entry->helping_flag != PROPOSE_NOT_LOCALLY_ACKED);
  if (DEBUG_RMW) {
    my_printf(yellow, "Wrkr %u Inserting an accept, bcast size %u, "
                "rmw_id %lu, fifo push_ptr %u, fifo pull ptr %u\n",
              t_id, p_ops->w_fifo->bcast_size, loc_entry->rmw_id.id,
              p_ops->w_fifo->push_ptr, p_ops->w_fifo->bcast_pull_ptr);
  }
  struct accept *acc = (struct accept *)
    get_w_ptr(p_ops, ACCEPT_OP, loc_entry->sess_id, t_id);

  acc->l_id = loc_entry->l_id;
  acc->t_rmw_id = loc_entry->rmw_id.id;
  assign_ts_to_netw_ts(&acc->base_ts, &loc_entry->base_ts);
  assign_ts_to_netw_ts(&acc->ts, &loc_entry->new_ts);
  memcpy(&acc->key, &loc_entry->key, KEY_SIZE);
  acc->opcode = ACCEPT_OP;
  if (!helping && !loc_entry->rmw_is_successful)
    memcpy(acc->value, loc_entry->value_to_read, (size_t) RMW_VALUE_SIZE);
  else memcpy(acc->value, loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
  acc->log_no = loc_entry->log_no;
  acc->val_len = (uint8_t) loc_entry->rmw_val_len;
  signal_conf_bit_flip_in_accept(loc_entry, acc, t_id);

  p_ops->w_fifo->bcast_size++;
  if (ENABLE_ASSERTIONS) {
//    assert(acc->l_id < p_ops->prop_info->l_id);
  }
}


// Insert a new local or remote write to the pending writes
static inline void insert_write(p_ops_t *p_ops, trace_op_t *op, const uint8_t source,
                                const uint32_t incoming_pull_ptr, uint16_t t_id)
{
  r_info_t *r_info = NULL;
  loc_entry_t *loc_entry = (loc_entry_t *) op;
  if (source == FROM_READ) r_info = &p_ops->read_info[incoming_pull_ptr];
  uint32_t w_ptr = p_ops->w_push_ptr;
  uint8_t opcode = get_write_opcode(source, op, r_info, loc_entry);
  uint16_t sess_id =  get_w_sess_id(p_ops, op, source, incoming_pull_ptr, t_id);
  set_w_sess_info_and_index_to_req_array(p_ops, op, source, w_ptr, incoming_pull_ptr,
                                         sess_id, t_id);

  if (ENABLE_ASSERTIONS && source == FROM_READ &&
      r_info->opcode == KVS_OP_PUT) {
    assert(p_ops->sess_info[sess_id].writes_not_yet_inserted > 0);
    p_ops->sess_info[sess_id].writes_not_yet_inserted--;
  }

  write_t *write = (write_t *)
    get_w_ptr(p_ops, opcode, (uint16_t) p_ops->w_meta[w_ptr].sess_id, t_id);

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
  increase_virt_w_size(p_ops, write, source, t_id);
  MOD_INCR(p_ops->w_push_ptr, PENDING_WRITES);
}


// Insert a new r_rep to the r_rep reply fifo: used only for OP_ACQIUIRE_FLIP_BIT
// i.e. the message spawned by acquires that detected a false positive, meant to merely flip the owned bit
static inline void insert_r_rep(p_ops_t *p_ops, uint64_t l_id, uint16_t t_id,
                                uint8_t rem_m_id, bool coalesce,  uint8_t read_opcode)
{
  check_state_with_allowed_flags(2, read_opcode, OP_ACQUIRE_FLIP_BIT);
  struct r_rep_big *r_rep = get_r_rep_ptr(p_ops, l_id, rem_m_id, read_opcode, coalesce, t_id);
  r_rep->opcode = TS_EQUAL;
  finish_r_rep_bookkeeping(p_ops, r_rep, false, rem_m_id, t_id);
}



// Fill the trace_op to be passed to the KVS. Returns whether no more requests can be processed
static inline bool fill_trace_op(p_ops_t *p_ops, trace_op_t *op,
                                 trace_t *trace,
                                 uint16_t op_i, int working_session, uint16_t *writes_num_, uint16_t *reads_num_,
                                 struct session_dbg *ses_dbg,latency_info_t *latency_info,
                                 uint32_t *sizes_dbg_cntr,
                                 uint16_t t_id)
{
  create_inputs_of_op(&op->value_to_write, &op->value_to_read, &op->real_val_len,
                      &op->opcode, &op->index_to_req_array,
                      &op->key, op->value, trace, working_session, t_id);
  if (!ENABLE_CLIENTS) check_trace_req(p_ops, trace, op, working_session, t_id);
  uint16_t writes_num = *writes_num_, reads_num = *reads_num_;
  // Create some back pressure from the buffers, since the sessions may never be stalled
  if (!EMULATE_ABD) {
    if (op->opcode == (uint8_t) KVS_OP_PUT) writes_num++;
    //if (opcode == (uint8_t) OP_RELEASE) writes_num+= 2;
    // A write (relaxed or release) may first trigger a read
    reads_num += op->opcode == (uint8_t) OP_ACQUIRE ? 2 : 1;
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
  bool is_update = (op->opcode == (uint8_t) KVS_OP_PUT ||
                    op->opcode == (uint8_t) OP_RELEASE);
  bool is_rmw = opcode_is_rmw(op->opcode);
  bool is_read = !is_update && !is_rmw;

  if (ENABLE_ASSERTIONS) assert(is_read || is_update || is_rmw);
  //if (is_update || is_rmw) op->value_to_write = value_to_write;
  if (ENABLE_ASSERTIONS && !ENABLE_CLIENTS && op->opcode == FETCH_AND_ADD) {
    assert(is_rmw);
    assert(op->value_to_write == op->value);
    assert(*(uint64_t *) op->value_to_write == 1);
  }
  if (is_rmw && ENABLE_ALL_ABOARD) {
    op->attempt_all_aboard = p_ops->q_info->missing_num == 0;
  }

  if (op->opcode == KVS_OP_PUT) {
    add_request_to_sess_info(&p_ops->sess_info[working_session], t_id);
  }
  increment_per_req_counters(op->opcode, t_id);


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
    MOD_INCR(interface[t_id].wrkr_pull_ptr[working_session], PER_SESSION_REQ_NUM);
  }
  debug_set_version_of_op_to_one(op, op->opcode, t_id);
  return false;
}


/*----------------------------------------------------
 * ----------SENDING/RECEIVING RPCs-------------------
 * --------------------------------------------------*/


// When forging a write
static inline void set_w_state_for_each_write(p_ops_t *p_ops, w_mes_info_t *info,
                                              struct w_message *w_mes, uint32_t backward_ptr,
                                              uint8_t coalesce_num, struct ibv_sge *send_sgl,
                                              uint16_t br_i, quorum_info_t *q_info, uint16_t t_id)
{
  uint16_t byte_ptr = W_MES_HEADER;
  bool failure = false;

  if (info->is_release ) {
    failure = add_failure_to_release_from_sess_id(p_ops, w_mes, info, q_info, backward_ptr, t_id);
  }
  for (uint8_t i = 0; i < coalesce_num; i++) {
    write_t *write = (write_t *)(((void *)w_mes) + byte_ptr);
    //printf("Write %u/%u opcode %u \n", i, coalesce_num, write->opcode);
    byte_ptr += get_write_size_from_opcode(write->opcode);

    per_write_meta_t *w_meta = &p_ops->w_meta[backward_ptr];
    uint8_t *w_state = &w_meta->w_state;

    sess_info_t *sess_info = &p_ops->sess_info[info->per_message_sess_id[i]];
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
      case COMMIT_OP_NO_VAL:
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
        if (!TURN_OFF_KITE && failure) {
          write->opcode = NO_OP_RELEASE;
          //write_t *first_rel = (((write *)w_mes) + info->first_release_byte_ptr);
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

      MOD_INCR(backward_ptr, PENDING_WRITES);
    }


  }
}



static inline bool release_not_ready(p_ops_t *p_ops,
                                     w_mes_info_t *info, struct w_message *w_mes,
                                     uint32_t *release_rdy_dbg_cnt, uint16_t t_id)
{
  if (TURN_OFF_KITE) return false;
  if (!info->is_release)
    return false; // not even a release

  //sess_info_t *sess_info = p_ops->sess_info;
  // We know the message contains releases. let's check their sessions!
  for (uint8_t i = 0; i < w_mes->coalesce_num; i++) {
    if (info->per_message_release_flag[i]) {
      sess_info_t *sess_info = &p_ops->sess_info[info->per_message_sess_id[i]];
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


// return true if the loc_entry cannot be inspected
static inline bool cannot_accept_if_unsatisfied_release(loc_entry_t* loc_entry,
                                                        sess_info_t *sess_info)
{
  if (TURN_OFF_KITE || (!ACCEPT_IS_RELEASE)) return false;
  if (loc_entry->must_release && !sess_info->ready_to_release) {
    loc_entry->stalled_reason = STALLED_BECAUSE_ACC_RELEASE;
    return true;
  }
  else if (loc_entry->must_release) {
    loc_entry->stalled_reason = NO_REASON;
    loc_entry->must_release = false;
  }
  return false;
}

/*----------------------------------------------------
 * ---------------COMMITTING----------------------------
 * --------------------------------------------------*/

// Release performs two writes when the first round must carry the send vector
static inline void commit_first_round_of_release_and_spawn_the_second (p_ops_t *p_ops,
                                                                       uint16_t t_id)
{
  uint32_t w_pull_ptr = p_ops->w_pull_ptr;
  bool is_no_op = p_ops->w_meta[p_ops->w_pull_ptr].w_state == READY_NO_OP_RELEASE;
  write_t *rel = p_ops->ptrs_to_local_w[w_pull_ptr];
  if (ENABLE_ASSERTIONS) {
    assert (rel != NULL);
    if (is_no_op) assert(rel->opcode == NO_OP_RELEASE);
    else assert(rel->opcode == OP_RELEASE_BIT_VECTOR);
  }
  // because we overwrite the value,
  if (!is_no_op)
    memcpy(rel->value, &p_ops->overwritten_values[SEND_CONF_VEC_SIZE * w_pull_ptr], SEND_CONF_VEC_SIZE);
  trace_op_t op;
  op.session_id = (uint16_t) p_ops->w_meta[w_pull_ptr].sess_id;
  memcpy((void *) &op.ts, rel, W_SIZE); // We are treating the trace op as a sess_id + write_t
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
static inline void attempt_to_free_partially_acked_write(p_ops_t *p_ops, uint16_t t_id)
{
  per_write_meta_t *w_meta = &p_ops->w_meta[p_ops->w_pull_ptr];

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
        MOD_INCR(w_pull_ptr, PENDING_WRITES);
      }
    }
  }
}


//
static inline void clear_after_release_quorum(p_ops_t *p_ops,
                                              uint32_t w_ptr, uint16_t t_id)
{
  uint32_t sess_id = p_ops->w_meta[w_ptr].sess_id;
  if (ENABLE_ASSERTIONS) assert( sess_id < SESSIONS_PER_THREAD);
  sess_info_t *sess_info = &p_ops->sess_info[sess_id];
  if (ENABLE_ASSERTIONS && !sess_info->stalled)
    printf("state %u ptr %u \n", p_ops->w_meta[w_ptr].w_state, w_ptr);
  // Releases, and Acquires/RMW-Acquires that needed a "write" round complete here
  signal_completion_to_client(sess_id, p_ops->w_index_to_req_array[w_ptr], t_id);
  check_sess_info_after_completing_release(sess_info, t_id);
  sess_info->stalled = false;
  p_ops->all_sessions_stalled = false;
}

/*------------------_READS_------------------------------------------------------------ */
// When committing reads
static inline void set_flags_before_committing_a_read(r_info_t *read_info,
                                                      bool *acq_second_round_to_flip_bit, bool *insert_write_flag,
                                                      bool *write_local_kvs,
                                                      bool *signal_completion, bool *signal_completion_after_kvs_write,
                                                      uint16_t t_id)
{

  bool acq_needs_second_round = (!read_info->seen_larger_ts && read_info->times_seen_ts < REMOTE_QUORUM) ||
                                (read_info->seen_larger_ts && read_info->times_seen_ts <= REMOTE_QUORUM);


  (*insert_write_flag) = (read_info->opcode != KVS_OP_GET) &&
                         (read_info->opcode == OP_RELEASE ||
                          read_info->opcode == KVS_OP_PUT || acq_needs_second_round);


  (*write_local_kvs) = (read_info->opcode != OP_RELEASE) &&
                       (read_info->seen_larger_ts ||
                        (read_info->opcode == KVS_OP_GET) || // a simple read is quorum only if the kvs epoch is behind..
                        (read_info->opcode == KVS_OP_PUT));  // out-of-epoch write

  (*acq_second_round_to_flip_bit) = read_info->fp_detected;


  (*signal_completion) = (read_info->opcode == OP_ACQUIRE) && !(*insert_write_flag) &&
                         !(*write_local_kvs);

  //all requests that will not be early signaled except: releases and acquires that actually have a second round
  //That leaves: out-of-epoch writes/reads & acquires that want to write the KVS
  (*signal_completion_after_kvs_write) = !(*signal_completion) &&
                                         !((read_info->opcode == OP_ACQUIRE && acq_needs_second_round) ||
                                           (read_info->opcode == OP_RELEASE) || (read_info->opcode == OP_ACQUIRE_FLIP_BIT) );

}


static inline bool
is_there_buffer_space_to_commmit_the_read (p_ops_t *p_ops,
                                           bool insert_write_flag,
                                           bool write_local_kvs,
                                           uint16_t writes_for_cache)
{
  return ((insert_write_flag) && ((p_ops->virt_w_size + 1) >= MAX_ALLOWED_W_SIZE)) ||
         (write_local_kvs && (writes_for_cache >= MAX_INCOMING_R));
}

static inline void read_commit_bookkeeping_to_write_local_kvs(p_ops_t *p_ops,
                                                              r_info_t *read_info,
                                                              uint32_t pull_ptr,
                                                              uint16_t *writes_for_cache)
{
  check_state_with_allowed_flags(4, read_info->opcode, OP_ACQUIRE, KVS_OP_GET, KVS_OP_PUT);
  // if a read did not see a larger base_ts it should only change the epoch
  if (read_info->opcode == KVS_OP_GET && !read_info->seen_larger_ts) {
    read_info->opcode = UPDATE_EPOCH_OP_GET;
  }
  if (read_info->seen_larger_ts) {
    if (ENABLE_CLIENTS) {
      if (ENABLE_ASSERTIONS) assert(read_info->value_to_read != NULL);
      memcpy(read_info->value_to_read, read_info->value, read_info->val_len);
    }
  }
  p_ops->ptrs_to_mes_ops[*writes_for_cache] = (void *) &p_ops->read_info[pull_ptr];
  (*writes_for_cache)++;
}

static inline void read_commit_bookkeeping_to_insert_write(p_ops_t *p_ops,
                                                           r_info_t *read_info,
                                                           uint32_t pull_ptr,
                                                           uint16_t t_id)
{
  if (read_info->opcode == OP_RELEASE || read_info->opcode == KVS_OP_PUT) {
    read_info->ts_to_read.m_id = (uint8_t) machine_id;
    read_info->ts_to_read.version++;
    if (read_info->opcode == OP_RELEASE)
      memcpy(&p_ops->read_info[pull_ptr], &p_ops->r_session_id[pull_ptr], SESSION_BYTES);
  }
  else if (ENABLE_STAT_COUNTING) t_stats[t_id].read_to_write++;
  if(ENABLE_ASSERTIONS && read_info->is_read) assert(read_info->opcode == OP_ACQUIRE);
  insert_write(p_ops, NULL, FROM_READ, pull_ptr, t_id);
}


static inline void read_commit_spawn_flip_bit_message(p_ops_t *p_ops,
                                                      r_info_t *read_info,
                                                      uint32_t pull_ptr,
                                                      uint16_t t_id)
{
  // epoch_id should be incremented always even it has been incremented since the acquire fired
  increment_epoch_id(read_info->epoch_id, t_id);
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

static inline void read_commit_acquires_free_sess(p_ops_t *p_ops,
                                                  uint32_t pull_ptr,
                                                  latency_info_t *latency_info,
                                                  uint16_t t_id)
{
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


static inline void
read_commit_complete_and_empty_read_info(p_ops_t *p_ops,
                                         r_info_t *read_info,
                                         bool signal_completion,
                                         bool signal_completion_after_kvs_write,
                                         uint32_t pull_ptr,
                                         uint16_t t_id)
{
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
  memset(&p_ops->read_info[pull_ptr], 0, 3); // a release uses these bytes for the session id but it's still fine to clear them
  p_ops->r_state[pull_ptr] = INVALID;
  p_ops->r_size--;
  p_ops->virt_r_size -= read_info->opcode == OP_ACQUIRE ? 2 : 1;
  read_info->is_read = false;
  if (ENABLE_ASSERTIONS) {
    assert(p_ops->virt_r_size < PENDING_READS);
    if (p_ops->r_size == 0) assert(p_ops->virt_r_size == 0);
  }
  p_ops->local_r_id++;
}


/*----------------------------------------------------
 * ---------------POLLING----------------------------
 * --------------------------------------------------*/

// Returns true, if you should move to the next message
static inline bool find_the_r_ptr_rep_refers_to(uint32_t *r_ptr, uint64_t l_id, uint64_t pull_lid,
                                                p_ops_t *p_ops,
                                                uint8_t mes_opcode, uint8_t r_rep_num, uint16_t  t_id)
{
  if (p_ops->r_size == 0 && mes_opcode == READ_REPLY) {
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

static inline void fill_read_info_from_read_rep(struct r_rep_big *r_rep, r_info_t *read_info,
                                                uint16_t t_id)
{
  if (ENABLE_ASSERTIONS && read_info->is_read) {
    if (compare_netw_carts_with_carts(&r_rep->base_ts, r_rep->log_no,
                                      &read_info->ts_to_read, read_info->log_no)
        != GREATER) {
      my_printf(red, "Rep version/m_id/log: %u/%u/%u, r_info version/m_id/logno: %u/%u/%u \n",
                r_rep->base_ts.version, r_rep->base_ts.m_id, r_rep->log_no,
                read_info->ts_to_read.version, read_info->ts_to_read.m_id, read_info->log_no);
      assert(false);
    }
    assert(r_rep->base_ts.version >= read_info->ts_to_read.version);
    if (r_rep->log_no == 0) assert(r_rep->rmw_id == 0);
  }
  if (read_info->is_read) {
    read_info->log_no = r_rep->log_no;
    read_info->rmw_id.id = r_rep->rmw_id;
    memcpy(read_info->value, r_rep->value, read_info->val_len);
  }
  assign_netw_ts_to_ts(&read_info->ts_to_read, &r_rep->base_ts);

}


// Each read has an associated read_info structure that keeps track of the incoming replies, value, opcode etc.
static inline void read_info_bookkeeping(struct r_rep_big *r_rep,
                                         r_info_t *read_info, uint16_t t_id)
{

  detect_false_positives_on_read_info_bookkeeping(r_rep, read_info, t_id);
  if (r_rep->opcode == CARTS_TOO_SMALL || r_rep->opcode == TS_TOO_SMALL) {
    if (!read_info->seen_larger_ts) {
      fill_read_info_from_read_rep(r_rep, read_info, t_id);
      read_info->times_seen_ts = 1;
      read_info->seen_larger_ts = true;
    }
    else { // if the read has already received a "greater" base_ts
      compare_t compare = read_info->is_read ? compare_netw_carts_with_carts(&r_rep->base_ts, r_rep->log_no,
                                                                                &read_info->ts_to_read, read_info->log_no) :
                                                  compare_netw_ts_with_ts(&r_rep->base_ts, &read_info->ts_to_read);
      if (compare == GREATER) {
        fill_read_info_from_read_rep(r_rep, read_info, t_id);
        read_info->times_seen_ts = 1;
      }
      else if (compare == EQUAL) read_info->times_seen_ts++;
      // Nothing to do if the already stored base_ts is greater than the incoming
    }
  }
  else if (r_rep->opcode == CARTS_EQUAL || r_rep->opcode == TS_EQUAL) {
    if (!read_info->seen_larger_ts)
      read_info->times_seen_ts++;
  }
  else if (r_rep->opcode == CARTS_TOO_HIGH || r_rep->opcode == TS_TOO_HIGH) {
    // Nothing to do if the already stored base_ts is greater than the incoming
  }
  read_info->rep_num++;
}

//When polling read replies, handle a reply to read, acquire, readts, rmw acquire-- return true to continue to next rep
static inline bool handle_single_r_rep(struct r_rep_big *r_rep, uint32_t *r_ptr_, uint64_t l_id, uint64_t pull_lid,
                                       p_ops_t *p_ops, int read_i, uint16_t r_rep_i,
                                       uint32_t *outstanding_reads, uint16_t t_id)
{
  uint32_t r_ptr = *r_ptr_;
  if (p_ops->r_size == 0) return true;
  check_r_rep_l_id(l_id, (uint8_t) read_i, pull_lid, p_ops->r_size, t_id);
  if (pull_lid >= l_id) {
    if (l_id + read_i < pull_lid) return true;
  }
  r_info_t *read_info = &p_ops->read_info[r_ptr];
  if (DEBUG_READ_REPS)
    my_printf(yellow, "Read reply %u, Received replies %u/%d at r_ptr %u \n",
              r_rep_i, read_info->rep_num, REMOTE_QUORUM, r_ptr);

  read_info_bookkeeping(r_rep, read_info, t_id);

  if (read_info->rep_num >= REMOTE_QUORUM) {
    //my_printf(yellow, "%u r_ptr becomes ready, l_id %u,   \n", r_ptr, l_id);
    p_ops->r_state[r_ptr] = READY;
    if (ENABLE_ASSERTIONS) {
      (*outstanding_reads)--;
      assert(read_info->rep_num <= REM_MACH_NUM);
    }
  }
  MOD_INCR(r_ptr, PENDING_READS);
  r_rep->opcode = INVALID_OPCODE;
  *r_ptr_ = r_ptr;
  return false;
}





#endif //KITE_RESERVE_STATIONS_UTIL_H

//
// Created by vasilis on 11/05/20.
//

#ifndef KITE_CONFIG_UTIL_H
#define KITE_CONFIG_UTIL_H

#include "common_func.h"
#include "main.h"
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

// Make sure we avoid aliasing between local_r_ids of regular reads and rmw-ids, when trying to own configuration bits
static inline void tag_rmw_id_when_owning_a_conf_bit(uint8_t *rmw_id)
{
  if (ENABLE_ASSERTIONS) assert(rmw_id[7] == 0);
  rmw_id[7] = ACCEPT_FLIPS_BIT_OP;
}

// Call this after an acquire/propose have detected a failure
static inline void increment_epoch_id (uint64_t req_epoch_id,  uint16_t t_id)
{
  if (epoch_id <= req_epoch_id)
    epoch_id++;
}


// When an out-of-epoch request completes rectify the keys epoch
static inline void rectify_key_epoch_id(uint64_t epoch_id, mica_op_t *kv_ptr, uint16_t t_id) {
  if (!MEASURE_SLOW_PATH) {
    if (epoch_id > kv_ptr->epoch_id)
      kv_ptr->epoch_id = epoch_id;
  }
}


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
//    if some remote release has raised the bit, then increase epoch id and flip the bit
static inline void on_starting_an_acquire_query_the_conf(const uint16_t t_id, uint64_t req_epoch_id)
{
  if (unlikely(conf_bit_vec[machine_id].bit == DOWN_STABLE)) {
    increment_epoch_id(req_epoch_id, t_id);
    set_conf_bit_to_new_state(t_id, (uint16_t) machine_id, UP_STABLE);
    if (DEBUG_BIT_VECS)
      my_printf(yellow, "Thread %u, acquire increases the epoch id "
        "as a remote release has notified the machine "
        "it has lost messages, new epoch id %u\n", t_id, epoch_id);
  }
}

// 4. On receiving the first round of an Acquire bring the bit to DOWN_TRANSIENT_OWNED
// and register the acquire's local_r_id as one of the bit's owners
// Returns true if ownership of the conf bit has been successful
static inline bool take_ownership_of_a_conf_bit(const uint64_t local_r_id, const uint16_t acq_m_id,
                                                bool is_rmw, const uint16_t t_id)
{
  if (conf_bit_vec[acq_m_id].bit == UP_STABLE) return false;
  if (DEBUG_BIT_VECS)
    my_printf(yellow, "Wrkr %u An acquire from machine %u  is looking to take ownership "
      "of a bit: local_r_id %u \n", t_id, acq_m_id, local_r_id);
  bool owned_a_failure = false;
  // if it's down, own it, even if it is already owned
  if (is_rmw) {
    tag_rmw_id_when_owning_a_conf_bit((uint8_t *) &local_r_id);
    if (ENABLE_ASSERTIONS) assert(local_r_id > B_512);
  }

  while (!atomic_flag_test_and_set_explicit(&conf_bit_vec[acq_m_id].lock, memory_order_acquire));
  if (conf_bit_vec[acq_m_id].bit != UP_STABLE) {
    uint32_t ses_i = conf_bit_vec[acq_m_id].sess_num[t_id];
    conf_bit_vec[acq_m_id].owners[t_id][ses_i] = local_r_id;
    MOD_ADD(conf_bit_vec[acq_m_id].sess_num[t_id], SESSIONS_PER_THREAD);
    owned_a_failure = true;
    conf_bit_vec[acq_m_id].bit = DOWN_TRANSIENT_OWNED;
  }
  atomic_flag_clear_explicit(&conf_bit_vec[acq_m_id].lock, memory_order_release);

  if (DEBUG_BIT_VECS) {
    if (t_id == 0 && owned_a_failure) {
      uint32_t ses_i = (conf_bit_vec[acq_m_id].sess_num[t_id] + SESSIONS_PER_THREAD - 1) % SESSIONS_PER_THREAD;
      my_printf(green, "Wrkr %u acquire from machine %u got ownership of its failure, "
                  "bit %u  owned t_id %u, owned local_r_id %u/%u \n",
                t_id, acq_m_id, conf_bit_vec[acq_m_id].bit,
                t_id, local_r_id, conf_bit_vec[acq_m_id].owners[t_id][ses_i]);
    }
  }
  return owned_a_failure;
}

// 5. On receiving the second round of an Acquire that owns a bit (DOWN_TRANSIENT_OWNED)
//    bring that bit to UP_STABLE
static inline void raise_conf_bit_iff_owned(const uint64_t local_r_id,  const uint16_t acq_m_id,
                                            bool is_rmw, const uint16_t t_id)
{
  if (DEBUG_BIT_VECS)
    my_printf(yellow, "Wrkr %u An acquire from machine %u  is looking if it owns a failure local_r_id %u \n",
              t_id, acq_m_id, local_r_id);
  // First change the state  of the owned bits
  bool bit_gets_flipped = false;
  if (conf_bit_vec[acq_m_id].bit != DOWN_TRANSIENT_OWNED) {
    return;
  }

  // add an offset to the id, to make sure rmw-ids cannot conflict with local_r_ids
  if (is_rmw) {
    tag_rmw_id_when_owning_a_conf_bit((uint8_t *) &local_r_id);
    if (ENABLE_ASSERTIONS) assert(local_r_id > B_512);
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


  if (DEBUG_BIT_VECS ) {
    if (bit_gets_flipped) {
      if (t_id == 0 && conf_bit_vec[acq_m_id].bit == UP_STABLE);
      my_printf(green, "Wrkr %u Acquire  from machine %u had ownership of its failure bit %u/%d, "
                  "owned t_id %u, owned local_w_id %u\n",
                t_id, acq_m_id, conf_bit_vec[acq_m_id].bit, UP_STABLE,
                t_id, local_r_id);
    }
  }

}

// Detect a failure: Bring a given bit of config_bit_vector to state DOWN_STABLE
static inline void set_conf_bit_after_detecting_failure(const uint16_t m_id, const uint16_t t_id)
{
  if (DEBUG_BIT_VECS)
    my_printf(yellow, "Wrkr %u handles Send and conf bit vec after failure to machine %u,"
                " send bit %u, state %u, conf_bit %u \n",
              t_id, m_id, send_bit_vector.bit_vec[m_id].bit, send_bit_vector.state,
              conf_bit_vec[m_id].bit);

  set_conf_bit_to_new_state(t_id, m_id, DOWN_STABLE);
  if (DEBUG_BIT_VECS)
    my_printf(green, "Wrkr %u After: send bit %u, state %u, conf_bit %u \n",
              t_id, send_bit_vector.bit_vec[m_id].bit, send_bit_vector.state,
              conf_bit_vec[m_id].bit);
}

// returns the number of failures
static inline uint8_t create_bit_vec_of_failures(p_ops_t *p_ops, struct w_message *w_mes,
                                                 w_mes_info_t *info, struct quorum_info *q_info,
                                                 uint8_t *bit_vector_to_send, uint16_t t_id)
{
  bool bit_vec[MACHINE_NUM] = {0};
  uint8_t failed_machine_num = 0 ;
  // Then look at each release in the message sess_info
  for (uint8_t w_i = 0; w_i < w_mes->coalesce_num; w_i++) {
    if (!info->per_message_release_flag[w_i]) continue;
    if (ENABLE_ASSERTIONS) assert(info->per_message_sess_id[w_i] <= SESSIONS_PER_THREAD);
    sess_info_t *sess_info = &p_ops->sess_info[info->per_message_sess_id[w_i]];
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
  (p_ops_t *p_ops, struct w_message *w_mes,
   w_mes_info_t *info, struct quorum_info *q_info,
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
      //sess_info_t *sess_info = &p_ops->sess_info[info->per_message_sess_id[w_i]];
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


// When creating the accept message have it try to flip the remote bits,
// if a false positive has been previously detected by a propose
static inline void signal_conf_bit_flip_in_accept(loc_entry_t *loc_entry,
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
                                                                   r_info_t *read_info,
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





#endif //KITE_CONFIG_UTIL_H

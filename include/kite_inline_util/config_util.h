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



#endif //KITE_CONFIG_UTIL_H

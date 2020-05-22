//
// Created by vasilis on 11/05/20.
//

#ifndef KITE_GENERIC_UTILITY_H
#define KITE_GENERIC_UTILITY_H


#include <stdint.h>
#include "main.h"

static inline bool is_odd(uint64_t var) {
  return ((var % 2) == 1);
}

static inline bool is_even(uint64_t var) {
  return ((var % 2) == 0);
}

static inline void lock_seqlock(seqlock_t *seqlock)
{
  if (WORKERS_PER_MACHINE == 1) return;
  uint64_t tmp_lock, new_lock;
  tmp_lock = (uint64_t) atomic_load_explicit(seqlock, memory_order_acquire);
  do {
    // First spin in your L1, reading until the lock is
    while (is_odd(tmp_lock)) {
      tmp_lock = (uint64_t) atomic_load_explicit(seqlock, memory_order_acquire);
    }

    new_lock = tmp_lock + 1;
    if (DEBUG_SEQLOCKS) assert(is_odd(new_lock));
  } while(!(atomic_compare_exchange_strong_explicit(seqlock, &tmp_lock,
                                                    new_lock,
                                                    memory_order_acquire,
                                                    memory_order_acquire)));
  if (DEBUG_SEQLOCKS) assert(is_odd ((uint64_t) atomic_load_explicit (seqlock, memory_order_acquire)));


}

static inline void unlock_seqlock(seqlock_t *seqlock)
{
  if (WORKERS_PER_MACHINE == 1) return;
  uint64_t tmp = *seqlock;
  if (DEBUG_SEQLOCKS) {
    assert(is_odd(tmp));
  }
  atomic_store_explicit(seqlock, tmp + 1, memory_order_release);
}

// LOCK-free read
static inline uint64_t read_seqlock_lock_free(seqlock_t *seqlock)
{
  if (!ENABLE_LOCK_FREE_READING) {
    lock_seqlock(seqlock);
    return 0;
  }
  uint64_t tmp_lock;
  do {
    tmp_lock = (uint64_t) atomic_load_explicit (seqlock, memory_order_acquire);
  } while (is_odd(tmp_lock));

  return tmp_lock;
}

// return true if the check was successful (loop while it returns false!)
static inline bool check_seqlock_lock_free(seqlock_t *seqlock,
                                           uint64_t *read_lock)
{
  if (!ENABLE_LOCK_FREE_READING) {
    unlock_seqlock(seqlock);
    return true;
  }
  COMPILER_BARRIER();
  uint64_t tmp_lock = (uint64_t) atomic_load_explicit (seqlock, memory_order_acquire);
  if (*read_lock == tmp_lock) return true;
  else {
    *read_lock = tmp_lock;
    return false;
  }
}


static inline void my_assert(bool cond, const char *message)
{
  if (ENABLE_ASSERTIONS) {
    if (!cond) {
      my_printf(red, "%s\n", message);
      assert(false);
    }
  }
}

static inline void print_version(const uint32_t version)
{
  my_printf(yellow, "Version: %u\n", version);
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
static inline bool keys_are_equal(struct key *key1, struct key *key2) {
  return memcmp(key1, key2, TRUE_KEY_SIZE) == 0;
}

// Compares two network timestamps, returns SMALLER if ts1 < ts2
static inline enum ts_compare compare_netw_ts(struct network_ts_tuple *ts1, struct network_ts_tuple *ts2)
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

static inline enum ts_compare compare_ts_with_flat(struct ts_tuple *ts1, uint32_t version2, uint8_t m_id2) {
  if ((ts1->version == version2) &&
      (ts1->m_id == m_id2))
    return EQUAL;
  else if ((ts1->version < version2) ||
           ((ts1->version == version2) &&
            (ts1->m_id < m_id2)))
    return SMALLER;
  else if ((ts1->version > version2) ||
           ((ts1->version == version2)) &&
           (ts1->m_id > m_id2))
    return GREATER;

  return ERROR;
}

static inline enum ts_compare compare_ts_with_netw_ts(struct ts_tuple *ts1, struct network_ts_tuple *ts2) {
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



static inline enum ts_compare compare_ts_generic(struct ts_tuple *ts1, uint8_t flag1, struct ts_tuple *ts2, uint8_t flag2)
{
  uint32_t version1 = ts1->version;
  uint32_t version2 = ts1->version;
  uint8_t m_id1 = ts1->m_id;
  uint8_t m_id2 = ts1->m_id;
  switch (flag1) {
    case NETW_TS:
      version1 =((struct network_ts_tuple*) ts1)->version;
      m_id1 = ((struct network_ts_tuple*) ts1)->m_id;
      break;
    case META_TS:
      version1 =((struct network_ts_tuple*) ts1)->version;
      m_id1 = ((struct network_ts_tuple*) ts1)->m_id;
      break;
    case REGULAR_TS: break;
    default: if (ENABLE_ASSERTIONS) assert(false);
  }
  switch (flag2) {
    case NETW_TS:
      version2 =((struct network_ts_tuple*) ts2)->version;
      m_id2 = ((struct network_ts_tuple*) ts2)->m_id;
      break;
    case META_TS:
      version2 =((struct network_ts_tuple*) ts2)->version;
      m_id2 = ((struct network_ts_tuple*) ts2)->m_id;
      break;
    case REGULAR_TS: break;
    default: if (ENABLE_ASSERTIONS) assert(false);
  }

  if ((version1 == version2) &&
      (m_id1 == m_id2))
    return EQUAL;
  else if ((version1 < version2) ||
           ((version1 == version2) &&
            (m_id1 < m_id2)))
    return SMALLER;
  else if ((version1 > version2) ||
           ((version1 == version2)) &&
           (m_id1 > m_id2))
    return GREATER;

  return ERROR;

}


// First argument is the network ts
static inline void assign_ts_to_netw_ts(struct network_ts_tuple *ts1, struct ts_tuple *ts2)
{
  ts1->m_id = ts2->m_id;
  ts1->version = ts2->version;
}

// First argument is the ts
static inline void assign_netw_ts_to_ts(struct ts_tuple *ts1, struct network_ts_tuple *ts2)
{
  ts1->m_id = ts2->m_id;
  ts1->version = ts2->version;
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

static inline uint8_t sum_of_reps(struct rmw_rep_info* rmw_reps)
{
  return rmw_reps->acks + rmw_reps->rmw_id_commited +
         rmw_reps->log_too_small + rmw_reps->already_accepted +
         rmw_reps->seen_higher_prop_acc + rmw_reps->log_too_high;
}

static inline struct key create_key(uint32_t key_id)
{
  uint64_t key_hash = CityHash128((char *) &(key_id), 4).second;
  struct key key;
  memcpy(&key, &key_hash, TRUE_KEY_SIZE);
  return key;
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


/* ---------------------------------------------------------------------------
//------------------------------ OPCODE HANDLING----------------------------
//---------------------------------------------------------------------------*/

static inline bool opcode_is_rmw(uint8_t opcode)
{
  return opcode == FETCH_AND_ADD || opcode == COMPARE_AND_SWAP_WEAK ||
         opcode == COMPARE_AND_SWAP_STRONG || opcode == RMW_PLAIN_WRITE;
}

static inline bool opcode_is_compare_rmw(uint8_t opcode)
{
  return opcode == COMPARE_AND_SWAP_WEAK || opcode == COMPARE_AND_SWAP_STRONG;
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


#endif //KITE_GENERIC_UTILITY_H

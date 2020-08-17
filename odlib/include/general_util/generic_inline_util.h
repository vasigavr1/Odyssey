//
// Created by vasilis on 24/06/2020.
//

#ifndef KITE_GENERIC_INLINE_UTIL_H
#define KITE_GENERIC_INLINE_UTIL_H

#include "top.h"



/*----------------------------------------------------
 * ---------------SEQLOCKS----------------------------
 * ----------------------------------------------------*/

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
    // First spin in your L1, reading until the lock is even
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
    while (is_odd(tmp_lock)) {
      tmp_lock = (uint64_t) atomic_load_explicit (seqlock, memory_order_acquire);
    }
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

/*----------------------------------------------------
 * ---------------KEYS----------------------------
 * ----------------------------------------------------*/

static inline void print_key(struct key *key)
{
  printf("bkt: %u, server: %u, tag : %u, \n", key->bkt,key->server, key->tag);
}

static inline void cust_print_key(const char* mes, struct key *key)
{
  printf("%s: bkt: %u, server: %u, tag : %u, \n", mes, key->bkt,key->server, key->tag);
}


// Check whether 2 key hashes are equal
static inline bool keys_are_equal(struct key *key1, struct key *key2) {
  return memcmp(key1, key2, KEY_SIZE) == 0;
}

//static inline struct key create_key(uint32_t key_id)
//{
//  uint64_t key_hash = CityHash128((char *) &(key_id), 4).second;
//  struct key key;
//  memcpy(&key, &key_hash, KEY_SIZE);
//  return key;
//}



/*----------------------------------------------------
 * ---------------------------TS- CARTS--------------
 * ----------------------------------------------------*/
static inline void print_ts(struct ts_tuple ts, const char* mess, color_t color)
{
  my_printf(color, "%s: <%u, %u> \n", mess, ts.version, ts.m_id);
}

// Compares two network timestamps, returns SMALLER if ts1 < ts2
static inline compare_t compare_netw_ts(struct network_ts_tuple *ts1, struct network_ts_tuple *ts2)
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
static inline compare_t compare_ts(struct ts_tuple *ts1, struct ts_tuple *ts2)
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

// Compares a network base_ts with a regular base_ts, returns SMALLER if ts1 < ts2
static inline compare_t compare_netw_ts_with_ts(struct network_ts_tuple *ts1, struct ts_tuple *ts2)
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

static inline compare_t compare_ts_with_flat(struct ts_tuple *ts1, uint32_t version2, uint8_t m_id2) {
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

static inline compare_t compare_ts_with_netw_ts(struct ts_tuple *ts1, struct network_ts_tuple *ts2) {
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



static inline compare_t compare_ts_generic(struct ts_tuple *ts1, uint8_t flag1, struct ts_tuple *ts2, uint8_t flag2)
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


// First argument is the network base_ts
static inline void assign_ts_to_netw_ts(struct network_ts_tuple *ts1, struct ts_tuple *ts2)
{
  ts1->m_id = ts2->m_id;
  ts1->version = ts2->version;
}

// First argument is the base_ts
static inline void assign_netw_ts_to_ts(struct ts_tuple *ts1, struct network_ts_tuple *ts2)
{
  ts1->m_id = ts2->m_id;
  ts1->version = ts2->version;
}


static inline compare_t compare_carts_esoteric(compare_t ts_comp, uint32_t log1, uint32_t log2)
{
  compare_t log_comp = log1 == log2 ? EQUAL : SMALLER;
  if (log1 > log2) log_comp = GREATER;

  switch (ts_comp) {
    case EQUAL:
      return log_comp;
    case  GREATER:
      //if (ENABLE_ASSERTIONS) assert(log_comp != SMALLER);
      return GREATER;
    default:
      if (ENABLE_ASSERTIONS) {
        assert(ts_comp == SMALLER);
      }
      return SMALLER;
  }
}

// Compares two timestamps, returns SMALLER if ts1 < ts2
static inline compare_t compare_carts(struct ts_tuple *ts1, uint32_t log1, struct ts_tuple *ts2, uint32_t log2)
{
  compare_t ts_comp = compare_ts(ts1, ts2);
  return compare_carts_esoteric(ts_comp, log1, log2);

}

// Compares a network base_ts with a regular base_ts, returns SMALLER if ts1 < ts2
static inline compare_t compare_netw_carts_with_carts(struct network_ts_tuple *ts1, uint32_t log1,
                                                      struct ts_tuple *ts2, uint32_t log2)
{
  compare_t ts_comp = compare_netw_ts_with_ts(ts1, ts2);
  return compare_carts_esoteric(ts_comp, log1, log2);
}

/*------------------------------------------------------
 * ----------------Gid/Sid/Tid/Mid------------------------
 * ----------------------------------------------------*/



// Calculate the thread global id
static inline uint16_t get_gid(uint8_t m_id, uint16_t t_id)
{
  return (uint16_t) (m_id * WORKERS_PER_MACHINE + t_id);
}

// Calculate the machine id out of the thread global id
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
static inline uint32_t get_glob_sess_id(uint8_t m_id, uint16_t t_id, uint16_t sess_id)
{
  return (uint16_t) ((m_id * SESSIONS_PER_MACHINE) +
                     (t_id * SESSIONS_PER_THREAD)  +
                     sess_id);
}

// Get the machine id out of a global session id
static inline uint8_t glob_ses_id_to_m_id(uint32_t glob_sess_id)
{
  return (uint8_t) (glob_sess_id / SESSIONS_PER_MACHINE);
}

// Get the machine id out of a global session id
static inline uint16_t glob_ses_id_to_t_id(uint32_t glob_sess_id)
{
  return (uint16_t) ((glob_sess_id % SESSIONS_PER_MACHINE) / SESSIONS_PER_THREAD);
}

// Get the sess id out of a global session id
static inline uint16_t glob_ses_id_to_sess_id(uint32_t glob_sess_id)
{
  return (uint16_t) ((glob_sess_id % SESSIONS_PER_MACHINE) % SESSIONS_PER_THREAD);
}

static inline bool is_global_ses_id_local(uint32_t glob_sess_id, uint16_t t_id)
{
  return glob_ses_id_to_t_id(glob_sess_id) == t_id &&
         glob_ses_id_to_m_id(glob_sess_id) == machine_id;
}











#endif //KITE_GENERIC_INLINE_UTIL_H

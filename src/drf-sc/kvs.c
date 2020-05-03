#ifndef _GNU_SOURCE
# define _GNU_SOURCE
#endif

// Optik Options
//#define DEFAULT
//#define CORE_NUM 4

//#include <optik_mod.h>
#include <inline_util.h>
#include <common_func.h>
//#include "optik_mod.h"
#include "kvs.h"

mica_kv_t *KVS;

int is_power_of_2(int x)
{
  return (
    x == 1 || x == 2 || x == 4 || x == 8 || x == 16 || x == 32 ||
    x == 64 || x == 128 || x == 256 || x == 512 || x == 1024 ||
    x == 2048 || x == 4096 || x == 8192 || x == 16384 ||
    x == 32768 || x == 65536 || x == 131072 || x == 262144 ||
    x == 524288 || x == 1048576 || x == 2097152 ||
    x == 4194304 || x == 8388608 || x == 16777216 ||
    x == 33554432 || x == 67108864 || x == 134217728 ||
    x == 268435456 || x == 536870912 || x == 1073741824);
}

/*
 * Initialize the kvs using a Mica instances and adding the timestamps
 * and locks to the keys of mica structure
 */
//void kvs_init(int cache_id, int num_threads) {
//	int i;
//	assert(sizeof(cache_meta) == 8); //make sure that the kvs meta are 8B and thus can fit in mica unused key
//	mica_init(&KVS, cache_id, KVS_SOCKET, KVS_NUM_BKTS, KVS_LOG_CAP);
//	cache_populate_fixed_len(&KVS, KVS_NUM_KEYS, VALUE_SIZE);
//}

//void cache_populate_fixed_len(struct mica_kv* kv, int n, int val_len) {
//  //assert(KVS != NULL);
//  assert(n > 0);
//  assert(val_len > 0 && val_len <= MICA_MAX_VALUE);
//
//  /* This is needed for the eviction message below to make sense */
//  assert(kv->num_insert_op == 0 && kv->num_index_evictions == 0);
//
//  int i;
//  struct cache_op op;
//  struct mica_resp resp;
//  unsigned long long *op_key = (unsigned long long *) &op.key;
//
//  /* Generate the keys to insert */
//  uint128 *key_arr = mica_gen_keys(n);
//
//  for(i = n - 1; i >= 0; i--) {
//    optik_init(&op.key.meta);
//    memset((void *)op.key.meta.epoch_id, 0, EPOCH_BYTES);
////		op.key.meta.state = VALID_STATE;
//    op_key[1] = key_arr[i].second;
//    if (ENABLE_RMWS && i < NUM_OF_RMW_KEYS)
//      op.opcode = KEY_HAS_NEVER_BEEN_RMWED;
//    else op.opcode = KEY_IS_NOT_RMWABLE;
//
//    //printf("Key Metadata: Lock(%u), State(%u), Counter(%u:%u)\n", op.key.meta.lock, op.key.meta.state, op.key.meta.version, op.key.meta.cid);
//    op.val_len = (uint8_t) (val_len >> SHIFT_BITS);
//    uint8_t val = 0;//(uint8_t) (op_key[1] & 0xff);
//    memset(op.value, val, (uint32_t) val_len);
//    //if (i < NUM_OF_RMW_KEYS)
//    // green_printf("Inserting key %d: bkt %u, server %u, tag %u \n",i, op.key.bkt, op.key.server, op.key.tag);
//    mica_insert_one(kv, (mica_op_t *) &op, &resp);
//  }
//
//  assert(kv->num_insert_op == n);
//  // printf("Cache: Populated instance %d with %d keys, length = %d. "
//  // 			   "Index eviction fraction = %.4f.\n",
//  // 	   KVS->instance_id, n, val_len,
//  // 	   (double) KVS->num_index_evictions / KVS->num_insert_op);
//}


void mica_init(mica_kv_t *kvs, int instance_id,
               int node_id, int num_bkts, uint64_t log_cap)
{
  int i, j;
  /* Verify struct sizes */
  assert(sizeof(struct mica_slot) == 8);
//  assert(sizeof(struct mica_key_t) == 8);
//  printf("size %u \n", sizeof(mica_op_t) );
  assert(sizeof(mica_op_t) % 64 == 0);

  assert(kvs != NULL);
  assert(node_id == 0 || node_id == 1);

  /* 16 million buckets = a 1 GB index */
  assert(is_power_of_2(num_bkts) == 1 && num_bkts <= M_16);
  //assert(log_cap > 0 && log_cap <= M_1024 &&
  //	log_cap % M_2 == 0 && is_power_of_2(log_cap));

  assert(MICA_LOG_BITS >= 24);	/* Minimum log w_size = 16 MB */

  // my_printf(red, "mica: Initializing MICA instance %d.\n"
  // 	"NUMA node = %d, buckets = %d (w_size = %u B), log capacity = %d B.\n",
  // 	instance_id,
  // 	node_id, num_bkts, num_bkts * sizeof(struct mica_bkt), log_cap);


  /* Initialize metadata and stats */
  kvs->instance_id = instance_id;
  kvs->node_id = node_id;

  kvs->num_bkts = num_bkts;
  kvs->bkt_mask = num_bkts - 1;	/* num_bkts is power of 2 */

  kvs->log_cap = log_cap;
  kvs->log_mask = log_cap - 1;	/* log_cap is a power of 2 */
  kvs->log_head = 0;

//  kv->num_get_op = 0;
//  kv->num_get_fail = 0;
//  kv->num_put_fail = 0;
//  kv->num_put_op = 0;
//  kv->num_insert_op = 0;
//  kv->num_index_evictions = 0;

  /* Alloc index and initialize all entries to invalid */
  // printf("mica: Allocting hash table index for instance %d\n", instance_id);
  int ht_index_key = MICA_INDEX_SHM_KEY + instance_id;
  my_printf(green, "asking for %lu MB for the buckets \n",
            num_bkts * sizeof(struct mica_bkt)/ (M_1));
  kvs->ht_index = (struct mica_bkt *) hrd_malloc_socket(ht_index_key,
                                                       num_bkts * sizeof(struct mica_bkt), node_id);

  for(i = 0; i < num_bkts; i++) {
    for(j = 0; j < 8; j++) {
      kvs->ht_index[i].slots[j].in_use = 0;
    }
  }


  /* Alloc log */
//	printf("mica: Allocting hash table log for instance %d\n", instance_id);
  int ht_log_key = MICA_LOG_SHM_KEY + instance_id;
  my_printf(green, "asking for %lu MB for the log  \n", num_bkts * sizeof(struct mica_bkt) / M_1);
  kvs->ht_log = (uint8_t *) hrd_malloc_socket(ht_log_key, log_cap, node_id);
}

void mica_insert_one(mica_kv_t *kvs, mica_op_t *op)
{

  assert(kvs != NULL);
  assert(op != NULL);
  //assert(op->opcode == MICA_OP_PUT);
  //assert(op->val_len > 0 && op->val_len <= MICA_MAX_VALUE);
  //assert(resp != NULL);
  int i;
  unsigned int bkt = op->key.bkt & kvs->bkt_mask;
  struct mica_bkt *bkt_ptr = &kvs->ht_index[bkt];
  unsigned int tag = op->key.tag;

#if MICA_DEBUG == 2
  mica_print_op(op);
#endif

  kvs->num_insert_op++;

  /* Find a slot to use for this key. If there is a slot with the same
   * tag as ours, we are sure to find it because the used slots are at
   * the beginning of the 8-slot array. */
  int slot_to_use = -1;
  for(i = 0; i < 8; i++) {
    if(bkt_ptr->slots[i].tag == tag || bkt_ptr->slots[i].in_use == 0) {
      slot_to_use = i;
    }
  }
  bool evict_flag = false;
  /* If no slot found, choose one to evict */
  if(slot_to_use == -1) {
    slot_to_use = tag & 7;	/* tag is ~ randomly distributed */
    kvs->num_index_evictions++;
    evict_flag = true;
  }

  /* Encode the empty slot */
  bkt_ptr->slots[slot_to_use].in_use = 1;
  bkt_ptr->slots[slot_to_use].offset = kvs->log_head;	/* Virtual head */
  bkt_ptr->slots[slot_to_use].tag = tag;

  /* Paste the key-value into the log */
  uint8_t *log_ptr = &kvs->ht_log[kvs->log_head & kvs->log_mask];


  size_t len_to_copy = sizeof(mica_op_t);

  /* Ensure that we don't wrap around in the *virtual* log space even
   * after 8-byte alignment below.*/
  assert((1ULL << MICA_LOG_BITS) - kvs->log_head > len_to_copy + 8);
  if (evict_flag) {
    mica_op_t * evic_op = (mica_op_t *) log_ptr;
    my_printf(red, "Evicting key:bkt %u, server %u, tag %u \n",
              evic_op->key.bkt, evic_op->key.server, evic_op->key.tag);
    assert(false);
  }
  memcpy(log_ptr, op, len_to_copy);


  mica_op_t * saved_kv_ptr = (mica_op_t *) log_ptr;
  mica_op_t * first_kv_ptr = (mica_op_t *) kvs->ht_log;


  /*my_printf(green, "New key:bkt %u, server %u, tag %u, in position %lu/%p \n",
            saved_kv_ptr->key.bkt, saved_kv_ptr->key.server, saved_kv_ptr->key.tag,
            kv->log_head, log_ptr);
  my_printf(green, "First key:bkt %u, server %u, tag %u, in position %lu/%p \n",
            first_kv_ptr->key.bkt, first_kv_ptr->key.server, first_kv_ptr->key.tag,
            0, kv->ht_log);*/



  kvs->log_head += len_to_copy;

  /* Ensure that the key field of each log entry is 8-byte aligned. This
   * makes subsequent comparisons during GETs faster. */
  kvs->log_head = (kvs->log_head + 7) & ~7;

  /* If we're close to overflowing in the physical log, wrap around to
   * the beginning, but go forward in the virtual log. */
  if(unlikely(kvs->log_cap - kvs->log_head <= MICA_OP_SIZE + 32)) {
    kvs->log_head = (kvs->log_head + kvs->log_cap) & ~kvs->log_mask;
    my_printf(red, "mica: Instance %d wrapping around. Wraps = %llu\n",
              kvs->instance_id, kvs->log_head / kvs->log_cap);
    assert(false);
  }
}



void custom_mica_init(int kvs_id) {
  //assert(sizeof(cache_meta) == 8); //make sure that the cache meta are 8B and thus can fit in mica unused key
  KVS = calloc(1, sizeof(mica_kv_t));
  mica_init(KVS, kvs_id, KVS_SOCKET, KVS_NUM_BKTS, KVS_LOG_CAP);
  custom_mica_populate_fixed_len(KVS, KVS_NUM_KEYS, VALUE_SIZE);
}

void custom_mica_populate_fixed_len(mica_kv_t * kvs, int n, int val_len) {
  //assert(cache != NULL);
  assert(n > 0);
//  assert(val_len > 0 && val_len <= MICA_MAX_VALUE);

  /* This is needed for the eviction message below to make sense */
  assert(kvs->num_insert_op == 0 && kvs->num_index_evictions == 0);

  int i;
  mica_op_t * op = (mica_op_t *) calloc(1, sizeof(mica_op_t));
  unsigned long long * op_key = (unsigned long long *) &op->key;

  /* Generate the keys to insert */
  uint128 *key_arr = mica_gen_keys(n);

  for(i = n - 1; i >= 0; i--) {
    (*op_key) = key_arr[i].second;
    //printf("%u \n", sizeof(decltype(op_key[1])));
    //printf("Key Metadata: Lock(%u), State(%u), Counter(%u:%u)\n", op.key.meta.lock, op.key.meta.state, op.key.meta.version, op.key.meta.cid);
    //op.val_len = (uint8_t) (val_len >> SHIFT_BITS);
    //uint8_t val = 0;//(uint8_t) (op_key[1] & 0xff);
    //memset(op.value, val, (uint32_t) val_len);
    // green_printf("Inserting key %d: bkt %u, server %u, tag %u \n",i, op.key.bkt, op.key.server, op.key.tag);
    mica_insert_one(kvs, op);
  }

  assert(kvs->num_insert_op == n);
  // printf("Cache: Populated instance %d with %d keys, length = %d. "
  // 			   "Index eviction fraction = %.4f.\n",
  // 	   cache->instance_id, n, val_len,
  // 	   (double) cache->num_index_evictions / cache->num_insert_op);
}



/* ---------------------------------------------------------------------------
------------------------------ DRF-SC--------------------------------
---------------------------------------------------------------------------*


/* The worker sends its local requests to this, reads check the ts_tuple and copy it to the op to get broadcast
 * Writes do not get served either, writes are only propagated here to see whether their keys exist */
inline void cache_batch_op_trace(uint16_t op_num, uint16_t t_id, struct trace_op *op,
                                 struct cache_resp *resp,
                                 struct pending_ops *p_ops)
{
	uint16_t op_i;
  if (ENABLE_ASSERTIONS) assert (op_num <= MAX_OP_BATCH);
	unsigned int bkt[MAX_OP_BATCH];
	struct mica_bkt *bkt_ptr[MAX_OP_BATCH];
	unsigned int tag[MAX_OP_BATCH];
	uint8_t key_in_store[MAX_OP_BATCH];	/* Is this key in the datastore? */
	mica_op_t *kv_ptr[MAX_OP_BATCH];	/* Ptr to KV item in log */
	/*
	 * We first lookup the key in the datastore. The first two @I loops work
	 * for both GETs and PUTs.
	 */
  for(op_i = 0; op_i < op_num; op_i++) {
//    struct cache_op *c_op = (struct cache_op*) &op[op_i];
    KVS_locate_one_bucket(op_i, bkt, op->key, bkt_ptr, tag, kv_ptr,
                          key_in_store, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);

  uint64_t rmw_l_id = p_ops->prop_info->l_id;
  uint32_t r_push_ptr = p_ops->r_push_ptr;
	for(op_i = 0; op_i < op_num; op_i++) {
		if(kv_ptr[op_i] != NULL) {
			/* We had a tag match earlier. Now compare log entry. */
			long long *key_ptr_log = (long long *) kv_ptr[op_i];
			long long *key_ptr_req = (long long *) &op[op_i];

			if(key_ptr_log[1] == key_ptr_req[1]) { //Cache Hit
        key_in_store[op_i] = 1;
        if (kv_ptr[op_i]->opcode == KEY_IS_NOT_RMWABLE) {
          if (op[op_i].opcode == KVS_OP_GET || op[op_i].opcode == OP_ACQUIRE) {
            KVS_from_trace_reads_and_acquires(&op[op_i], kv_ptr[op_i], &resp[op_i],
                                              p_ops, &r_push_ptr, t_id);
          }
            // Put has to be 2 rounds (readTS + write) if it is out-of-epoch
          else if (op[op_i].opcode == KVS_OP_PUT) {
            KVS_from_trace_writes(&op[op_i], kv_ptr[op_i], &resp[op_i],
                                  p_ops, &r_push_ptr, t_id);
          }
          else if (op[op_i].opcode == OP_RELEASE) { // read the timestamp
            KVS_from_trace_releases(&op[op_i], kv_ptr[op_i], &resp[op_i],
                                    p_ops, &r_push_ptr, t_id);
          }
          else if (ENABLE_ASSERTIONS) {
            red_printf("Wrkr %u: cache_batch_op_trace wrong opcode %d, for not-rmwable key,  req %d \n",
                       t_id, op[op_i].opcode, op_i);
            assert(false);
          }
        }
        else if (ENABLE_RMWS) {
          if (opcode_is_rmw(op[op_i].opcode)) {
            KVS_from_trace_rmw(&op[op_i], kv_ptr[op_i], &resp[op_i],
                               p_ops, &rmw_l_id, op_i, t_id);
          }
          else if (ENABLE_RMW_ACQUIRES && op[op_i].opcode == OP_ACQUIRE) {
            KVS_from_trace_rmw_acquire(&op[op_i], kv_ptr[op_i], &resp[op_i],
                                       p_ops, &r_push_ptr, t_id);
          }
          else if (op[op_i].opcode == KVS_OP_GET) {
            KVS_from_trace_rmw_rlxd_read(&op[op_i], kv_ptr[op_i], &resp[op_i],
                                         p_ops, &r_push_ptr, t_id);
          }
          else if (ENABLE_ASSERTIONS) {
            red_printf("Wrkr %u: cache_batch_op_trace wrong opcode in KVS: %d, req %d \n",
                       t_id, op[op_i].opcode, op_i);
            assert(0);
          }
        }
        else if (ENABLE_ASSERTIONS) assert(false);
      }
		}
		if(key_in_store[op_i] == 0) {  //Cache miss --> We get here if either tag or log key match failed
      red_printf("miss\n");
      //red_printf("Cache_miss %u : bkt %u/%u, server %u/%u, tag %u/%u \n",
            //    op_i, op[op_i].key.bkt, kv_ptr[op_i]->key.bkt ,op[op_i].key.server,
           //     kv_ptr[op_i]->key.server, op[op_i].key.tag, kv_ptr[op_i]->key.tag);
			resp[op_i].type = KVS_MISS;
		}
    else printf("hit \n");
	}
}

/* The worker sends the remote writes to be committed with this function*/
// THE API IS DIFFERENT HERE, THIS TAKES AN ARRAY OF POINTERS RATHER THAN A POINTER TO AN ARRAY
// YOU have to give a pointer to the beggining of the array of the pointers or else you will not
// be able to wrap around to your array
inline void cache_batch_op_updates(uint16_t op_num, uint16_t t_id, struct write **writes,
                                   struct pending_ops *p_ops,
                                   uint32_t pull_ptr,  uint32_t max_op_size, bool zero_ops)
{
  uint16_t op_i;	/* I is batch index */

  if (ENABLE_ASSERTIONS) assert(op_num <= MAX_INCOMING_W);
  unsigned int bkt[MAX_INCOMING_W];
  struct mica_bkt *bkt_ptr[MAX_INCOMING_W];
  unsigned int tag[MAX_INCOMING_W];
  uint8_t key_in_store[MAX_INCOMING_W];	/* Is this key in the datastore? */
  mica_op_t *kv_ptr[MAX_INCOMING_W];	/* Ptr to KV item in log */
  /*
     * We first lookup the key in the datastore. The first two @I loops work
     * for both GETs and PUTs.
     */
  for(op_i = 0; op_i < op_num; op_i++) {
    struct trace_op *op = (struct trace_op*) writes[(pull_ptr + op_i) % max_op_size];
    KVS_locate_one_bucket(op_i, bkt, op->key, bkt_ptr, tag, kv_ptr,
                          key_in_store, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);

  // the following variables used to validate atomicity between a lock-free r_rep of an object
  for(op_i = 0; op_i < op_num; op_i++) {
    struct trace_op *op = (struct trace_op *) writes[(pull_ptr + op_i) % max_op_size];
    if (unlikely (op->opcode == OP_RELEASE_BIT_VECTOR)) continue;
    if (kv_ptr[op_i] != NULL) {
      /* We had a tag match earlier. Now compare log entry. */
      long long *key_ptr_log = (long long *) kv_ptr[op_i];
      long long *key_ptr_req = (long long *) op;
      if (key_ptr_log[1] == key_ptr_req[1]) { //Cache Hit
        key_in_store[op_i] = 1;
        if (kv_ptr[op_i]->opcode == KEY_IS_NOT_RMWABLE) {
          if (op->opcode == KVS_OP_PUT || op->opcode == OP_RELEASE ||
              op->opcode == OP_ACQUIRE) {
            KVS_updates_writes_or_releases_or_acquires(op, kv_ptr[op_i], t_id);
          }
          else if (ENABLE_ASSERTIONS) {
            red_printf("Wrkr %u, kvs batch update: wrong opcode in kvs: %d, req %d, "
                         "m_id %u, val_len %u, version %u , \n",
                       t_id, op->opcode, op_i, op->ts.m_id,
                       op->val_len, op->ts.version);
            assert(0);
          }
        }
        else if (ENABLE_RMWS) {
          if (op->opcode == ACCEPT_OP) {
            KVS_updates_accepts(op, kv_ptr[op_i], p_ops, op_i, t_id);
          }
          else if (op->opcode == COMMIT_OP) {
            KVS_updates_commits(op, kv_ptr[op_i], p_ops, op_i, t_id);
          }
          else if (ENABLE_ASSERTIONS) {
            red_printf("Wrkr %u, kvs batch update: wrong opcode in kvs: %d, req %d, "
                         "m_id %u, val_len %u, version %u , \n",
                       t_id, op->opcode, op_i, op->ts.m_id,
                       op->val_len, op->ts.version);
            assert(0);
          }
        }
        else if (ENABLE_ASSERTIONS) assert(false);
      }
      if (key_in_store[op_i] == 0) {  //Cache miss --> We get here if either tag or log key match failed
        if (ENABLE_ASSERTIONS) assert(false);
      }
      if (zero_ops) {
        //printf("Zero out %d at address %lu \n", op->opcode, &op->opcode);
        op->opcode = 5;
      }
    }
  }
}

// The worker send here the incoming reads, the reads check the incoming ts if it is  bigger/equal to the local
// the just ack it, otherwise they send the value back
inline void cache_batch_op_reads(uint32_t op_num, uint16_t t_id, struct pending_ops *p_ops,
                                 uint32_t pull_ptr, uint32_t max_op_size, bool zero_ops)
{
  uint16_t op_i;	/* I is batch index */
  struct read **reads = (struct read **) p_ops->ptrs_to_mes_ops;

  if (ENABLE_ASSERTIONS) assert(op_num <= MAX_INCOMING_R);
  unsigned int bkt[MAX_INCOMING_R];
  struct mica_bkt *bkt_ptr[MAX_INCOMING_R];
  unsigned int tag[MAX_INCOMING_R];
  uint8_t key_in_store[MAX_INCOMING_R];	/* Is this key in the datastore? */
  mica_op_t *kv_ptr[MAX_INCOMING_R];	/* Ptr to KV item in log */
  /*
     * We first lookup the key in the datastore. The first two @I loops work
     * for both GETs and PUTs.
     */
  for(op_i = 0; op_i < op_num; op_i++) {
    struct trace_op *op = (struct trace_op*) reads[(pull_ptr + op_i) % max_op_size];
    if (unlikely(op->opcode == OP_ACQUIRE_FLIP_BIT)) continue; // This message is only meant to flip a bit and is thus a NO-OP
    KVS_locate_one_bucket(op_i, bkt, op->key , bkt_ptr, tag, kv_ptr,
                          key_in_store, KVS);
  }
  for(op_i = 0; op_i < op_num; op_i++) {
    struct trace_op *op = (struct trace_op*) reads[(pull_ptr + op_i) % max_op_size];
    if (unlikely(op->opcode == OP_ACQUIRE_FLIP_BIT)) continue;
    KVS_locate_one_kv_pair(op_i, tag, bkt_ptr, kv_ptr, KVS);
  }

  for(op_i = 0; op_i < op_num; op_i++) {
    struct trace_op *op = (struct trace_op*) reads[(pull_ptr + op_i) % max_op_size];
    if (op->opcode == OP_ACQUIRE_FLIP_BIT) {
      insert_r_rep(p_ops, p_ops->ptrs_to_mes_headers[op_i]->l_id, t_id,
                   p_ops->ptrs_to_mes_headers[op_i]->m_id,
                   p_ops->coalesce_r_rep[op_i], op->opcode);
      continue;
    }
    if(kv_ptr[op_i] != NULL) {
      /* We had a tag match earlier. Now compare log entry. */
      long long *key_ptr_log = (long long *) kv_ptr[op_i];
      long long *key_ptr_req = (long long *) op;
      if(key_ptr_log[1] == key_ptr_req[1]) { //Cache Hit
        key_in_store[op_i] = 1;
        if (kv_ptr[op_i]->opcode == KEY_IS_NOT_RMWABLE) {
          check_state_with_allowed_flags(5, op->opcode, KVS_OP_GET, OP_ACQUIRE, OP_ACQUIRE_FP,
                                         CACHE_OP_GET_TS);
          if (op->opcode == KVS_OP_GET || op->opcode == OP_ACQUIRE ||
              op->opcode == OP_ACQUIRE_FP) {
            KVS_reads_gets_or_acquires_or_acquires_fp(op, kv_ptr[op_i], p_ops, op_i, t_id);
          }
          else if (op->opcode == CACHE_OP_GET_TS) {
            KVS_reads_get_TS(op, kv_ptr[op_i], p_ops, op_i, t_id);
          }
          else if (ENABLE_ASSERTIONS) {
            red_printf("Wrkr %u wrong Opcode in kvs: %d, req %d \n",
                       t_id, op->opcode, op_i);
             assert(false);
          }
        }
        else if (ENABLE_RMWS) {
          check_state_with_allowed_flags(3, kv_ptr[op_i]->opcode, KEY_HAS_BEEN_RMWED, KEY_HAS_NEVER_BEEN_RMWED);
          if (op->opcode == PROPOSE_OP) {
            KVS_reads_proposes(op, kv_ptr[op_i], p_ops, op_i, t_id);
          }
          else if (op->opcode == OP_ACQUIRE || op->opcode == OP_ACQUIRE_FP) {
            assert(ENABLE_RMW_ACQUIRES);
            KVS_reads_rmw_acquires(op, kv_ptr[op_i], p_ops, op_i, t_id);
          }
          else if (ENABLE_ASSERTIONS){
            //red_printf("wrong Opcode in KVS: %d, req %d, m_id %u, val_len %u, version %u , \n",
            //           op->opcode, I, reads[(pull_ptr + I) % max_op_size]->m_id,
            //           reads[(pull_ptr + I) % max_op_size]->val_len,
            //          reads[(pull_ptr + I) % max_op_size]->version);
            assert(false);
          }
        }
        else if (ENABLE_ASSERTIONS) assert(false);
      }
    }
    if(key_in_store[op_i] == 0) {  //Cache miss --> We get here if either tag or log key match failed
      red_printf("Opcode %u Cache_miss: bkt %u, server %u, tag %u \n", op->opcode, op->key.bkt, op->key.server, op->key.tag);
      assert(false); // cant have a miss since, it hit in the source's kvs
    }
    if (zero_ops) {
//      printf("Zero out %d at address %lu \n", op->opcode, &op->opcode);
      op->opcode = 5;
    } // TODO is this needed?
  }
}

// The  worker sends (out-of-epoch) reads that received a higher timestamp and thus have to be applied as writes
// Could also be that the first round of an out-of-epoch write received a high TS
// All out of epoch reads/writes must come in to update the epoch
inline void cache_batch_op_first_read_round(uint16_t op_num, uint16_t t_id, struct read_info **writes,
                                            struct pending_ops *p_ops,
                                            uint32_t pull_ptr, uint32_t max_op_size, bool zero_ops)
{
  uint16_t op_i;
  if (ENABLE_ASSERTIONS) assert(op_num <= MAX_INCOMING_R);
  unsigned int bkt[MAX_INCOMING_R];
  struct mica_bkt *bkt_ptr[MAX_INCOMING_R];
  unsigned int tag[MAX_INCOMING_R];
  uint8_t key_in_store[MAX_INCOMING_R];	/* Is this key in the datastore? */
  mica_op_t *kv_ptr[MAX_INCOMING_R];	/* Ptr to KV item in log */
  /*
     * We first lookup the key in the datastore. The first two @I loops work
     * for both GETs and PUTs.
     */
  for(op_i = 0; op_i < op_num; op_i++) {
    struct key *op_key = &writes[(pull_ptr + op_i) % max_op_size]->key;
    KVS_locate_one_bucket_with_key(op_i, bkt, op_key, bkt_ptr, tag, kv_ptr,
                                   key_in_store, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);


  // the following variables used to validate atomicity between a lock-free r_rep of an object
  for(op_i = 0; op_i < op_num; op_i++) {
    struct read_info *op = writes[(pull_ptr + op_i) % max_op_size];
    if(kv_ptr[op_i] != NULL) {
      /* We had a tag match earlier. Now compare log entry. */
      long long *key_ptr_log = (long long *) kv_ptr[op_i];
      long long *key_ptr_req = (long long *) &op->key;
      if(key_ptr_log[1] == key_ptr_req[0]) { //Cache Hit
        key_in_store[op_i] = 1;
        if (kv_ptr[op_i]->opcode == KEY_IS_NOT_RMWABLE) {
          // The write must be performed with the max TS out of the one stored in the KV and read_info
          if (op->opcode == KVS_OP_PUT) {
            KVS_out_of_epoch_writes(op, kv_ptr[op_i], p_ops, t_id);
          } else if (op->opcode == OP_ACQUIRE ||
                     op->opcode == KVS_OP_GET) { // a read resulted on receiving a higher timestamp than expected
            KVS_acquires_and_out_of_epoch_reads(op, kv_ptr[op_i], t_id);
          } else if (op->opcode == UPDATE_EPOCH_OP_GET) {
            if (!MEASURE_SLOW_PATH && op->epoch_id > kv_ptr[op_i]->epoch_id) {
              optik_lock(&kv_ptr[op_i]->seqlock);
              kv_ptr[op_i]->epoch_id = op->epoch_id;
              optik_unlock(&kv_ptr[op_i]->seqlock);
              if (ENABLE_STAT_COUNTING) t_stats[t_id].rectified_keys++;
            }
          } else {
            red_printf("Wrkr %u: read-first-round wrong opcode in kvs: %d, req %d, m_id %u,version %u , \n",
                       t_id, op->opcode, op_i, writes[(pull_ptr + op_i) % max_op_size]->ts_to_read.m_id,
                       writes[(pull_ptr + op_i) % max_op_size]->ts_to_read.version);
            assert(0);
          }
        }
        else if (ENABLE_RMWS) {
          if (op->opcode == OP_ACQUIRE) {
            //assert(op->is_rmw);
            KVS_rmw_acquire_commits(op, kv_ptr[op_i], op_i, t_id);
          }
          else if (ENABLE_ASSERTIONS){
            assert(false);
          }
        }
        else if (ENABLE_ASSERTIONS) assert(false);
      }
    }
    if(key_in_store[op_i] == 0) {  //Cache miss --> We get here if either tag or log key match failed
      if (ENABLE_ASSERTIONS) assert(false);
    }
    if (zero_ops) {
      // printf("Zero out %d at address %lu \n", op->opcode, &op->opcode);
      op->opcode = 5;
    }
    if (op->complete_flag) {
      if (ENABLE_ASSERTIONS) assert(&p_ops->read_info[op->r_ptr] == op);
      if (op->opcode == OP_ACQUIRE || op->opcode == KVS_OP_GET)
        memcpy(op->value_to_read, op->value, op->val_len);
      signal_completion_to_client(p_ops->r_session_id[op->r_ptr],
                                  p_ops->r_index_to_req_array[op->r_ptr], t_id);
      op->complete_flag = false;
    }
    else if (ENABLE_ASSERTIONS)
      check_state_with_allowed_flags(3, op->opcode, UPDATE_EPOCH_OP_GET,
                                     OP_ACQUIRE, OP_RELEASE);
  }

}


// Send an isolated write to the kvs-no batching
inline void cache_isolated_op(int t_id, struct write *write)
{
  uint32_t op_num = 1;
  int j;	/* I is batch index */

  unsigned int bkt;
  struct mica_bkt *bkt_ptr;
  unsigned int tag;
  int key_in_store;	/* Is this key in the datastore? */
  mica_op_t *kv_ptr;	/* Ptr to KV item in log */
  /*
   * We first lookup the key in the datastore. The first two @I loops work
   * for both GETs and PUTs.
   */
  struct trace_op *op = (struct trace_op*) (((void *) write) - 3);
  //print_true_key((struct key *) write->key);
  //printf("op bkt %u\n", op->key.bkt);
  bkt = op->key.bkt & KVS->bkt_mask;
  bkt_ptr = &KVS->ht_index[bkt];
  //__builtin_prefetch(bkt_ptr, 0, 0);
  tag = op->key.tag;

  key_in_store = 0;
  kv_ptr = NULL;


  for(j = 0; j < 8; j++) {
    if(bkt_ptr->slots[j].in_use == 1 &&
       bkt_ptr->slots[j].tag == tag) {
      uint64_t log_offset = bkt_ptr->slots[j].offset &
                            KVS->log_mask;
      /*
               * We can interpret the log entry as mica_op, even though it
               * may not contain the full MICA_MAX_VALUE value.
               */
      kv_ptr = (mica_op_t *) &KVS->ht_log[log_offset];
      /* Detect if the head has wrapped around for this index entry */
      if(KVS->log_head - bkt_ptr->slots[j].offset >= KVS->log_cap) {
        kv_ptr = NULL;	/* If so, we mark it "not found" */
      }
      break;
    }
  }

  // the following variables used to validate atomicity between a lock-free r_rep of an object
  if(kv_ptr != NULL) {
    /* We had a tag match earlier. Now compare log entry. */
    long long *key_ptr_log = (long long *) kv_ptr;
    long long *key_ptr_req = (long long *) op;
    if(key_ptr_log[1] == key_ptr_req[1]) { //Cache Hit
      key_in_store = 1;
      if (ENABLE_ASSERTIONS) {
        if (op->opcode != OP_RELEASE) {
          red_printf("Wrkr %u: cache_isolated_op: wrong opcode : %d, m_id %u, val_len %u, version %u , \n",
                     t_id, op->opcode,  op->ts.m_id,
                     op->val_len, op->ts.version);
          assert(false);
        }
      }
      //red_printf("op val len %d in ptr %d, total ops %d \n", op->val_len, (pull_ptr + I) % max_op_size, op_num );
      optik_lock(&kv_ptr->seqlock);
      if (compare_netw_ts_with_ts(&op->ts, &kv_ptr->ts) == GREATER) {
        memcpy(kv_ptr->value, op->value, VALUE_SIZE);
        kv_ptr->ts.m_id = op->ts.m_id;
        kv_ptr->ts.version = op->ts.version;
      }
      optik_unlock(&kv_ptr->seqlock);
    }
  }
  if(key_in_store == 0) {  //Cache miss --> We get here if either tag or log key match failed
    if (ENABLE_ASSERTIONS) assert(false);
  }



}


uint128* mica_gen_keys(int n)
{
  int i;
  assert(n > 0 && n <= M_1024 / sizeof(uint128));
  assert(sizeof(uint128) == 16);

  // printf("mica: Generating %d keys\n", n);

  uint128 *key_arr = malloc(n * sizeof(uint128));
  assert(key_arr != NULL);

  for(i = 0; i < n; i++) {
    key_arr[i] = CityHash128((char *) &i, 4);
  }

  return key_arr;
}











//void mica_init(struct mica_kv *kv,
//               int instance_id, int node_id, int num_bkts, u_int64_t log_cap)
//{
//  int i, j;
//
//  /* Verify struct sizes */
//  assert(sizeof(struct mica_slot) == 8);
//  assert(sizeof(struct mica_key) == 16);
//  assert(sizeof(mica_op_t) % 64 == 0);
//
//  assert(kv != NULL);
//  assert(node_id == 0 || node_id == 1);
//
//  /* 16 million buckets = a 1 GB index */
//  assert(is_power_of_2(num_bkts) == 1 && num_bkts <= M_16);
//  //assert(log_cap > 0 && log_cap <= M_1024 &&
//  //	log_cap % M_2 == 0 && is_power_of_2(log_cap));
//
//  assert(MICA_LOG_BITS >= 24);	/* Minimum log w_size = 16 MB */
//
//  // red_printf("mica: Initializing MICA instance %d.\n"
//  // 	"NUMA node = %d, buckets = %d (w_size = %u B), log capacity = %d B.\n",
//  // 	instance_id,
//  // 	node_id, num_bkts, num_bkts * sizeof(struct mica_bkt), log_cap);
//
//  if(MICA_DEBUG != 0) {
//    printf("mica: Debug mode is ON! This might reduce performance.\n");
//    sleep(2);
//  }
//
//  /* Initialize metadata and stats */
//  kv->instance_id = instance_id;
//  kv->node_id = node_id;
//
//  kv->num_bkts = num_bkts;
//  kv->bkt_mask = num_bkts - 1;	/* num_bkts is power of 2 */
//
//  kv->log_cap = log_cap;
//  kv->log_mask = log_cap - 1;	/* log_cap is a power of 2 */
//  kv->log_head = 0;
//
//  kv->num_get_op = 0;
//  kv->num_get_fail = 0;
//  kv->num_put_fail = 0;
//  kv->num_put_op = 0;
//  kv->num_insert_op = 0;
//  kv->num_index_evictions = 0;
//
//  /* Alloc index and initialize all entries to invalid */
//  // printf("mica: Allocting hash table index for instance %d\n", instance_id);
//  int ht_index_key = MICA_INDEX_SHM_KEY + instance_id;
//  kv->ht_index = (struct mica_bkt *) hrd_malloc_socket(ht_index_key,
//                                                       num_bkts * sizeof(struct mica_bkt), node_id);
//
//  for(i = 0; i < num_bkts; i++) {
//    for(j = 0; j < 8; j++) {
//      kv->ht_index[i].slots[j].in_use = 0;
//    }
//  }
//
//  /* Alloc log */
////	printf("mica: Allocting hash table log for instance %d\n", instance_id);
//  int ht_log_key = MICA_LOG_SHM_KEY + instance_id;
//  kv->ht_log = (uint8_t *) hrd_malloc_socket(ht_log_key, log_cap, node_id);
//}

//void mica_insert_one(struct mica_kv *kv,
//                     mica_op_t *op)
//{
//#if MICA_DEBUG == 1
//  assert(kv != NULL);
//	assert(op != NULL);
//	assert(op->opcode == MICA_OP_PUT);
//	assert(op->val_len > 0 && op->val_len <= MICA_MAX_VALUE);
//	assert(resp != NULL);
//#endif
//
//  int i;
//  unsigned int bkt = op->key.bkt & kv->bkt_mask;
//  struct mica_bkt *bkt_ptr = &kv->ht_index[bkt];
//  unsigned int tag = op->key.tag;
//
//#if MICA_DEBUG == 2
//  mica_print_op(op);
//#endif
//
//  kv->num_insert_op++;
//
//  /* Find a slot to use for this key. If there is a slot with the same
//   * tag as ours, we are sure to find it because the used slots are at
//   * the beginning of the 8-slot array. */
//  int slot_to_use = -1;
//  for(i = 0; i < 8; i++) {
//    if(bkt_ptr->slots[i].tag == tag || bkt_ptr->slots[i].in_use == 0) {
//      slot_to_use = i;
//    }
//  }
//  bool evict_flag = false;
//  /* If no slot found, choose one to evict */
//  if(slot_to_use == -1) {
//    slot_to_use = tag & 7;	/* tag is ~ randomly distributed */
//    kv->num_index_evictions++;
//    evict_flag = true;
//
//  }
//
//  /* Encode the empty slot */
//  bkt_ptr->slots[slot_to_use].in_use = 1;
//  bkt_ptr->slots[slot_to_use].offset = kv->log_head;	/* Virtual head */
//  bkt_ptr->slots[slot_to_use].tag = tag;
//
//  /* Paste the key-value into the log */
//  uint8_t *log_ptr = &kv->ht_log[kv->log_head & kv->log_mask];
//
//  /* Data copied: key, opcode, val_len, value */
//  //int len_to_copy = sizeof(struct mica_key) + sizeof(uint8_t) +
//  //	sizeof(uint8_t) + op->val_len;
//  int len_to_copy = sizeof(struct mica_key) + sizeof(uint8_t) +
//                    sizeof(uint8_t) + VALUE_SIZE;
//
//  /* Ensure that we don't wrap around in the *virtual* log space even
//   * after 8-byte alignment below.*/
//  assert((1ULL << MICA_LOG_BITS) - kv->log_head > len_to_copy + 8);
//  if (evict_flag) {
//    struct cache_op *evic_op = (struct cache_op *) log_ptr;
//    red_printf("Evicting key:bkt %u, server %u, tag %u \n", evic_op->key.bkt, evic_op->key.server, evic_op->key.tag);
//  }
//  memcpy(log_ptr, op, len_to_copy);
//  kv->log_head += len_to_copy;
//
//  /* Ensure that the key field of each log entry is 8-byte aligned. This
//   * makes subsequent comparisons during GETs faster. */
//  kv->log_head = (kv->log_head + 7) & ~7;
//
//  /* If we're close to overflowing in the physical log, wrap around to
//   * the beginning, but go forward in the virtual log. */
//  if(unlikely(kv->log_cap - kv->log_head <= MICA_MAX_VALUE + 32)) {
//    kv->log_head = (kv->log_head + kv->log_cap) & ~kv->log_mask;
//    red_printf("mica: Instance %d wrapping around. Wraps = %llu\n",
//               kv->instance_id, kv->log_head / kv->log_cap);
//  }
//}


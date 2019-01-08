#ifndef _GNU_SOURCE
# define _GNU_SOURCE
#endif

// Optik Options
#define DEFAULT
#define CORE_NUM 4

#include <optik_mod.h>
#include <inline_util.h>
#include "optik_mod.h"
#include "cache.h"

struct cache cache;

//local file functions
char* code_to_str(uint8_t code);
void cache_meta_aggregate(void);
void cache_meta_reset(struct cache_meta_stats* meta);
void extended_cache_meta_reset(struct extended_cache_meta_stats* meta);
void cache_reset_total_ops_issued(void);



/*
 * Initialize the cache using a Mica instances and adding the timestamps
 * and locks to the keys of mica structure
 */
void cache_init(int cache_id, int num_threads) {
	int i;
	assert(sizeof(cache_meta) == 8); //make sure that the cache meta are 8B and thus can fit in mica unused key

	cache.num_threads = num_threads;
	cache_reset_total_ops_issued();
	/// allocate and init metadata for the cache & threads
	extended_cache_meta_reset(&cache.aggregated_meta);
	cache.meta = malloc(num_threads * sizeof(struct cache_meta_stats));
	for(i = 0; i < num_threads; i++)
		cache_meta_reset(&cache.meta[i]);
	mica_init(&cache.hash_table, cache_id, CACHE_SOCKET, CACHE_NUM_BKTS, HERD_LOG_CAP);
	cache_populate_fixed_len(&cache.hash_table, CACHE_NUM_KEYS, VALUE_SIZE);
}


/* ---------------------------------------------------------------------------
------------------------------ DRF-SC--------------------------------
---------------------------------------------------------------------------*


/* The worker sends its local requests to this, reads check the ts_tuple and copy it to the op to get broadcast
 * Writes do not get served either, writes are only propagated here to see whether their keys exist */
inline void cache_batch_op_trace(uint16_t op_num, uint16_t t_id, struct cache_op *op,
                                 struct cache_resp *resp,
                                 struct pending_ops *p_ops)
{
	uint16_t op_i;
#if CACHE_DEBUG == 1
	//assert(cache.hash_table != NULL);
	assert(op != NULL);
	assert(op_num > 0 && op_num <= CACHE_BATCH_SIZE);
	assert(resp != NULL);
#endif

#if CACHE_DEBUG == 2
	for(I = 0; I < op_num; I++)
		mica_print_op(&op[I]);
#endif
  if (ENABLE_ASSERTIONS) assert (op_num <= MAX_OP_BATCH);
	unsigned int bkt[MAX_OP_BATCH];
	struct mica_bkt *bkt_ptr[MAX_OP_BATCH];
	unsigned int tag[MAX_OP_BATCH];
	uint8_t key_in_store[MAX_OP_BATCH];	/* Is this key in the datastore? */
	struct cache_op *kv_ptr[MAX_OP_BATCH];	/* Ptr to KV item in log */
	/*
	 * We first lookup the key in the datastore. The first two @I loops work
	 * for both GETs and PUTs.
	 */
  KVS_locate_all_buckets(op_num, bkt, op, bkt_ptr, tag, kv_ptr,
                         key_in_store, &cache);
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, &cache);

	// the following variables used to validate atomicity between a lock-free r_rep of an object
	cache_meta prev_meta;
  uint64_t rmw_l_id = p_ops->prop_info->l_id;
  uint32_t r_push_ptr = p_ops->r_push_ptr;
	for(op_i = 0; op_i < op_num; op_i++) {
		if(kv_ptr[op_i] != NULL) {
			/* We had a tag match earlier. Now compare log entry. */
			long long *key_ptr_log = (long long *) kv_ptr[op_i];
			long long *key_ptr_req = (long long *) &op[op_i];

			if(key_ptr_log[1] == key_ptr_req[1]) { //Cache Hit
				key_in_store[op_i] = 1;
        if (ENABLE_ASSERTIONS && op[op_i].opcode != PROPOSE_OP)
          assert(kv_ptr[op_i]->opcode != KEY_HAS_BEEN_RMWED);

				if (op[op_i].opcode == CACHE_OP_GET || op[op_i].opcode == OP_ACQUIRE) {
          KVS_from_trace_reads_and_acquires(&op[op_i], kv_ptr[op_i], &resp[op_i],
                                            p_ops, &r_push_ptr, t_id);
        }
          // Put has to be 2 rounds (readTS + write) if it is out-of-epoch
        else if (op[op_i].opcode == CACHE_OP_PUT) {
          KVS_from_trace_writes(&op[op_i], kv_ptr[op_i], &resp[op_i],
                                p_ops, &r_push_ptr, t_id);
				}
        else if (op[op_i].opcode == OP_RELEASE) { // read the timestamp
          KVS_from_trace_releases(&op[op_i], kv_ptr[op_i], &resp[op_i],
                                  p_ops, &r_push_ptr, t_id);
        }
        else if (ENABLE_RMWS && op[op_i].opcode == PROPOSE_OP) {
          KVS_from_trace_rmw(&op[op_i], kv_ptr[op_i], &resp[op_i],
                             p_ops, &rmw_l_id, op_i, t_id);
        }
        else {
        red_printf("Wrkr %u: cache_batch_op_trace wrong opcode in cache: %d, req %d \n",
                   t_id, op[op_i].opcode, op_i);
        assert(0);
				}
			}
		}
		if(key_in_store[op_i] == 0) {  //Cache miss --> We get here if either tag or log key match failed
      //red_printf("Cache_miss: bkt %u/%u, server %u/%u, tag %u/%u \n",
      //          op[I].key.bkt, kv_ptr[I]->key.bkt ,op[I].key.server,
      //          kv_ptr[I]->key.server, op[I].key.tag, kv_ptr[I]->key.tag);
			resp[op_i].type = CACHE_MISS;
		}
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
#if CACHE_DEBUG == 1
  //assert(cache.hash_table != NULL);
	assert(op != NULL);
	assert(op_num > 0 && op_num <= CACHE_BATCH_SIZE);
	assert(resp != NULL);
#endif
#if CACHE_DEBUG == 2
  for(I = 0; I < op_num; I++)
		mica_print_op(&(*op)[I]);
#endif
  if (ENABLE_ASSERTIONS) assert(op_num <= MAX_INCOMING_W);
  unsigned int bkt[MAX_INCOMING_W];
  struct mica_bkt *bkt_ptr[MAX_INCOMING_W];
  unsigned int tag[MAX_INCOMING_W];
  uint8_t key_in_store[MAX_INCOMING_W];	/* Is this key in the datastore? */
  struct cache_op *kv_ptr[MAX_INCOMING_W];	/* Ptr to KV item in log */
  /*
     * We first lookup the key in the datastore. The first two @I loops work
     * for both GETs and PUTs.
     */
  for(op_i = 0; op_i < op_num; op_i++) {
    struct cache_op *op = (struct cache_op*) writes[(pull_ptr + op_i) % max_op_size];
    KVS_locate_one_bucket(op_i, bkt, op, bkt_ptr, tag, kv_ptr,
                          key_in_store, &cache);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, &cache);

  // the following variables used to validate atomicity between a lock-free r_rep of an object
  for(op_i = 0; op_i < op_num; op_i++) {
    struct cache_op *op = (struct cache_op *) writes[(pull_ptr + op_i) % max_op_size];
    if (unlikely (op->opcode == OP_RELEASE_BIT_VECTOR)) continue;
    if (kv_ptr[op_i] != NULL) {
      /* We had a tag match earlier. Now compare log entry. */
      long long *key_ptr_log = (long long *) kv_ptr[op_i];
      long long *key_ptr_req = (long long *) op;
      if (key_ptr_log[1] == key_ptr_req[1]) { //Cache Hit
        key_in_store[op_i] = 1;
        if (op->opcode == CACHE_OP_PUT || op->opcode == OP_RELEASE ||
            op->opcode == OP_ACQUIRE) {
          if (ENABLE_ASSERTIONS)
            assert(kv_ptr[op_i]->opcode != KEY_HAS_BEEN_RMWED);
          KVS_updates_writes_or_releases_or_acquires(op, kv_ptr[op_i], t_id);
        }
        else if (op->opcode == ACCEPT_OP) {
          KVS_updates_accepts(op, kv_ptr[op_i], p_ops, op_i, t_id);
        }
        else if (op->opcode == COMMIT_OP) {
          KVS_updates_commits(op, kv_ptr[op_i], p_ops, op_i, t_id);
        }
        else if (ENABLE_ASSERTIONS) {
          red_printf("Wrkr %u, cache batch update: wrong opcode in cache: %d, req %d, "
                       "m_id %u, val_len %u, version %u , \n",
                     t_id, op->opcode, op_i, op->key.meta.m_id,
                     op->val_len, op->key.meta.version);
          assert(0);
        }
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
  struct read **reads = p_ops->ptrs_to_r_ops;

  if (ENABLE_ASSERTIONS) assert(op_num <= MAX_INCOMING_R);
  unsigned int bkt[MAX_INCOMING_R];
  struct mica_bkt *bkt_ptr[MAX_INCOMING_R];
  unsigned int tag[MAX_INCOMING_R];
  uint8_t key_in_store[MAX_INCOMING_R];	/* Is this key in the datastore? */
  struct cache_op *kv_ptr[MAX_INCOMING_R];	/* Ptr to KV item in log */
  /*
     * We first lookup the key in the datastore. The first two @I loops work
     * for both GETs and PUTs.
     */
  for(op_i = 0; op_i < op_num; op_i++) {
    struct cache_op *op = (struct cache_op*) reads[(pull_ptr + op_i) % max_op_size];
    if (unlikely(op->opcode == OP_ACQUIRE_FLIP_BIT)) continue; // This message is only meant to flip a bit and is thus a NO-OP
    KVS_locate_one_bucket(op_i, bkt, op, bkt_ptr, tag, kv_ptr,
                          key_in_store, &cache);
  }
  for(op_i = 0; op_i < op_num; op_i++) {
    struct cache_op *op = (struct cache_op*) reads[(pull_ptr + op_i) % max_op_size];
    if (unlikely(op->opcode == OP_ACQUIRE_FLIP_BIT)) continue;
    KVS_locate_one_kv_pair(op_i, tag, bkt_ptr, kv_ptr, &cache);
  }
  cache_meta prev_meta;
  // the following variables used to validate atomicity between a lock-free r_rep of an object
  for(op_i = 0; op_i < op_num; op_i++) {
    struct cache_op *op = (struct cache_op*) reads[(pull_ptr + op_i) % max_op_size];
    if (op->opcode == OP_ACQUIRE_FLIP_BIT) {
      insert_r_rep(p_ops, NULL, NULL,
                   p_ops->ptrs_to_r_headers[op_i]->l_id, t_id,
                   p_ops->ptrs_to_r_headers[op_i]->m_id, (uint16_t) op_i, NULL, NO_OP_ACQ_FLIP_BIT, op->opcode);
      continue;
    }
    if(kv_ptr[op_i] != NULL) {
      /* We had a tag match earlier. Now compare log entry. */
      long long *key_ptr_log = (long long *) kv_ptr[op_i];
      long long *key_ptr_req = (long long *) op;
      if(key_ptr_log[1] == key_ptr_req[1]) { //Cache Hit
        key_in_store[op_i] = 1;

        if (ENABLE_ASSERTIONS && op->opcode != PROPOSE_OP)
          assert(kv_ptr[op_i]->opcode != KEY_HAS_BEEN_RMWED);

        if (op->opcode == CACHE_OP_GET || op->opcode == OP_ACQUIRE ||
            op->opcode == OP_ACQUIRE_FP) {
          KVS_reads_gets_or_acquires_or_acquires_fp(op, kv_ptr[op_i], p_ops, op_i, t_id);
        }
        else if (ENABLE_RMWS && op->opcode == PROPOSE_OP) {
          KVS_reads_proposes(op, kv_ptr[op_i], p_ops, op_i, t_id);
        }
        else if (op->opcode == CACHE_OP_GET_TS) {
          uint32_t debug_cntr = 0;
          do {
            if (ENABLE_ASSERTIONS) {
              debug_cntr++;
              if (debug_cntr % M_4 == 0) {
                printf("Worker %u stuck on a remote read version %u m_id %u, times %u \n",
                       t_id, prev_meta.version, prev_meta.m_id, debug_cntr / M_4);
                //debug_cntr = 0;
              }
            }
            prev_meta = kv_ptr[op_i]->key.meta;
          } while (!optik_is_same_version_and_valid(prev_meta, kv_ptr[op_i]->key.meta));
          insert_r_rep(p_ops, (struct network_ts_tuple *)&prev_meta.m_id, (struct network_ts_tuple *)&op->key.meta.m_id,
                       p_ops->ptrs_to_r_headers[op_i]->l_id, t_id,
                       p_ops->ptrs_to_r_headers[op_i]->m_id, (uint16_t) op_i, NULL, READ_TS, op->opcode);
        }
        else {
          //red_printf("wrong Opcode in cache: %d, req %d, m_id %u, val_len %u, version %u , \n",
          //           op->opcode, I, reads[(pull_ptr + I) % max_op_size]->m_id,
          //           reads[(pull_ptr + I) % max_op_size]->val_len,
          //          reads[(pull_ptr + I) % max_op_size]->version);
          assert(false);
        }
      }
    }
    if(key_in_store[op_i] == 0) {  //Cache miss --> We get here if either tag or log key match failed
//      red_printf("Cache_miss: bkt %u, server %u, tag %u \n", op->key.bkt, op->key.server, op->key.tag);
      assert(false); // cant have a miss since, it hit in the source's cache
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
  struct cache_op *kv_ptr[MAX_INCOMING_R];	/* Ptr to KV item in log */
  /*
     * We first lookup the key in the datastore. The first two @I loops work
     * for both GETs and PUTs.
     */
  for(op_i = 0; op_i < op_num; op_i++) {
    struct key *op_key = (struct key*) writes[(pull_ptr + op_i) % max_op_size]->key;
    KVS_locate_one_bucket_with_key(op_i, bkt, op_key, bkt_ptr, tag, kv_ptr,
                          key_in_store, &cache);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, &cache);


  // the following variables used to validate atomicity between a lock-free r_rep of an object
  for(op_i = 0; op_i < op_num; op_i++) {
    struct read_info *op = writes[(pull_ptr + op_i) % max_op_size];
    if(kv_ptr[op_i] != NULL) {
      /* We had a tag match earlier. Now compare log entry. */
      long long *key_ptr_log = (long long *) kv_ptr[op_i];
      long long *key_ptr_req = (long long *) op->key;
      if(key_ptr_log[1] == key_ptr_req[0]) { //Cache Hit
        key_in_store[op_i] = 1;
        cache_meta op_meta = * (cache_meta *) (((void*)op) - 3);
        // The write must be performed with the max TS out of the one stored in the KV and read_info
        if (op->opcode == CACHE_OP_PUT) {
          KVS_out_of_epoch_writes(op, kv_ptr[op_i], p_ops, t_id);
        }
        else if (op->opcode == OP_ACQUIRE || op->opcode == CACHE_OP_GET) { // a read resulted on receiving a higher timestamp than expected
          KVS_acquires_and_out_of_epoch_reads(op, kv_ptr[op_i], t_id);
        }
        else if (op->opcode == UPDATE_EPOCH_OP_GET) {
          if (op->epoch_id > *(uint16_t *)kv_ptr[op_i]->key.meta.epoch_id) {
            optik_lock(&kv_ptr[op_i]->key.meta);
            *(uint16_t*)kv_ptr[op_i]->key.meta.epoch_id = op->epoch_id;
            optik_unlock_decrement_version(&kv_ptr[op_i]->key.meta);
            if (ENABLE_STAT_COUNTING) t_stats[t_id].rectified_keys++;
          }
        }
        else {
          red_printf("Wrkr %u: read-first-round wrong opcode in cache: %d, req %d, m_id %u,version %u , \n",
                     t_id, op->opcode, op_i, writes[(pull_ptr + op_i) % max_op_size]->ts_to_read.m_id,
                     writes[(pull_ptr + op_i) % max_op_size]->ts_to_read.version);
          assert(0);
        }
      }
    }
    if(key_in_store[op_i] == 0) {  //Cache miss --> We get here if either tag or log key match failed
      if (ENABLE_ASSERTIONS) assert(false);
    }
    if (zero_ops) {
      // printf("Zero out %d at address %lu \n", op->opcode, &op->opcode);
      op->opcode = 5;
    }
  }

}


// Send an isolated write to the cache-no batching
inline void cache_isolated_op(int t_id, struct write *write)
{
  uint32_t op_num = 1;
  int j;	/* I is batch index */
#if CACHE_DEBUG == 1
  //assert(cache.hash_table != NULL);
	assert(op != NULL);
	assert(op_num > 0 && op_num <= CACHE_BATCH_SIZE);
	assert(resp != NULL);
#endif

#if CACHE_DEBUG == 2
  for(I = 0; I < op_num; I++)
		mica_print_op(&(*op)[I]);
#endif

  unsigned int bkt;
  struct mica_bkt *bkt_ptr;
  unsigned int tag;
  int key_in_store;	/* Is this key in the datastore? */
  struct cache_op *kv_ptr;	/* Ptr to KV item in log */
  /*
   * We first lookup the key in the datastore. The first two @I loops work
   * for both GETs and PUTs.
   */
  struct cache_op *op = (struct cache_op*) (((void *) write) - 3);
  //print_true_key((struct key *) write->key);
  //printf("op bkt %u\n", op->key.bkt);
  bkt = op->key.bkt & cache.hash_table.bkt_mask;
  bkt_ptr = &cache.hash_table.ht_index[bkt];
  //__builtin_prefetch(bkt_ptr, 0, 0);
  tag = op->key.tag;

  key_in_store = 0;
  kv_ptr = NULL;


  for(j = 0; j < 8; j++) {
    if(bkt_ptr->slots[j].in_use == 1 &&
       bkt_ptr->slots[j].tag == tag) {
      uint64_t log_offset = bkt_ptr->slots[j].offset &
                            cache.hash_table.log_mask;
      /*
               * We can interpret the log entry as mica_op, even though it
               * may not contain the full MICA_MAX_VALUE value.
               */
      kv_ptr = (struct cache_op *) &cache.hash_table.ht_log[log_offset];

      /* Small values (1--64 bytes) can span 2 cache lines */
      //__builtin_prefetch(kv_ptr, 0, 0);
      //__builtin_prefetch((uint8_t *) kv_ptr + 64, 0, 0);

      /* Detect if the head has wrapped around for this index entry */
      if(cache.hash_table.log_head - bkt_ptr->slots[j].offset >= cache.hash_table.log_cap) {
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
          red_printf("Wrkr %u: cache_isolated_op: wrong pcode : %d, m_id %u, val_len %u, version %u , \n",
                     t_id, op->opcode,  op->key.meta.m_id,
                     op->val_len, op->key.meta.version);
          assert(false);
        }
      }
      //red_printf("op val len %d in ptr %d, total ops %d \n", op->val_len, (pull_ptr + I) % max_op_size, op_num );
      if (ENABLE_ASSERTIONS) assert(op->val_len == kv_ptr->val_len);
      optik_lock(&kv_ptr->key.meta);
      if (optik_is_greater_version(kv_ptr->key.meta, op->key.meta)) {
        memcpy(kv_ptr->value, op->value, VALUE_SIZE);
        optik_unlock(&kv_ptr->key.meta, op->key.meta.m_id, op->key.meta.version);
      }
      else {
        optik_unlock_decrement_version(&kv_ptr->key.meta);
      }
    }
  }
  if(key_in_store == 0) {  //Cache miss --> We get here if either tag or log key match failed
    if (ENABLE_ASSERTIONS) assert(false);
  }



}


void cache_populate_fixed_len(struct mica_kv* kv, int n, int val_len) {
	//assert(cache != NULL);
	assert(n > 0);
	assert(val_len > 0 && val_len <= MICA_MAX_VALUE);

	/* This is needed for the eviction message below to make sense */
	assert(kv->num_insert_op == 0 && kv->num_index_evictions == 0);

	int i;
	struct cache_op op;
	struct mica_resp resp;
	unsigned long long *op_key = (unsigned long long *) &op.key;

	/* Generate the keys to insert */
	uint128 *key_arr = mica_gen_keys(n);

	for(i = n - 1; i >= 0; i--) {
		optik_init(&op.key.meta);
		memset((void *)op.key.meta.epoch_id, 0, EPOCH_BYTES);
//		op.key.meta.state = VALID_STATE;
		op_key[1] = key_arr[i].second;
		op.opcode = 0;

		//printf("Key Metadata: Lock(%u), State(%u), Counter(%u:%u)\n", op.key.meta.lock, op.key.meta.state, op.key.meta.version, op.key.meta.cid);
		op.val_len = (uint8_t) (val_len >> SHIFT_BITS);
		uint8_t val = 'a';//(uint8_t) (op_key[1] & 0xff);
		memset(op.value, val, (uint32_t) val_len);
    //if (i < NUM_OF_RMW_KEYS)
     // green_printf("Inserting key %d: bkt %u, server %u, tag %u \n",i, op.key.bkt, op.key.server, op.key.tag);
		mica_insert_one(kv, (struct mica_op *) &op, &resp);
	}

	assert(kv->num_insert_op == n);
	// printf("Cache: Populated instance %d with %d keys, length = %d. "
	// 			   "Index eviction fraction = %.4f.\n",
	// 	   cache.hash_table.instance_id, n, val_len,
	// 	   (double) cache.hash_table.num_index_evictions / cache.hash_table.num_insert_op);
}

/*
 * WARNING: the following functions related to cache stats are not tested on this version of code
 */

void cache_meta_reset(struct cache_meta_stats* meta){
	meta->num_get_success = 0;
	meta->num_put_success = 0;
	meta->num_upd_success = 0;
	meta->num_inv_success = 0;
	meta->num_ack_success = 0;
	meta->num_get_stall = 0;
	meta->num_put_stall = 0;
	meta->num_upd_fail = 0;
	meta->num_inv_fail = 0;
	meta->num_ack_fail = 0;
	meta->num_get_miss = 0;
	meta->num_put_miss = 0;
	meta->num_unserved_get_miss = 0;
	meta->num_unserved_put_miss = 0;
}

void extended_cache_meta_reset(struct extended_cache_meta_stats* meta){
	meta->num_hit = 0;
	meta->num_miss = 0;
	meta->num_stall = 0;
	meta->num_coherence_fail = 0;
	meta->num_coherence_success = 0;

	cache_meta_reset(&meta->metadata);
}

void cache_reset_total_ops_issued(){
	cache.total_ops_issued = 0;
}

void cache_meta_aggregate(){
	int i = 0;
	for(i = 0; i < cache.num_threads; i++){
		cache.aggregated_meta.metadata.num_get_success += cache.meta[i].num_get_success;
		cache.aggregated_meta.metadata.num_put_success += cache.meta[i].num_put_success;
		cache.aggregated_meta.metadata.num_upd_success += cache.meta[i].num_upd_success;
		cache.aggregated_meta.metadata.num_inv_success += cache.meta[i].num_inv_success;
		cache.aggregated_meta.metadata.num_ack_success += cache.meta[i].num_ack_success;
		cache.aggregated_meta.metadata.num_get_stall += cache.meta[i].num_get_stall;
		cache.aggregated_meta.metadata.num_put_stall += cache.meta[i].num_put_stall;
		cache.aggregated_meta.metadata.num_upd_fail += cache.meta[i].num_upd_fail;
		cache.aggregated_meta.metadata.num_inv_fail += cache.meta[i].num_inv_fail;
		cache.aggregated_meta.metadata.num_ack_fail += cache.meta[i].num_ack_fail;
		cache.aggregated_meta.metadata.num_get_miss += cache.meta[i].num_get_miss;
		cache.aggregated_meta.metadata.num_put_miss += cache.meta[i].num_put_miss;
		cache.aggregated_meta.metadata.num_unserved_get_miss += cache.meta[i].num_unserved_get_miss;
		cache.aggregated_meta.metadata.num_unserved_put_miss += cache.meta[i].num_unserved_put_miss;
	}
	cache.aggregated_meta.num_hit = cache.aggregated_meta.metadata.num_get_success + cache.aggregated_meta.metadata.num_put_success;
	cache.aggregated_meta.num_miss = cache.aggregated_meta.metadata.num_get_miss + cache.aggregated_meta.metadata.num_put_miss;
	cache.aggregated_meta.num_stall = cache.aggregated_meta.metadata.num_get_stall + cache.aggregated_meta.metadata.num_put_stall;
	cache.aggregated_meta.num_coherence_fail = cache.aggregated_meta.metadata.num_upd_fail + cache.aggregated_meta.metadata.num_inv_fail
											   + cache.aggregated_meta.metadata.num_ack_fail;
	cache.aggregated_meta.num_coherence_success = cache.aggregated_meta.metadata.num_upd_success + cache.aggregated_meta.metadata.num_inv_success
												  + cache.aggregated_meta.metadata.num_ack_success;
}

void print_IOPS_and_time(struct timespec start, long long ops, int id){
	struct timespec end;
	clock_gettime(CLOCK_REALTIME, &end);
	double seconds = (end.tv_sec - start.tv_sec) +
					 (double) (end.tv_nsec - start.tv_nsec) / 1000000000;
	printf("Cache %d: %.2f IOPS. time: %.2f\n", id, ops / seconds, seconds);
}

void print_cache_stats(struct timespec start, int cache_id){
	long long total_reads, total_writes, total_ops, total_retries;
	struct extended_cache_meta_stats* meta = &cache.aggregated_meta;
	extended_cache_meta_reset(meta);
	cache_meta_aggregate();
	total_reads = meta->metadata.num_get_success + meta->metadata.num_get_miss;
	total_writes = meta->metadata.num_put_success + meta->metadata.num_put_miss;
	total_retries = meta->metadata.num_get_stall + meta->metadata.num_put_stall;
	total_ops = total_reads + total_writes;

	printf("~~~~~~~~ Cache %d Stats ~~~~~~~ :\n", cache_id);
	if(total_ops != cache.total_ops_issued){
		printf("Total_ops: %llu, 2nd total ops: %llu\n",total_ops, cache.total_ops_issued);
	}
	//assert(total_ops == cache.total_ops_issued);
	print_IOPS_and_time(start, cache.total_ops_issued, cache_id);
	printf("\t Total (GET/PUT) Ops: %.2f %% %lld   \n", 100.0 * total_ops / total_retries, total_ops);
	printf("\t\t Hit Rate: %.2f %%  \n", 100.0 * (meta->metadata.num_get_success + meta->metadata.num_put_success)/ total_ops);
	printf("\t\t Total Retries: %lld   \n", total_retries);
	printf("\t\t\t Reads: %.2f %% (%lld)   \n", 100.0 * total_reads / total_ops, total_reads);
	printf("\t\t\t\t Read Hit Rate  : %.2f %%  \n", 100.0 * meta->metadata.num_get_success / total_reads);
	printf("\t\t\t\t\t Hits   : %lld \n", meta->metadata.num_get_success);
	printf("\t\t\t\t\t Misses : %lld \n", meta->metadata.num_get_miss);
	printf("\t\t\t\t\t Retries: %lld \n", meta->metadata.num_get_stall);
	printf("\t\t\t Writes: %.2f %% (%lld)   \n", 100.0 * total_writes / total_ops, total_writes);
	printf("\t\t\t\t Write Hit Rate: %.2f %% \n", 100.0 * meta->metadata.num_put_success / total_writes);
	printf("\t\t\t\t\t Hits  : %llu \n", meta->metadata.num_put_success);
	printf("\t\t\t\t\t Misses: %llu \n", meta->metadata.num_put_miss);
	printf("\t\t\t\t\t Retries: %lld \n", meta->metadata.num_put_stall);
	printf("\t Total Coherence Ops: %lld   \n", meta->num_coherence_fail + meta->num_coherence_success);
	printf("\t\t Successful: %lld  (%.2f %%) \n", meta->num_coherence_success, 100.0 * meta->num_coherence_success / (meta->num_coherence_fail + meta->num_coherence_success));
	printf("\t\t Failed: %lld  (%.2f %%) \n", meta->num_coherence_fail, 100.0 * meta->num_coherence_fail / (meta->num_coherence_fail + meta->num_coherence_success));
	printf("\t\t\t Updates: %.2f %% (%lld)   \n", 100.0 * meta->metadata.num_upd_success / (meta->num_coherence_fail + meta->num_coherence_success), meta->metadata.num_upd_success + meta->metadata.num_upd_fail);
	printf("\t\t\t\t Successful: %lld  (%.2f %%) \n", meta->metadata.num_upd_success, 100.0 * meta->metadata.num_upd_success / (meta->metadata.num_upd_success + meta->metadata.num_upd_fail));
	printf("\t\t\t\t Failed: %lld  (%.2f %%) \n", meta->metadata.num_upd_fail, 100.0 * meta->metadata.num_upd_fail / (meta->metadata.num_upd_success + meta->metadata.num_upd_fail));
	printf("\t\t\t Invalidates: %.2f %% (%lld)   \n", 100.0 * meta->metadata.num_inv_success / (meta->num_coherence_fail + meta->num_coherence_success), meta->metadata.num_inv_success + meta->metadata.num_inv_fail  );
	printf("\t\t\t\t Successful: %lld  (%.2f %%) \n", meta->metadata.num_inv_success, 100.0 * meta->metadata.num_inv_success / (meta->metadata.num_inv_success + meta->metadata.num_inv_fail));
	printf("\t\t\t\t Failed: %lld  (%.2f %%) \n", meta->metadata.num_inv_fail, 100.0 * meta->metadata.num_inv_fail / (meta->metadata.num_inv_success + meta->metadata.num_inv_fail));
	printf("\t\t\t Acks: %.2f %% (%lld)   \n", 100.0 * meta->metadata.num_ack_success / (meta->num_coherence_fail + meta->num_coherence_success), meta->metadata.num_ack_success + meta->metadata.num_ack_fail );
	printf("\t\t\t\t Successful: %lld  (%.2f %%) \n", meta->metadata.num_ack_success, 100.0 * meta->metadata.num_ack_success / (meta->metadata.num_ack_success + meta->metadata.num_ack_fail));
	printf("\t\t\t\t Failed: %lld  (%.2f %%) \n", meta->metadata.num_ack_fail, 100.0 * meta->metadata.num_ack_fail / (meta->metadata.num_ack_success + meta->metadata.num_ack_fail));
}

void str_to_binary(uint8_t* value, char* str, int size){
	int i;
	for(i = 0; i < size; i++)
		value[i] = (uint8_t) str[i];
	value[size] = '\0';
}

char* code_to_str(uint8_t code){
	switch (code){
		case EMPTY:
			return "EMPTY";
		case RETRY:
			return "RETRY";
		case CACHE_GET_SUCCESS:
			return "CACHE_GET_SUCCESS";
		case CACHE_PUT_SUCCESS:
			return "CACHE_PUT_SUCCESS";
		case CACHE_LOCAL_GET_SUCCESS:
			return "CACHE_LOCAL_GET_SUCCESS";
    case KEY_HIT:
      return "KEY_HIT";
		case CACHE_INV_SUCCESS:
			return "CACHE_INV_SUCCESS";
		case CACHE_ACK_SUCCESS:
			return "CACHE_ACK_SUCCESS";
		case CACHE_LAST_ACK_SUCCESS:
			return "CACHE_LAST_ACK_SUCCESS";
		case CACHE_MISS:
			return "CACHE_MISS";
		case CACHE_GET_STALL:
			return "CACHE_GET_STALL";
		case CACHE_PUT_STALL:
			return "CACHE_PUT_STALL";
		case CACHE_UPD_FAIL:
			return "CACHE_UPD_FAIL";
		case CACHE_INV_FAIL:
			return "CACHE_INV_FAIL";
		case CACHE_ACK_FAIL:
			return "CACHE_ACK_FAIL";
		case CACHE_PUT_FAIL:
			return "CACHE_PUT_FAIL";
		case CACHE_GET_FAIL:
			return "CACHE_GET_FAIL";
		case CACHE_OP_GET:
			return "CACHE_OP_GET";
		case CACHE_OP_PUT:
			return "CACHE_OP_PUT";
		case CACHE_OP_ACK:
			return "CACHE_OP_ACK";
		case UNSERVED_CACHE_MISS:
			return "UNSERVED_CACHE_MISS";
		case VALID_STATE:
			return "VALID_STATE";
		case INVALID_STATE:
			return "INVALID_STATE";
		case INVALID_REPLAY_STATE:
			return "INVALID_REPLAY_STATE";
		case WRITE_STATE:
			return "WRITE_STATE";
		case WRITE_REPLAY_STATE:
			return "WRITE_REPLAY_STATE";
		default: {
			printf("Wrong code (%d)\n", code);
			assert(0);
		}
	}
}

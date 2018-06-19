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
	cache_populate_fixed_len(&cache.hash_table, CACHE_NUM_KEYS, HERD_VALUE_SIZE);
}


/* ---------------------------------------------------------------------------
------------------------------ ABD--------------------------------
---------------------------------------------------------------------------*


/* The worker sends its local requests to this, reads check the ts_tuple and copy it to the op to get broadcast
 * Writes do not get served either, writes are only propagated here to see whether their keys exist */
inline void cache_batch_op_trace(int op_num, int t_id, struct cache_op **op, struct mica_resp *resp,
                                 struct pending_ops *p_ops)
{
	int I, j;	/* I is batch index */
	long long stalled_brces = 0;
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

	unsigned int bkt[CACHE_BATCH_SIZE];
	struct mica_bkt *bkt_ptr[CACHE_BATCH_SIZE];
	unsigned int tag[CACHE_BATCH_SIZE];
	int key_in_store[CACHE_BATCH_SIZE];	/* Is this key in the datastore? */
	struct cache_op *kv_ptr[CACHE_BATCH_SIZE];	/* Ptr to KV item in log */
	/*
	 * We first lookup the key in the datastore. The first two @I loops work
	 * for both GETs and PUTs.
	 */
	for(I = 0; I < op_num; I++) {
		bkt[I] = (*op)[I].key.bkt & cache.hash_table.bkt_mask;
		bkt_ptr[I] = &cache.hash_table.ht_index[bkt[I]];
		__builtin_prefetch(bkt_ptr[I], 0, 0);
		tag[I] = (*op)[I].key.tag;

		key_in_store[I] = 0;
		kv_ptr[I] = NULL;
	}

	for(I = 0; I < op_num; I++) {
		for(j = 0; j < 8; j++) {
			if(bkt_ptr[I]->slots[j].in_use == 1 &&
				 bkt_ptr[I]->slots[j].tag == tag[I]) {
				uint64_t log_offset = bkt_ptr[I]->slots[j].offset &
															cache.hash_table.log_mask;
				/*
				 * We can interpret the log entry as mica_op, even though it
				 * may not contain the full MICA_MAX_VALUE value.
				 */
				kv_ptr[I] = (struct cache_op *) &cache.hash_table.ht_log[log_offset];

				/* Small values (1--64 bytes) can span 2 cache lines */
				__builtin_prefetch(kv_ptr[I], 0, 0);
				__builtin_prefetch((uint8_t *) kv_ptr[I] + 64, 0, 0);

				/* Detect if the head has wrapped around for this index entry */
				if(cache.hash_table.log_head - bkt_ptr[I]->slots[j].offset >= cache.hash_table.log_cap) {
					kv_ptr[I] = NULL;	/* If so, we mark it "not found" */
				}

				break;
			}
		}
	}

	// the following variables used to validate atomicity between a lock-free r_rep of an object
	cache_meta prev_meta;
  uint32_t r_push_ptr = p_ops->r_push_ptr;
	for(I = 0; I < op_num; I++) {
		if(kv_ptr[I] != NULL) {

			/* We had a tag match earlier. Now compare log entry. */
			long long *key_ptr_log = (long long *) kv_ptr[I];
			long long *key_ptr_req = (long long *) &(*op)[I];

			if(key_ptr_log[1] == key_ptr_req[1]) { //Cache Hit
				key_in_store[I] = 1;
				if ((*op)[I].opcode == CACHE_OP_GET || (*op)[I].opcode == OP_ACQUIRE) {
					//Lock free reads through versioning (successful when version is even)
          uint32_t debug_cntr = 0;
					do {
//						memcpy((void*) &prev_meta, (void*) &(kv_ptr[I]->key.meta), sizeof(cache_meta));
            prev_meta = kv_ptr[I]->key.meta;
            if (ENABLE_ASSERTIONS) {
              debug_cntr++;
              if (debug_cntr == M_4) {
                printf("Worker %u stuck on a local read \n", t_id);
                debug_cntr = 0;
              }
            }
            memcpy(p_ops->read_info[r_push_ptr].value, kv_ptr[I]->value, VALUE_SIZE);
					} while (!optik_is_same_version_and_valid(prev_meta, kv_ptr[I]->key.meta));
          // Do a quorum read if the stored value is old and may be stale or it is an Acquire!
          if (*(uint16_t *)prev_meta.epoch_id < epoch_id || (*op)[I].opcode == OP_ACQUIRE) {
            p_ops->read_info[r_push_ptr].opcode = (*op)[I].opcode;
            MOD_ADD(r_push_ptr, PENDING_READS);
            resp[I].type = CACHE_GET_SUCCESS;
          }
          else resp[I].type = CACHE_LOCAL_GET_SUCCESS; //stored value can be read locally
          memcpy((void *)&(*op)[I].key.meta.m_id, (void *)&prev_meta.m_id, TS_TUPLE_SIZE);
				}
        else if ((*op)[I].opcode == CACHE_OP_PUT || (*op)[I].opcode == OP_RELEASE) {
          if (ENABLE_ASSERTIONS) assert((*op)[I].val_len == kv_ptr[I]->val_len);
          optik_lock(&kv_ptr[I]->key.meta);
          memcpy(kv_ptr[I]->value, (*op)[I].value, VALUE_SIZE);
          optik_unlock_write(&kv_ptr[I]->key.meta, (uint8_t) machine_id, (uint32_t *) &(*op)[I].key.meta.version);
//          resp[I].val_len = 0;
//          resp[I].val_ptr = NULL;
          resp[I].type = CACHE_PUT_SUCCESS;
				}
        else if ((*op)[I].opcode == CACHE_OP_LIN_PUT) {
          //Lock free reads through versioning (successful when version is even)
          uint32_t debug_cntr = 0;
          do {
            prev_meta = kv_ptr[I]->key.meta;
//            memcpy((void*) &prev_meta, (void*) &(kv_ptr[I]->key.meta), sizeof(cache_meta));
            if (ENABLE_ASSERTIONS) {
              debug_cntr++;
              if (debug_cntr == M_4) {
                printf("Worker %u stuck on reading the TS for a lin write\n", t_id);
                debug_cntr = 0;
              }
            }
          } while (!optik_is_same_version_and_valid(prev_meta, kv_ptr[I]->key.meta));
          (*op)[I].key.meta.m_id = (uint8_t) machine_id;
          (*op)[I].key.meta.version = prev_meta.version;
          p_ops->read_info[r_push_ptr].opcode = CACHE_OP_LIN_PUT;
          MOD_ADD(r_push_ptr, PENDING_READS);
          resp[I].type = CACHE_GET_SUCCESS;
        }
        else {
        red_printf("wrong Opcode in cache: %d, req %d \n", (*op)[I].opcode, I);
        assert(0);
				}
			}
		}

		if(key_in_store[I] == 0) {  //Cache miss --> We get here if either tag or log key match failed
//			resp[I].val_len = 0;
//			resp[I].val_ptr = NULL;
			resp[I].type = CACHE_MISS;
		}
	}
}

/* The worker sends the remote writes to be committed with this function*/
// THE API IS DIFFERENT HERE, THIS TAKES AN ARRAY OF POINTERS RATHER THAN A POINTER TO AN ARRAY
// YOU have to give a pointer to the beggining of the array of the pointers or else you will not
// be able to wrap around to your array
inline void cache_batch_op_updates(uint32_t op_num, int thread_id, struct write **writes,
                                   uint32_t pull_ptr,  uint32_t max_op_size, bool zero_ops)
{
  int I, j;	/* I is batch index */
  long long stalled_brces = 0;
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

  unsigned int bkt[CACHE_BATCH_SIZE];
  struct mica_bkt *bkt_ptr[CACHE_BATCH_SIZE];
  unsigned int tag[CACHE_BATCH_SIZE];
  int key_in_store[CACHE_BATCH_SIZE];	/* Is this key in the datastore? */
  struct cache_op *kv_ptr[CACHE_BATCH_SIZE];	/* Ptr to KV item in log */
  /*
     * We first lookup the key in the datastore. The first two @I loops work
     * for both GETs and PUTs.
     */
  for(I = 0; I < op_num; I++) {
    struct cache_op *op = (struct cache_op*) writes[(pull_ptr + I) % max_op_size];
    bkt[I] = op->key.bkt & cache.hash_table.bkt_mask;
    bkt_ptr[I] = &cache.hash_table.ht_index[bkt[I]];
    __builtin_prefetch(bkt_ptr[I], 0, 0);
    tag[I] = op->key.tag;

    key_in_store[I] = 0;
    kv_ptr[I] = NULL;  }
  for(I = 0; I < op_num; I++) {
    for(j = 0; j < 8; j++) {
      if(bkt_ptr[I]->slots[j].in_use == 1 &&
         bkt_ptr[I]->slots[j].tag == tag[I]) {
        uint64_t log_offset = bkt_ptr[I]->slots[j].offset &
                              cache.hash_table.log_mask;
        /*
                 * We can interpret the log entry as mica_op, even though it
                 * may not contain the full MICA_MAX_VALUE value.
                 */
        kv_ptr[I] = (struct cache_op *) &cache.hash_table.ht_log[log_offset];

        /* Small values (1--64 bytes) can span 2 cache lines */
        __builtin_prefetch(kv_ptr[I], 0, 0);
        __builtin_prefetch((uint8_t *) kv_ptr[I] + 64, 0, 0);

        /* Detect if the head has wrapped around for this index entry */
        if(cache.hash_table.log_head - bkt_ptr[I]->slots[j].offset >= cache.hash_table.log_cap) {
          kv_ptr[I] = NULL;	/* If so, we mark it "not found" */
        }

        break;
      }
    }
  }
  // the following variables used to validate atomicity between a lock-free r_rep of an object
  for(I = 0; I < op_num; I++) {
    struct cache_op *op = (struct cache_op*) writes[(pull_ptr + I) % max_op_size];
    if(kv_ptr[I] != NULL) {
      /* We had a tag match earlier. Now compare log entry. */
      long long *key_ptr_log = (long long *) kv_ptr[I];
      long long *key_ptr_req = (long long *) op;
      if(key_ptr_log[1] == key_ptr_req[1]) { //Cache Hit
        key_in_store[I] = 1;
        if (ENABLE_ASSERTIONS) {
          if (op->opcode != CACHE_OP_PUT && op->opcode != OP_RELEASE && op->opcode != OP_ACQUIRE) {
            red_printf("wrong Opcode in cache: %d, req %d, m_id %u, val_len %u, version %u , \n",
                       op->opcode, I, op->key.meta.m_id,
                       op->val_len, op->key.meta.version);
            assert(0);
          }
        }
        //red_printf("op val len %d in ptr %d, total ops %d \n", op->val_len, (pull_ptr + I) % max_op_size, op_num );
        if (ENABLE_ASSERTIONS) assert(op->val_len == kv_ptr[I]->val_len);
        optik_lock(&kv_ptr[I]->key.meta);
        if (optik_is_greater_version(kv_ptr[I]->key.meta, op->key.meta)) {
          memcpy(kv_ptr[I]->value, op->value, VALUE_SIZE);
          optik_unlock(&kv_ptr[I]->key.meta, op->key.meta.m_id, op->key.meta.version);
        }
        else {
          optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
          t_stats[thread_id].failed_rem_writes++;
        }
      }
    }
    if(key_in_store[I] == 0) {  //Cache miss --> We get here if either tag or log key match failed
    }
    if (zero_ops) {
//      printf("Zero out %d at address %lu \n", op->opcode, &op->opcode);
      op->opcode = 5;
    }
  }

}

// The worker send here the incoming reads, the reads check the incoming ts if it is  bigger/equal to the local
// the just ack it, otherwise they send the value back
inline void cache_batch_op_reads(uint32_t op_num, uint16_t t_id, struct pending_ops *p_ops,
                                 uint32_t pull_ptr, uint32_t max_op_size, bool zero_ops)
{
  int I, j;	/* I is batch index */
  long long stalled_brces = 0;
  struct read **reads = p_ops->ptrs_to_r_ops;
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

  unsigned int bkt[CACHE_BATCH_SIZE];
  struct mica_bkt *bkt_ptr[CACHE_BATCH_SIZE];
  unsigned int tag[CACHE_BATCH_SIZE];
  int key_in_store[CACHE_BATCH_SIZE];	/* Is this key in the datastore? */
  struct cache_op *kv_ptr[CACHE_BATCH_SIZE];	/* Ptr to KV item in log */
  /*
     * We first lookup the key in the datastore. The first two @I loops work
     * for both GETs and PUTs.
     */
  for(I = 0; I < op_num; I++) {
    struct cache_op *op = (struct cache_op*) reads[(pull_ptr + I) % max_op_size];
    bkt[I] = op->key.bkt & cache.hash_table.bkt_mask;
    bkt_ptr[I] = &cache.hash_table.ht_index[bkt[I]];
    __builtin_prefetch(bkt_ptr[I], 0, 0);
    tag[I] = op->key.tag;

    key_in_store[I] = 0;
    kv_ptr[I] = NULL;  }
  for(I = 0; I < op_num; I++) {
    for(j = 0; j < 8; j++) {
      if(bkt_ptr[I]->slots[j].in_use == 1 &&
         bkt_ptr[I]->slots[j].tag == tag[I]) {
        uint64_t log_offset = bkt_ptr[I]->slots[j].offset &
                              cache.hash_table.log_mask;
        /*
                 * We can interpret the log entry as mica_op, even though it
                 * may not contain the full MICA_MAX_VALUE value.
                 */
        kv_ptr[I] = (struct cache_op *) &cache.hash_table.ht_log[log_offset];

        /* Small values (1--64 bytes) can span 2 cache lines */
        __builtin_prefetch(kv_ptr[I], 0, 0);
        __builtin_prefetch((uint8_t *) kv_ptr[I] + 64, 0, 0);

        /* Detect if the head has wrapped around for this index entry */
        if(cache.hash_table.log_head - bkt_ptr[I]->slots[j].offset >= cache.hash_table.log_cap) {
          kv_ptr[I] = NULL;	/* If so, we mark it "not found" */
        }

        break;
      }
    }
  }
  cache_meta prev_meta;
  // the following variables used to validate atomicity between a lock-free r_rep of an object
  for(I = 0; I < op_num; I++) {
    struct cache_op *op = (struct cache_op*) reads[(pull_ptr + I) % max_op_size];
    if(kv_ptr[I] != NULL) {
      /* We had a tag match earlier. Now compare log entry. */
      long long *key_ptr_log = (long long *) kv_ptr[I];
      long long *key_ptr_req = (long long *) op;
      if(key_ptr_log[1] == key_ptr_req[1]) { //Cache Hit
        key_in_store[I] = 1;
        if (op->opcode == CACHE_OP_GET || op->opcode == OP_ACQUIRE) {
          //Lock free reads through versioning (successful when version is even)
          uint32_t debug_cntr = 0;
          uint8_t tmp_value[VALUE_SIZE];
          do {
            //memcpy((void*) &prev_meta, (void*) &(kv_ptr[I]->key.meta), sizeof(cache_meta));
            if (ENABLE_ASSERTIONS) {
							debug_cntr++;
							if (debug_cntr % M_4 == 0) {
								printf("Worker %u stuck on a remote read version %u m_id %u, times %u \n",
											 t_id, prev_meta.version, prev_meta.m_id, debug_cntr / M_4);
								//debug_cntr = 0;
							}
						}
            prev_meta = kv_ptr[I]->key.meta;
            if (compare_ts((struct ts_tuple *)&prev_meta.m_id, (struct ts_tuple *)&op->key.meta.m_id) == GREATER)
              memcpy(tmp_value, kv_ptr[I]->value, VALUE_SIZE);
          } while (!optik_is_same_version_and_valid(prev_meta, kv_ptr[I]->key.meta));
          bool false_positive = op->opcode == OP_ACQUIRE && (!config_vector[p_ops->ptrs_to_r_headers[I]->m_id]);
          insert_r_rep(p_ops, (struct ts_tuple *)&prev_meta.m_id, (struct ts_tuple *)&op->key.meta.m_id,
                       *(uint64_t*) p_ops->ptrs_to_r_headers[I]->l_id, t_id,
                       p_ops->ptrs_to_r_headers[I]->m_id, (uint16_t) I, tmp_value, false, false_positive);

        }
        else if (op->opcode == CACHE_OP_LIN_PUT) {
          //Lock free reads through versioning (successful when version is even)
          uint32_t debug_cntr = 0;
          do {
            //memcpy((void*) &prev_meta, (void*) &(kv_ptr[I]->key.meta), sizeof(cache_meta));
            if (ENABLE_ASSERTIONS) {
              debug_cntr++;
              if (debug_cntr % M_4 == 0) {
                printf("Worker %u stuck on a remote read version %u m_id %u, times %u \n",
                       t_id, prev_meta.version, prev_meta.m_id, debug_cntr / M_4);
                //debug_cntr = 0;
              }
            }
            prev_meta = kv_ptr[I]->key.meta;
          } while (!optik_is_same_version_and_valid(prev_meta, kv_ptr[I]->key.meta));
          insert_r_rep(p_ops, (struct ts_tuple *)&prev_meta.m_id, (struct ts_tuple *)&op->key.meta.m_id,
                       *(uint64_t*) p_ops->ptrs_to_r_headers[I]->l_id, t_id,
                       p_ops->ptrs_to_r_headers[I]->m_id, (uint16_t) I, NULL, true, false);

        }
        else {
//          red_printf("wrong Opcode in cache: %d, req %d, m_id %u, val_len %u, version %u , \n",
//                     op->opcode, I, reads[(pull_ptr + I) % max_op_size]->m_id,
//                     reads[(pull_ptr + I) % max_op_size]->val_len,
//                     *(uint32_t *)reads[(pull_ptr + I) % max_op_size]->version);
          assert(0);
        }
      }
    }
    if(key_in_store[I] == 0) {  //Cache miss --> We get here if either tag or log key match failed
//      resp[I].val_len = 0;
//      resp[I].val_ptr = NULL;
      // resp[I].type = CACHE_MISS;
      assert(false); // cant have a miss since, it hit in the source's cache
    }
    if (zero_ops) {
//      printf("Zero out %d at address %lu \n", op->opcode, &op->opcode);
      op->opcode = 5;
    }
  }
}

// The worker uses this to send in the lin writes after receiving a write_quorum of read replies for them
// Additionally worker sends reads that received a higher timestamp and thus have to be applied as writes
inline void cache_batch_op_lin_writes_and_unseen_reads(uint32_t op_num, int thread_id, struct read_info **writes,
                                                       uint32_t pull_ptr, uint32_t max_op_size, bool zero_ops)
{
  int I, j;	/* I is batch index */
  long long stalled_brces = 0;
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

  unsigned int bkt[CACHE_BATCH_SIZE];
  struct mica_bkt *bkt_ptr[CACHE_BATCH_SIZE];
  unsigned int tag[CACHE_BATCH_SIZE];
  int key_in_store[CACHE_BATCH_SIZE];	/* Is this key in the datastore? */
  struct cache_op *kv_ptr[CACHE_BATCH_SIZE];	/* Ptr to KV item in log */
  /*
     * We first lookup the key in the datastore. The first two @I loops work
     * for both GETs and PUTs.
     */
  for(I = 0; I < op_num; I++) {
    struct key *key = (struct key*) writes[(pull_ptr + I) % max_op_size]->key;
    bkt[I] = key->bkt & cache.hash_table.bkt_mask;
    bkt_ptr[I] = &cache.hash_table.ht_index[bkt[I]];
    __builtin_prefetch(bkt_ptr[I], 0, 0);
    tag[I] = key->tag;

    key_in_store[I] = 0;
    kv_ptr[I] = NULL;  }
  for(I = 0; I < op_num; I++) {
    for(j = 0; j < 8; j++) {
      if(bkt_ptr[I]->slots[j].in_use == 1 &&
         bkt_ptr[I]->slots[j].tag == tag[I]) {
        uint64_t log_offset = bkt_ptr[I]->slots[j].offset &
                              cache.hash_table.log_mask;
        /*
                 * We can interpret the log entry as mica_op, even though it
                 * may not contain the full MICA_MAX_VALUE value.
                 */
        kv_ptr[I] = (struct cache_op *) &cache.hash_table.ht_log[log_offset];

        /* Small values (1--64 bytes) can span 2 cache lines */
        __builtin_prefetch(kv_ptr[I], 0, 0);
        __builtin_prefetch((uint8_t *) kv_ptr[I] + 64, 0, 0);

        /* Detect if the head has wrapped around for this index entry */
        if(cache.hash_table.log_head - bkt_ptr[I]->slots[j].offset >= cache.hash_table.log_cap) {
          kv_ptr[I] = NULL;	/* If so, we mark it "not found" */
        }

        break;
      }
    }
  }
  // the following variables used to validate atomicity between a lock-free r_rep of an object
  for(I = 0; I < op_num; I++) {
    struct read_info *op = writes[(pull_ptr + I) % max_op_size];
    if(kv_ptr[I] != NULL) {
      /* We had a tag match earlier. Now compare log entry. */
      long long *key_ptr_log = (long long *) kv_ptr[I];
      long long *key_ptr_req = (long long *) op->key;
      if(key_ptr_log[1] == key_ptr_req[0]) { //Cache Hit
        key_in_store[I] = 1;
        cache_meta op_meta = * (cache_meta *) (((void*)op) - 3);
        if (op->opcode == CACHE_OP_LIN_PUT) {
          optik_lock(&kv_ptr[I]->key.meta);
          if (!optik_is_greater_version(kv_ptr[I]->key.meta, op_meta)) //{
            (*(uint32_t *) op->ts_to_read.version) = kv_ptr[I]->key.meta.version + 1;
          memcpy(kv_ptr[I]->value, op->value, VALUE_SIZE);
          optik_unlock(&kv_ptr[I]->key.meta, op->ts_to_read.m_id, *(uint32_t *)op->ts_to_read.version);
        }
        else if (op->opcode == CACHE_OP_GET || op->opcode == OP_ACQUIRE) { // a read resulted on receiving a higher timestamp than expected
          optik_lock(&kv_ptr[I]->key.meta);
          if (optik_is_greater_version(kv_ptr[I]->key.meta, op_meta)) {
            if (ENABLE_ASSERTIONS) assert(op->ts_to_read.m_id != machine_id);
            memcpy(kv_ptr[I]->value, op->value, VALUE_SIZE);
            optik_unlock(&kv_ptr[I]->key.meta, op->ts_to_read.m_id, *(uint32_t *)op->ts_to_read.version);
          }
          else {
            optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
            t_stats[thread_id].failed_rem_writes++;
          }
        }
        else {
          red_printf("wrong Opcode in cache: %d, req %d, m_id %u,version %u , \n",
                     op->opcode, I, writes[(pull_ptr + I) % max_op_size]->ts_to_read.m_id,
                     *(uint32_t *)writes[(pull_ptr + I) % max_op_size]->ts_to_read.version);
          assert(0);
        }
      }
    }
    if(key_in_store[I] == 0) {  //Cache miss --> We get here if either tag or log key match failed
    }
    if (zero_ops) {
//      printf("Zero out %d at address %lu \n", op->opcode, &op->opcode);
      op->opcode = 5;
    }
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
		op.opcode = CACHE_OP_PUT;

		//printf("Key Metadata: Lock(%u), State(%u), Counter(%u:%u)\n", op.key.meta.lock, op.key.meta.state, op.key.meta.version, op.key.meta.cid);
		op.val_len = (uint8_t) (val_len >> SHIFT_BITS);
		uint8_t val = 'a';//(uint8_t) (op_key[1] & 0xff);
		memset(op.value, val, (uint8_t) val_len);

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
		case CACHE_OP_UPD:
			return "CACHE_OP_UPD";
		case CACHE_OP_INV:
			return "CACHE_OP_INV";
		case CACHE_OP_ACK:
			return "CACHE_OP_ACK";
		case CACHE_OP_BRC:
			return "CACHE_OP_BRC";
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

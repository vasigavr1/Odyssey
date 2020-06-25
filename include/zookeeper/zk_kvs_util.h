//
// Created by vasilis on 23/06/2020.
//

#ifndef Z_KVS_UTIL_H
#define Z_KVS_UTIL_H

#include "kvs.h"
#include "zk_main.h"

/* The leader and follower send their local requests to this, reads get served
 * But writes do not get served, writes are only propagated here to see whether their keys exist */
inline void cache_batch_op_trace(int op_num, int thread_id, struct cache_op **op, struct mica_resp *resp)
{
  int I, j;	/* I is batch index */
  long long stalled_brces = 0;
#if CACHE_DEBUG == 1
  //assert(cache.hash_table != NULL);
	assert(op != NULL);
	assert(op_num > 0 && op_num <= MAX_OP_BATCH);
	assert(resp != NULL);
#endif

#if CACHE_DEBUG == 2
  for(I = 0; I < op_num; I++)
		mica_print_op(&(*op)[I]);
#endif

  unsigned int bkt[MAX_OP_BATCH];
  struct mica_bkt *bkt_ptr[MAX_OP_BATCH];
  unsigned int tag[MAX_OP_BATCH];
  int key_in_store[MAX_OP_BATCH];	/* Is this key in the datastore? */
  struct cache_op *kv_ptr[MAX_OP_BATCH];	/* Ptr to KV item in log */
  /*
   * We first lookup the key in the datastore. The first two @I loops work
   * for both GETs and PUTs.
   */
  for(I = 0; I < op_num; I++) {
    if(resp[I].type == UNSERVED_CACHE_MISS) continue;
    bkt[I] = (*op)[I].key.bkt & cache.hash_table.bkt_mask;
    bkt_ptr[I] = &cache.hash_table.ht_index[bkt[I]];
    __builtin_prefetch(bkt_ptr[I], 0, 0);
    tag[I] = (*op)[I].key.tag;

    key_in_store[I] = 0;
    kv_ptr[I] = NULL;
  }

  for(I = 0; I < op_num; I++) {
    if(resp[I].type == UNSERVED_CACHE_MISS) continue;
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

  // the following variables used to validate atomicity between a lock-free read of an object
  cache_meta prev_meta;
  for(I = 0; I < op_num; I++) {
    if(kv_ptr[I] != NULL) {

      /* We had a tag match earlier. Now compare log entry. */
      long long *key_ptr_log = (long long *) kv_ptr[I];
      long long *key_ptr_req = (long long *) &(*op)[I];

      if(key_ptr_log[1] == key_ptr_req[1]) { //Cache Hit
        key_in_store[I] = 1;
        if ((*op)[I].opcode == CACHE_OP_GET) {
          //Lock free reads through versioning (successful when version is even)
          do {
            //memcpy((void*) &prev_meta, (void*) &(kv_ptr[I]->key.meta), sizeof(cache_meta));
            prev_meta = kv_ptr[I]->key.meta;
            memcpy((*op)[I].value, kv_ptr[I]->value, VALUE_SIZE);
            //resp[I].val_ptr = kv_ptr[I]->value;
            //resp[I].val_len = kv_ptr[I]->val_len;
          } while (!optik_is_same_version_and_valid(prev_meta, kv_ptr[I]->key.meta));
          resp[I].type = CACHE_GET_SUCCESS;

        } else if ((*op)[I].opcode == CACHE_OP_PUT) {
          resp[I].type = KEY_HIT;
        } else {
          red_printf("wrong Opcode in cache: %d, req %d \n", (*op)[I].opcode, I);
          assert(0);
        }
      }
    }

    if(key_in_store[I] == 0) {  //Cache miss --> We get here if either tag or log key match failed
      if (WRITE_RATIO > 0) assert(false);
      else t_stats[thread_id].cache_hits_per_thread--;
      resp[I].val_len = 0;
      resp[I].val_ptr = NULL;
      resp[I].type = CACHE_MISS;
    }
  }
//	if(ENABLE_CACHE_STATS == 1)
//		update_cache_stats(op_num, thread_id, op, resp, stalled_brces);

}

/* The leader sends the writes to be committed with this function*/
// THE API IS DIFFERENT HERE, THIS TAKES AN ARRAY OF POINTERS RATHER THAN A POINTER TO AN ARRAY
// YOU have to give a pointer to the beggining of the array of the pointers or else you will not
// be able to wrap around to your array
inline void cache_batch_op_updates(uint32_t op_num, int thread_id, struct prepare **preps,
                                   struct mica_resp *resp, uint32_t pull_ptr,  uint32_t max_op_size, bool zero_ops)
{
  int I, j;	/* I is batch index */
  long long stalled_brces = 0;
#if CACHE_DEBUG == 1
  //assert(cache.hash_table != NULL);
	assert(op != NULL);
	assert(op_num > 0 && op_num <= MAX_OP_BATCH);
	assert(resp != NULL);
#endif

#if CACHE_DEBUG == 2
  for(I = 0; I < op_num; I++)
		mica_print_op(&(*op)[I]);
#endif

  unsigned int bkt[MAX_OP_BATCH];
  struct mica_bkt *bkt_ptr[MAX_OP_BATCH];
  unsigned int tag[MAX_OP_BATCH];
  int key_in_store[MAX_OP_BATCH];	/* Is this key in the datastore? */
  struct cache_op *kv_ptr[MAX_OP_BATCH];	/* Ptr to KV item in log */
  /*
     * We first lookup the key in the datastore. The first two @I loops work
     * for both GETs and PUTs.
     */
  for(I = 0; I < op_num; I++) {
    struct cache_op *op = (struct cache_op*) preps[(pull_ptr + I) % max_op_size];
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
  // the following variables used to validate atomicity between a lock-free read of an object
  for(I = 0; I < op_num; I++) {
    struct cache_op *op = (struct cache_op*) preps[(pull_ptr + I) % max_op_size];
    if(kv_ptr[I] != NULL) {
      /* We had a tag match earlier. Now compare log entry. */
      long long *key_ptr_log = (long long *) kv_ptr[I];
      long long *key_ptr_req = (long long *) op;
      if(key_ptr_log[1] == key_ptr_req[1]) { //Cache Hit
        key_in_store[I] = 1;
        if (op->opcode == CACHE_OP_PUT) {
//          red_printf("op val len %d in ptr %d, total ops %d \n", op->val_len, (pull_ptr + I) % max_op_size, op_num );
          if (ENABLE_ASSERTIONS) assert(op->val_len == kv_ptr[I]->val_len);
          optik_lock(&kv_ptr[I]->key.meta);
          memcpy(kv_ptr[I]->value, op->value, VALUE_SIZE);
          optik_unlock_write(&kv_ptr[I]->key.meta, (uint8_t) machine_id,(uint32_t*) &op->key.meta.version);
          resp[I].val_len = 0;
          resp[I].val_ptr = NULL;
          resp[I].type = CACHE_PUT_SUCCESS;
        }
        else {
          red_printf("wrong Opcode in cache: %d, req %d, flr_id %u, val_len %u, g_id %u , \n",
                     op->opcode, I, preps[(pull_ptr + I) % max_op_size]->flr_id,
                     preps[(pull_ptr + I) % max_op_size]->val_len,
                     *(uint32_t *)preps[(pull_ptr + I) % max_op_size]->g_id);
          assert(0);
        }
      }
    }
    if(key_in_store[I] == 0) {  //Cache miss --> We get here if either tag or log key match failed
      resp[I].val_len = 0;
      resp[I].val_ptr = NULL;
      resp[I].type = CACHE_MISS;
    }
    if (zero_ops) {
//      printf("Zero out %d at address %lu \n", op->opcode, &op->opcode);
      op->opcode = 5;
    }
  }

}

#endif //Z_KVS_UTIL_H

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
inline void cache_batch_op_trace(uint16_t op_num, uint16_t t_id, struct cache_op *op, struct cache_resp *resp,
                                 struct pending_ops *p_ops)
{
	int I, j;	/* I is batch index */
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
	int key_in_store[MAX_OP_BATCH];	/* Is this key in the datastore? */
	struct cache_op *kv_ptr[MAX_OP_BATCH];	/* Ptr to KV item in log */
	/*
	 * We first lookup the key in the datastore. The first two @I loops work
	 * for both GETs and PUTs.
	 */
	for(I = 0; I < op_num; I++) {
		bkt[I] = op[I].key.bkt & cache.hash_table.bkt_mask;
		bkt_ptr[I] = &cache.hash_table.ht_index[bkt[I]];
		__builtin_prefetch(bkt_ptr[I], 0, 0);
		tag[I] = op[I].key.tag;

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
  uint64_t rmw_l_id = p_ops->prop_info->l_id;
  uint32_t r_push_ptr = p_ops->r_push_ptr;
	for(I = 0; I < op_num; I++) {
		if(kv_ptr[I] != NULL) {

			/* We had a tag match earlier. Now compare log entry. */
			long long *key_ptr_log = (long long *) kv_ptr[I];
			long long *key_ptr_req = (long long *) &op[I];

			if(key_ptr_log[1] == key_ptr_req[1]) { //Cache Hit
				key_in_store[I] = 1;
				if (op[I].opcode == CACHE_OP_GET || op[I].opcode == OP_ACQUIRE) {
					//Lock free reads through versioning (successful when version is even)
          uint32_t debug_cntr = 0;
          bool value_forwarded = false; // has a pending out-of-epoch write forwarded its value to this
          if (op[I].opcode == CACHE_OP_GET && p_ops->p_ooe_writes->size > 0) {
            uint8_t *val_ptr;
            if (search_out_of_epoch_writes(p_ops, (struct key *)&op[I].key.bkt, t_id, (void **) &val_ptr)) {
              memcpy(p_ops->read_info[r_push_ptr].value, val_ptr, VALUE_SIZE);
              //red_printf("Wrkr %u Forwarding a value \n", t_id);
              value_forwarded = true;
            }
          }
          if (!value_forwarded) {
            do {
              // memcpy((void*) &prev_meta, (void*) &(kv_ptr[I]->key.meta), sizeof(cache_meta));
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
          }
          // Do a quorum read if the stored value is old and may be stale or it is an Acquire!
          if (!value_forwarded &&
             (*(uint16_t *)prev_meta.epoch_id < epoch_id || op[I].opcode == OP_ACQUIRE)) {
            p_ops->read_info[r_push_ptr].opcode = op[I].opcode;
            MOD_ADD(r_push_ptr, PENDING_READS);
            resp[I].type = CACHE_GET_SUCCESS;
            if (ENABLE_STAT_COUNTING && op[I].opcode == CACHE_OP_GET) {
              t_stats[t_id].quorum_reads++;
            }
            memcpy((void *)&op[I].key.meta.m_id, (void *)&prev_meta.m_id, TS_TUPLE_SIZE);
          }
          else { //stored value can be read locally or has been forwarded
            resp[I].type = CACHE_LOCAL_GET_SUCCESS;
            // this is needed to trick the version check in batch_from_trace_to_cache()
            if (ENABLE_ASSERTIONS) op[I].key.meta.version = 0;
          }
				}
          // Put has to be 2 rounds (readTS + write) if it is out-of-epoch
        else if (op[I].opcode == CACHE_OP_PUT) {
          if (ENABLE_ASSERTIONS) assert(op[I].val_len == kv_ptr[I]->val_len);
          optik_lock(&kv_ptr[I]->key.meta);
          // OUT_OF_EPOCH
          if (*(uint16_t *)kv_ptr[I]->key.meta.epoch_id < epoch_id) {
            op[I].key.meta.version = kv_ptr[I]->key.meta.version - 1;
            optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
            op[I].key.meta.m_id = (uint8_t) machine_id;
            p_ops->read_info[r_push_ptr].opcode = op[I].opcode;
            // Store the value to be written in the read_info to be used in the second round
            memcpy(p_ops->read_info[r_push_ptr].value, op[I].value, VALUE_SIZE);
            memcpy(&p_ops->read_info[r_push_ptr].ts_to_read, (void *) &op[I].key.meta.m_id, TS_TUPLE_SIZE + TRUE_KEY_SIZE);
            p_ops->p_ooe_writes->r_info_ptrs[p_ops->p_ooe_writes->push_ptr] = r_push_ptr;
            p_ops->p_ooe_writes->size++;
            MOD_ADD(p_ops->p_ooe_writes->push_ptr, PENDING_READS);
            MOD_ADD(r_push_ptr, PENDING_READS);
            resp[I].type = CACHE_GET_TS_SUCCESS;
          }
          else { // IN-EPOCH
            memcpy(kv_ptr[I]->value, op[I].value, VALUE_SIZE);
            // This also writes the new version to op
            optik_unlock_write(&kv_ptr[I]->key.meta, (uint8_t) machine_id, (uint32_t *) &op[I].key.meta.version);
            resp[I].type = CACHE_PUT_SUCCESS;
          }
				}
        else if (op[I].opcode == OP_RELEASE) { // read the timestamp
          uint32_t debug_cntr = 0;
          do {
            prev_meta = kv_ptr[I]->key.meta;
            // memcpy((void*) &prev_meta, (void*) &(kv_ptr[I]->key.meta), sizeof(cache_meta));
            if (ENABLE_ASSERTIONS) {
              debug_cntr++;
              if (debug_cntr == M_4) {
                printf("Worker %u stuck on reading the TS for a read TS\n", t_id);
                debug_cntr = 0;
              }
            }
          } while (!optik_is_same_version_and_valid(prev_meta, kv_ptr[I]->key.meta));
          op[I].key.meta.m_id = (uint8_t) machine_id;
          op[I].key.meta.version = prev_meta.version;
          p_ops->read_info[r_push_ptr].opcode = op[I].opcode;
          // Store the value to be written in the read_info to be used in the second round
          memcpy(p_ops->read_info[r_push_ptr].value, op[I].value, VALUE_SIZE);
          MOD_ADD(r_push_ptr, PENDING_READS);
          resp[I].type = CACHE_GET_TS_SUCCESS;
        }
        else if (ENABLE_RMWS && op[I].opcode == PROPOSE_OP) {
          if (DEBUG_RMW) green_printf("Worker %u trying a local RMW on op %u\n", t_id, I);
          uint32_t entry = 0;
          optik_lock(&kv_ptr[I]->key.meta);
          // if it's the first RMW
          if (kv_ptr[I]->opcode == KEY_HAS_NEVER_BEEN_RMWED) {
            // sess_id is stored in the first bytes of op
            uint32_t new_log_no = 1;
            entry = grab_RMW_entry(PROPOSED, kv_ptr[I], op[I].opcode,
                                  (uint8_t) machine_id, kv_ptr[I]->key.meta.version + 1,
                                   rmw_l_id, new_log_no,
                                   get_glob_sess_id((uint8_t) machine_id, t_id, *((uint16_t *) &op[I])),
                                   t_id);
            if (ENABLE_ASSERTIONS) assert(entry == *(uint32_t *)kv_ptr[I]->value);
            resp[I].type = RMW_SUCCESS;
            resp[I].log_no = new_log_no;
            kv_ptr[I]->opcode = KEY_HAS_BEEN_RMWED;
          }
          // key has been RMWed before
          else if (kv_ptr[I]->opcode == KEY_HAS_BEEN_RMWED) {
            entry = *(uint32_t *) kv_ptr[I]->value;
            check_keys_with_two_cache_ops(&op[I], kv_ptr[I], entry);
            check_log_nos_of_glob_entry(&rmw.entry[entry], "cache_batch_op_trace", t_id);
            struct rmw_entry *rmw_entry = &rmw.entry[entry];
            if (rmw_entry->state == INVALID_RMW) {
              // remember that key is locked and thus this entry is also locked
              activate_RMW_entry(PROPOSED, kv_ptr[I]->key.meta.version + 1, rmw_entry, op[I].opcode,
                                 (uint8_t)machine_id, rmw_l_id,
                                 get_glob_sess_id((uint8_t) machine_id, t_id, *((uint16_t *) &op[I])),
                                 rmw_entry->last_committed_log_no + 1, t_id, ENABLE_ASSERTIONS ? "batch to trace" : NULL);
              resp[I].log_no = rmw_entry->log_no;
              if (ENABLE_ASSERTIONS) assert(resp[I].log_no == rmw_entry->last_committed_log_no + 1);
              resp[I].type = RMW_SUCCESS;
            }
            else {
              // This is the state the RMW will wait on
              resp[I].glob_entry_state = rmw_entry->state;
              resp[I].glob_entry_rmw_id = rmw_entry->rmw_id;
              resp[I].type = RETRY_RMW_KEY_EXISTS;
            }
          }
          resp[I].kv_pair_ptr = &kv_ptr[I]->key.meta;
          // We need to put the new timestamp in the op too, both to send it and to store it for later
          op[I].key.meta.version = kv_ptr[I]->key.meta.version + 1;
          optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
          resp[I].rmw_entry = entry;
          rmw_l_id++;
        }
        else {
        red_printf("Wrkr %u: cache_batch_op_trace wrong opcode in cache: %d, req %d \n",
                   t_id, op[I].opcode, I);
        assert(0);
				}
			}
		}

		if(key_in_store[I] == 0) {  //Cache miss --> We get here if either tag or log key match failed
      //red_printf("Cache_miss: bkt %u/%u, server %u/%u, tag %u/%u \n",
      //          op[I].key.bkt, kv_ptr[I]->key.bkt ,op[I].key.server,
      //          kv_ptr[I]->key.server, op[I].key.tag, kv_ptr[I]->key.tag);
			resp[I].type = CACHE_MISS;
		}
	}
}

/* The worker sends the remote writes to be committed with this function*/
// THE API IS DIFFERENT HERE, THIS TAKES AN ARRAY OF POINTERS RATHER THAN A POINTER TO AN ARRAY
// YOU have to give a pointer to the beggining of the array of the pointers or else you will not
// be able to wrap around to your array
inline void cache_batch_op_updates(uint32_t op_num, uint16_t t_id, struct write **writes,
                                   struct pending_ops *p_ops,
                                   uint32_t pull_ptr,  uint32_t max_op_size, bool zero_ops)
{
  int I, j;	/* I is batch index */
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
  int key_in_store[MAX_INCOMING_W];	/* Is this key in the datastore? */
  struct cache_op *kv_ptr[MAX_INCOMING_W];	/* Ptr to KV item in log */
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
    struct cache_op *op = (struct cache_op *) writes[(pull_ptr + I) % max_op_size];
    if (unlikely (op->opcode == OP_RELEASE_BIT_VECTOR)) continue;
    if (kv_ptr[I] != NULL) {
      /* We had a tag match earlier. Now compare log entry. */
      long long *key_ptr_log = (long long *) kv_ptr[I];
      long long *key_ptr_req = (long long *) op;
      if (key_ptr_log[1] == key_ptr_req[1]) { //Cache Hit
        key_in_store[I] = 1;
        if (op->opcode == CACHE_OP_PUT || op->opcode == OP_RELEASE ||
            op->opcode == OP_ACQUIRE) {
          //red_printf("op val len %d in ptr %d, total ops %d \n", op->val_len, (pull_ptr + I) % max_op_size, op_num );
          if (ENABLE_ASSERTIONS) assert(op->val_len == kv_ptr[I]->val_len);
          optik_lock(&kv_ptr[I]->key.meta);
          if (optik_is_greater_version(kv_ptr[I]->key.meta, op->key.meta)) {
            memcpy(kv_ptr[I]->value, op->value, VALUE_SIZE);
            optik_unlock(&kv_ptr[I]->key.meta, op->key.meta.m_id, op->key.meta.version);
          } else {
            optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
            t_stats[t_id].failed_rem_writes++;
          }
        }
        else if (op->opcode == ACCEPT_OP) {
          struct accept *acc =(struct accept *) (((void *)op) - 5); // the accept starts at an offset of 5 bytes
          if (ENABLE_ASSERTIONS) assert(*(uint32_t *)acc->ts.version > 0);
          uint8_t flag;
          // on replying to the accept we may need to send on or more of TS, VALUE, RMW-id, log-no
          struct rmw_help_entry reply_rmw;
          uint64_t rmw_l_id = acc->t_rmw_id;
          uint16_t glob_sess_id = *(uint16_t*) acc->glob_sess_id;
          //cyan_printf("Received accept with rmw_id %u, glob_sess %u \n", rmw_l_id, glob_sess_id);
          uint32_t log_no = *(uint32_t*) acc->log_no;
          uint64_t l_id = acc->l_id;
          // TODO Finding the sender machine id here is a hack that will not work with coalescing
          static_assert(MAX_ACC_COALESCE == 1, " ");
          struct accept_message *acc_mes = (((void *)op) -5 - ACCEPT_MES_HEADER);

          if (ENABLE_ASSERTIONS) check_accept_mes(acc_mes);
          uint8_t prop_m_id = acc_mes->m_id;
          uint32_t entry;
          if (DEBUG_RMW) green_printf("Worker %u is handling a remote RMW accept on op %u from m_id %u "
                                        "l_id %u, rmw_l_id %u, glob_ses_id %u, log_no %u, version %u  \n",
                                      t_id, I, prop_m_id, l_id, rmw_l_id, glob_sess_id, log_no, *(uint32_t *)acc->ts.version);
          optik_lock(&kv_ptr[I]->key.meta);



          // 1. check if it has been committed
          // 2. first check the log number to see if it's SMALLER!! (leave the "higher" part after the KVS ts is also checked)
          // Either way fill the reply_rmw fully, but have a specialized flag!
          if (!is_log_smaller_or_has_rmw_committed(log_no, kv_ptr[I], rmw_l_id, glob_sess_id,
                                                   t_id, &entry, &flag, &reply_rmw)) {
            // 3. Check that the TS is higher than the KVS TS, setting the flag accordingly
            if (!accept_ts_is_not_greater_than_kvs_ts(kv_ptr[I], acc, prop_m_id, t_id, &flag, &reply_rmw)) {
              // 4. If the kv-pair has not been RMWed before grab an entry and ack
              // 5. Else if log number is bigger than the current one, ack without caring about the ongoing RMWs
              // 6. Else check the global entry and send a response depending on whether there is an ongoing RMW and what that is
              flag = handle_remote_accept_in_cache(kv_ptr[I], acc, prop_m_id, t_id, &reply_rmw, &entry);
              // if the accepted is going to be acked record its information in the global entry
              if (flag == RMW_ACK_ACCEPT)
                activate_RMW_entry(ACCEPTED, *(uint32_t *) acc->ts.version, &rmw.entry[entry], acc->opcode,
                                   acc->ts.m_id, rmw_l_id, glob_sess_id, log_no, t_id,
                                   ENABLE_ASSERTIONS ? "received accept" : NULL);
            }
          }
          register_last_committed_rmw_id_by_remote_accept(&rmw.entry[entry], acc , t_id);
          check_log_nos_of_glob_entry(&rmw.entry[entry], "Unlocking after received accept", t_id);
          optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
          insert_r_rep(p_ops, NULL, NULL, l_id, t_id, prop_m_id, (uint16_t) I,
                       (void*) &reply_rmw, flag, acc->opcode);
        }
        else if (op->opcode == COMMIT_OP) {
          struct commit *com = (struct commit *) (((void *) op) + 3); // the commit starts at an offset of 3 bytes
          if (ENABLE_ASSERTIONS) assert(*(uint32_t *) com->ts.version > 0);
          //uint8_t flag;
          bool overwrite_kv;
          uint64_t rmw_l_id = *(uint64_t *) com->t_rmw_id;
          uint16_t glob_sess_id = *(uint16_t *) com->glob_sess_id;
          uint32_t log_no = *(uint32_t *) com->log_no;
          uint32_t entry;
          if (DEBUG_RMW)
            green_printf("Worker %u is handling a remote RMW commit on op %u, "
                           "rmw_l_id %u, glob_ses_id %u, log_no %u, version %u  \n",
                         t_id, I, rmw_l_id, glob_sess_id, log_no, *(uint32_t *) com->ts.version);
          optik_lock(&kv_ptr[I]->key.meta);
          if (kv_ptr[I]->opcode == KEY_HAS_NEVER_BEEN_RMWED) {
            entry = grab_RMW_entry(COMMITTED, kv_ptr[I], 0, 0, 0,
                                   rmw_l_id, log_no, glob_sess_id, t_id);
            if (ENABLE_ASSERTIONS) {
              assert(kv_ptr[I]->key.meta.version == 1);
              assert(entry == *(uint32_t *) kv_ptr[I]->value);
            }
            overwrite_kv = true;
            kv_ptr[I]->opcode = KEY_HAS_BEEN_RMWED;
          } else if (kv_ptr[I]->opcode == KEY_HAS_BEEN_RMWED) {
            entry = *(uint32_t *) kv_ptr[I]->value;
            check_keys_with_one_cache_op((struct key *) com->key, kv_ptr[I], entry);
            struct rmw_entry *rmw_entry = &rmw.entry[entry];
            overwrite_kv = handle_remote_commit(p_ops, rmw_entry, log_no, rmw_l_id, glob_sess_id, com, t_id);
          } else if (ENABLE_ASSERTIONS) assert(false);
          // The commit must be applied to the KVS
          if (overwrite_kv) {
            kv_ptr[I]->key.meta.m_id = com->ts.m_id;
            kv_ptr[I]->key.meta.version = (*(uint32_t *) com->ts.version) + 1; // the unlock function will decrement 1
            memcpy(&kv_ptr[I]->value[BYTES_OVERRIDEN_IN_KVS_VALUE], com->value, (size_t) RMW_VALUE_SIZE);
          }
          struct rmw_entry *glob_entry = &rmw.entry[entry];
          check_log_nos_of_glob_entry(glob_entry, "Unlocking after received commit", t_id);
          if (ENABLE_ASSERTIONS) {
            if (glob_entry->state != INVALID_RMW)
              assert(!rmw_id_is_equal_with_id_and_glob_sess_id(&glob_entry->rmw_id, rmw_l_id, glob_sess_id));
          }
          //if (is_global_ses_id_local(glob_sess_id, t_id) &&
          // p_ops->prop_info->entry[glob_ses_id_to_sess_id(glob_sess_id)].rmw_id.id == rmw_l_id )//&&
          //p_ops->prop_info->entry[glob_ses_id_to_sess_id(glob_sess_id)].state > INVALID_RMW &&
          // p_ops->prop_info->entry[glob_ses_id_to_sess_id(glob_sess_id)].state < MUST_BCAST_COMMITS  &&
          // p_ops->prop_info->entry[glob_ses_id_to_sess_id(glob_sess_id)].log_no == log_no) {
          //cyan_printf("Received commit for local RMW id: bkt %u log no %u\n", glob_entry->key.bkt, log_no);
          //}
          register_committed_global_sess_id (glob_sess_id, rmw_l_id, t_id);
          optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
        }
        else if (ENABLE_ASSERTIONS) {
          red_printf("Wrkr %u, cache batch update: wrong opcode in cache: %d, req %d, "
                       "m_id %u, val_len %u, version %u , \n",
                     t_id, op->opcode, I, op->key.meta.m_id,
                     op->val_len, op->key.meta.version);
          assert(0);
        }
      }
      if (key_in_store[I] == 0) {  //Cache miss --> We get here if either tag or log key match failed
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
  int I, j;	/* I is batch index */
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
  if (ENABLE_ASSERTIONS) assert(op_num <= MAX_INCOMING_R);
  unsigned int bkt[MAX_INCOMING_R];
  struct mica_bkt *bkt_ptr[MAX_INCOMING_R];
  unsigned int tag[MAX_INCOMING_R];
  int key_in_store[MAX_INCOMING_R];	/* Is this key in the datastore? */
  struct cache_op *kv_ptr[MAX_INCOMING_R];	/* Ptr to KV item in log */
  /*
     * We first lookup the key in the datastore. The first two @I loops work
     * for both GETs and PUTs.
     */
  for(I = 0; I < op_num; I++) {
    struct cache_op *op = (struct cache_op*) reads[(pull_ptr + I) % max_op_size];
    if (unlikely(op->opcode == OP_ACQUIRE_FLIP_BIT)) continue; // This message is only meant to flip a bit and is thus a NO-OP
    bkt[I] = op->key.bkt & cache.hash_table.bkt_mask;
    bkt_ptr[I] = &cache.hash_table.ht_index[bkt[I]];
    __builtin_prefetch(bkt_ptr[I], 0, 0);
    tag[I] = op->key.tag;

    key_in_store[I] = 0;
    kv_ptr[I] = NULL;
  }
  for(I = 0; I < op_num; I++) {
    struct cache_op *op = (struct cache_op*) reads[(pull_ptr + I) % max_op_size];
    if (unlikely(op->opcode == OP_ACQUIRE_FLIP_BIT)) continue;
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
    if (op->opcode == OP_ACQUIRE_FLIP_BIT) {
      insert_r_rep(p_ops, NULL, NULL,
                   *(uint64_t *) p_ops->ptrs_to_r_headers[I]->l_id, t_id,
                   p_ops->ptrs_to_r_headers[I]->m_id, (uint16_t) I, NULL, NO_OP_ACQ_FLIP_BIT, op->opcode);
      continue;
    }
    if(kv_ptr[I] != NULL) {
      /* We had a tag match earlier. Now compare log entry. */
      long long *key_ptr_log = (long long *) kv_ptr[I];
      long long *key_ptr_req = (long long *) op;
      if(key_ptr_log[1] == key_ptr_req[1]) { //Cache Hit
        key_in_store[I] = 1;
        if (op->opcode == CACHE_OP_GET || op->opcode == OP_ACQUIRE ||
            op->opcode == OP_ACQUIRE_FP) {
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
            if (compare_netw_ts((struct network_ts_tuple *) &prev_meta.m_id,
                                (struct network_ts_tuple *) &op->key.meta.m_id) == GREATER)
              memcpy(tmp_value, kv_ptr[I]->value, VALUE_SIZE);
          } while (!optik_is_same_version_and_valid(prev_meta, kv_ptr[I]->key.meta));
          insert_r_rep(p_ops, (struct network_ts_tuple *)&prev_meta.m_id, (struct network_ts_tuple *)&op->key.meta.m_id,
                       *(uint64_t*) p_ops->ptrs_to_r_headers[I]->l_id, t_id,
                       p_ops->ptrs_to_r_headers[I]->m_id, (uint16_t) I, (void*) tmp_value, READ, op->opcode);

        }
        else if (ENABLE_RMWS && op->opcode == PROPOSE_OP) {
          struct propose *prop =(struct propose *) (((void *)op) - 5); // the propose starts at an offset of 5 bytes
          if (DEBUG_RMW) green_printf("Worker %u trying a remote RMW propose on op %u\n", t_id, I);
          if (ENABLE_ASSERTIONS) assert(*(uint32_t *)prop->ts.version > 0);
          uint8_t flag;
          struct rmw_help_entry reply_rmw; // on replying to the propose we may need to send on or more of TS, VALUE, RMW-id, log-no
          uint64_t rmw_l_id = *(uint64_t*) prop->t_rmw_id;
          uint64_t l_id = *(uint64_t*) prop->l_id;
          uint16_t glob_sess_id = *(uint16_t*) prop->glob_sess_id;
          //cyan_printf("Received propose with rmw_id %u, glob_sess %u \n", rmw_l_id, glob_sess_id);
          uint32_t log_no = *(uint32_t*) prop->log_no;
          uint8_t prop_m_id = p_ops->ptrs_to_r_headers[I]->m_id;
          uint32_t entry;
          optik_lock(&kv_ptr[I]->key.meta);
          //check_for_same_ts_as_already_proposed(kv_ptr[I], prop, t_id);
          // 1. check if it has been committed
          // 2. first check the log number to see if it's SMALLER!! (leave the "higher" part after the KVS ts is also checked)
          // Either way fill the reply_rmw fully, but have a specialized flag!
          if (!is_log_smaller_or_has_rmw_committed(log_no, kv_ptr[I], rmw_l_id, glob_sess_id, t_id, &entry,
                                                   &flag, &reply_rmw)) {
            // 3. Check that the TS is higher than the KVS TS, setting the flag accordingly
            if (!propose_ts_is_not_greater_than_kvs_ts(kv_ptr[I], prop, prop_m_id, t_id, &flag, &reply_rmw)) {
              // 4. If the kv-pair has not been RMWed before grab an entry and ack
              // 5. Else if log number is bigger than the current one, ack without caring about the ongoing RMWs
              // 6. Else check the global entry and send a response depending on whether there is an ongoing RMW and what that is
              flag = handle_remote_propose_in_cache(kv_ptr[I], prop, prop_m_id, t_id, &reply_rmw, &entry);
              // if the propose is going to be acked record its information in the global entry
              if (flag == RMW_ACK_PROPOSE)
                activate_RMW_entry(PROPOSED, *(uint32_t *) prop->ts.version, &rmw.entry[entry], prop->opcode,
                                   prop->ts.m_id, rmw_l_id, glob_sess_id, log_no, t_id,
                                   ENABLE_ASSERTIONS ? "received propose" : NULL);
            }
          }
          check_log_nos_of_glob_entry(&rmw.entry[entry], "Unlocking after received propose", t_id);
          optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
          insert_r_rep(p_ops, NULL, NULL, l_id, t_id, prop_m_id, (uint16_t) I,
                       (void*) &reply_rmw, flag, prop->opcode);
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
            prev_meta = kv_ptr[I]->key.meta;
          } while (!optik_is_same_version_and_valid(prev_meta, kv_ptr[I]->key.meta));
          insert_r_rep(p_ops, (struct network_ts_tuple *)&prev_meta.m_id, (struct network_ts_tuple *)&op->key.meta.m_id,
                       *(uint64_t*) p_ops->ptrs_to_r_headers[I]->l_id, t_id,
                       p_ops->ptrs_to_r_headers[I]->m_id, (uint16_t) I, NULL, READ_TS, op->opcode);

        }
        else {
          //red_printf("wrong Opcode in cache: %d, req %d, m_id %u, val_len %u, version %u , \n",
          //           op->opcode, I, reads[(pull_ptr + I) % max_op_size]->m_id,
          //           reads[(pull_ptr + I) % max_op_size]->val_len,
          //          *(uint32_t *)reads[(pull_ptr + I) % max_op_size]->version);
          assert(false);
        }
      }
    }
    if(key_in_store[I] == 0) {  //Cache miss --> We get here if either tag or log key match failed
//      red_printf("Cache_miss: bkt %u, server %u, tag %u \n", op->key.bkt, op->key.server, op->key.tag);
      assert(false); // cant have a miss since, it hit in the source's cache
    }
    if (zero_ops) {
//      printf("Zero out %d at address %lu \n", op->opcode, &op->opcode);
      op->opcode = 5;
    } // TODO is this needed?
  }
}

// The worker uses this to send in the lin writes after receiving a write_quorum of read replies for them
// Additionally worker sends reads that received a higher timestamp and thus have to be applied as writes
inline void cache_batch_op_first_read_round(uint32_t op_num, uint16_t t_id, struct read_info **writes,
                                            struct pending_ops *p_ops,
                                            uint32_t pull_ptr, uint32_t max_op_size, bool zero_ops)
{
  int I, j;	/* I is batch index */
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
  if (ENABLE_ASSERTIONS) assert(op_num <= MAX_INCOMING_R);
  unsigned int bkt[MAX_INCOMING_R];
  struct mica_bkt *bkt_ptr[MAX_INCOMING_R];
  unsigned int tag[MAX_INCOMING_R];
  int key_in_store[MAX_INCOMING_R];	/* Is this key in the datastore? */
  struct cache_op *kv_ptr[MAX_INCOMING_R];	/* Ptr to KV item in log */
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
        // The write must be performed with the max TS out of the one stored in the KV and read_info
        if (op->opcode == CACHE_OP_PUT) {
          uint32_t r_info_version = *(uint32_t *) op->ts_to_read.version;
          optik_lock(&kv_ptr[I]->key.meta);
          // Change epoch if needed
          if (op->epoch_id > *(uint16_t *)kv_ptr[I]->key.meta.epoch_id)
            *(uint16_t*)kv_ptr[I]->key.meta.epoch_id = op->epoch_id;
          // find the the max ts and write it in the kvs
          if (!optik_is_greater_version(kv_ptr[I]->key.meta, op_meta))
            (*(uint32_t *) op->ts_to_read.version) = kv_ptr[I]->key.meta.version + 1;
          memcpy(kv_ptr[I]->value, op->value, VALUE_SIZE);
          optik_unlock(&kv_ptr[I]->key.meta, op->ts_to_read.m_id, *(uint32_t *)op->ts_to_read.version);
          if (ENABLE_ASSERTIONS) {
            assert(op->ts_to_read.m_id == machine_id);
            assert(r_info_version <= *(uint32_t *)op->ts_to_read.version);
          }
          // rectifying is not needed!
          //if (r_info_version < *(uint32_t *)op->ts_to_read.version)
           // rectify_version_of_w_mes(p_ops, op, r_info_version, t_id);
          // remove the write from the pending out-of-epoch writes
          p_ops->p_ooe_writes->size--;
          MOD_ADD(p_ops->p_ooe_writes->pull_ptr, PENDING_READS);
        }
        else if (op->opcode == OP_ACQUIRE || op->opcode == CACHE_OP_GET) { // a read resulted on receiving a higher timestamp than expected
          optik_lock(&kv_ptr[I]->key.meta);

          if (optik_is_greater_version(kv_ptr[I]->key.meta, op_meta)) {
            if (op->epoch_id > *(uint16_t *)kv_ptr[I]->key.meta.epoch_id)
              *(uint16_t *) kv_ptr[I]->key.meta.epoch_id = op->epoch_id;
            memcpy(kv_ptr[I]->value, op->value, VALUE_SIZE);
            optik_unlock(&kv_ptr[I]->key.meta, op->ts_to_read.m_id, *(uint32_t *)op->ts_to_read.version);
          }
          else {
            optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
            t_stats[t_id].failed_rem_writes++;
          }
        }
        else if (op->opcode == UPDATE_EPOCH_OP_GET) {
          if (op->epoch_id > *(uint16_t *)kv_ptr[I]->key.meta.epoch_id) {
            optik_lock(&kv_ptr[I]->key.meta);
            *(uint16_t*)kv_ptr[I]->key.meta.epoch_id = op->epoch_id;
            optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
            if (ENABLE_STAT_COUNTING) t_stats[t_id].rectified_keys++;
          }
        }
        else {
          red_printf("Wrkr %u: read-first-round wrong opcode in cache: %d, req %d, m_id %u,version %u , \n",
                     t_id, op->opcode, I, writes[(pull_ptr + I) % max_op_size]->ts_to_read.m_id,
                     *(uint32_t *)writes[(pull_ptr + I) % max_op_size]->ts_to_read.version);
          assert(0);
        }
      }
    }
    if(key_in_store[I] == 0) {  //Cache miss --> We get here if either tag or log key match failed
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

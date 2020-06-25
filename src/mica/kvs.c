#ifndef _GNU_SOURCE
# define _GNU_SOURCE
#endif


//#include "../../include/kite_inline_util/inline_util.h"
#include <hrd.h>
#include "kvs.h"
#include <city.h>


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

inline void check_mica_op_t_allignement(mica_op_t *kv_ptr)
{
//  if (kv_ptr->key.bkt == 2173420268 && kv_ptr->key.server == 36126 &&
//      kv_ptr->key.tag == 13372) {
//    uint64_t key_al = ((uint64_t) &kv_ptr->key) % MICA_OP_SIZE;
//    uint64_t opc_al = ((uint64_t) &kv_ptr->opcode) % MICA_OP_SIZE;
//    uint64_t state_al = ((uint64_t) &kv_ptr->state) % MICA_OP_SIZE;
//    uint64_t log_no = ((uint64_t) &kv_ptr->log_no) % MICA_OP_SIZE;
//    //uint64_t last_registered_log_no = ((uint64_t) &kv_ptr->last_registered_log_no) % MICA_OP_SIZE;
//    uint64_t accepted_log_no = ((uint64_t) &kv_ptr->accepted_log_no) % MICA_OP_SIZE;
//    uint64_t last_committed_log_no = ((uint64_t) &kv_ptr->last_committed_log_no) % MICA_OP_SIZE;
//
//    uint64_t seq_lock = ((uint64_t) &kv_ptr->seqlock) % MICA_OP_SIZE;
//    uint64_t ts = ((uint64_t) &kv_ptr->ts) % MICA_OP_SIZE;
//    uint64_t prop_ts = ((uint64_t) &kv_ptr->prop_ts) % MICA_OP_SIZE;
//    uint64_t accepted_ts = ((uint64_t) &kv_ptr->accepted_ts) % MICA_OP_SIZE;
//
//
//    uint64_t rmw_id = ((uint64_t) &kv_ptr->rmw_id) % MICA_OP_SIZE;
//   // uint64_t last_registered_rmw_id = ((uint64_t) &kv_ptr->last_registered_rmw_id) % MICA_OP_SIZE;
//    uint64_t last_committed_rmw_id = ((uint64_t) &kv_ptr->last_committed_rmw_id) % MICA_OP_SIZE;
//    uint64_t accepted_rmw_id = ((uint64_t) &kv_ptr->accepted_rmw_id) % MICA_OP_SIZE;
//
//
//    printf(" key: %lu \n opc %lu \n state %lu \n log %lu \n"
//             "accepted log %lu \n last committed log %lu \n"
//             "seq_lock %lu \n base_ts log %lu \n prop_ts %lu \n accepted_ts %lu \n"
//             "rmw_id %lu \nlast_committed_rmw_id %lu \naccepted_rmw_id%lu \n",
//           key_al, opc_al, state_al, log_no, accepted_log_no, last_committed_log_no,
//           seq_lock, ts, prop_ts, accepted_ts,
//           rmw_id, last_committed_rmw_id, accepted_rmw_id);
//
//  }
}

void mica_init(mica_kv_t *kvs, int instance_id,
               int node_id, int num_bkts, uint64_t log_cap)
{
  int i, j;
  /* Verify struct sizes */
  assert(sizeof(struct mica_slot) == 8);
  assert(sizeof(mica_op_t) % 64 == 0);
  assert(kvs != NULL);
  assert(node_id == 0 || node_id == 1);
  /* 16 million buckets = a 1 GB index */
  assert(is_power_of_2(num_bkts) == 1 && num_bkts <= M_16);
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
  int i;
  unsigned int bkt = op->key.bkt & kvs->bkt_mask;
  struct mica_bkt *bkt_ptr = &kvs->ht_index[bkt];
  unsigned int tag = op->key.tag;

  kvs->num_insert_op++;

  /* Find a slot to use for this key. If there is a slot with the same
   * tag as ours, we are sure to find it because the used slots are at
   * the beginning of the 8-slot array. */
  int slot_to_use = -1;
  for(i = 0; i < 8; i++) {
    if(bkt_ptr->slots[i].tag == tag || bkt_ptr->slots[i].in_use == 0) {
      slot_to_use = i;
      if (bkt_ptr->slots[i].in_use == 1) {
        mica_op_t *evic_op = (mica_op_t *)
          &kvs->ht_log[bkt_ptr->slots[i].offset & kvs->log_mask];
        my_printf(yellow, "Key %u overwrites %u \n", op->key_id, evic_op->key_id);
        assert(false);
      }
    }
  }
  bool evict_flag = false;
  /* If no slot found, choose one to evict */
  if(slot_to_use == -1) {
    assert(false);
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

#ifdef KITE
  mica_op_t * kv_ptr = (mica_op_t *) log_ptr;
  assert(IS_ALIGNED(&kv_ptr->key, 64));
  assert(IS_ALIGNED(&kv_ptr->value, 64));

  //check_mica_op_t_allignement(kv_ptr);
#endif

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
  assert(n > 0);
  /* This is needed for the eviction message below to make sense */
  assert(kvs->num_insert_op == 0 && kvs->num_index_evictions == 0);

  mica_op_t *op = (mica_op_t *) calloc(1, sizeof(mica_op_t));
  unsigned long *op_key = (unsigned long *) &op->key;


  for(uint32_t key_id = 0; key_id < KVS_NUM_KEYS; key_id++) {
    uint128 key_hash = CityHash128((char *) &(key_id), 4);
    struct key *tmp = (struct key *) &key_hash.second;
    (*op_key) = key_hash.second;
    assert(tmp->bkt == op->key.bkt);
    op->key_id = key_id;
    mica_insert_one(kvs, op);
  }

  assert(kvs->num_insert_op == n);
  // printf("Cache: Populated instance %d with %d keys, length = %d. "
  // 			   "Index eviction fraction = %.4f.\n",
  // 	   cache->instance_id, n, val_len,
  // 	   (double) cache->num_index_evictions / cache->num_insert_op);
}




//
// Created by vasilis on 23/06/2020.
//

#ifndef Z_KVS_UTIL_H
#define Z_KVS_UTIL_H

#include "kvs.h"
#include "zk_main.h"
#include "generic_inline_util.h"
#include "debug_util.h"




/* The leader and follower send their local requests to this, reads get served
 * But writes do not get served, writes are only propagated here to see whether their keys exist */
static inline void zk_KVS_batch_op_trace(uint16_t op_num, zk_trace_op_t *op, zk_resp_t *resp, uint16_t t_id)
{
  uint16_t op_i;	/* op_i is batch index */
  long long stalled_brces = 0;
 if (ENABLE_ASSERTIONS) {
   assert(op != NULL);
   assert(op_num > 0 && op_num <= ZK_TRACE_BATCH);
   assert(resp != NULL);
 }

  unsigned int bkt[ZK_TRACE_BATCH];
  struct mica_bkt *bkt_ptr[ZK_TRACE_BATCH];
  unsigned int tag[ZK_TRACE_BATCH];
  mica_op_t *kv_ptr[ZK_TRACE_BATCH];	/* Ptr to KV item in log */
  /*
   * We first lookup the key in the datastore. The first two @op_i loops work
   * for both GETs and PUTs.
   */
  for(op_i = 0; op_i < op_num; op_i++) {
    KVS_locate_one_bucket(op_i, bkt, &op[op_i].key, bkt_ptr, tag, kv_ptr, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);


  // the following variables used to validate atomicity between a lock-free read of an object
  for(op_i = 0; op_i < op_num; op_i++) {
    if (ENABLE_ASSERTIONS && kv_ptr[op_i] == NULL) assert(false);
    bool key_found = memcmp(&kv_ptr[op_i]->key, &op[op_i].key, KEY_SIZE) == 0;
    if (unlikely(ENABLE_ASSERTIONS && !key_found)) {
      my_printf(red, "Kvs miss %u\n", op_i);
      cust_print_key("Op", &op[op_i].key);
      cust_print_key("KV_ptr", &kv_ptr[op_i]->key);
      resp[op_i].type = KVS_MISS;
      assert(false);
    }
    if (op[op_i].opcode == KVS_OP_GET) {
      uint32_t debug_cntr = 0;
      uint64_t tmp_lock = read_seqlock_lock_free(&kv_ptr[op_i]->seqlock);
      do {
        debug_stalling_on_lock(&debug_cntr, "local read", t_id);
        memcpy(op[op_i].value, kv_ptr[op_i]->value, (size_t) VALUE_SIZE); //TODO change for clients
      } while (!(check_seqlock_lock_free(&kv_ptr[op_i]->seqlock, &tmp_lock)));
      resp[op_i].type = KVS_GET_SUCCESS;

    }
    else if (op[op_i].opcode == KVS_OP_PUT) {
      resp[op_i].type = KVS_PUT_SUCCESS;
    }
    else if (ENABLE_ASSERTIONS) {
      my_printf(red, "wrong Opcode in cache: %d, req %d \n", op[op_i].opcode, op_i);
      assert(0);
    }
  }
}

///* The leader and follower send the writes to be committed with this function*/
static inline void zk_KVS_batch_op_updates(uint16_t op_num, zk_prepare_t **preps,
                                           uint32_t pull_ptr, uint32_t max_op_size,
                                           bool zero_ops, uint16_t t_id)
{

  if (DISABLE_UPDATING_KVS) return;
  uint16_t op_i;  /* op_i is batch index */
  if (ENABLE_ASSERTIONS) {
    assert(preps != NULL);
    assert(op_num > 0 && op_num <= ZK_UPDATE_BATCH);
  }

  unsigned int bkt[ZK_UPDATE_BATCH];
  struct mica_bkt *bkt_ptr[ZK_UPDATE_BATCH];
  unsigned int tag[ZK_UPDATE_BATCH];
  mica_op_t *kv_ptr[ZK_UPDATE_BATCH];	/* Ptr to KV item in log */

  for(op_i = 0; op_i < op_num; op_i++) {
    zk_prepare_t *op = preps[(pull_ptr + op_i) % max_op_size];
    KVS_locate_one_bucket(op_i, bkt, &op->key, bkt_ptr, tag, kv_ptr, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);

  for(op_i = 0; op_i < op_num; op_i++) {
    if (ENABLE_ASSERTIONS && kv_ptr[op_i] == NULL) assert(false);
    zk_prepare_t *op = preps[(pull_ptr + op_i) % max_op_size];
    bool key_found = memcmp(&kv_ptr[op_i]->key, &op->key, KEY_SIZE) == 0;
    if (unlikely(ENABLE_ASSERTIONS && !key_found)) {
      my_printf(red, "Kvs update miss %u\n", op_i);
      cust_print_key("Op", &op->key);
      cust_print_key("KV_ptr", &kv_ptr[op_i]->key);
      assert(false);
    }
    if (op->opcode == KVS_OP_PUT) {
      lock_seqlock(&kv_ptr[op_i]->seqlock);
      memcpy(kv_ptr[op_i]->value, op->value, (size_t) VALUE_SIZE);
      unlock_seqlock(&kv_ptr[op_i]->seqlock);
    }
    else {
      my_printf(red, "wrong Opcode to an update in kvs: %d, req %d, flr_id %u, val_len %u, g_id %lu , \n",
                 op->opcode, op_i, op->flr_id, op->val_len, op->g_id);
      assert(0);
    }
    if (zero_ops) {
      //printf("Zero out %d at address %p \n", op->opcode, (void *)&op->opcode);
      op->opcode = 5;
    }
  }

}

#endif //Z_KVS_UTIL_H

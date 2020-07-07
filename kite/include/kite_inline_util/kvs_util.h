//
// Created by vasilis on 11/05/20.
//

#ifndef KITE_KVS_UTILITY_H
#define KITE_KVS_UTILITY_H

#include <config.h>
#include "kvs.h"
#include "generic_util.h"
#include "kite_debug_util.h"
#include "kite_config_util.h"
#include "client_if_util.h"
#include "paxos_util.h"



/* ---------------------------------------------------------------------------
//------------------------------ KVS-specific utility-------------------------
//---------------------------------------------------------------------------*/
// returns true if the key was found
static inline bool search_out_of_epoch_writes(p_ops_t *p_ops,
                                              struct key *read_key,
                                              uint16_t t_id, void **val_ptr)
{
  struct pending_out_of_epoch_writes *writes = p_ops->p_ooe_writes;
  uint32_t w_i = writes->pull_ptr;
  for (uint32_t i = 0; i < writes->size; i++) {
    if (keys_are_equal(&p_ops->read_info[writes->r_info_ptrs[w_i]].key, read_key)) {
      *val_ptr = (void*) p_ops->read_info[writes->r_info_ptrs[w_i]].value;
      //my_printf(red, "Wrkr %u: Forwarding value from out-of-epoch write, read key: ", t_id);
      //print_key(read_key);
      //my_printf(red, "write key: "); print_key((struct key*)p_ops->read_info[writes->r_info_ptrs[w_i]].key);
      //my_printf(red, "size: %u, push_ptr %u, pull_ptr %u, r_info ptr %u \n",
      //          writes->size, writes->push_ptr, writes->pull_ptr, writes->r_info_ptrs[w_i]);
      return true;
    }
    MOD_INCR(w_i, PENDING_READS);
  }
  return false;
}


/* ---------------------------------------------------------------------------
//------------------------------ KVS------------------------------------------
//---------------------------------------------------------------------------*/

/*-----------------------------FROM TRACE---------------------------------------------*/


static inline bool KVS_from_trace_reads_value_forwarded(trace_op_t *op,
                                                        mica_op_t *kv_ptr, kv_resp_t *resp,
                                                        p_ops_t *p_ops, uint16_t t_id)
{
  if (TURN_OFF_KITE) return false;
  bool value_forwarded = false; // has a pending out-of-epoch write forwarded its value to this
  if (op->opcode == KVS_OP_GET && p_ops->p_ooe_writes->size > 0) {
    uint8_t *val_ptr;
    if (search_out_of_epoch_writes(p_ops, &op->key, t_id, (void **) &val_ptr)) {
      memcpy(op->value_to_read, val_ptr, op->real_val_len);
      //my_printf(red, "Wrkr %u Forwarding a value \n", t_id);
      value_forwarded = true;
    }
  }
  return value_forwarded;
}

// Handle a local read/acquire in the KVS
static inline bool KVS_from_trace_reads(trace_op_t *op,
                                        mica_op_t *kv_ptr, kv_resp_t *resp,
                                        p_ops_t *p_ops,
                                        uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) assert(op->real_val_len <= VALUE_SIZE);

  resp->type = KVS_LOCAL_GET_SUCCESS;
  if (KVS_from_trace_reads_value_forwarded(op, kv_ptr, resp, p_ops,t_id)) return true;

  uint64_t kv_epoch = 0;
  uint32_t debug_cntr = 0;

  uint64_t tmp_lock = read_seqlock_lock_free(&kv_ptr->seqlock);
  do {
    if (!TURN_OFF_KITE) kv_epoch = kv_ptr->epoch_id;
    debug_stalling_on_lock(&debug_cntr, "trace read/acquire", t_id);
    if (ENABLE_ASSERTIONS) assert(op->value_to_read != NULL);
    memcpy(op->value_to_read, kv_ptr->value, op->real_val_len);
    //printf("Reading val %u from key %u \n", kv_ptr->value[0], kv_ptr->key.bkt);
  } while (!(check_seqlock_lock_free(&kv_ptr->seqlock, &tmp_lock)));

  if (TURN_OFF_KITE) {
    return true;
  }
  else return kv_epoch >= epoch_id; //return success if the kv_epoch is not left behind the epoch-id
}

// Handle a local write in the KVS
static inline void KVS_from_trace_writes(trace_op_t *op,
                                         mica_op_t *kv_ptr, kv_resp_t *resp,
                                         p_ops_t *p_ops, uint32_t *r_push_ptr_,
                                         uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) assert(op->real_val_len <= VALUE_SIZE);
//  if (ENABLE_ASSERTIONS) assert(op->val_len == kv_ptr->val_len);
  lock_seqlock(&kv_ptr->seqlock);
  // OUT_OF_EPOCH--first round will be a read TS
  if (kv_ptr->epoch_id < epoch_id) {
    uint32_t r_push_ptr = *r_push_ptr_;
    r_info_t *r_info = &p_ops->read_info[r_push_ptr];
    r_info->ts_to_read.m_id = kv_ptr->ts.m_id;
    r_info->ts_to_read.version = kv_ptr->ts.version;
    unlock_seqlock(&kv_ptr->seqlock);
    r_info->opcode = op->opcode;
    r_info->key = op->key;
    r_info->r_ptr = r_push_ptr;
    if (ENABLE_ASSERTIONS) op->ts.version = r_info->ts_to_read.version;
    // Store the value to be written in the read_info to be used in the second round
    memcpy(r_info->value, op->value_to_write, op->real_val_len);

    //my_printf(yellow, "Out of epoch write key %u, node-next key_id %u \n",
    //             op->key.bkt, new_node->next_key_id);
    r_info->val_len = op->real_val_len;
    p_ops->p_ooe_writes->r_info_ptrs[p_ops->p_ooe_writes->push_ptr] = r_push_ptr;
    p_ops->p_ooe_writes->size++;
    MOD_INCR(p_ops->p_ooe_writes->push_ptr, PENDING_READS);
    MOD_INCR(r_push_ptr, PENDING_READS);
    resp->type = KVS_GET_TS_SUCCESS;
    (*r_push_ptr_) =  r_push_ptr;
  }
  else { // IN-EPOCH
    if (ENABLE_ASSERTIONS) {
      update_commit_logs(t_id, kv_ptr->key.bkt, op->ts.version, kv_ptr->value,
                         op->value_to_write, "local write", LOG_WS);
    }
    write_kv_ptr_val(kv_ptr, op->value_to_write, op->real_val_len, FROM_TRACE_WRITE);
    //printf("Wrote val %u to key %u \n", kv_ptr->value[0], kv_ptr->key.bkt);
    // This also writes the new version to op
    kv_ptr->ts.m_id = (uint8_t) machine_id;
    kv_ptr->ts.version++;
    op->ts.version = kv_ptr->ts.version;
    unlock_seqlock(&kv_ptr->seqlock);
    resp->type = KVS_PUT_SUCCESS;
  }
}


// Handle a local release in the KVS
static inline void KVS_from_trace_releases(trace_op_t *op,
                                           mica_op_t *kv_ptr, kv_resp_t *resp,
                                           p_ops_t *p_ops, uint32_t *r_push_ptr_,
                                           uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) assert(op->real_val_len <= VALUE_SIZE);
  struct ts_tuple kvs_tuple;
  uint32_t r_push_ptr = *r_push_ptr_;
  r_info_t *r_info = &p_ops->read_info[r_push_ptr];
  uint32_t debug_cntr = 0;
  uint64_t tmp_lock = read_seqlock_lock_free(&kv_ptr->seqlock);
  do {
    kvs_tuple = kv_ptr->ts;
    debug_stalling_on_lock(&debug_cntr, "trace releases", t_id);
  } while (!(check_seqlock_lock_free(&kv_ptr->seqlock, &tmp_lock)));

  if (ENABLE_ASSERTIONS) op->ts.version = kvs_tuple.version;
  r_info->ts_to_read.m_id = kvs_tuple.m_id;
  r_info->ts_to_read.version = kvs_tuple.version;
  r_info->key = op->key;
  r_info->opcode = op->opcode;
  r_info->r_ptr = r_push_ptr;
  // Store the value to be written in the read_info to be used in the second round
  memcpy(r_info->value, op->value_to_write, op->real_val_len);
  r_info->val_len = op->real_val_len;
  MOD_INCR(r_push_ptr, PENDING_READS);
  resp->type = KVS_GET_TS_SUCCESS;
  (*r_push_ptr_) =  r_push_ptr;
}

// Handle a local rmw in the KVS
static inline void KVS_from_trace_rmw(trace_op_t *op,
                                      mica_op_t *kv_ptr,
                                      p_ops_t *p_ops,
                                      uint16_t op_i, uint16_t t_id)
{
  loc_entry_t *loc_entry = &p_ops->prop_info->entry[op->session_id];
  init_loc_entry(p_ops, op, t_id, loc_entry);
  if (DEBUG_RMW) my_printf(green, "Worker %u trying a local RMW on op %u\n", t_id, op_i);
  uint32_t new_version = (ENABLE_ALL_ABOARD && op->attempt_all_aboard) ?
                         ALL_ABOARD_TS : PAXOS_TS;
  uint8_t state = (uint8_t) (loc_entry->all_aboard ? ACCEPTED : PROPOSED);
  __builtin_prefetch(loc_entry->compare_val, 0, 0);
  lock_seqlock(&kv_ptr->seqlock);
  {
    check_trace_op_key_vs_kv_ptr(op, kv_ptr);
    check_log_nos_of_kv_ptr(kv_ptr, "KVS_batch_op_trace", t_id);
    if (does_rmw_fail_early(op, kv_ptr, t_id)) {
      loc_entry->state = CAS_FAILED;
    }
    else if (kv_ptr->state == INVALID_RMW) {
      activate_kv_pair(state, new_version, kv_ptr, op->opcode,
                       (uint8_t) machine_id, loc_entry, loc_entry->rmw_id.id,
                       kv_ptr->last_committed_log_no + 1, t_id, ENABLE_ASSERTIONS ? "batch to trace" : NULL);
      loc_entry->state = state;
      if (ENABLE_ASSERTIONS) assert(kv_ptr->log_no == kv_ptr->last_committed_log_no + 1);
      loc_entry->log_no = kv_ptr->log_no;
    }
    else {
      // This is the state the RMW will wait on
      loc_entry->state = NEEDS_KV_PTR;
      // Set up the state that the RMW should wait on
      loc_entry->help_rmw->rmw_id = kv_ptr->rmw_id;
      loc_entry->help_rmw->state = kv_ptr->state;
      loc_entry->help_rmw->ts = kv_ptr->prop_ts;
      loc_entry->help_rmw->log_no = kv_ptr->log_no;
    }
  }
  loc_entry->base_ts = kv_ptr->ts;
  unlock_seqlock(&kv_ptr->seqlock);

  loc_entry->kv_ptr = kv_ptr;
  if (ENABLE_ASSERTIONS) {
    loc_entry->help_loc_entry->kv_ptr = kv_ptr;
  }
  // We need to put the new timestamp in the op too, both to send it and to store it for later
  op->ts.version = new_version;
}

// Handle a local rmw acquire in the KVS
static inline void KVS_from_trace_acquires_ooe_reads(trace_op_t *op, mica_op_t *kv_ptr,
                                                     kv_resp_t *resp, p_ops_t *p_ops,
                                                     uint32_t *r_push_ptr_, uint16_t t_id)
{
  if(ENABLE_ASSERTIONS)
    if (op->opcode != OP_ACQUIRE) assert(!TURN_OFF_KITE);
  uint32_t r_push_ptr = *r_push_ptr_;
  r_info_t *r_info = &p_ops->read_info[r_push_ptr];

  uint64_t tmp_lock = read_seqlock_lock_free(&kv_ptr->seqlock);
  do {
    check_keys_with_one_trace_op(&op->key, kv_ptr);
    r_info->log_no = kv_ptr->last_committed_log_no;
    r_info->rmw_id = kv_ptr->last_committed_rmw_id;
    r_info->ts_to_read.version = kv_ptr->ts.version;
    r_info->ts_to_read.m_id = kv_ptr->ts.m_id;
    memcpy(op->value_to_read, kv_ptr->value, op->real_val_len);
  } while (!(check_seqlock_lock_free(&kv_ptr->seqlock, &tmp_lock)));

  // Copy the value to the read_info too
  memcpy(r_info->value, op->value_to_read, op->real_val_len);
  r_info->value_to_read = op->value_to_read;
  r_info->val_len = op->real_val_len;
  r_info->key = op->key;
  r_info->is_read = true;
  r_info->opcode = op->opcode; // it could be an acquire or an out-of-epoch read
  r_info->r_ptr = r_push_ptr;
  MOD_INCR(r_push_ptr, PENDING_READS);
  resp->type = KVS_GET_SUCCESS;
  (*r_push_ptr_) =  r_push_ptr;
}


/*-----------------------------UPDATES---------------------------------------------*/

// Handle a remote release/write or acquire-write the KVS
static inline void KVS_updates_writes_or_releases_or_acquires(write_t *op,
                                                              mica_op_t *kv_ptr, uint16_t t_id)
{
  //my_printf(red, "received op %u with value %u \n", op->opcode, op->value[0]);
//  if (ENABLE_ASSERTIONS) assert(op->val_len == kv_ptr->val_len);
  lock_seqlock(&kv_ptr->seqlock);
  if (compare_netw_ts_with_ts((struct network_ts_tuple*) &op, &kv_ptr->ts) == GREATER) {
    update_commit_logs(t_id, kv_ptr->key.bkt, op->version, kv_ptr->value,
                       op->value, "rem write", LOG_WS);
    write_kv_ptr_val(kv_ptr, op->value, (size_t) VALUE_SIZE, FROM_REMOTE_WRITE_RELEASE);
    //printf("Wrote val %u to key %u \n", kv_ptr->value[0], kv_ptr->key.bkt);
    kv_ptr->ts.m_id = op->m_id;
    kv_ptr->ts.version = op->version;
    unlock_seqlock(&kv_ptr->seqlock);
  } else {
    unlock_seqlock(&kv_ptr->seqlock);
    if (ENABLE_STAT_COUNTING) t_stats[t_id].failed_rem_writes++;
  }
}


// Handle a remote RMW accept message in the KVS
static inline void KVS_updates_accepts(struct accept *acc, mica_op_t *kv_ptr,
                                       p_ops_t *p_ops,
                                       uint16_t op_i, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
//    assert(acc->last_registered_rmw_id.id != acc->t_rmw_id ||
//           acc->last_registered_rmw_id.glob_sess_id != acc->glob_sess_id);
    assert(acc->ts.version > 0);
  }
  // on replying to the accept we may need to send on or more of TS, VALUE, RMW-id, log-no
  uint64_t rmw_l_id = acc->t_rmw_id;
  //my_printf(cyan, "Received accept with rmw_id %u, glob_sess %u \n", rmw_l_id, glob_sess_id);
  uint32_t log_no = acc->log_no;
  uint64_t l_id = acc->l_id;

  struct w_message *acc_mes = (struct w_message *) p_ops->ptrs_to_mes_headers[op_i];
  if (ENABLE_ASSERTIONS) check_accept_mes(acc_mes);
  uint8_t acc_m_id = acc_mes->m_id;
  uint8_t opcode_for_r_rep = (uint8_t)
    (acc_mes->opcode == ONLY_ACCEPTS ? ACCEPT_OP : ACCEPT_OP_NO_CREDITS);
  struct rmw_rep_last_committed *acc_rep =
    (struct rmw_rep_last_committed *) get_r_rep_ptr(p_ops, l_id, acc_m_id, opcode_for_r_rep,
                                                    p_ops->coalesce_r_rep[op_i], t_id);
  acc_rep->l_id = l_id;

  if (DEBUG_RMW) my_printf(green, "Worker %u is handling a remote RMW accept on op %u from m_id %u "
                             "l_id %u, rmw_l_id %u, glob_ses_id %u, log_no %u, version %u  \n",
                           t_id, op_i, acc_m_id, l_id, rmw_l_id, (uint32_t) rmw_l_id % GLOBAL_SESSION_NUM, log_no, acc->ts.version);
  lock_seqlock(&kv_ptr->seqlock);
  // 1. check if it has been committed
  // 2. first check the log number to see if it's SMALLER!! (leave the "higher" part after the KVS base_ts is also checked)
  // Either way fill the reply_rmw fully, but have a specialized flag!
  if (!is_log_smaller_or_has_rmw_committed(log_no, kv_ptr, rmw_l_id, t_id, acc_rep)) {
    if (!is_log_too_high(log_no, kv_ptr, t_id, acc_rep)) {
      // 3. Check that the TS is higher than the KVS TS, setting the flag accordingly
      //if (!ts_is_not_greater_than_kvs_ts(kv_ptr, &acc->base_ts, acc_m_id, t_id, acc_rep)) {
      // 4. If the kv-pair has not been RMWed before grab an entry and ack
      // 5. Else if log number is bigger than the current one, ack without caring about the ongoing RMWs
      // 6. Else check the kv_ptr and send a response depending on whether there is an ongoing RMW and what that is
      acc_rep->opcode = handle_remote_prop_or_acc_in_kvs(kv_ptr, (void *) acc, acc_m_id, t_id, acc_rep, log_no, false);
      // if the accepted is going to be acked record its information in the kv_ptr
      if (acc_rep->opcode == RMW_ACK) {
        activate_kv_pair(ACCEPTED, acc->ts.version, kv_ptr, acc->opcode,
                         acc->ts.m_id, NULL, rmw_l_id, log_no, t_id,
                         ENABLE_ASSERTIONS ? "received accept" : NULL);
        memcpy(kv_ptr->last_accepted_value, acc->value, (size_t) RMW_VALUE_SIZE);
        //print_treiber_top((struct top *) kv_ptr->last_accepted_value, "Receiving remote accept", green);
//        my_printf(green, " kv_ptr Last committed log no log_no %u acc logno %u\n",
//                  kv_ptr->last_committed_log_no, kv_ptr->log_no, acc->log_no);
        assign_netw_ts_to_ts(&kv_ptr->base_acc_ts, &acc->base_ts);
      }
    }
  }
  uint64_t number_of_reqs = 0;
  if (ENABLE_DEBUG_RMW_KV_PTR) {
    // kv_ptr->dbg->prop_acc_num++;
    // number_of_reqs = kv_ptr->dbg->prop_acc_num;
  }
  check_log_nos_of_kv_ptr(kv_ptr, "Unlocking after received accept", t_id);
  unlock_seqlock(&kv_ptr->seqlock);
  if (PRINT_LOGS)
    fprintf(rmw_verify_fp[t_id], "Key: %u, log %u: Req %lu, Acc: m_id:%u, rmw_id %lu, glob_sess id: %u, "
              "version %u, m_id: %u, resp: %u \n",
            kv_ptr->key.bkt, log_no, number_of_reqs, acc_m_id, rmw_l_id, (uint32_t) rmw_l_id % GLOBAL_SESSION_NUM, acc->ts.version, acc->ts.m_id, acc_rep->opcode);
  //set_up_rmw_rep_message_size(p_ops, acc_rep->opcode, t_id);
  p_ops->r_rep_fifo->message_sizes[p_ops->r_rep_fifo->push_ptr]+= get_size_from_opcode(acc_rep->opcode);
  if (ENABLE_ASSERTIONS) assert(p_ops->r_rep_fifo->message_sizes[p_ops->r_rep_fifo->push_ptr] <= R_REP_SEND_SIZE);
  finish_r_rep_bookkeeping(p_ops, (struct r_rep_big*) acc_rep, false, acc_m_id, t_id);
}

// Handle a remote RMW commit message in the KVS
static inline void KVS_updates_commits(struct commit *com, mica_op_t *kv_ptr,
                                       p_ops_t *p_ops,
                                       uint16_t op_i, uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(green, "Worker %u is handling a remote RMW commit on com %u, "
                "rmw_l_id %u, glob_ses_id %u, log_no %u, version %u  \n",
              t_id, op_i, com->t_rmw_id, com->t_rmw_id % GLOBAL_SESSION_NUM, com->log_no, com->base_ts.version);

  uint64_t number_of_reqs;
//  number_of_reqs = handle_remote_commit_message(kv_ptr, (void*) com, true, t_id);
  commit_rmw(kv_ptr, (void*) com, NULL, FROM_REMOTE_COMMIT, t_id);

  if (PRINT_LOGS) {
    struct w_message *com_mes = (struct w_message *) p_ops->ptrs_to_mes_headers[op_i];
    uint8_t acc_m_id = com_mes->m_id;
    fprintf(rmw_verify_fp[t_id], "Key: %u, log %u: Req %lu, Com: m_id:%u, rmw_id %lu, glob_sess id: %u, "
              "version %u, m_id: %u \n",
            kv_ptr->key.bkt, com->log_no, number_of_reqs, acc_m_id, com->t_rmw_id,
            (uint32_t) (com->t_rmw_id % GLOBAL_SESSION_NUM), com->base_ts.version, com->base_ts.m_id);
  }
}

/*-----------------------------READS---------------------------------------------*/

// Handle remote acquires, acquires-fp and reads (out-of-epoch)
// (acquires-fp are acquires renamed by the receiver when a false positive is detected)
static inline void KVS_reads_acquires_acquire_fp_and_reads(struct read *read, mica_op_t *kv_ptr,
                                                           p_ops_t *p_ops, uint16_t op_i,
                                                           uint16_t t_id)
{
  uint32_t debug_cntr = 0;
  uint64_t l_id = p_ops->ptrs_to_mes_headers[op_i]->l_id;
  uint8_t rem_m_id = p_ops->ptrs_to_mes_headers[op_i]->m_id;
  struct r_rep_big *acq_rep =  get_r_rep_ptr(p_ops, l_id, rem_m_id, read->opcode,
                                             p_ops->coalesce_r_rep[op_i], t_id);

  uint32_t acq_log_no = read->log_no;

  uint64_t tmp_lock = read_seqlock_lock_free(&kv_ptr->seqlock);
  do {
    debug_stalling_on_lock(&debug_cntr, "reads: gets_or_acquires_or_acquires_fp", t_id);
    check_keys_with_one_trace_op(&read->key, kv_ptr);
    compare_t carts_comp = compare_netw_carts_with_carts(&read->ts, acq_log_no,
                                                         &kv_ptr->ts, kv_ptr->last_committed_log_no);
    if (carts_comp == SMALLER) {
      if (ENABLE_ASSERTIONS) {
        assert(read->ts.version <= kv_ptr->ts.version);
      }
      acq_rep->opcode = CARTS_TOO_SMALL;
      acq_rep->rmw_id = kv_ptr->last_committed_rmw_id.id;
      acq_rep->log_no = kv_ptr->last_committed_log_no;
      memcpy(acq_rep->value, kv_ptr->value, (size_t) VALUE_SIZE);
      acq_rep->base_ts.version = kv_ptr->ts.version;
      acq_rep->base_ts.m_id = kv_ptr->ts.m_id;
    }
    else if (carts_comp == EQUAL) {
      acq_rep->opcode = CARTS_EQUAL;
    }
    else {
      if (ENABLE_ASSERTIONS) assert(carts_comp == GREATER);
      acq_rep->opcode = CARTS_TOO_HIGH;
    }
  } while (!(check_seqlock_lock_free(&kv_ptr->seqlock, &tmp_lock)));

  set_up_rmw_acq_rep_message_size(p_ops, acq_rep->opcode, t_id);
  finish_r_rep_bookkeeping(p_ops, (struct r_rep_big *) acq_rep,
                           read->opcode == OP_ACQUIRE_FP, rem_m_id, t_id);
}


// Handle remote requests to get TS that are the first round of a release or of an out-of-epoch write
static inline void KVS_reads_get_TS(struct read *read, mica_op_t *kv_ptr,
                                    p_ops_t *p_ops, uint16_t op_i,
                                    uint16_t t_id)
{
  uint32_t debug_cntr = 0;
  uint8_t rem_m_id = p_ops->ptrs_to_mes_headers[op_i]->m_id;
  struct r_rep_big *r_rep = get_r_rep_ptr(p_ops, p_ops->ptrs_to_mes_headers[op_i]->l_id,
                                          rem_m_id, read->opcode, p_ops->coalesce_r_rep[op_i], t_id);

  uint64_t tmp_lock = read_seqlock_lock_free(&kv_ptr->seqlock);
  do {
    debug_stalling_on_lock(&debug_cntr, "reads: get-TS-read version", t_id);
    r_rep->base_ts.m_id = kv_ptr->ts.m_id;
    r_rep->base_ts.version = kv_ptr->ts.version;
  } while (!(check_seqlock_lock_free(&kv_ptr->seqlock, &tmp_lock)));
  set_up_r_rep_message_size(p_ops, r_rep, &read->ts, true, t_id);
  finish_r_rep_bookkeeping(p_ops, r_rep, false, rem_m_id, t_id);
}

// Handle remote proposes
static inline void KVS_reads_proposes(struct read *read, mica_op_t *kv_ptr,
                                      p_ops_t *p_ops, uint16_t op_i,
                                      uint16_t t_id)
{
  struct propose *prop = (struct propose *) read; //(((void *)read) + 3); // the propose starts at an offset of 5 bytes
  if (DEBUG_RMW) my_printf(green, "Worker %u trying a remote RMW propose on op %u\n", t_id, op_i);
  if (ENABLE_ASSERTIONS) assert(prop->ts.version > 0);
  uint64_t number_of_reqs = 0;
  uint64_t rmw_l_id = prop->t_rmw_id;
  uint64_t l_id = prop->l_id;
  //my_printf(cyan, "Received propose with rmw_id %u, glob_sess %u \n", rmw_l_id, glob_sess_id);
  uint32_t log_no = prop->log_no;
  uint8_t prop_m_id = p_ops->ptrs_to_mes_headers[op_i]->m_id;
  struct rmw_rep_last_committed *prop_rep =
    (struct rmw_rep_last_committed *) get_r_rep_ptr(p_ops, l_id, prop_m_id, read->opcode,
                                                    p_ops->coalesce_r_rep[op_i], t_id);
  prop_rep->l_id = prop->l_id;
  //my_printf(green, "Sending prop_rep lid %u to m _id %u \n", prop_rep->l_id, prop_m_id);

  lock_seqlock(&kv_ptr->seqlock);
  {
    //check_for_same_ts_as_already_proposed(kv_ptr[I], prop, t_id);
    // 1. check if it has been committed
    // 2. first check the log number to see if it's SMALLER!! (leave the "higher" part after the KVS base_ts is also checked)
    // Either way fill the reply_rmw fully, but have a specialized flag!
    if (!is_log_smaller_or_has_rmw_committed(log_no, kv_ptr, rmw_l_id, t_id, prop_rep)) {
      if (!is_log_too_high(log_no, kv_ptr, t_id, prop_rep)) {
        // 3. Check that the TS is higher than the KVS TS, setting the flag accordingly
        //if (!ts_is_not_greater_than_kvs_ts(kv_ptr, &prop->base_ts, prop_m_id, t_id, prop_rep)) {
        // 4. If the kv-pair has not been RMWed before grab an entry and ack
        // 5. Else if log number is bigger than the current one, ack without caring about the ongoing RMWs
        // 6. Else check the kv_ptr and send a response depending on whether there is an ongoing RMW and what that is
        prop_rep->opcode = handle_remote_prop_or_acc_in_kvs(kv_ptr, (void *) prop, prop_m_id, t_id,
                                                            prop_rep, prop->log_no, true);
        // if the propose is going to be acked record its information in the kv_ptr
        if (prop_rep->opcode == RMW_ACK) {
          if (ENABLE_ASSERTIONS) assert(prop->log_no >= kv_ptr->log_no);
          activate_kv_pair(PROPOSED, prop->ts.version, kv_ptr, prop->opcode,
                           prop->ts.m_id, NULL, rmw_l_id, log_no, t_id,
                           ENABLE_ASSERTIONS ? "received propose" : NULL);
        }
        if (prop_rep->opcode == RMW_ACK || prop_rep->opcode == RMW_ACK_ACC_SAME_RMW) {
          prop_rep->opcode = is_base_ts_too_small(kv_ptr, prop, prop_rep, t_id);
        }
        if (ENABLE_ASSERTIONS) {
          assert(kv_ptr->prop_ts.version >= prop->ts.version);
          check_keys_with_one_trace_op(&prop->key, kv_ptr);
        }
        //}
      }
    }
    if (ENABLE_DEBUG_RMW_KV_PTR) {
      // kv_ptr->dbg->prop_acc_num++;
      // number_of_reqs = kv_ptr->dbg->prop_acc_num;
    }
    check_log_nos_of_kv_ptr(kv_ptr, "Unlocking after received propose", t_id);
  }
  unlock_seqlock(&kv_ptr->seqlock);
  if (PRINT_LOGS && ENABLE_DEBUG_RMW_KV_PTR)
    fprintf(rmw_verify_fp[t_id], "Key: %u, log %u: Req %lu, Prop: m_id:%u, rmw_id %lu, glob_sess id: %u, "
              "version %u, m_id: %u, resp: %u \n",  kv_ptr->key.bkt, log_no, number_of_reqs, prop_m_id,
            rmw_l_id, (uint32_t) rmw_l_id % GLOBAL_SESSION_NUM, prop->ts.version, prop->ts.m_id, prop_rep->opcode);
  p_ops->r_rep_fifo->message_sizes[p_ops->r_rep_fifo->push_ptr]+= get_size_from_opcode(prop_rep->opcode);
  if (ENABLE_ASSERTIONS) assert(p_ops->r_rep_fifo->message_sizes[p_ops->r_rep_fifo->push_ptr] <= R_REP_SEND_SIZE);
  bool false_pos = take_ownership_of_a_conf_bit(rmw_l_id, prop_m_id, true, t_id);
  finish_r_rep_bookkeeping(p_ops, (struct r_rep_big*) prop_rep, false_pos, prop_m_id, t_id);
  //struct rmw_rep_message *rmw_mes = (struct rmw_rep_message *) &p_ops->r_rep_fifo->r_rep_message[p_ops->r_rep_fifo->push_ptr];

}



/*-----------------------------READ-COMMITTING---------------------------------------------*/
// Perform the ooe-write after reading TSes
static inline void KVS_out_of_epoch_writes(r_info_t *r_info, mica_op_t *kv_ptr,
                                           p_ops_t *p_ops, uint16_t t_id)
{
  assert(!TURN_OFF_KITE);
  lock_seqlock(&kv_ptr->seqlock);
  rectify_key_epoch_id(r_info->epoch_id, kv_ptr, t_id);
  // Do the write with the version that has been chosen and has been sent to the rest of the machines
  // Do *not* attempt to use the present version and overwrite the local kvs, as this would not match with the write that we inserted
  if (compare_ts(&r_info->ts_to_read, &kv_ptr->ts) == GREATER) {
    write_kv_ptr_val(kv_ptr, r_info->value, r_info->val_len, FROM_OOE_LOCAL_WRITE);
    kv_ptr->ts = r_info->ts_to_read;
  }
  unlock_seqlock(&kv_ptr->seqlock);
  if (ENABLE_ASSERTIONS) assert(r_info->ts_to_read.m_id == machine_id);

  p_ops->p_ooe_writes->size--;
  MOD_INCR(p_ops->p_ooe_writes->pull_ptr, PENDING_READS);
}

// Handle committing an RMW/write from a response to an acquire or ooe-read
static inline void KVS_acquire_commits(r_info_t *r_info, mica_op_t *kv_ptr,
                                       uint16_t op_i, uint16_t t_id)
{
 if (ENABLE_ASSERTIONS) assert(WRITE_RATIO > 0 || RMW_RATIO > 0);
  if (DEBUG_RMW)
    my_printf(green, "Worker %u is handling a remote RMW commit on r_info %u, "
                "rmw_l_id %u,log_no %u, version %u  \n",
              t_id, op_i, r_info->rmw_id.id,
              r_info->log_no, r_info->ts_to_read.version);
  uint64_t number_of_reqs = 0;
  commit_rmw(kv_ptr, (void *) r_info, NULL, r_info->opcode == OP_ACQUIRE? FROM_LOCAL_ACQUIRE : FROM_OOE_READ, t_id);
  if (PRINT_LOGS) {
    fprintf(rmw_verify_fp[t_id], "Key: %u, log %u: Req %lu, Acq-RMW: rmw_id %lu, "
              "version %u, m_id: %u \n",
            kv_ptr->key.bkt, r_info->log_no, number_of_reqs,  r_info->rmw_id.id,
            r_info->ts_to_read.version, r_info->ts_to_read.m_id);
  }
}


/* ---------------------------------------------------------------------------
//------------------------------ KVS UTILITY SPECIFIC -----------------------------
//---------------------------------------------------------------------------*/


/* The worker sends its local requests to this, reads check the ts_tuple and copy it to the op to get broadcast
 * Writes do not get served either, writes are only propagated here to see whether their keys exist */
static inline void KVS_batch_op_trace(uint16_t op_num, uint16_t t_id, trace_op_t *op,
                                      kv_resp_t *resp,
                                      p_ops_t *p_ops)
{
  uint16_t op_i;
  if (ENABLE_ASSERTIONS) assert (op_num <= MAX_OP_BATCH);
  unsigned int bkt[MAX_OP_BATCH];
  struct mica_bkt *bkt_ptr[MAX_OP_BATCH];
  unsigned int tag[MAX_OP_BATCH];
  mica_op_t *kv_ptr[MAX_OP_BATCH];	/* Ptr to KV item in log */
  /*
   * We first lookup the key in the datastore. The first two @I loops work
   * for both GETs and PUTs.
   */
  for(op_i = 0; op_i < op_num; op_i++) {
    KVS_locate_one_bucket(op_i, bkt, &op[op_i].key, bkt_ptr, tag, kv_ptr, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);

  //uint64_t rmw_l_id = p_ops->prop_info->l_id;
  uint32_t r_push_ptr = p_ops->r_push_ptr;
  for(op_i = 0; op_i < op_num; op_i++) {
    if (ENABLE_ASSERTIONS && kv_ptr[op_i] == NULL) assert(false);
    /* We had a tag match earlier. Now compare log entry. */
    bool key_found = memcmp(&kv_ptr[op_i]->key, &op[op_i].key, KEY_SIZE) == 0;
    if(unlikely(ENABLE_ASSERTIONS && !key_found)) {
      my_printf(red, "Kvs miss %u\n", op_i);
      cust_print_key("Op", &op[op_i].key);
      cust_print_key("KV_ptr", &kv_ptr[op_i]->key);
      resp[op_i].type = KVS_MISS;
      return;
    }
    // Hit
    //my_printf(red, "Hit %u : bkt %u/%u, server %u/%u, tag %u/%u \n",
    //           op_i, op[op_i].key.bkt, kv_ptr[op_i]->key.bkt ,op[op_i].key.server,
    //           kv_ptr[op_i]->key.server, op[op_i].key.tag, kv_ptr[op_i]->key.tag);
    switch (op[op_i].opcode) {
      case KVS_OP_GET:
        if (KVS_from_trace_reads(&op[op_i], kv_ptr[op_i], &resp[op_i],
                                 p_ops, t_id))
          break;
      case OP_ACQUIRE:
        KVS_from_trace_acquires_ooe_reads(&op[op_i], kv_ptr[op_i], &resp[op_i],
                                          p_ops, &r_push_ptr, t_id);
        break;
      case KVS_OP_PUT:
        KVS_from_trace_writes(&op[op_i], kv_ptr[op_i], &resp[op_i],
                              p_ops, &r_push_ptr, t_id);
        break;
      case OP_RELEASE:
        KVS_from_trace_releases(&op[op_i], kv_ptr[op_i], &resp[op_i],
                                p_ops, &r_push_ptr, t_id);
        break;
      case FETCH_AND_ADD:
      case COMPARE_AND_SWAP_WEAK:
      case COMPARE_AND_SWAP_STRONG:
      case RMW_PLAIN_WRITE:
        KVS_from_trace_rmw(&op[op_i], kv_ptr[op_i],
                           p_ops, op_i, t_id);
        break;
      default: if (ENABLE_ASSERTIONS) {
          my_printf(red, "Wrkr %u: KVS_batch_op_trace wrong opcode in KVS: %d, req %d \n",
                    t_id, op[op_i].opcode, op_i);
          assert(0);
        }
    }
  }
}

/* The worker sends the remote writes to be committed with this function*/
static inline void KVS_batch_op_updates(uint16_t op_num, uint16_t t_id, write_t **writes,
                                        p_ops_t *p_ops,
                                        uint32_t pull_ptr, uint32_t max_op_size)
{
  uint16_t op_i;	/* I is batch index */

  if (ENABLE_ASSERTIONS) assert(op_num <= MAX_INCOMING_W);
  unsigned int bkt[MAX_INCOMING_W];
  struct mica_bkt *bkt_ptr[MAX_INCOMING_W];
  unsigned int tag[MAX_INCOMING_W];
  mica_op_t *kv_ptr[MAX_INCOMING_W];	/* Ptr to KV item in log */
  /*
     * We first lookup the key in the datastore. The first two @I loops work
     * for both GETs and PUTs.
     */
  for(op_i = 0; op_i < op_num; op_i++) {
    write_t *op = writes[(pull_ptr + op_i) % max_op_size];
    KVS_locate_one_bucket(op_i, bkt, &op->key, bkt_ptr, tag, kv_ptr, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);

  // the following variables used to validate atomicity between a lock-free r_rep of an object
  for(op_i = 0; op_i < op_num; op_i++) {
    write_t *write =  writes[(pull_ptr + op_i) % max_op_size];
    if (unlikely (write->opcode == OP_RELEASE_BIT_VECTOR)) continue;
    if (ENABLE_ASSERTIONS && kv_ptr[op_i] == NULL) { assert(false);}
      /* We had a tag match earlier. Now compare log entry. */
      bool key_found = memcmp(&kv_ptr[op_i]->key, &write->key, KEY_SIZE) == 0;
      if (likely(key_found)) { //Cache Hit
        if (write->opcode == KVS_OP_PUT || write->opcode == OP_RELEASE ||
            write->opcode == OP_ACQUIRE) {
          assert(write->opcode != OP_ACQUIRE);
          KVS_updates_writes_or_releases_or_acquires(write, kv_ptr[op_i], t_id);
        }
        else if (ENABLE_RMWS) {
          if (write->opcode == ACCEPT_OP) {
            KVS_updates_accepts((struct accept*) write, kv_ptr[op_i], p_ops, op_i, t_id);
          }
          else if (write->opcode == COMMIT_OP || write->opcode == COMMIT_OP_NO_VAL) {
            KVS_updates_commits((struct commit*) write, kv_ptr[op_i], p_ops, op_i, t_id);
          }
          else if (ENABLE_ASSERTIONS) {
            my_printf(red, "Wrkr %u, kvs batch update: wrong opcode in kvs: %d, req %d, "
                        "m_id %u, val_len %u, version %u , \n",
                      t_id, write->opcode, op_i, write->m_id,
                      write->val_len, write->version);
            assert(false);
          }
        }
        else if (ENABLE_ASSERTIONS) assert(false);
      }
      else {  //Cache miss --> We get here if either tag or log key match failed
        if (ENABLE_ASSERTIONS) assert(false);
      }
  }
}

// The worker send here the incoming reads, the reads check the incoming base_ts if it is  bigger/equal to the local
// the just ack it, otherwise they send the value back
static inline void KVS_batch_op_reads(uint32_t op_num, uint16_t t_id, p_ops_t *p_ops,
                                      uint32_t pull_ptr, uint32_t max_op_size)
{
  uint16_t op_i;	/* I is batch index */
  struct read **reads = (struct read **) p_ops->ptrs_to_mes_ops;

  if (ENABLE_ASSERTIONS) assert(op_num <= MAX_INCOMING_R);
  unsigned int bkt[MAX_INCOMING_R];
  struct mica_bkt *bkt_ptr[MAX_INCOMING_R];
  unsigned int tag[MAX_INCOMING_R];
  mica_op_t *kv_ptr[MAX_INCOMING_R];	/* Ptr to KV item in log */
  /*
     * We first lookup the key in the datastore. The first two @I loops work
     * for both GETs and PUTs.
     */
  for(op_i = 0; op_i < op_num; op_i++) {
    struct read *read = reads[(pull_ptr + op_i) % max_op_size];
    if (unlikely(read->opcode == OP_ACQUIRE_FLIP_BIT)) continue; // This message is only meant to flip a bit and is thus a NO-OP
    KVS_locate_one_bucket(op_i, bkt, &read->key , bkt_ptr, tag, kv_ptr, KVS);
  }
  for(op_i = 0; op_i < op_num; op_i++) {
    struct read *read = reads[(pull_ptr + op_i) % max_op_size];
    if (unlikely(read->opcode == OP_ACQUIRE_FLIP_BIT)) continue;
    KVS_locate_one_kv_pair(op_i, tag, bkt_ptr, kv_ptr, KVS);
  }

  for(op_i = 0; op_i < op_num; op_i++) {
    struct read *read = reads[(pull_ptr + op_i) % max_op_size];
    if (read->opcode == OP_ACQUIRE_FLIP_BIT) {
      insert_r_rep(p_ops, p_ops->ptrs_to_mes_headers[op_i]->l_id, t_id,
                   p_ops->ptrs_to_mes_headers[op_i]->m_id,
                   p_ops->coalesce_r_rep[op_i], read->opcode);
      continue;
    }
    if(ENABLE_ASSERTIONS && kv_ptr[op_i] == NULL) {assert(false);}
    /* We had a tag match earlier. Now compare log entry. */
    bool key_found = memcmp(&kv_ptr[op_i]->key, &read->key, KEY_SIZE) == 0;
    if(likely(key_found)) { //Cache Hit
      check_state_with_allowed_flags(6, read->opcode, KVS_OP_GET, OP_ACQUIRE, OP_ACQUIRE_FP,
                                     OP_GET_TS, PROPOSE_OP);
      if (read->opcode == KVS_OP_GET || read->opcode == OP_ACQUIRE ||
        read->opcode == OP_ACQUIRE_FP) {
        KVS_reads_acquires_acquire_fp_and_reads(read, kv_ptr[op_i], p_ops, op_i, t_id);
      }
      else if (read->opcode == OP_GET_TS) {
        KVS_reads_get_TS(read, kv_ptr[op_i], p_ops, op_i, t_id);
      }
      else if (ENABLE_RMWS) {
        if (read->opcode == PROPOSE_OP) {
          KVS_reads_proposes(read, kv_ptr[op_i], p_ops, op_i, t_id);
        }
        else if (read->opcode == OP_ACQUIRE || read->opcode == OP_ACQUIRE_FP) {
          //assert(ENABLE_RMW_ACQUIRES);
//            KVS_reads_acquires_acquire_fp_and_reads(read, kv_ptr[op_i], p_ops, op_i, t_id);
        }
        else if (ENABLE_ASSERTIONS){
          //my_printf(red, "wrong Opcode in KVS: %d, req %d, m_id %u, val_len %u, version %u , \n",
          //           op->opcode, I, reads[(pull_ptr + I) % max_op_size]->m_id,
          //           reads[(pull_ptr + I) % max_op_size]->val_len,
          //          reads[(pull_ptr + I) % max_op_size]->version);
          assert(false);
        }
      }
      else if (ENABLE_ASSERTIONS) assert(false);
    }
    else {  //Cache miss --> We get here if either tag or log key match failed
      my_printf(red, "Opcode %u Kvs miss: bkt %u, server %u, tag %u \n",
                read->opcode, read->key.bkt, read->key.server, read->key.tag);
      assert(false); // cant have a miss since, it hit in the source's kvs
    }
  }
}

// The  worker sends (out-of-epoch) reads that received a higher timestamp and thus have to be applied as writes
// Could also be that the first round of an out-of-epoch write received a high TS
// All out of epoch reads/writes must come in to update the epoch
static inline void KVS_batch_op_first_read_round(uint16_t op_num, uint16_t t_id, r_info_t **writes,
                                                 p_ops_t *p_ops,
                                                 uint32_t pull_ptr, uint32_t max_op_size)
{
  uint16_t op_i;
  if (ENABLE_ASSERTIONS) assert(op_num <= MAX_INCOMING_R);
  unsigned int bkt[MAX_INCOMING_R];
  struct mica_bkt *bkt_ptr[MAX_INCOMING_R];
  unsigned int tag[MAX_INCOMING_R];
  mica_op_t *kv_ptr[MAX_INCOMING_R];	/* Ptr to KV item in log */
  /*
     * We first lookup the key in the datastore. The first two @I loops work
     * for both GETs and PUTs.
     */
  for(op_i = 0; op_i < op_num; op_i++) {
    struct key *op_key = &writes[(pull_ptr + op_i) % max_op_size]->key;
    KVS_locate_one_bucket(op_i, bkt, op_key, bkt_ptr, tag, kv_ptr, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);


  // the following variables used to validate atomicity between a lock-free r_rep of an object
  for(op_i = 0; op_i < op_num; op_i++) {
    r_info_t *r_info = writes[(pull_ptr + op_i) % max_op_size];
    if(ENABLE_ASSERTIONS && kv_ptr[op_i] == NULL) {assert(false);}
    /* We had a tag match earlier. Now compare log entry. */
    bool key_found = memcmp(&kv_ptr[op_i]->key, &r_info->key, KEY_SIZE) == 0;
    if(likely(key_found)) { //Cache Hit
      // The write must be performed with the max TS out of the one stored in the KV and read_info
      if (r_info->opcode == KVS_OP_PUT) {
        KVS_out_of_epoch_writes(r_info, kv_ptr[op_i], p_ops, t_id);
      }
      else if (r_info->opcode == UPDATE_EPOCH_OP_GET) {
        if (!MEASURE_SLOW_PATH && r_info->epoch_id > kv_ptr[op_i]->epoch_id) {
          lock_seqlock(&kv_ptr[op_i]->seqlock);
          kv_ptr[op_i]->epoch_id = r_info->epoch_id;
          unlock_seqlock(&kv_ptr[op_i]->seqlock);
          if (ENABLE_STAT_COUNTING) t_stats[t_id].rectified_keys++;
        }
      }
      else if (r_info->opcode == OP_ACQUIRE || KVS_OP_GET) {
        KVS_acquire_commits(r_info, kv_ptr[op_i], op_i, t_id);
      }
      else if (ENABLE_ASSERTIONS) assert(false);
    }
    else {  //Cache miss --> We get here if either tag or log key match failed
      if (ENABLE_ASSERTIONS) assert(false);
    }

    if (r_info->complete_flag) {
      if (ENABLE_ASSERTIONS) assert(&p_ops->read_info[r_info->r_ptr] == r_info);
      if (r_info->opcode == OP_ACQUIRE || r_info->opcode == KVS_OP_GET)
        memcpy(r_info->value_to_read, r_info->value, r_info->val_len);
      signal_completion_to_client(p_ops->r_session_id[r_info->r_ptr],
                                  p_ops->r_index_to_req_array[r_info->r_ptr], t_id);
      r_info->complete_flag = false;
    }
    else if (ENABLE_ASSERTIONS)
      check_state_with_allowed_flags(3, r_info->opcode, UPDATE_EPOCH_OP_GET,
                                     OP_ACQUIRE, OP_RELEASE);
  }

}


// Send an isolated write to the kvs-no batching
static inline void KVS_isolated_op(int t_id, write_t *write)
{
  int j;	/* I is batch index */

  unsigned int bkt;
  struct mica_bkt *bkt_ptr;
  unsigned int tag;
  mica_op_t *kv_ptr;	/* Ptr to KV item in log */
  /*
   * We first lookup the key in the datastore. The first two @I loops work
   * for both GETs and PUTs.
   */
//  trace_op_t *op = (trace_op_t*) (((void *) write) - 3);
  //print_key((struct key *) write->key);
  //printf("op bkt %u\n", op->key.bkt);
  bkt = write->key.bkt & KVS->bkt_mask;
  bkt_ptr = &KVS->ht_index[bkt];
  tag = write->key.tag;
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
    bool key_found = memcmp(&kv_ptr->key, &write->key, KEY_SIZE) == 0;
    if(key_found) { //Cache Hit
      if (ENABLE_ASSERTIONS) {
        if (write->opcode != OP_RELEASE) {
          my_printf(red, "Wrkr %u: KVS_isolated_op: wrong opcode : %d, m_id %u, val_len %u, version %u , \n",
                    t_id, write->opcode,  write->m_id,
                    write->val_len, write->version);
          assert(false);
        }
      }
      //my_printf(red, "op val len %d in ptr %d, total ops %d \n", op->val_len, (pull_ptr + I) % max_op_size, op_num );
      struct ts_tuple base_ts = {write->m_id, write->version};
      write_kv_if_conditional_on_ts(kv_ptr, write->value,
                                    (size_t) VALUE_SIZE, FROM_ISOLATED_OP,
                                    base_ts);
    }
  }
  else {  //Cache miss --> We get here if either tag or log key match failed
    if (ENABLE_ASSERTIONS) assert(false);
  }



}

#endif //KITE_KVS_UTILITY_H

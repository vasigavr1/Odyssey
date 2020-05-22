//
// Created by vasilis on 22/05/20.
//

#ifndef KITE_PAXOS_UTIL_H
#define KITE_PAXOS_UTIL_H

#include "paxos_generic_utility.h"


/*--------------------------------------------------------------------------
 * --------------------KVS_FILTERING--RECEIVING PROPOSE ACCEPT---------------
 * --------------------------------------------------------------------------*/

// Check the global RMW-id structure, to see if an RMW has already been committed
static inline bool the_rmw_has_committed(uint16_t glob_sess_id, uint64_t rmw_l_id,
                                         bool same_log, uint16_t t_id,
                                         struct rmw_rep_last_committed *rep)
{
  if (ENABLE_ASSERTIONS) assert(glob_sess_id < GLOBAL_SESSION_NUM);
  if (committed_glob_sess_rmw_id[glob_sess_id] >= rmw_l_id) {
    if (DEBUG_RMW)
      my_printf(green, "Worker %u: A Remote machine  is trying a propose with global sess_id %u, "
                  "rmw_id %lu, that has been already committed \n",
                t_id, glob_sess_id, rmw_l_id);
    rep->opcode = (uint8_t) (same_log ? RMW_ID_COMMITTED_SAME_LOG : RMW_ID_COMMITTED);
    return true;
  }
  else return false;
}

// Returns true if the received log-no is smaller than the committed.
static inline bool is_log_smaller_or_has_rmw_committed(uint32_t log_no, mica_op_t *kv_ptr,
                                                       uint64_t rmw_l_id,
                                                       uint16_t glob_sess_id, uint16_t t_id,
                                                       struct rmw_rep_last_committed *rep)
{
  check_log_nos_of_kv_ptr(kv_ptr, "is_log_smaller_or_has_rmw_committed", t_id);
  bool same_log = kv_ptr->last_committed_log_no == log_no;
  if (the_rmw_has_committed(glob_sess_id, rmw_l_id, same_log, t_id, rep)) {
    return true;
  }
  else if (kv_ptr->last_committed_log_no >= log_no ||
           kv_ptr->log_no > log_no) {
    if (DEBUG_RMW)
      my_printf(yellow, "Wkrk %u Log number is too small %u/%u entry state %u, propose/accept with rmw_lid %u,"
                  " global_sess_id %u\n", t_id, log_no, kv_ptr->last_committed_log_no,
                kv_ptr->state, rmw_l_id, glob_sess_id);
    rep->opcode = LOG_TOO_SMALL;
    fill_reply_entry_with_committed_RMW (kv_ptr, rep, t_id);
    return true;
  }
  else if (DEBUG_RMW) { // remote log is higher than the locally stored!
    if (kv_ptr->log_no < log_no )
      my_printf(yellow, "Wkrk %u Log number is higher than expected %u/%u, entry state %u, "
                  "propose/accept with rmw_lid %u, global_sess_id %u \n",
                t_id, log_no, kv_ptr->log_no,
                kv_ptr->state, rmw_l_id, glob_sess_id);
  }
  return false;
}


// Returns true if the received log is higher than the last committed log no + 1
static inline bool is_log_too_high(uint32_t log_no, mica_op_t *kv_ptr,
                                   uint16_t t_id,
                                   struct rmw_rep_last_committed *rep)
{
  check_log_nos_of_kv_ptr(kv_ptr, "is_log_too_high", t_id);
  // If the request is for the working log_no, it does not have to equal committed + 1
  // because we may have received an accept for log 10, w/o having committed log 9,
  // then it's okay to process the propose for log 10
  if (log_no > kv_ptr->log_no &&
      log_no > kv_ptr->last_committed_log_no + 1) {
    if (DEBUG_RMW)
      my_printf(yellow, "Wkrk %u Log number is too high %u/%u entry state %u \n",
                t_id, log_no, kv_ptr->last_committed_log_no,
                kv_ptr->state);
    rep->opcode = LOG_TOO_HIGH;
    return true;
  }
  else if (log_no > kv_ptr->last_committed_log_no + 1) {
    if (ENABLE_ASSERTIONS) {
      assert(log_no == kv_ptr->log_no);
      if (log_no != kv_ptr->accepted_log_no)
        printf("log_no %u, kv_ptr accepted_log_no %u, kv_ptr log no %u, kv_ptr->state %u \n",
               log_no, kv_ptr->accepted_log_no, kv_ptr->log_no, kv_ptr->state);
      //assert(log_no == kv_ptr->accepted_log_no);
      //assert(kv_ptr->state == ACCEPTED);
    }
  }
  return false;
}


/*--------------------------------------------------------------------------
 * --------------------RECEIVING REPLY-UTILITY-------------------------------------
 * --------------------------------------------------------------------------*/

// Search in the prepare entries for an lid (used when receiving a prep reply)
static inline int search_prop_entries_with_l_id(struct prop_info *prop_info, uint8_t state, uint64_t l_id)
{
  for (uint16_t i = 0; i < LOCAL_PROP_NUM; i++) {
    if (prop_info->entry[i].state == state &&
        prop_info->entry[i].l_id == l_id)
      return i;
  }
  return -1; // i.e. l_id not found!!

}



/*--------------------------------------------------------------------------
 * --------------------RMW-INIT---------------------------------------------
 * --------------------------------------------------------------------------*/

// If a local RMW managed to grab a kv_ptr, then it sets up its local entry
static inline void fill_loc_rmw_entry_on_grabbing_kv_ptr(struct pending_ops *p_ops,
                                                         struct rmw_local_entry *loc_entry,
                                                         uint32_t version, uint8_t state,
                                                         uint16_t sess_i, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    check_global_sess_id((uint8_t) machine_id, t_id,
                         (uint16_t) sess_i, loc_entry->rmw_id.glob_sess_id);
    check_version(version, "fill_loc_rmw_entry_on_grabbing_global");
  }
  loc_entry->help_loc_entry->state = INVALID_RMW;
  memset(&loc_entry->rmw_reps, 0, sizeof(struct rmw_rep_info));
  local_rmw_ack(loc_entry);
  loc_entry->state = state;
  loc_entry->epoch_id = (uint64_t) epoch_id;
  loc_entry->new_ts.version = version;
  loc_entry->new_ts.m_id = (uint8_t) machine_id;
}

// Initialize a local  RMW entry on the first time it gets allocated
static inline void init_loc_entry(struct kvs_resp* resp, struct pending_ops* p_ops,
                                  struct trace_op *prop,
                                  uint16_t t_id, struct rmw_local_entry* loc_entry)
{
  loc_entry->opcode = prop->opcode;
  if (ENABLE_ASSERTIONS) assert(prop->real_val_len <= RMW_VALUE_SIZE);
  if (opcode_is_compare_rmw(prop->opcode) || prop->opcode == RMW_PLAIN_WRITE)
    memcpy(loc_entry->value_to_write, prop->value_to_write, prop->real_val_len);
  loc_entry->killable = prop->opcode == COMPARE_AND_SWAP_WEAK;
  if (opcode_is_compare_rmw(prop->opcode))
    loc_entry->compare_val = prop->value_to_read; //expected value
  else if (prop->opcode == FETCH_AND_ADD)
    loc_entry->compare_val = prop->value_to_write; // value to be added


  loc_entry->must_release = ACCEPT_IS_RELEASE != 0; // TODO That can be a programmer input
  loc_entry->fp_detected = false;
  loc_entry->rmw_val_len = prop->real_val_len;
  loc_entry->rmw_is_successful = false;
  loc_entry->all_aboard = false;
  loc_entry->all_aboard_time_out = 0;
  memcpy(&loc_entry->key, &prop->key, TRUE_KEY_SIZE);
  memset(&loc_entry->rmw_reps, 0, sizeof(struct rmw_rep_info));
  loc_entry->kv_ptr = resp->kv_ptr;
  if (ENABLE_ASSERTIONS) {
    loc_entry->help_loc_entry->kv_ptr = resp->kv_ptr;
  }
  // loc_entry->sess_id = prop->session_id;
  loc_entry->index_to_req_array = prop->index_to_req_array;
  //loc_entry->accept_acks = 0;
  //loc_entry->accept_replies = 0;
  loc_entry->back_off_cntr = 0;
  loc_entry->log_too_high_cntr = 0;
  loc_entry->helping_flag = NOT_HELPING;
  // Give it an RMW-id as soon as it has a local entry, because the RMW must happen eventually
  loc_entry->rmw_id.id = p_ops->prop_info->l_id;
  if (ENABLE_ASSERTIONS) assert(loc_entry->rmw_id.id < B_4);
  loc_entry->l_id = p_ops->prop_info->l_id;
  loc_entry->help_loc_entry->l_id = p_ops->prop_info->l_id;
  loc_entry->rmw_id.glob_sess_id = get_glob_sess_id((uint8_t) machine_id, t_id, prop->session_id);
  loc_entry->accepted_log_no = 0;
  //my_printf(yellow, "Init  RMW-id %u glob_sess_id %u \n", loc_entry->rmw_id.id, loc_entry->rmw_id.glob_sess_id);
  //loc_entry->help_loc_entry->log_no = 0;
  loc_entry->help_loc_entry->state = INVALID_RMW;
}


// Activate the entry that belongs to a given key to initiate an RMW (either a local or a remote)
static inline void activate_kv_pair(uint8_t state, uint32_t new_version, mica_op_t *kv_ptr,
                                    uint8_t opcode, uint8_t new_ts_m_id, uint64_t l_id, uint16_t glob_sess_id,
                                    uint32_t log_no, uint16_t t_id, const char *message)
{
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->log_no == log_no && kv_ptr->state == ACCEPTED && state != ACCEPTED) {
      printf("%s \n", message);
      assert(false);
    }
    assert(kv_ptr->log_no <= log_no);
  }
  // pass the new ts!
  kv_ptr->opcode = opcode;
  kv_ptr->prop_ts.m_id = new_ts_m_id;
  //if (ENABLE_ASSERTIONS) assert(new_version >= kv_ptr->prop_ts.version);
  kv_ptr->prop_ts.version = new_version;
  //my_printf(cyan, "Kv_ptr version %u, m_id %u \n", kv_ptr->prop_ts.version, kv_ptr->new_ts.m_id);
  kv_ptr->rmw_id.glob_sess_id = glob_sess_id;
  kv_ptr->rmw_id.id = l_id;
  kv_ptr->state = state;
  kv_ptr->log_no = log_no;

  if (state == ACCEPTED) {
    if (ENABLE_ASSERTIONS) {
      assert(kv_ptr->prop_ts.version == new_version);
      assert(kv_ptr->prop_ts.m_id == new_ts_m_id);
    }
    kv_ptr->accepted_ts = kv_ptr->prop_ts;
    kv_ptr->accepted_log_no = log_no;
    if (ENABLE_ASSERTIONS)  kv_ptr->accepted_rmw_id = kv_ptr->rmw_id;
  }
  if (ENABLE_ASSERTIONS) {
    assert(kv_ptr->rmw_id.glob_sess_id < GLOBAL_SESSION_NUM);
    if (committed_glob_sess_rmw_id[kv_ptr->rmw_id.glob_sess_id] >= kv_ptr->rmw_id.id) {
      //my_printf(red, "Wrkr %u, attempts to activate with already committed RMW id %u/%u glob_sess id %u, state %u: %s \n",
      //           t_id, kv_ptr->rmw_id.id, committed_glob_sess_rmw_id[kv_ptr->rmw_id.glob_sess_id],
      //           kv_ptr->rmw_id.glob_sess_id, state, message);
    }
    assert(glob_sess_id < GLOBAL_SESSION_NUM);
    //assert(kv_ptr->new_ts.version % 2 == 0);
    assert(state == PROPOSED || state == ACCEPTED); // TODO accepted is allowed?
    assert(kv_ptr->last_committed_log_no < kv_ptr->log_no);
  }
}


/*--------------------------------------------------------------------------
 * --------------------REMOTE RMW REQUESTS-------------------------------------
 * --------------------------------------------------------------------------*/

// Look at the kv_ptr to answer to a propose message-- kv pair lock is held when calling this
static inline uint8_t propose_snoops_entry(struct propose *prop, mica_op_t *kv_ptr, uint8_t m_id,
                                           uint16_t t_id, struct rmw_rep_last_committed *rep)
{
  uint8_t return_flag;
  if (ENABLE_ASSERTIONS)  {
    assert(prop->opcode == PROPOSE_OP);
    assert(prop->log_no > kv_ptr->last_committed_log_no);
    assert(prop->log_no == kv_ptr->log_no);
  }

  if (ENABLE_ASSERTIONS)
    assert(check_entry_validity_with_key(&prop->key, kv_ptr));
  enum ts_compare prop_ts_comp = compare_netw_ts_with_ts(&prop->ts, &kv_ptr->prop_ts);

  if (prop_ts_comp == GREATER) {
    assign_netw_ts_to_ts(&kv_ptr->prop_ts, &prop->ts);
    enum ts_compare acc_ts_comp = compare_netw_ts_with_ts(&prop->ts, &kv_ptr->accepted_ts);
    if (kv_ptr->state == ACCEPTED && acc_ts_comp == GREATER) {
      if (kv_ptr->rmw_id.glob_sess_id == prop->glob_sess_id) {
        return_flag = RMW_ACK_ACC_SAME_RMW;
      }
      else {
        assign_ts_to_netw_ts(&rep->ts, &kv_ptr->accepted_ts);
        return_flag = SEEN_LOWER_ACC;
        rep->rmw_id = kv_ptr->rmw_id.id;
        rep->glob_sess_id = kv_ptr->rmw_id.glob_sess_id;
        memcpy(rep->value, kv_ptr->last_accepted_value, (size_t) RMW_VALUE_SIZE);
        rep->log_no_or_base_version = kv_ptr->base_acc_ts.version;
        rep->base_m_id = kv_ptr->base_acc_ts.m_id;
      }
    }
    else return_flag = RMW_ACK;
  }
  else {
    return_flag = SEEN_HIGHER_PROP;
    assign_ts_to_netw_ts(&rep->ts, &kv_ptr->prop_ts);
  }

  check_state_with_allowed_flags(5, return_flag, RMW_ACK, RMW_ACK_ACC_SAME_RMW,
                                 SEEN_HIGHER_PROP, SEEN_LOWER_ACC);
  return return_flag;
}


// Look at an RMW entry to answer to an accept message-- kv pair lock is held when calling this
static inline uint8_t accept_snoops_entry(struct accept *acc, mica_op_t *kv_ptr, uint8_t sender_m_id,
                                          uint16_t t_id, struct rmw_rep_last_committed *rep)
{
  uint8_t return_flag = RMW_ACK;

  if (ENABLE_ASSERTIONS)  {
    assert(acc->opcode == ACCEPT_OP);
    assert(acc->log_no > kv_ptr->last_committed_log_no);
    assert(acc->log_no == kv_ptr->log_no);
    assert(check_entry_validity_with_key(&acc->key, kv_ptr));
  }

  if (kv_ptr->state != INVALID_RMW) {
    // Higher Ts  = Success,  Lower Ts  = Failure
    enum ts_compare ts_comp = compare_netw_ts_with_ts(&acc->ts, &kv_ptr->prop_ts);
    // Higher Ts  = Success
    if (ts_comp == EQUAL || ts_comp == GREATER) {
      return_flag = RMW_ACK;
      if (ENABLE_ASSERTIONS) {
        if (DEBUG_RMW && ts_comp == EQUAL && kv_ptr->state == ACCEPTED)
          my_printf(red, "Wrkr %u Received Accept for the same TS as already accepted, "
                      "version %u/%u m_id %u/%u, rmw_id %u/%u, global_sess_id %u/%u \n",
                    t_id, acc->ts.version, kv_ptr->prop_ts.version, acc->ts.m_id,
                    kv_ptr->prop_ts.m_id, acc->t_rmw_id, kv_ptr->rmw_id.id,
                    acc->glob_sess_id, kv_ptr->rmw_id.glob_sess_id);
      }
    }
    else if (ts_comp == SMALLER) {
      if (kv_ptr->state == PROPOSED) {
        //reply_rmw->ts = kv_ptr-> new_ts;
        return_flag = SEEN_HIGHER_PROP;
      }
      else if (kv_ptr->state == ACCEPTED) {
        //memcpy(reply_rmw->value, kv_ptr->value, (size_t) RMW_VALUE_SIZE);
        //reply_rmw->ts = kv_ptr->new_ts; // Here you dont respond with Accepted-TS but with Proposed-TS
        //assign_second_rmw_id_to_first(&reply_rmw->rmw_id, &kv_ptr->rmw_id);
        return_flag = SEEN_HIGHER_ACC;
      }
      else if (ENABLE_ASSERTIONS) assert(false);
      assign_ts_to_netw_ts(&rep->ts, &kv_ptr->prop_ts);
    }
    else if (ENABLE_ASSERTIONS) assert(false);
  }

  if (DEBUG_RMW)
    my_printf(yellow, "Wrkr %u: %s Accept with rmw_id %u, glob_sess_id %u, log_no: %u, ts.version: %u, ts_m_id %u,"
                "locally stored state: %u, locally stored ts: version %u, m_id %u \n",
              t_id, return_flag == RMW_ACK ? "Acks" : "Nacks",
              acc->t_rmw_id,  acc->glob_sess_id, acc->log_no,
              acc->ts.version, acc->ts.m_id, kv_ptr->state, kv_ptr->prop_ts.version,
              kv_ptr->prop_ts.m_id);

  if (ENABLE_ASSERTIONS) assert(return_flag == RMW_ACK || rep->ts.version > 0);
  return return_flag;
}

//Handle a remote propose/accept whose log number is big enough
static inline uint8_t handle_remote_prop_or_acc_in_kvs(mica_op_t *kv_ptr, void *prop_or_acc,
                                                       uint8_t sender_m_id, uint16_t t_id,
                                                       struct rmw_rep_last_committed *rep, uint32_t log_no,
                                                       bool is_prop)
{
  uint8_t flag;
  // if the log number is higher than expected blindly ack
  if (log_no > kv_ptr->log_no) {
    check_that_log_is_high_enough(kv_ptr, log_no);
    flag = RMW_ACK;
  }
  else
    flag = is_prop ? propose_snoops_entry((struct propose *)prop_or_acc, kv_ptr, sender_m_id, t_id, rep) :
           accept_snoops_entry((struct accept *) prop_or_acc, kv_ptr, sender_m_id, t_id, rep);
  return flag;
}


/*--------------------------------------------------------------------------
 * --------------------ACCEPTING-------------------------------------
 * --------------------------------------------------------------------------*/


// After gathering a quorum of proposal acks, check if you can accept locally-- THIS IS STRICTLY LOCAL RMWS -- no helps
// Every RMW that gets committed must pass through this function successfully (at least one time)
static inline uint8_t attempt_local_accept(struct pending_ops *p_ops, struct rmw_local_entry *loc_entry,
                                           uint16_t t_id)
{
  uint8_t return_flag;
  mica_op_t *kv_ptr = loc_entry->kv_ptr;
  my_assert(keys_are_equal(&loc_entry->key, &kv_ptr->key),
            "Attempt local accept: Local entry does not contain the same key as kv_ptr");

  if (ENABLE_ASSERTIONS) assert(loc_entry->rmw_id.glob_sess_id < GLOBAL_SESSION_NUM);

  lock_seqlock(&loc_entry->kv_ptr->seqlock);
  if (if_already_committed_bcast_commits(p_ops, loc_entry, t_id)) {
    //if (loc_entry->rmw_id.id <= committed_glob_sess_rmw_id[loc_entry->rmw_id.glob_sess_id]) {
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
    return NACK_ALREADY_COMMITTED;
  }

  if (rmw_ids_are_equal(&loc_entry->rmw_id, &kv_ptr->rmw_id) &&
      kv_ptr->state != INVALID_RMW &&
      compare_ts(&loc_entry->new_ts, &kv_ptr->prop_ts) == EQUAL) {
    if (ENABLE_ASSERTIONS) {
      //assert(compare_ts(&loc_entry->new_ts, &kv_ptr->prop_ts) == EQUAL);
      assert(kv_ptr->log_no == loc_entry->log_no);
      assert(kv_ptr->last_committed_log_no == loc_entry->log_no - 1);
    }
    //state would be typically proposed, but may also be accepted if someone has helped
    if (DEBUG_RMW)
      my_printf(green, "Wrkr %u got rmw id %u, glob sess %u accepted locally \n",
                t_id, loc_entry->rmw_id.id, loc_entry->rmw_id.glob_sess_id);
    kv_ptr->state = ACCEPTED;
    // calculate the new value depending on the type of RMW
    perform_the_rmw_on_the_loc_entry(loc_entry, kv_ptr, t_id);
    // we need to remember the last accepted value
    if (loc_entry->rmw_is_successful) {
      memcpy(kv_ptr->last_accepted_value, loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
    }
    else {
      memcpy(kv_ptr->last_accepted_value, loc_entry->value_to_read, (size_t) RMW_VALUE_SIZE);
    }
    //when last_accepted_value is update also update the acc_base_ts
    kv_ptr->base_acc_ts = kv_ptr->ts;
    loc_entry->accepted_log_no = kv_ptr->log_no;
    kv_ptr->accepted_ts = loc_entry->new_ts;
    kv_ptr->accepted_log_no = kv_ptr->log_no;
    if (ENABLE_ASSERTIONS) {
      assert(loc_entry->accepted_log_no == loc_entry->log_no);
      assert(loc_entry->log_no == kv_ptr->last_committed_log_no + 1);
      assert(compare_ts(&kv_ptr->prop_ts, &kv_ptr->accepted_ts) != SMALLER);
      kv_ptr->accepted_rmw_id = kv_ptr->rmw_id;
    }
    if (ENABLE_DEBUG_GLOBAL_ENTRY) {
      //kv_ptr->dbg->proposed_ts = loc_entry->new_ts;
      //kv_ptr->dbg->proposed_log_no = loc_entry->log_no;
      //kv_ptr->dbg->proposed_rmw_id = loc_entry->rmw_id;
    }
    check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept and succeed", t_id);
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
    return_flag = ACCEPT_ACK;
  }
  else { // the entry stores a different rmw_id and thus our proposal has been won by another
    // Some other RMW has won the RMW we are trying to get accepted
    // If the other RMW has been committed then the last_committed_log_no will be bigger/equal than the current log no
    if (kv_ptr->last_committed_log_no >= loc_entry->log_no)
      return_flag = NACK_ACCEPT_LOG_OUT_OF_DATE;
      //Otherwise the RMW that won is still in progress
    else return_flag = NACK_ACCEPT_SEEN_HIGHER_TS;
    if (DEBUG_RMW)
      my_printf(green, "Wrkr %u failed to get rmw id %u, glob sess %u accepted locally opcode %u,"
                  "kv_ptr rmw id %u, glob sess %u, state %u \n",
                t_id, loc_entry->rmw_id.id, loc_entry->rmw_id.glob_sess_id, return_flag,
                kv_ptr->rmw_id.id, kv_ptr->rmw_id.glob_sess_id, kv_ptr->state);
    // --CHECKS--
    if (ENABLE_ASSERTIONS) {
      if (kv_ptr->state == PROPOSED || kv_ptr->state == ACCEPTED) {
        if(!(compare_ts(&kv_ptr->prop_ts, &loc_entry->new_ts) == GREATER ||
             kv_ptr->log_no > loc_entry->log_no)) {
          my_printf(red, "State: %s,  loc-entry-helping %d, Kv prop/ts %u/%u -- loc-entry ts %u/%u, "
                      "kv-log/loc-log %u/%u kv-rmw_id/loc-rmw-id %u/%u\n",
                    kv_ptr->state == ACCEPTED ? "ACCEPTED" : "PROPOSED",
                    loc_entry->helping_flag,
                    kv_ptr->prop_ts.version, kv_ptr->prop_ts.m_id,
                    loc_entry->new_ts.version, loc_entry->new_ts.m_id,
                    kv_ptr->log_no, loc_entry->log_no,
                    kv_ptr->rmw_id.id, loc_entry->rmw_id.id);
          assert(false);
        }
      }
      else if (kv_ptr->state == INVALID_RMW) // some other rmw committed
        assert(kv_ptr->last_committed_log_no >= loc_entry->log_no);
      // The RMW can have been committed, because some may have helped it
      //assert (committed_glob_sess_rmw_id[loc_entry->rmw_id.glob_sess_id] < loc_entry->rmw_id.id);
    }


    check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept and fail", t_id);
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
  }
  return return_flag;
}

// After gathering a quorum of proposal reps, one of them was a lower TS accept, try and help it
static inline uint8_t attempt_local_accept_to_help(struct pending_ops *p_ops, struct rmw_local_entry *loc_entry,
                                                   uint16_t t_id)
{
  uint8_t return_flag;
  mica_op_t *kv_ptr = loc_entry->kv_ptr;
  struct rmw_local_entry* help_loc_entry = loc_entry->help_loc_entry;
  help_loc_entry->new_ts = loc_entry->new_ts;
  my_assert(keys_are_equal(&help_loc_entry->key, &kv_ptr->key),
            "Attempt local accpet to help: Local entry does not contain the same key as kv_ptr");
  my_assert(loc_entry->help_loc_entry->log_no == loc_entry->log_no,
            " the help entry and the regular have not the same log nos");
  if (ENABLE_ASSERTIONS) assert(help_loc_entry->rmw_id.glob_sess_id < GLOBAL_SESSION_NUM);
  lock_seqlock(&loc_entry->kv_ptr->seqlock);


  //We don't need to check the registered RMW here -- it's not wrong to do so--
  // but if the RMW has been committed, it will be in the present log_no
  // and we will not be able to accept locally anyway.

  // the kv_ptr has seen a higher ts if
  // (its state is not invalid and its TS is higher) or  (it has committed the log)
  enum ts_compare comp = compare_ts(&kv_ptr->prop_ts, &loc_entry->new_ts);


  bool kv_ptr_is_the_same = kv_ptr->state == PROPOSED  &&
                            help_loc_entry->log_no == kv_ptr->log_no &&
                            comp == EQUAL &&
                            rmw_ids_are_equal(&kv_ptr->rmw_id, &loc_entry->rmw_id);

  bool kv_ptr_is_invalid_but_not_committed = kv_ptr->state == INVALID_RMW &&
                                             kv_ptr->last_committed_log_no < help_loc_entry->log_no;// && comp != GREATER;

  bool helping_stuck_accept = loc_entry->helping_flag == PROPOSE_NOT_LOCALLY_ACKED &&
                              help_loc_entry->log_no == kv_ptr->log_no &&
                              kv_ptr->state == ACCEPTED &&
                              rmw_ids_are_equal(&kv_ptr->rmw_id, &help_loc_entry->rmw_id) &&
                              comp != GREATER;
  // When retrying after accepts fail, i must first send proposes but if the local state is still accepted,
  // i can't downgrade it to proposed, so if i am deemed to help another RMW, i may come back to find
  // my original Accept still here
  bool propose_locally_accepted = kv_ptr->state == ACCEPTED  &&
                                  loc_entry->log_no == kv_ptr->log_no &&
                                  comp == EQUAL &&
                                  rmw_ids_are_equal(&kv_ptr->rmw_id, &loc_entry->rmw_id);

  if (kv_ptr_is_the_same   || kv_ptr_is_invalid_but_not_committed ||
      helping_stuck_accept || propose_locally_accepted) {
    checks_and_prints_local_accept_help(loc_entry, help_loc_entry, kv_ptr, kv_ptr_is_the_same,
                                        kv_ptr_is_invalid_but_not_committed,
                                        helping_stuck_accept, propose_locally_accepted, t_id);
    kv_ptr->state = ACCEPTED;
    assign_second_rmw_id_to_first(&kv_ptr->rmw_id, &help_loc_entry->rmw_id);
    kv_ptr->accepted_ts = help_loc_entry->new_ts;
    kv_ptr->accepted_log_no = kv_ptr->log_no;
    if (ENABLE_ASSERTIONS) {
      assert(compare_ts(&kv_ptr->prop_ts, &kv_ptr->accepted_ts) != SMALLER);
      kv_ptr->accepted_rmw_id = kv_ptr->rmw_id;
    }
    memcpy(kv_ptr->last_accepted_value, help_loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
    kv_ptr->base_acc_ts = help_loc_entry->base_ts;// the base ts of the RMW we are helping
    check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept_to_help and succeed", t_id);
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
    return_flag = ACCEPT_ACK;
  }
  else {
    //If the log entry is committed locally abort the help
    //if the kv_ptr has accepted a higher TS then again abort help
    return_flag = ABORT_HELP;
    if (DEBUG_RMW)// || (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED))
      my_printf(green, "Wrkr %u sess %u failed to get rmw id %u, glob sess %u accepted locally opcode %u,"
                  "kv_ptr rmw id %u, glob sess %u, state %u \n",
                t_id, loc_entry->sess_id, loc_entry->rmw_id.id, loc_entry->rmw_id.glob_sess_id, return_flag,
                kv_ptr->rmw_id.id, kv_ptr->rmw_id.glob_sess_id, kv_ptr->state);


    check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept_to_help and fail", t_id);
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
  }
  return return_flag;
}


/*--------------------------------------------------------------------------
 * --------------------COMMITING-------------------------------------
 * --------------------------------------------------------------------------*/

// Write the committed log in the kv_ptr and the new value in the KVS
static inline void take_actions_to_commit_rmw(struct rmw_local_entry *loc_entry_to_commit,
                                              struct rmw_local_entry *loc_entry, // this may be different if helping
                                              uint16_t t_id)
{
  /*
  if (ENABLE_DEBUG_GLOBAL_ENTRY) {
      if (loc_entry->helping_flag == NOT_HELPING)
        kv_ptr->dbg->last_committed_flag = LOCAL_RMW;
      else kv_ptr->dbg->last_committed_flag = LOCAL_RMW_FROM_HELP;
    kv_ptr->dbg->last_committed_ts = loc_entry_to_commit->new_ts;
    kv_ptr->dbg->last_committed_log_no = loc_entry_to_commit->log_no;
    kv_ptr->dbg->last_committed_rmw_id = loc_entry_to_commit->rmw_id;
  }*/
  mica_op_t *kv_ptr = loc_entry->kv_ptr;

  kv_ptr->last_committed_log_no = loc_entry_to_commit->log_no;
  kv_ptr->last_committed_rmw_id = loc_entry_to_commit->rmw_id;
  bool overwrite_kv = compare_ts(&loc_entry_to_commit->base_ts, &kv_ptr->ts) != SMALLER;
  // Update the KVS if the base ts of the entry to be committed is equal or greater than the locally stored
  // Beyond that there are two cases:
  // 1. When not helping and the RMW is successful
  // 2  When helping
  if (loc_entry->rmw_is_successful && loc_entry->helping_flag != HELPING) {
    update_commit_logs(t_id, kv_ptr->key.bkt, loc_entry_to_commit->log_no,
                       kv_ptr->value, loc_entry_to_commit->value_to_write,
                       " local_commit", LOG_COMS);
    if (overwrite_kv) memcpy(kv_ptr->value, loc_entry_to_commit->value_to_write, loc_entry->rmw_val_len);
  }
  else if (loc_entry->helping_flag == HELPING) {
    update_commit_logs(t_id, kv_ptr->key.bkt, loc_entry_to_commit->log_no,
                       kv_ptr->value, loc_entry_to_commit->value_to_write,
                       " local_commit", LOG_COMS);
    if (overwrite_kv) memcpy(kv_ptr->value, loc_entry_to_commit->value_to_write, (size_t) RMW_VALUE_SIZE);
  }
  if (overwrite_kv) {
    kv_ptr->ts.m_id = loc_entry_to_commit->base_ts.m_id;
    kv_ptr->ts.version = loc_entry_to_commit->base_ts.version;
  }

}


// Call holding the kv_lock to commit an RMW that is initiated locally (helped or from local session)
static inline void commit_helped_or_local_from_loc_entry(mica_op_t *kv_ptr,
                                                         struct rmw_local_entry *loc_entry,
                                                         struct rmw_local_entry *loc_entry_to_commit, uint16_t t_id)
{
  // If the RMW has not been committed yet locally, commit it if it is not helping,
  // Otherwise, stay in 'accepted' state until a quorum of commit-acks come back, then commit
  if (kv_ptr->last_committed_log_no < loc_entry_to_commit->log_no) {
    take_actions_to_commit_rmw(loc_entry_to_commit, loc_entry, t_id);
    if (DEBUG_RMW)
      my_printf(green, "Wrkr %u got rmw id %u, kv_ptr sess %u, log %u committed locally,"
                  "kv_ptr stats: state %u, rmw_id &u, glob sess_id %u, log no %u  \n",
                t_id, loc_entry_to_commit->rmw_id.id, loc_entry_to_commit->rmw_id.glob_sess_id, loc_entry_to_commit->log_no,
                kv_ptr->state, kv_ptr->rmw_id.id, kv_ptr->rmw_id.glob_sess_id, kv_ptr->log_no);
  }
  // Check if the entry is still working on that log, or whether it has moved on
  // (because the current rmw is already committed)
  if (kv_ptr->log_no == loc_entry_to_commit->log_no && kv_ptr->state != INVALID_RMW) {
    //the log numbers should match
    if (ENABLE_ASSERTIONS) {
      if (!rmw_ids_are_equal(&loc_entry_to_commit->rmw_id, &kv_ptr->rmw_id)) {
        my_printf(red, "Wrkr %u kv_ptr is on same log as what is about to be committed but on different rmw-id:"
                    " committed rmw id %u, glob sess %u, "
                    "kv_ptr rmw id %u, glob sess %u, state %u,"
                    " committed version %u/%u m_id %u/%u \n",
                  t_id, loc_entry_to_commit->rmw_id.id, loc_entry_to_commit->rmw_id.glob_sess_id,
                  kv_ptr->rmw_id.id, kv_ptr->rmw_id.glob_sess_id, kv_ptr->state,
                  loc_entry_to_commit->new_ts.version, kv_ptr->prop_ts.version,
                  loc_entry_to_commit->new_ts.m_id, kv_ptr->prop_ts.m_id);// this is a hard error
      }
      assert(rmw_ids_are_equal(&loc_entry_to_commit->rmw_id, &kv_ptr->last_committed_rmw_id));
      assert(kv_ptr->last_committed_log_no == kv_ptr->log_no);
      if (kv_ptr->state != ACCEPTED) {
        my_printf(red, "Wrkr %u, sess %u  Logs are equal, rmw-ids are equal "
          "but state is not accepted %u \n", t_id, loc_entry->sess_id, kv_ptr->state);
        assert(false);
      }
      assert(kv_ptr->state == ACCEPTED);
    }
    kv_ptr->state = INVALID_RMW;
  }
  else {
    // if the log has moved on then the RMW has been helped,
    // it has been committed in the other machines so there is no need to change its state
    check_log_nos_of_kv_ptr(kv_ptr, "commit_helped_or_local_from_loc_entry", t_id);
    if (ENABLE_ASSERTIONS) {
      if (kv_ptr->state != INVALID_RMW)
        assert(!rmw_ids_are_equal(&kv_ptr->rmw_id, &loc_entry_to_commit->rmw_id));
    }
  }
}


// The commitment of the rmw_id is certain here: it has either already been committed or it will be committed here
// Additionally we will always broadcast commits because we need to make sure that
// a quorum of machines have seen the RMW before, committing the RMW
static inline void attempt_local_commit(struct pending_ops *p_ops, struct rmw_local_entry *loc_entry,
                                        uint16_t t_id)
{
  assert(loc_entry->helping_flag != HELP_PREV_COMMITTED_LOG_TOO_HIGH);
  // use only index_to_rmw and kv_ptr from the loc_entry
  struct rmw_local_entry *loc_entry_to_commit =
    loc_entry->helping_flag == HELPING ? loc_entry->help_loc_entry : loc_entry;


  mica_op_t *kv_ptr = loc_entry->kv_ptr;
  my_assert(keys_are_equal(&loc_entry->key, &kv_ptr->key),
            "Attempt local commit: Local entry does not contain the same key as kv_ptr");

  lock_seqlock(&loc_entry->kv_ptr->seqlock);
  if (loc_entry->state == COMMITTED)
    commit_helped_or_local_from_loc_entry(kv_ptr, loc_entry, loc_entry_to_commit, t_id);
  // Register the RMW-id
  register_committed_global_sess_id(loc_entry_to_commit->rmw_id.glob_sess_id,
                                    loc_entry_to_commit->rmw_id.id, t_id);
  check_registered_against_kv_ptr_last_committed(kv_ptr, loc_entry_to_commit->rmw_id.id,
                                                 loc_entry_to_commit->rmw_id.glob_sess_id,
                                                 "attempt_local_commit", t_id);

  unlock_seqlock(&loc_entry->kv_ptr->seqlock);
  if (DEBUG_RMW)
    my_printf(green, "Wrkr %u will broadcast commits for rmw id %u, glob sess %u, "
                "kv_ptr rmw id %u, glob sess %u, state %u \n",
              t_id, loc_entry_to_commit->rmw_id.id, loc_entry_to_commit->rmw_id.glob_sess_id,
              kv_ptr->rmw_id.id, kv_ptr->rmw_id.glob_sess_id, kv_ptr->state);

  if (DEBUG_LOG)
    my_printf(green, "Log %u: RMW_id %u glob_sess %u, loc entry state %u, local commit\n",
              loc_entry_to_commit->log_no, loc_entry_to_commit->rmw_id.id,
              loc_entry_to_commit->rmw_id.glob_sess_id, loc_entry->state);
}

// A reply to a propose/accept may include an RMW to be committed,
static inline void attempt_local_commit_from_rep(struct pending_ops *p_ops, struct rmw_rep_last_committed *rmw_rep,
                                                 struct rmw_local_entry* loc_entry, uint16_t t_id)
{
  check_ptr_is_valid_rmw_rep(rmw_rep);
  mica_op_t *kv_ptr = loc_entry->kv_ptr;
  uint32_t new_log_no = rmw_rep->log_no_or_base_version;
  uint64_t new_rmw_id = rmw_rep->rmw_id;
  uint16_t new_glob_sess_id = rmw_rep->glob_sess_id;
  if (kv_ptr->last_committed_log_no >= new_log_no) return;
  my_assert(keys_are_equal(&loc_entry->key, &kv_ptr->key),
            "Attempt local commit from rep: Local entry does not contain the same key as kv_ptr");


  lock_seqlock(&kv_ptr->seqlock);
  // If the RMW has not been committed yet locally, commit it
  if (kv_ptr->last_committed_log_no < new_log_no) {
    //if (ENABLE_DEBUG_GLOBAL_ENTRY) kv_ptr->dbg->last_committed_flag = REMOTE_RMW_FROM_REP;
    kv_ptr->last_committed_log_no = new_log_no;
    kv_ptr->last_committed_rmw_id.id = new_rmw_id;
    kv_ptr->last_committed_rmw_id.glob_sess_id = new_glob_sess_id;
    update_commit_logs(t_id, kv_ptr->key.bkt, new_log_no, kv_ptr->value,
                       rmw_rep->value, "From rep ", LOG_COMS);

    if (compare_ts_with_netw_ts(&kv_ptr->ts, &rmw_rep->ts) != GREATER) {
      memcpy(kv_ptr->value, rmw_rep->value, (size_t) RMW_VALUE_SIZE);
      kv_ptr->ts.m_id = rmw_rep->ts.m_id;
      kv_ptr->ts.version = rmw_rep->ts.version;
    }


    if (DEBUG_RMW)
      my_printf(green, "Wrkr %u commits locally rmw id %u, glob sess %u after resp with opcode %u \n",
                t_id, new_rmw_id, new_glob_sess_id, rmw_rep->opcode);
    // if the kv_ptr was working on an already committed log, or
    // if it's not active advance its log no and in both cases transition to INVALID RMW
    if (kv_ptr->log_no <= new_log_no ||
        rmw_id_is_equal_with_id_and_glob_sess_id(&kv_ptr->rmw_id, new_rmw_id, new_glob_sess_id)) {
      kv_ptr->log_no = new_log_no;
      kv_ptr->state = INVALID_RMW;
    }
  }
  else if (kv_ptr->last_committed_log_no == new_log_no) {
    check_that_the_rmw_ids_match(kv_ptr,  new_rmw_id, new_glob_sess_id, new_log_no, rmw_rep->ts.version,
                                 rmw_rep->ts.m_id, "attempt_local_commit_from_rep", t_id);
  }
  check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_commit_from_rep", t_id);
  check_local_commit_from_rep(kv_ptr, loc_entry, rmw_rep, t_id);

  register_committed_global_sess_id(new_glob_sess_id, new_rmw_id, t_id);
  check_registered_against_kv_ptr_last_committed(kv_ptr, new_rmw_id, new_glob_sess_id,
                                                 "attempt_local_commit_from_rep", t_id);
  unlock_seqlock(&kv_ptr->seqlock);


  if (DEBUG_LOG)
    my_printf(green, "Log %u: RMW_id %u glob_sess %u, loc entry state %u from reply \n",
              new_log_no, new_rmw_id, new_glob_sess_id, loc_entry->state);

}

// Check if the commit must be applied to the KVS and
// transition the kv_ptr to INVALID_RMW if it has been waiting for this commit
static inline bool attempt_remote_commit(mica_op_t *kv_ptr, struct commit *com,
                                         struct read_info *r_info, bool use_commit,
                                         uint16_t t_id)
{
  uint32_t new_log_no = use_commit ? com->log_no : r_info->log_no;
  uint64_t new_rmw_id = use_commit ? com->t_rmw_id : r_info->rmw_id.id;
  uint16_t glob_sess_id = use_commit ? com->glob_sess_id : r_info->rmw_id.glob_sess_id;
  uint32_t new_version = use_commit ? com->base_ts.version : r_info->ts_to_read.version;
  uint8_t new_m_id = use_commit ? com->base_ts.m_id : r_info->ts_to_read.m_id;
  // the function is called with the lock in hand
  bool is_log_higher = false;
  // First check if that log no (or a higher) has been committed
  if (kv_ptr->last_committed_log_no < new_log_no) {
    is_log_higher = true;
    //if (ENABLE_DEBUG_GLOBAL_ENTRY) kv_ptr->dbg->last_committed_flag = REMOTE_RMW;
    kv_ptr->last_committed_log_no = new_log_no;
    kv_ptr->last_committed_rmw_id.id = new_rmw_id;
    kv_ptr->last_committed_rmw_id.glob_sess_id = glob_sess_id;
    if (DEBUG_LOG)
      my_printf(green, "Log %u: RMW_id %u glob_sess %u ,from remote_commit \n",
                new_log_no, new_rmw_id, glob_sess_id);
  }
  else if (kv_ptr->last_committed_log_no == new_log_no) {
    check_that_the_rmw_ids_match(kv_ptr,  new_rmw_id, glob_sess_id, new_log_no,
                                 new_version, new_m_id, "attempt_remote_commit", t_id);
  }

  // now check if the entry was waiting for this message to get cleared
  if (kv_ptr->state != INVALID_RMW && (kv_ptr->log_no <= new_log_no ||
                                       rmw_id_is_equal_with_id_and_glob_sess_id(&kv_ptr->rmw_id, new_rmw_id, glob_sess_id))) {
    kv_ptr->state = INVALID_RMW;
  }
  else if (kv_ptr->log_no > new_log_no && kv_ptr->state != INVALID_RMW) {
    if (kv_ptr->rmw_id.id == new_rmw_id && kv_ptr->rmw_id.glob_sess_id == glob_sess_id)
      my_printf(red, "Wrkr %u, committed rmw_id %u and glob ses id %u on log no %u, but the kv_ptr is working on "
                  "log %u, state %u rmw_id %u glob sess id %u \n", t_id, new_rmw_id, glob_sess_id, new_log_no,
                kv_ptr->log_no, kv_ptr->state, kv_ptr->rmw_id.id,  kv_ptr->rmw_id.glob_sess_id);
  }
  return is_log_higher;
}

static inline uint64_t handle_remote_commit_message(mica_op_t *kv_ptr, void* op, bool use_commit, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) if (!use_commit) assert(ENABLE_RMW_ACQUIRES);
  bool is_log_higher;
  struct read_info * r_info = (struct read_info*) op;
  struct commit *com = (struct commit*) op;
  uint64_t rmw_l_id = !use_commit ? r_info->rmw_id.id : com->t_rmw_id;
  uint16_t glob_sess_id = !use_commit ? r_info->rmw_id.glob_sess_id : com->glob_sess_id;
  uint32_t log_no = !use_commit ? r_info->log_no : com->log_no;
  struct key *key = !use_commit ? &r_info->key : &com->key;
  uint8_t *value = !use_commit ? r_info->value : com->value;
  uint32_t version = !use_commit ? r_info->ts_to_read.version : com->base_ts.version;
  uint8_t m_id = !use_commit ? r_info->ts_to_read.m_id : com->base_ts.m_id;
  uint32_t entry;

  lock_seqlock(&kv_ptr->seqlock);
  check_keys_with_one_trace_op(key, kv_ptr);
  is_log_higher = attempt_remote_commit(kv_ptr, com, r_info, use_commit, t_id);

//  else if (ENABLE_ASSERTIONS) assert(false);
  // The commit must be applied to the KVS if
  //   1. the commit has a higher base-ts than the kv or
  //   2. the commit has the same base with the kv, but higher log
  // But if the kv-ptr has a greater base-ts, then its value should not be overwritten
  if (is_log_higher) {
    if (compare_ts_with_flat(&kv_ptr->ts, version, m_id) != GREATER) {
      kv_ptr->ts.m_id = m_id;
      kv_ptr->ts.version = version;
      memcpy(kv_ptr->value, value, (size_t) RMW_VALUE_SIZE);
    }
    update_commit_logs(t_id, kv_ptr->key.bkt, log_no, kv_ptr->value,
                       value, "From remote commit ", LOG_COMS);
  }
  check_log_nos_of_kv_ptr(kv_ptr, "Unlocking after received commit", t_id);
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->state != INVALID_RMW)
      assert(!rmw_id_is_equal_with_id_and_glob_sess_id(&kv_ptr->rmw_id, rmw_l_id, glob_sess_id));
  }
  uint64_t number_of_reqs = 0;
  if (ENABLE_DEBUG_GLOBAL_ENTRY) {
    //kv_ptr->dbg->prop_acc_num++;
    //number_of_reqs = kv_ptr->.dbg->prop_acc_num;
  }
  register_committed_global_sess_id (glob_sess_id, rmw_l_id, t_id);
  check_registered_against_kv_ptr_last_committed(kv_ptr, rmw_l_id, glob_sess_id,
                                                 "handle remote commit", t_id);

  unlock_seqlock(&kv_ptr->seqlock);
  return number_of_reqs;

}

// On gathering quorum of acks for commit, commit locally and signal that the session must be freed if not helping
static inline void act_on_quorum_of_commit_acks(struct pending_ops *p_ops, uint32_t ack_ptr, uint16_t t_id)
{
  struct rmw_local_entry *loc_entry = &p_ops->prop_info->entry[p_ops->w_meta[ack_ptr].sess_id];
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry != NULL);
    assert(loc_entry->state = COMMITTED);
  }

  if (loc_entry->helping_flag == HELPING &&
      rmw_ids_are_equal(&loc_entry->help_loc_entry->rmw_id, &loc_entry->rmw_id))
    loc_entry->helping_flag = HELPING_MYSELF;

  if (loc_entry->helping_flag != HELP_PREV_COMMITTED_LOG_TOO_HIGH)
    attempt_local_commit(p_ops, loc_entry, t_id);

  switch(loc_entry->helping_flag)
  {
    case NOT_HELPING:
    case PROPOSE_NOT_LOCALLY_ACKED:
    case HELPING_MYSELF:
    case PROPOSE_LOCALLY_ACCEPTED:
      loc_entry->state = INVALID_RMW;
      free_session_from_rmw(p_ops, loc_entry->sess_id, true, t_id);
      break;
    case HELPING:
      reinstate_loc_entry_after_helping(loc_entry, t_id);
      break;
    case HELP_PREV_COMMITTED_LOG_TOO_HIGH:
      //if (loc_entry->helping_flag == HELP_PREV_COMMITTED_LOG_TOO_HIGH)
      //  my_printf(yellow, "Wrkr %u, sess %u, rmw-id %u, sess stalled %d \n",
      //  t_id, loc_entry->sess_id, loc_entry->rmw_id.id, p_ops->sess_info[loc_entry->sess_id].stalled);
      loc_entry->state = PROPOSED;
      loc_entry->helping_flag = NOT_HELPING;

      break;
    default: if (ENABLE_ASSERTIONS) assert(false);
  }
}



/*--------------------------------------------------------------------------
 * --------------------HANDLE REPLIES-------------------------------------
 * --------------------------------------------------------------------------*/

// The help_loc_entry is used when receiving an already committed reply or an already accepted
static inline void store_rmw_rep_to_help_loc_entry(struct rmw_local_entry* loc_entry,
                                                   struct rmw_rep_last_committed* prop_rep, uint16_t t_id)
{
  struct rmw_local_entry *help_loc_entry = loc_entry->help_loc_entry;
  if (ENABLE_ASSERTIONS) {
    if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED) {
      assert(help_loc_entry->new_ts.version > 0);
      assert(help_loc_entry->state == ACCEPTED);
    }
    assert(help_loc_entry->state == INVALID_RMW || help_loc_entry->state == ACCEPTED);
  }
  enum ts_compare ts_comp = compare_netw_ts_with_ts(&prop_rep->ts, &help_loc_entry->new_ts);
  // If i have locally accepted my own RMW for a higher TS,
  // then i can treat these replies as acks,
  // because their end result is asking me to broadcast accepts for me RMWs
//  if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED &&
//      help_loc_entry->state == INVALID_RMW &&
//      ts_comp != GREATER) {
//    //my_printf(green, "Locally accepted/incoming ts  %u,%u/%u,%u \n",
//    //       help_loc_entry->new_ts.version, help_loc_entry->new_ts.m_id,
//    //       prop_rep->ts.version, prop_rep->ts.m_id);
//    loc_entry->rmw_reps.already_accepted--;
//    loc_entry->rmw_reps.acks++;
//  }
//  else
  if (help_loc_entry->state == INVALID_RMW ||
      ts_comp == GREATER) {
    if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED)
      loc_entry->helping_flag = NOT_HELPING;
    assign_netw_ts_to_ts(&help_loc_entry->new_ts, &prop_rep->ts);
    help_loc_entry->base_ts.version = prop_rep->log_no_or_base_version;
    help_loc_entry->base_ts.m_id = prop_rep->base_m_id;
    help_loc_entry->log_no = loc_entry->log_no;
    help_loc_entry->state = ACCEPTED;
    help_loc_entry->rmw_id.id = prop_rep->rmw_id;
    if (ENABLE_ASSERTIONS) assert(help_loc_entry->rmw_id.id < B_4);
    help_loc_entry->rmw_id.glob_sess_id = prop_rep->glob_sess_id;
    memcpy(help_loc_entry->value_to_write, prop_rep->value, (size_t) RMW_VALUE_SIZE);
    help_loc_entry->key = loc_entry->key;
  }
}


// Handle a proposal/accept reply
static inline void handle_prop_or_acc_rep(struct pending_ops *p_ops, struct rmw_rep_message *rep_mes,
                                          struct rmw_rep_last_committed *rep,
                                          struct rmw_local_entry *loc_entry,
                                          bool is_accept,
                                          const uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (is_accept) assert(loc_entry->state == ACCEPTED);
    else {
      assert(loc_entry->rmw_reps.tot_replies > 0);
      assert(loc_entry->state == PROPOSED);
      // this checks that the performance optimization of NO-op reps is valid
      assert(rep->opcode != NO_OP_PROP_REP);
      check_state_with_allowed_flags(4, loc_entry->helping_flag, NOT_HELPING,
                                     PROPOSE_NOT_LOCALLY_ACKED, PROPOSE_LOCALLY_ACCEPTED);
    }
  }
  loc_entry->rmw_reps.tot_replies++;
  if (rep->opcode > RMW_ACK_ACC_SAME_RMW) loc_entry->rmw_reps.nacks++;
  switch (rep->opcode) {
    case RMW_ACK:
    case RMW_ACK_ACC_SAME_RMW:
      loc_entry->rmw_reps.acks++;
      if (ENABLE_ASSERTIONS)
        assert(rep_mes->m_id < MACHINE_NUM && rep_mes->m_id != machine_id);
      if (DEBUG_RMW)
        my_printf(green, "Wrkr %u, the received rep is an %s ack, "
                    "total acks %u \n", t_id, is_accept ? "acc" : "prop",
                  loc_entry->rmw_reps.acks);
      break;
    case LOG_TOO_HIGH:
      loc_entry->rmw_reps.log_too_high++;
      break;
    case RMW_ID_COMMITTED:
      loc_entry->rmw_reps.no_need_to_bcast = true;
    case RMW_ID_COMMITTED_SAME_LOG:
      loc_entry->rmw_reps.rmw_id_commited++;
      if (loc_entry->helping_flag != HELPING)
        loc_entry->log_no = loc_entry->accepted_log_no;
      attempt_local_commit(p_ops, loc_entry, t_id);
      //attempt_local_commit_from_rep(p_ops, rep, loc_entry, t_id);
      // store the reply in the help loc_entry
      //store_rmw_rep_to_help_loc_entry(loc_entry, rep, t_id);
      break;
    case LOG_TOO_SMALL:
      loc_entry->rmw_reps.log_too_small++;
      attempt_local_commit_from_rep(p_ops, rep, loc_entry, t_id);
      break;
    case SEEN_LOWER_ACC:
      loc_entry->rmw_reps.already_accepted++;
      if (ENABLE_ASSERTIONS) {
        assert(compare_netw_ts_with_ts(&rep->ts, &loc_entry->new_ts) == SMALLER);
        assert(rep->rmw_id < B_4);
        assert(!is_accept);
      }
      // Store the accepted rmw only if no higher priority reps have been seen
      if (!loc_entry->rmw_reps.seen_higher_prop_acc +
          (loc_entry->rmw_reps.rmw_id_commited + loc_entry->rmw_reps.log_too_small == 0)) {
        store_rmw_rep_to_help_loc_entry(loc_entry, rep, t_id);
      }
      break;
    case SEEN_HIGHER_ACC:
    case SEEN_HIGHER_PROP:
      // make sure no lower accepts will be helped, useful only for proposes
      //loc_entry->rmw_reps.seen_higher_acc = true;

      loc_entry->rmw_reps.seen_higher_prop_acc++;
      if (DEBUG_RMW)
        my_printf(yellow, "Wrkr %u: the %s rep is %u, %u sum of all other reps %u \n", t_id,
                  is_accept ? "acc" : "prop",rep->opcode,
                  loc_entry->rmw_reps.seen_higher_prop_acc,
                  loc_entry->rmw_reps.rmw_id_commited + loc_entry->rmw_reps.log_too_small +
                  loc_entry->rmw_reps.already_accepted);
      if (rep->ts.version > loc_entry->rmw_reps.seen_higher_prop_version) {
        loc_entry->rmw_reps.seen_higher_prop_version = rep->ts.version;
        if (DEBUG_RMW)
          my_printf(yellow, "Wrkr %u: overwriting the TS version %u \n",
                    t_id, loc_entry->rmw_reps.seen_higher_prop_version);
      }
      break;
    default:
      if (ENABLE_ASSERTIONS) assert(false);
  }

  if (ENABLE_ASSERTIONS)
    check_sum_of_reps(loc_entry);
}


// Handle one accept or propose reply
static inline void handle_single_rmw_rep(struct pending_ops *p_ops, struct rmw_rep_last_committed *rep,
                                         struct rmw_rep_message *rep_mes, uint16_t byte_ptr,
                                         bool is_accept, uint16_t r_rep_i, uint16_t t_id)
{
  struct prop_info *prop_info = p_ops->prop_info;
  if (ENABLE_ASSERTIONS) {
    if (!opcode_is_rmw_rep(rep->opcode)) {
      printf("Rep_i %u, current opcode %u first opcode: %u, byte_ptr %u \n",
             r_rep_i, rep->opcode, rep_mes->rmw_rep[0].opcode, byte_ptr);
    }
    assert(opcode_is_rmw_rep(rep->opcode));
    if (prop_info->l_id <= rep->l_id)
      my_printf(red, "Wrkr %u, rep_i %u, opcode %u, is_accept %d, incoming rep l_id %u, max prop lid %u \n",
                t_id, r_rep_i, rep->opcode, is_accept, rep->l_id, prop_info->l_id);

    assert(prop_info->l_id > rep->l_id);
  }
  //my_printf(cyan, "RMW rep opcode %u, l_id %u \n", rep->opcode, rep->l_id);
  int entry_i = search_prop_entries_with_l_id(prop_info, (uint8_t) (is_accept ? ACCEPTED : PROPOSED),
                                              rep->l_id);
  if (entry_i == -1) return;
  struct rmw_local_entry *loc_entry = &prop_info->entry[entry_i];
  if (unlikely(rep->opcode) > NO_OP_PROP_REP) {
    increment_epoch_id(loc_entry->epoch_id, t_id);
    rep->opcode -= FALSE_POSITIVE_OFFSET;
    loc_entry->fp_detected = true;
  }
  handle_prop_or_acc_rep(p_ops, rep_mes, rep, loc_entry, is_accept, t_id);
}

// Handle read replies that refer to RMWs (either replies to accepts or proposes)
static inline void handle_rmw_rep_replies(struct pending_ops *p_ops, struct r_rep_message *r_rep_mes,
                                          bool is_accept, uint16_t t_id)
{
  struct rmw_rep_message *rep_mes = (struct rmw_rep_message *) r_rep_mes;
  check_state_with_allowed_flags(4, r_rep_mes->opcode, ACCEPT_REPLY,
                                 PROP_REPLY, ACCEPT_REPLY_NO_CREDITS);
  uint8_t rep_num = rep_mes->coalesce_num;
  //my_printf(yellow, "Received opcode %u, prop_rep num %u \n", r_rep_mes->opcode, rep_num);
  uint16_t byte_ptr = R_REP_MES_HEADER; // same for both accepts and replies
  for (uint16_t r_rep_i = 0; r_rep_i < rep_num; r_rep_i++) {
    struct rmw_rep_last_committed *rep = (struct rmw_rep_last_committed *) (((void *) rep_mes) + byte_ptr);
    handle_single_rmw_rep(p_ops, rep, rep_mes, byte_ptr, is_accept, r_rep_i, t_id);
    byte_ptr += get_size_from_opcode(rep->opcode);
  }
  r_rep_mes->opcode = INVALID_OPCODE;
}



/*--------------------------------------------------------------------------
 * -----------------------------RMW-FSM-------------------------------------
 * --------------------------------------------------------------------------*/


//------------------------------HELP STUCK RMW------------------------------------------
// When time-out-ing on a stuck Accepted value, and try to help it, you need to first propose your own
static inline void set_up_a_proposed_but_not_locally_acked_entry(struct pending_ops *p_ops, mica_op_t  *kv_ptr,
                                                                 struct rmw_local_entry *loc_entry, uint16_t t_id)
{
  struct rmw_local_entry *help_loc_entry = loc_entry->help_loc_entry;
  if (DEBUG_RMW)
    my_printf(cyan, "Wrkr %u, session %u helps RMW id %u glob_sess_id %u with version %u, m_id %u,"
                " kv_ptr log/help log %u/%u kv_ptr committed log %u , biggest committed rmw_id %u for kv_ptr sess %u"
                " stashed rmw_id: %u, global_sess id %u, state %u \n",
              t_id, loc_entry->sess_id, loc_entry->rmw_id.id, loc_entry->rmw_id.glob_sess_id,
              loc_entry->new_ts.version, loc_entry->new_ts.m_id,
              kv_ptr->log_no, loc_entry->log_no, kv_ptr->last_committed_log_no,
              committed_glob_sess_rmw_id[kv_ptr->rmw_id.glob_sess_id], kv_ptr->rmw_id.glob_sess_id,
              loc_entry->help_rmw->rmw_id.id, loc_entry->help_rmw->rmw_id.glob_sess_id, loc_entry->help_rmw->state);
  loc_entry->state = PROPOSED;
  zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
  help_loc_entry->state = ACCEPTED;
  if (ENABLE_ASSERTIONS) assert(p_ops->sess_info[loc_entry->sess_id].stalled);
  loc_entry->helping_flag = PROPOSE_NOT_LOCALLY_ACKED;
  //  my_printf(cyan, "Wrkr %u Sess %u initiates prop help, key %u, log no %u \n", t_id,
  //             loc_entry->sess_id, loc_entry->key.bkt, loc_entry->log_no);
  // help_loc_entry->sess_id = loc_entry->sess_id;
  help_loc_entry->log_no = loc_entry->log_no;
  help_loc_entry->key = loc_entry->key;
  loc_entry->rmw_reps.tot_replies = 1;
  loc_entry->rmw_reps.already_accepted = 1;
  if (PRINT_LOGS && ENABLE_DEBUG_GLOBAL_ENTRY)
    fprintf(rmw_verify_fp[t_id], "Key: %u, log %u: Prop-not-locally accepted: helping rmw_id %lu, glob_sess id: %u, "
              "version %u, m_id: %u, From: rmw_id %lu, glob_sess id: %u with version %u, m_id: %u \n",
            loc_entry->key.bkt, loc_entry->log_no, help_loc_entry->rmw_id.id, help_loc_entry->rmw_id.glob_sess_id,
            help_loc_entry->new_ts.version, help_loc_entry->new_ts.m_id, loc_entry->rmw_id.id,
            loc_entry->rmw_id.glob_sess_id,
            loc_entry->new_ts.version, loc_entry->new_ts.m_id);
}


// When inspecting an RMW that failed to grab a kv_ptr in the past
static inline bool attempt_to_grab_kv_ptr_after_waiting(struct pending_ops *p_ops,
                                                        mica_op_t *kv_ptr,
                                                        struct rmw_local_entry *loc_entry,
                                                        uint16_t sess_i, uint16_t t_id)
{
  bool kv_ptr_was_grabbed = false;
  bool rmw_fails = false;
  uint32_t version = PAXOS_TS;
  if (ENABLE_ASSERTIONS) assert(loc_entry->rmw_id.glob_sess_id < GLOBAL_SESSION_NUM);
  lock_seqlock(&kv_ptr->seqlock);
  if (if_already_committed_bcast_commits(p_ops, loc_entry, t_id)) {
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
    return true;
  }
  if (kv_ptr->state == INVALID_RMW) {
    if (!rmw_fails_with_loc_entry(loc_entry, kv_ptr, &rmw_fails, t_id)) {
      if (ENABLE_ASSERTIONS) assert(kv_ptr->rmw_id.glob_sess_id < GLOBAL_SESSION_NUM);
      if (ENABLE_ASSERTIONS && kv_ptr->state != INVALID_RMW &&
          committed_glob_sess_rmw_id[kv_ptr->rmw_id.glob_sess_id] >= kv_ptr->rmw_id.id) {
        //my_printf(red, "Wrkr: %u waiting on an rmw id %u/%u glob_sess_id %u that has been committed, so we free it"
        //             "last committed rmw id %u , glob sess id %u, "
        //             "state %u, committed log/log %u/%u, version %u \n",
        //           t_id, kv_ptr->rmw_id.id, committed_glob_sess_rmw_id[kv_ptr->rmw_id.glob_sess_id],
        //           kv_ptr->rmw_id.glob_sess_id, kv_ptr->last_committed_rmw_id.id,
        //           kv_ptr->last_committed_rmw_id.glob_sess_id,
        //           kv_ptr->state, kv_ptr->last_committed_log_no,
        //           kv_ptr->log_no, kv_ptr->new_ts.version);
      }
      loc_entry->log_no = kv_ptr->last_committed_log_no + 1;
      activate_kv_pair(PROPOSED, PAXOS_TS, kv_ptr, loc_entry->opcode,
                       (uint8_t) machine_id, loc_entry->rmw_id.id,
                       loc_entry->rmw_id.glob_sess_id, loc_entry->log_no, t_id,
                       ENABLE_ASSERTIONS ? "attempt_to_grab_kv_ptr_after_waiting" : NULL);

      kv_ptr_was_grabbed = true;
      if (DEBUG_RMW)
        my_printf(yellow, "Wrkr %u, after waiting for %u cycles, session %u  \n",
                  t_id, loc_entry->back_off_cntr, loc_entry->sess_id);
    }
  }
  else if (kv_ptr_state_has_changed(kv_ptr, loc_entry->help_rmw)) {
    if (ENABLE_ASSERTIONS) {
      if (committed_glob_sess_rmw_id[kv_ptr->rmw_id.glob_sess_id] >= kv_ptr->rmw_id.id) {
        //my_printf(red, "Wrkr: %u The saved rmw id %u/%u glob_sess_id %u has been committed, "
        //             "last committed rmw id %u , glob sess id %u, "
        //             "state %u, committed log/log %u/%u, version %u \n",
        //           t_id, kv_ptr->rmw_id.id, committed_glob_sess_rmw_id[kv_ptr->rmw_id.glob_sess_id],
        //           kv_ptr->rmw_id.glob_sess_id, kv_ptr->last_committed_rmw_id.id,
        //           kv_ptr->last_committed_rmw_id.glob_sess_id,
        //           kv_ptr->state, kv_ptr->last_committed_log_no,
        //           kv_ptr->log_no, kv_ptr->new_ts.version);
        //assert(false);
      }
    }
    if (DEBUG_RMW)
      my_printf(yellow, "Wrkr %u, session %u changed who is waiting: waited for %u cycles on "
                  "state %u rmw_id %u glob_sess_id %u, now waiting on rmw_id %u glob_sess_id %u, state %u\n",
                t_id, loc_entry->sess_id, loc_entry->back_off_cntr,
                loc_entry->help_rmw->state, loc_entry->help_rmw->rmw_id.id, loc_entry->help_rmw->rmw_id.glob_sess_id,
                kv_ptr->rmw_id.id, kv_ptr->rmw_id.glob_sess_id, kv_ptr->state);
    loc_entry->help_rmw->state = kv_ptr->state;
    assign_second_rmw_id_to_first(&loc_entry->help_rmw->rmw_id, &kv_ptr->rmw_id);
    loc_entry->help_rmw->ts = kv_ptr->prop_ts;
    loc_entry->help_rmw->log_no = kv_ptr->log_no;
    loc_entry->back_off_cntr = 0;
  }
  check_log_nos_of_kv_ptr(kv_ptr, "attempt_to_grab_kv_ptr_after_waiting", t_id);
  unlock_seqlock(&loc_entry->kv_ptr->seqlock);
  if (kv_ptr_was_grabbed) {
    fill_loc_rmw_entry_on_grabbing_kv_ptr(p_ops, loc_entry, PAXOS_TS,
                                          PROPOSED, sess_i, t_id);
  }
  else if (rmw_fails) {
    if (ENABLE_ASSERTIONS) {
      assert(loc_entry->accepted_log_no == 0);
      assert(loc_entry->killable);
    }
    loc_entry->state = INVALID_RMW;
    //printf("Cancelling on needing kv_ptr Wrkr%u, sess %u, entry %u rmw_failing \n",
    //     t_id, loc_entry->sess_id, loc_entry->index_to_rmw);
    free_session_from_rmw(p_ops, loc_entry->sess_id, false, t_id);
    return true;
  }
  return kv_ptr_was_grabbed;
}

// Insert a helping accept in the write fifo after waiting on it
static inline void attempt_to_help_a_locally_accepted_value(struct pending_ops *p_ops,
                                                            struct rmw_local_entry *loc_entry,
                                                            mica_op_t *kv_ptr, uint16_t t_id)
{
  bool help = false;
  struct rmw_local_entry *help_loc_entry = loc_entry->help_loc_entry;
  // The stat of the kv_ptr must not be downgraded from ACCEPTED
  lock_seqlock(&loc_entry->kv_ptr->seqlock);
  // check again with the lock in hand
  if (kv_ptr_state_has_not_changed(kv_ptr, loc_entry->help_rmw)) {
    loc_entry->log_no = kv_ptr->accepted_log_no;
    help_loc_entry->new_ts = kv_ptr->accepted_ts;
    help_loc_entry->rmw_id = kv_ptr->rmw_id;
    memcpy(help_loc_entry->value_to_write, kv_ptr->last_accepted_value, (size_t) RMW_VALUE_SIZE);
    help_loc_entry->base_ts = kv_ptr->base_acc_ts;

    // we must make it appear as if the kv_ptr has seen our propose
    // and has replied with a lower-ts-accept
    loc_entry->new_ts.version = kv_ptr->prop_ts.version + 1;
    loc_entry->new_ts.m_id = (uint8_t) machine_id;
    kv_ptr->prop_ts = loc_entry->new_ts;


    if (ENABLE_ASSERTIONS) {
      assert(kv_ptr->accepted_log_no == kv_ptr->log_no);
      assert(kv_ptr->prop_ts.version > kv_ptr->accepted_ts.version);
      assert(rmw_ids_are_equal(&kv_ptr->rmw_id, &kv_ptr->accepted_rmw_id));
      assert(loc_entry->key.bkt == kv_ptr->key.bkt);
      assert(kv_ptr->state == ACCEPTED);
    }
    help = true;
  }
  check_log_nos_of_kv_ptr(kv_ptr, "attempt_to_help_a_locally_accepted_value", t_id);
  unlock_seqlock(&loc_entry->kv_ptr->seqlock);

  loc_entry->back_off_cntr = 0;
  if (help) {
    // Helping means we are proposing, but we are not locally acked:
    // We store a reply from the local machine that says already ACCEPTED
    set_up_a_proposed_but_not_locally_acked_entry(p_ops, kv_ptr, loc_entry, t_id);
  }
}

// After backing off waiting on a PROPOSED kv_ptr try to steal it
static inline void attempt_to_steal_a_proposed_kv_ptr(struct pending_ops *p_ops,
                                                      struct rmw_local_entry *loc_entry,
                                                      mica_op_t *kv_ptr,
                                                      uint16_t sess_i, uint16_t t_id)
{
  bool kv_ptr_was_grabbed = false;
  lock_seqlock(&loc_entry->kv_ptr->seqlock);
  if (if_already_committed_bcast_commits(p_ops, loc_entry, t_id)) {
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
    return ;
  }
  uint32_t new_version = 0;

  if (kv_ptr->state == INVALID_RMW || kv_ptr_state_has_not_changed(kv_ptr, loc_entry->help_rmw)) {
    check_the_proposed_log_no(kv_ptr, loc_entry, t_id);
    loc_entry->log_no = kv_ptr->last_committed_log_no + 1;
    new_version = kv_ptr->prop_ts.version + 1;
    activate_kv_pair(PROPOSED, new_version, kv_ptr, loc_entry->opcode,
                     (uint8_t) machine_id, loc_entry->rmw_id.id,
                     loc_entry->rmw_id.glob_sess_id, loc_entry->log_no, t_id,
                     ENABLE_ASSERTIONS ? "attempt_to_steal_a_proposed_kv_ptr" : NULL);

    kv_ptr_was_grabbed = true;
  }
  else if (kv_ptr_state_has_changed(kv_ptr, loc_entry->help_rmw)) {
    if (DEBUG_RMW)
      my_printf(yellow, "Wrkr %u, session %u on attempting to steal the propose, changed who is "
                  "waiting: waited for %u cycles for state %u "
                  "rmw_id %u glob_sess_id %u, state %u,  now waiting on rmw_id % glob_sess_id %u, state %u\n",
                t_id, loc_entry->sess_id, loc_entry->back_off_cntr,
                loc_entry->help_rmw->state, loc_entry->help_rmw->rmw_id.id, loc_entry->help_rmw->rmw_id.glob_sess_id,
                kv_ptr->rmw_id.id, kv_ptr->rmw_id.glob_sess_id, kv_ptr->state);
    loc_entry->help_rmw->log_no = kv_ptr->log_no;
    loc_entry->help_rmw->state = kv_ptr->state;
    loc_entry->help_rmw->ts = kv_ptr->prop_ts;
    assign_second_rmw_id_to_first(&loc_entry->help_rmw->rmw_id, &kv_ptr->rmw_id);
  }
  else if (ENABLE_ASSERTIONS) assert(false);
  check_log_nos_of_kv_ptr(kv_ptr, "attempt_to_steal_a_proposed_kv_ptr", t_id);
  unlock_seqlock(&loc_entry->kv_ptr->seqlock);
  loc_entry->back_off_cntr = 0;
  if (kv_ptr_was_grabbed) {
    if (DEBUG_RMW)
      my_printf(cyan, "Wrkr %u: session %u steals kv_ptr to do its propose \n",
                t_id, loc_entry->sess_id);
    fill_loc_rmw_entry_on_grabbing_kv_ptr(p_ops, loc_entry, new_version,
                                          PROPOSED, sess_i, t_id);
  }
}


//------------------------------ALREADY-COMMITTED------------------------------------------

//When inspecting an accept/propose and have received already-committed Response
static inline void handle_already_committed_rmw(struct pending_ops *p_ops,
                                                struct rmw_local_entry *loc_entry,
                                                uint16_t t_id)
{
  // Broadcast commits iff you got back you own RMW
  if (!loc_entry->rmw_reps.no_need_to_bcast &&
      (loc_entry->rmw_reps.rmw_id_commited < QUORUM_NUM)) {
    if (ENABLE_ASSERTIONS) {
//      my_printf(yellow, "%s: committed rmw received had too "
//                      "low a log, bcasting from loc_entry \n",
//                    loc_entry->state == PROPOSED ? "Propose" : "Accept");
    }
    // Here we know the correct value/log to broadcast: it's the locally accepted ones
    loc_entry->log_no = loc_entry->accepted_log_no;
    loc_entry->state = MUST_BCAST_COMMITS;
    if (MACHINE_NUM <= 3 && ENABLE_ASSERTIONS) assert(false);
  }
  else {
    //free the session here as well
    loc_entry->state = INVALID_RMW;
    free_session_from_rmw(p_ops, loc_entry->sess_id, true, t_id);
  }
  check_state_with_allowed_flags(4, (int) loc_entry->state, INVALID_RMW,
                                 MUST_BCAST_COMMITS, MUST_BCAST_COMMITS_FROM_HELP);
}

//------------------------------RETRY------------------------------------------

// When receiving a response that says that a higher-TS RMW has been seen or TS was stale
static inline void take_kv_ptr_with_higher_TS(struct pending_ops *p_ops,
                                              struct rmw_local_entry *loc_entry,
                                              uint32_t new_version, bool from_propose,
                                              uint16_t t_id) {
  bool kv_ptr_was_grabbed  = false,
    is_still_proposed, is_still_accepted, kv_ptr_can_be_taken_with_higher_TS;
  bool rmw_fails = false;
  bool help = false;
  mica_op_t *kv_ptr = loc_entry->kv_ptr;
  lock_seqlock(&kv_ptr->seqlock);
  {
    if (if_already_committed_bcast_commits(p_ops, loc_entry, t_id)) {
      unlock_seqlock(&loc_entry->kv_ptr->seqlock);
      return;
    }
    is_still_proposed = rmw_ids_are_equal(&kv_ptr->rmw_id, &loc_entry->rmw_id) &&
                        kv_ptr->state == PROPOSED;

    is_still_accepted = rmw_ids_are_equal(&kv_ptr->rmw_id, &loc_entry->rmw_id) &&
                        kv_ptr->state == ACCEPTED &&
                        compare_ts(&kv_ptr->accepted_ts, &loc_entry->new_ts) == EQUAL;
    kv_ptr_can_be_taken_with_higher_TS =
      kv_ptr->state == INVALID_RMW || is_still_proposed || is_still_accepted;

    // if either state is invalid or we own it
    if (kv_ptr_can_be_taken_with_higher_TS) {
      if (!rmw_fails_with_loc_entry(loc_entry, kv_ptr, &rmw_fails, t_id)) {
        if (kv_ptr->state == INVALID_RMW) {
          kv_ptr->log_no = kv_ptr->last_committed_log_no + 1;
          kv_ptr->opcode = loc_entry->opcode;
          assign_second_rmw_id_to_first(&kv_ptr->rmw_id, &loc_entry->rmw_id);
        } else if (ENABLE_ASSERTIONS) {
          assert(loc_entry->log_no == kv_ptr->last_committed_log_no + 1);
          assert(kv_ptr->log_no == kv_ptr->last_committed_log_no + 1);
          if (kv_ptr->state == ACCEPTED) {
            assert(!from_propose);
            assert(compare_ts(&kv_ptr->accepted_ts, &loc_entry->new_ts) == EQUAL);
          }
        }
        loc_entry->log_no = kv_ptr->last_committed_log_no + 1;

        loc_entry->new_ts.version = MAX(new_version, kv_ptr->prop_ts.version) + 1;
        if (ENABLE_ASSERTIONS) {
          assert(loc_entry->new_ts.version > kv_ptr->prop_ts.version);
        }
        loc_entry->new_ts.m_id = (uint8_t) machine_id;
        kv_ptr->prop_ts = loc_entry->new_ts;
        if (!is_still_accepted) {
          if (ENABLE_ASSERTIONS) assert(kv_ptr->state != ACCEPTED);
          kv_ptr->state = PROPOSED;
        } else {
          // Attention: when retrying an RMW that has been locally accepted,
          // you need to start from Proposes, but the kv_ptr can NOT be downgraded to proposed
          help = true;
          //loc_entry->helping_flag = PROPOSE_LOCALLY_ACCEPTED;
          loc_entry->help_loc_entry->new_ts = kv_ptr->accepted_ts;
        }
        kv_ptr_was_grabbed = true;
      } else kv_ptr->state = INVALID_RMW;
    } else {
      if (DEBUG_RMW)
        my_printf(yellow, "Wrkr %u, session %u  failed when attempting to get/regain the kv_ptr, "
                    "waiting: waited for %u cycles for "
                    "now waiting on rmw_id % glob_sess_id %u, state %u\n",
                  t_id, loc_entry->sess_id,
                  kv_ptr->rmw_id.id, kv_ptr->rmw_id.glob_sess_id, kv_ptr->state);
    }
    check_log_nos_of_kv_ptr(kv_ptr, "take_kv_ptr_with_higher_TS", t_id);
  }
  unlock_seqlock(&loc_entry->kv_ptr->seqlock);

  zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);

  if (kv_ptr_was_grabbed) {
    if (DEBUG_RMW)
      my_printf(cyan, "Wrkr %u: session %u gets/regains the kv_ptr log %u to do its propose \n",
                t_id, loc_entry->sess_id, kv_ptr->log_no);
    loc_entry->state = PROPOSED;
    if (help) {
      struct rmw_local_entry *help_loc_entry = loc_entry->help_loc_entry;
      //help_loc_entry->log_no = loc_entry->accepted_log_no;
      if (ENABLE_ASSERTIONS) assert(loc_entry->accepted_log_no == loc_entry->log_no);
      //help_loc_entry->rmw_id = loc_entry->rmw_id;
      //memcpy(help_loc_entry->value_to_write, loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
      //help_loc_entry->base_ts = loc_entry->base_ts;
//      set_up_a_proposed_but_not_locally_acked_entry(p_ops, kv_ptr, loc_entry, t_id);
      loc_entry->rmw_reps.tot_replies = 1;
      loc_entry->rmw_reps.already_accepted = 1;
      help_loc_entry->state = ACCEPTED;
      loc_entry->helping_flag = PROPOSE_LOCALLY_ACCEPTED;
    }
    else local_rmw_ack(loc_entry);
  }
  else if (rmw_fails) {
    if (ENABLE_ASSERTIONS) {
      assert(loc_entry->accepted_log_no == 0);
      assert(loc_entry->killable);
      assert(!is_still_accepted);
    }
    loc_entry->state = INVALID_RMW;
    //printf("Cancelling on needing kv_ptr Wrkr%u, sess %u, entry %u rmw_failing \n",
    //     t_id, loc_entry->sess_id, loc_entry->index_to_rmw);
    free_session_from_rmw(p_ops, loc_entry->sess_id, false, t_id);
  }
  else loc_entry->state = NEEDS_KV_PTR;
}


//------------------------------ACKS------------------------------------------
// If a quorum of proposal acks have been gathered, try to broadcast accepts
static inline void act_on_quorum_of_prop_acks(struct pending_ops *p_ops, struct rmw_local_entry *loc_entry,
                                              uint16_t t_id)
{
  // first we need to accept locally,
  uint8_t local_state = attempt_local_accept(p_ops, loc_entry, t_id);
  if (ENABLE_ASSERTIONS)
    assert(local_state == ACCEPT_ACK || local_state == NACK_ACCEPT_SEEN_HIGHER_TS ||
           local_state == NACK_ACCEPT_LOG_OUT_OF_DATE || local_state == NACK_ALREADY_COMMITTED);

  zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
  if (local_state == ACCEPT_ACK) {
    local_rmw_ack(loc_entry);
    check_loc_entry_metadata_is_reset(loc_entry, "act_on_quorum_of_prop_acks", t_id);
    if (ENABLE_ASSERTIONS) assert(loc_entry->rmw_id.id < B_4);
    insert_accept_in_writes_message_fifo(p_ops, loc_entry, false, t_id);
    if (ENABLE_ASSERTIONS) {
      assert(glob_ses_id_to_t_id(loc_entry->rmw_id.glob_sess_id) == t_id &&
             glob_ses_id_to_m_id(loc_entry->rmw_id.glob_sess_id) == machine_id);
      assert(loc_entry->state == PROPOSED);
    }
    loc_entry->state = ACCEPTED;
    loc_entry->killable = false;
  }
  else if (local_state == NACK_ALREADY_COMMITTED) {
    //printf("Already-committed when accepting locally--bcast commits \n");
  }
  else loc_entry->state = NEEDS_KV_PTR;
}





//------------------------------SEEN-LOWER_ACCEPT------------------------------------------
// When a quorum of prop replies have been received, and one of the replies says it has accepted an RMW with lower TS
static inline void act_on_receiving_already_accepted_rep_to_prop(struct pending_ops *p_ops,
                                                                 struct rmw_local_entry* loc_entry,
                                                                 uint32_t* new_version,
                                                                 uint16_t t_id)
{
  struct rmw_local_entry* help_loc_entry = loc_entry->help_loc_entry;
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->log_no == help_loc_entry->log_no);
    assert(loc_entry->help_loc_entry->state == ACCEPTED);
    assert(compare_ts(&help_loc_entry->new_ts, &loc_entry->new_ts) == SMALLER);
    //assert(loc_entry->rmw_reps.acks < REMOTE_QUORUM);
    //assert(ts_comp != EQUAL);
  }
  // help the accepted
  uint8_t flag = attempt_local_accept_to_help(p_ops, loc_entry, t_id);
  if (flag == ACCEPT_ACK) {
    loc_entry->helping_flag = HELPING;
    loc_entry->state = ACCEPTED;
    zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
    local_rmw_ack(loc_entry);
    if (ENABLE_ASSERTIONS) assert(help_loc_entry->rmw_id.id < B_4);
    insert_accept_in_writes_message_fifo(p_ops, help_loc_entry, true, t_id);
  }
  else { // abort the help, on failing to accept locally
    loc_entry->state = NEEDS_KV_PTR;
    help_loc_entry->state = INVALID_RMW;
  }

}


//------------------------------LOG-TOO_HIGH------------------------------------------

static inline void react_on_log_too_high(struct rmw_local_entry *loc_entry, bool is_propose,
                                         uint16_t t_id)
{
  loc_entry->state = RETRY_WITH_BIGGER_TS;
  loc_entry->log_too_high_cntr++;
  if (loc_entry->log_too_high_cntr == LOG_TOO_HIGH_TIME_OUT) {
    my_printf(red, "Worker: %u session %u, %s for rmw-id %u Timed out on log_too-high \n",
              t_id, loc_entry->sess_id,
              is_propose? "Prop" : "Acc",
              loc_entry->rmw_id.id);
    mica_op_t *kv_ptr = loc_entry->kv_ptr;
    lock_seqlock(&kv_ptr->seqlock);
    if (kv_ptr->last_committed_log_no + 1 == loc_entry->log_no) {
      loc_entry->state = MUST_BCAST_COMMITS_FROM_HELP;
      struct rmw_local_entry *help_loc_entry = loc_entry->help_loc_entry;
      memcpy(help_loc_entry->value_to_write, kv_ptr->value, RMW_VALUE_SIZE);
      assign_second_rmw_id_to_first(&help_loc_entry->rmw_id, &kv_ptr->last_committed_rmw_id);
      help_loc_entry->base_ts = kv_ptr->ts;
    }
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);


    if (loc_entry->state == MUST_BCAST_COMMITS_FROM_HELP) {
      loc_entry->helping_flag = HELP_PREV_COMMITTED_LOG_TOO_HIGH;
      loc_entry->help_loc_entry->log_no = loc_entry->log_no - 1;
      loc_entry->help_loc_entry->key = loc_entry->key;
      // loc_entry->help_loc_entry->sess_id = loc_entry->sess_id;
    }

    loc_entry->log_too_high_cntr = 0;
  }
}


//------------------------------CLEAN-UP------------------------------------------
static inline void clean_up_after_inspecting_accept(struct pending_ops *p_ops,
                                                    struct rmw_local_entry *loc_entry,
                                                    uint32_t new_version,
                                                    struct rmw_local_entry *dbg_loc_entry,
                                                    uint16_t t_id)
{
  //advance the entry's l_id such that subsequent responses are disregarded
  advance_loc_entry_l_id(p_ops, loc_entry, t_id);
  // CLEAN_UP
  if (ENABLE_ALL_ABOARD && loc_entry->all_aboard) {
    if (ENABLE_STAT_COUNTING) {
      t_stats[t_id].all_aboard_rmws++;
    }
    loc_entry->all_aboard = false;
  }
  if (loc_entry->state == RETRY_WITH_BIGGER_TS) {
    check_version(new_version, "inspect_accepts: loc_entry->state == RETRY_WITH_BIGGER_TS");
    take_kv_ptr_with_higher_TS(p_ops, loc_entry, (new_version + 2), false, t_id);
    check_state_with_allowed_flags(5, (int) loc_entry->state, INVALID_RMW, PROPOSED,
                                   NEEDS_KV_PTR, MUST_BCAST_COMMITS);
    //zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
    if (loc_entry->state == PROPOSED) {
      insert_prop_to_read_fifo(p_ops, loc_entry, t_id);
    }
  }
  else if (loc_entry->state != PROPOSED)
    zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);

  if (loc_entry->state == INVALID_RMW || loc_entry->state == NEEDS_KV_PTR) {
    if (ENABLE_ASSERTIONS) {
      assert(dbg_loc_entry->log_no == loc_entry->log_no);
      assert(rmw_ids_are_equal(&dbg_loc_entry->rmw_id, &loc_entry->rmw_id));
      assert(compare_ts(&dbg_loc_entry->new_ts, &loc_entry->new_ts));
    }
  }
  /* The loc_entry can be in Proposed only when it retried with bigger TS */
}


//------------------------------REGULAR INSPECTIONS------------------------------------------

// Inspect each propose that has gathered a quorum of replies
static inline void inspect_proposes(struct pending_ops *p_ops,
                                    struct rmw_local_entry *loc_entry,
                                    uint16_t t_id)
{
  uint32_t new_version = 0;
  struct rmw_local_entry tmp;
  struct rmw_local_entry *dbg_loc_entry = &tmp;
  bool zero_out_log_too_high_cntr = true;
  memcpy(dbg_loc_entry, loc_entry, sizeof(struct rmw_local_entry));
  // RMW_ID COMMITTED
  if (loc_entry->rmw_reps.rmw_id_commited > 0) {
    debug_fail_help(loc_entry, " rmw id committed", t_id);
    // as an optimization clear the kv_ptr entry if it is still in proposed state
    if (loc_entry->accepted_log_no != loc_entry->log_no)
      free_kv_ptr_if_rmw_failed(loc_entry, PROPOSED, t_id);
    handle_already_committed_rmw(p_ops, loc_entry, t_id);
  }
    // LOG_NO TOO SMALL
  else if (loc_entry->rmw_reps.log_too_small > 0) {
    debug_fail_help(loc_entry, " log too small", t_id);
    //It is impossible for this RMW to still hold the kv_ptr
    loc_entry->state = NEEDS_KV_PTR;
  }
    // SEEN HIGHER-TS PROPOSE OR ACCEPT
  else if (loc_entry->rmw_reps.seen_higher_prop_acc > 0) {
    debug_fail_help(loc_entry, " seen higher prop", t_id);
    // retry by incrementing the highest ts seen
    loc_entry->state = RETRY_WITH_BIGGER_TS;
    new_version = loc_entry->rmw_reps.seen_higher_prop_version;
    check_version(new_version, "inspect_proposes: loc_entry->rmw_reps.seen_higher_prop > 0");
  }
    // ACK QUORUM
  else if (loc_entry->rmw_reps.acks >= QUORUM_NUM &&
           loc_entry->helping_flag != PROPOSE_NOT_LOCALLY_ACKED) {
    debug_fail_help(loc_entry, " quorum", t_id);
    // Quorum of prop acks gathered: send an accept
    act_on_quorum_of_prop_acks(p_ops, loc_entry, t_id);
    check_state_with_allowed_flags(5, (int) loc_entry->state, INVALID_RMW,
                                   ACCEPTED, NEEDS_KV_PTR, MUST_BCAST_COMMITS);
  }
    // ALREADY ACCEPTED AN RMW WITH LOWER_TS
  else if (loc_entry->rmw_reps.already_accepted > 0) {
    debug_fail_help(loc_entry, " already accepted", t_id);
    if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED)
      act_on_quorum_of_prop_acks(p_ops, loc_entry, t_id);
    else act_on_receiving_already_accepted_rep_to_prop(p_ops, loc_entry, &new_version, t_id);
    check_state_with_allowed_flags(5, (int) loc_entry->state, INVALID_RMW, ACCEPTED,
                                   NEEDS_KV_PTR, MUST_BCAST_COMMITS);
  }
    // LOG TOO HIGH
  else if (loc_entry->rmw_reps.log_too_high > 0) {
    react_on_log_too_high(loc_entry, true, t_id);
    new_version = loc_entry->new_ts.version;
    zero_out_log_too_high_cntr = false;

  }
  else if (ENABLE_ASSERTIONS) assert(false);

  if (zero_out_log_too_high_cntr) loc_entry->log_too_high_cntr = 0;

  // DECIDE WHETHER THE RMW IS KILLABLE
  if (ENABLE_CAS_CANCELLING) {
    loc_entry->killable = (loc_entry->state == RETRY_WITH_BIGGER_TS ||
                           loc_entry->state == NEEDS_KV_PTR) &&
                          loc_entry->accepted_log_no == 0 &&
                          loc_entry->opcode == COMPARE_AND_SWAP_WEAK;

  }
  // CLEAN_UP
  if (loc_entry->state == RETRY_WITH_BIGGER_TS) {
    check_version(new_version, "inspect_proposes: loc_entry->state == RETRY_WITH_BIGGER_TS");
    take_kv_ptr_with_higher_TS(p_ops, loc_entry, (new_version + 2), true, t_id);
    check_state_with_allowed_flags(5, (int) loc_entry->state, INVALID_RMW, PROPOSED,
                                   NEEDS_KV_PTR, MUST_BCAST_COMMITS);
    if (loc_entry->state == PROPOSED) {
      insert_prop_to_read_fifo(p_ops, loc_entry, t_id);
    }
  }
  else if (loc_entry->state != PROPOSED && loc_entry->helping_flag != HELP_PREV_COMMITTED_LOG_TOO_HIGH) {
    if (loc_entry->state != ACCEPTED) {
      check_state_with_allowed_flags(5, (int) loc_entry->state, INVALID_RMW, NEEDS_KV_PTR,
                                     MUST_BCAST_COMMITS, MUST_BCAST_COMMITS_FROM_HELP);
      if (ENABLE_ASSERTIONS && loc_entry->state != MUST_BCAST_COMMITS_FROM_HELP) {
        assert(dbg_loc_entry->log_no == loc_entry->log_no);
        assert(rmw_ids_are_equal(&dbg_loc_entry->rmw_id, &loc_entry->rmw_id));
        assert(compare_ts(&dbg_loc_entry->new_ts, &loc_entry->new_ts));
        //free_kv_ptr_if_rmw_failed(loc_entry, PROPOSED, t_id);
      }
    }
    zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
  }
/* The loc_entry can be in Proposed only when it retried with bigger TS */
}


// Inspect each propose that has gathered a quorum of replies
static inline void inspect_accepts(struct pending_ops *p_ops,
                                   struct rmw_local_entry *loc_entry,
                                   uint16_t t_id)
{
  struct rmw_local_entry tmp;
  struct rmw_local_entry *dbg_loc_entry = &tmp;
  if (ENABLE_ASSERTIONS)
    memcpy(dbg_loc_entry, loc_entry, sizeof(struct rmw_local_entry));

  uint8_t remote_quorum = (uint8_t) (loc_entry->all_aboard ?
                                     MACHINE_NUM : QUORUM_NUM);

  uint32_t new_version = 0;
  if (loc_entry->helping_flag != NOT_HELPING) {
    if (ENABLE_ASSERTIONS) assert(loc_entry->helping_flag == HELPING);
    if (loc_entry->rmw_reps.rmw_id_commited  + loc_entry->rmw_reps.log_too_small +
        loc_entry->rmw_reps.already_accepted + loc_entry->rmw_reps.seen_higher_prop_acc +
        loc_entry->rmw_reps.log_too_high > 0) {
      reinstate_loc_entry_after_helping(loc_entry, t_id);
      return;
    }
  }
  // RMW_ID COMMITTED
  if (loc_entry->rmw_reps.rmw_id_commited > 0) {
    handle_already_committed_rmw(p_ops, loc_entry, t_id);
    if (ENABLE_ASSERTIONS) assert(loc_entry->helping_flag == NOT_HELPING);
  }
    // LOG_NO TOO SMALL
  else if (loc_entry->rmw_reps.log_too_small > 0) {
    //It is impossible for this RMW to still hold the kv_ptr
    loc_entry->state = NEEDS_KV_PTR;
    if (ENABLE_ASSERTIONS) assert(loc_entry->helping_flag == NOT_HELPING);
  }
    // ACK QUORUM
  else if (loc_entry->rmw_reps.acks >= remote_quorum) {
    if (ENABLE_ASSERTIONS) {
      assert(loc_entry->state != COMMITTED);
      if (loc_entry->helping_flag == HELPING) assert(!loc_entry->all_aboard);
    }
    loc_entry->state = (uint8_t) (loc_entry->helping_flag == HELPING ?
                                  MUST_BCAST_COMMITS_FROM_HELP : MUST_BCAST_COMMITS);
  }
  // SEEN HIGHER-TS PROPOSE
  else if (loc_entry->rmw_reps.seen_higher_prop_acc > 0) {
    // retry by incrementing the highest ts seen
    loc_entry->state = RETRY_WITH_BIGGER_TS;
    new_version = loc_entry->rmw_reps.seen_higher_prop_version;
    check_version(new_version, "inspect_accepts: loc_entry->rmw_reps.seen_higher_prop > 0");
    if (ENABLE_ASSERTIONS) assert(loc_entry->helping_flag == NOT_HELPING);
  }
    // LOG TOO HIGH
  else if (loc_entry->rmw_reps.log_too_high > 0) {
    loc_entry->state = RETRY_WITH_BIGGER_TS;
    new_version = loc_entry->new_ts.version;
  }

    // if a quorum of messages have been received but
    // we are waiting for more, then we are doing all aboard
  else if (ENABLE_ALL_ABOARD) {
    if (ENABLE_ASSERTIONS) assert(loc_entry->all_aboard);
    loc_entry->all_aboard_time_out++;
    if (ENABLE_ASSERTIONS) assert(loc_entry->new_ts.version == 2);
    if (loc_entry->all_aboard_time_out > ALL_ABOARD_TIMEOUT_CNT) {
      // printf("Wrkr %u, Timing out on key %u \n", t_id, loc_entry->key.bkt);
      loc_entry->state = RETRY_WITH_BIGGER_TS;
      loc_entry->all_aboard_time_out = 0;
      new_version = 2;
    }
    else return; // avoid zeroing out the responses
  }
  else if (ENABLE_ASSERTIONS) assert(false);


  clean_up_after_inspecting_accept(p_ops, loc_entry, new_version, dbg_loc_entry, t_id);

}



/*--------------------------------------------------------------------------
 * --------------------INIT RMW-------------------------------------
 * --------------------------------------------------------------------------*/

// Insert an RMW in the local RMW structs
static inline void insert_rmw(struct pending_ops *p_ops, struct trace_op *prop,
                              struct kvs_resp *resp, uint16_t t_id)
{
  uint16_t session_id = prop->session_id;
  if (resp->type == RMW_FAILURE) {
    //printf("Wrkr%u, sess %u, entry %u rmw_failing \n", t_id, session_id, resp->rmw_entry);
    signal_completion_to_client(session_id, prop->index_to_req_array, t_id);
    p_ops->sess_info[session_id].stalled = false;
    p_ops->all_sessions_stalled = false;
    return;
  }
  if (ENABLE_ASSERTIONS) assert(session_id < SESSIONS_PER_THREAD);
  struct rmw_local_entry *loc_entry = &p_ops->prop_info->entry[session_id];
  if (ENABLE_ASSERTIONS) {
    if (loc_entry->state != INVALID_RMW) {
      my_printf(red, "Wrkr %u Expected an invalid loc entry for session %u, loc_entry state %u \n",
                t_id, session_id, loc_entry->state);
      assert(false);
    }
  }
  init_loc_entry(resp, p_ops, prop, t_id, loc_entry);
  p_ops->prop_info->l_id++;
  // if the kv_ptr was occupied, put in the next op to try next round
  if (resp->type == RETRY_RMW_KEY_EXISTS) {
    //if (DEBUG_RMW) my_printf(green, "Worker %u failed to do its RMW and moved "
    //      "it from position %u to %u \n", t_id, op_i, *old_op_i);
    loc_entry->state = NEEDS_KV_PTR;
    // Set up the state that the RMW should wait on
    loc_entry->help_rmw->rmw_id = resp->kv_ptr_rmw_id;
    loc_entry->help_rmw->state = resp->kv_ptr_state;
    loc_entry->help_rmw->ts = resp->kv_ptr_ts;
    loc_entry->help_rmw->log_no = resp->log_no;
  }
  else if (resp->type == RMW_SUCCESS) { // the RMW has gotten an entry and is to be sent
    fill_loc_rmw_entry_on_grabbing_kv_ptr(p_ops, loc_entry, prop->ts.version,
                                          PROPOSED, session_id, t_id);
    loc_entry->log_no = resp->log_no;
    if (ENABLE_ALL_ABOARD && prop->attempt_all_aboard) {
      if (ENABLE_ASSERTIONS) assert(prop->ts.version == ALL_ABOARD_TS);
      act_on_quorum_of_prop_acks(p_ops, loc_entry, t_id);
      //if the flag is ACCEPTED, that means that accept messages
      // are already lined up to be broadcast, and thus you MUST do All aboard
      if (loc_entry->state == ACCEPTED) {
        loc_entry->all_aboard = true;
        loc_entry->all_aboard_time_out = 0;
      }
    }
    else {
      if (ENABLE_ASSERTIONS) assert(prop->ts.version == PAXOS_TS);
      local_rmw_ack(loc_entry);
      insert_prop_to_read_fifo(p_ops, loc_entry, t_id);
    }
  }
  else my_assert(false, "Wrong resp type in RMW");
}


#endif //KITE_PAXOS_UTIL_H

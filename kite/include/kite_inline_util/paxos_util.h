//
// Created by vasilis on 22/05/20.
//

#ifndef KITE_PAXOS_UTIL_H
#define KITE_PAXOS_UTIL_H

#include <config.h>
#include "paxos_generic_util.h"


/*--------------------------------------------------------------------------
 * --------------------KVS_FILTERING--RECEIVING PROPOSE ACCEPT---------------
 * --------------------------------------------------------------------------*/

// Check the global RMW-id structure, to see if an RMW has already been committed
static inline bool the_rmw_has_committed(uint64_t rmw_id,
                                         bool same_log, uint16_t t_id,
                                         struct rmw_rep_last_committed *rep)
{
  uint64_t glob_sess_id = rmw_id % GLOBAL_SESSION_NUM;
  if (ENABLE_ASSERTIONS) assert(glob_sess_id < GLOBAL_SESSION_NUM);
  if (committed_glob_sess_rmw_id[glob_sess_id] >= rmw_id) {
    if (DEBUG_RMW)
      my_printf(green, "Worker %u: A Remote machine  is trying a propose with global sess_id %u, "
                  "rmw_id %lu, that has been already committed \n",
                t_id, glob_sess_id, rmw_id);
    rep->opcode = (uint8_t) (same_log ? RMW_ID_COMMITTED_SAME_LOG : RMW_ID_COMMITTED);
    return true;
  }
  else return false;
}

// Returns true if the received log-no is smaller than the committed.
static inline bool is_log_smaller_or_has_rmw_committed(uint32_t log_no, mica_op_t *kv_ptr,
                                                       uint64_t rmw_l_id,
                                                       uint16_t t_id,
                                                       struct rmw_rep_last_committed *rep)
{
  check_log_nos_of_kv_ptr(kv_ptr, "is_log_smaller_or_has_rmw_committed", t_id);
  bool same_log = kv_ptr->last_committed_log_no == log_no;
  if (the_rmw_has_committed(rmw_l_id, same_log, t_id, rep)) {
    return true;
  }
  else if (kv_ptr->last_committed_log_no >= log_no ||
           kv_ptr->log_no > log_no) {
    if (DEBUG_RMW)
      my_printf(yellow, "Wkrk %u Log number is too small %u/%u entry state %u, propose/accept with rmw_lid %u,"
                  " \n", t_id, log_no, kv_ptr->last_committed_log_no,
                kv_ptr->state, rmw_l_id);
    rep->opcode = LOG_TOO_SMALL;
    fill_reply_entry_with_committed_RMW (kv_ptr, rep, t_id);
    return true;
  }
  else if (DEBUG_RMW) { // remote log is higher than the locally stored!
    if (kv_ptr->log_no < log_no && log_no > 1 )
      my_printf(yellow, "Wkrk %u Log number is higher than expected %u/%u, entry state %u, "
                  "propose/accept with rmw_lid %u\n",
                t_id, log_no, kv_ptr->log_no,
                kv_ptr->state, rmw_l_id);
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

static inline uint8_t is_base_ts_too_small(mica_op_t *kv_ptr,
                                           struct propose *prop,
                                           struct rmw_rep_last_committed *rep,
                                           uint16_t t_id)
{
  if (prop->base_ts.version == DO_NOT_CHECK_BASE_TS) return RMW_ACK;

  compare_t  comp_ts = compare_ts(&kv_ptr->ts, &prop->base_ts);
  if (comp_ts == GREATER) {
    rep->ts.version = kv_ptr->ts.version;
    rep->ts.m_id = kv_ptr->ts.m_id;
    memcpy(rep->value, kv_ptr->value, (size_t) RMW_VALUE_SIZE);
    return RMW_ACK_BASE_TS_STALE;


  }
  else return RMW_ACK;
}
/*--------------------------------------------------------------------------
 * --------------------RECEIVING REPLY-UTILITY-------------------------------------
 * --------------------------------------------------------------------------*/

// Search in the prepare entries for an lid (used when receiving a prep reply)
static inline int search_prop_entries_with_l_id(struct prop_info *prop_info, uint8_t state, uint64_t l_id)
{
  //for (uint16_t i = 0; i < LOCAL_PROP_NUM; i++) {
  uint16_t entry = (uint16_t) (l_id % SESSIONS_PER_THREAD);
  if (ENABLE_ASSERTIONS) assert(entry < LOCAL_PROP_NUM);
  if (prop_info->entry[entry].state == state &&
      prop_info->entry[entry].l_id == l_id)
    return entry;
  //}
  return -1; // i.e. l_id not found!!

}



/*--------------------------------------------------------------------------
 * --------------------RMW-INIT---------------------------------------------
 * --------------------------------------------------------------------------*/

// If a local RMW managed to grab a kv_ptr, then it sets up its local entry
static inline void fill_loc_rmw_entry_on_grabbing_kv_ptr(p_ops_t *p_ops,
                                                         loc_entry_t *loc_entry,
                                                         uint32_t version, uint8_t state,
                                                         uint16_t sess_i, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    check_global_sess_id((uint8_t) machine_id, t_id,
                         (uint16_t) sess_i, loc_entry->rmw_id.id);
    check_version(version, "fill_loc_rmw_entry_on_grabbing_global");
    assert(!loc_entry->rmw_reps.ready_to_inspect);
    assert(loc_entry->rmw_reps.tot_replies == 0);
  }

  //zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);

  local_rmw_ack(loc_entry);
  loc_entry->state = state;
  if (!TURN_OFF_KITE) loc_entry->epoch_id = (uint64_t) epoch_id;
  loc_entry->new_ts.version = version;
  loc_entry->new_ts.m_id = (uint8_t) machine_id;
}

// Initialize a local  RMW entry on the first time it gets allocated
static inline void init_loc_entry(p_ops_t* p_ops,
                                  trace_op_t *op,
                                  uint16_t t_id, loc_entry_t* loc_entry)
{
  loc_entry->opcode = op->opcode;
  if (ENABLE_ASSERTIONS) {
    assert(op->real_val_len <= RMW_VALUE_SIZE);
    assert(!loc_entry->rmw_reps.ready_to_inspect);
    assert(loc_entry->rmw_reps.tot_replies == 0);
    assert(loc_entry->state == INVALID_RMW);

  }
  if (opcode_is_compare_rmw(op->opcode) || op->opcode == RMW_PLAIN_WRITE)
    memcpy(loc_entry->value_to_write, op->value_to_write, op->real_val_len);
  loc_entry->killable = op->opcode == COMPARE_AND_SWAP_WEAK;
  if (opcode_is_compare_rmw(op->opcode))
    loc_entry->compare_val = op->value_to_read; //expected value
  else if (op->opcode == FETCH_AND_ADD) {
    loc_entry->compare_val = op->value_to_write; // value to be added
  }
  loc_entry->must_release = ACCEPT_IS_RELEASE != 0;
  loc_entry->fp_detected = false;
  loc_entry->rmw_val_len = op->real_val_len;
  loc_entry->rmw_is_successful = false;
  loc_entry->all_aboard = ENABLE_ALL_ABOARD && op->attempt_all_aboard;
  loc_entry->avoid_val_in_com = false;
  loc_entry->base_ts_found = false;
  loc_entry->all_aboard_time_out = 0;
  memcpy(&loc_entry->key, &op->key, KEY_SIZE);
  memset(&loc_entry->rmw_reps, 0, sizeof(struct rmw_rep_info));
  loc_entry->index_to_req_array = op->index_to_req_array;

  loc_entry->back_off_cntr = 0;
  loc_entry->log_too_high_cntr = 0;
  loc_entry->helping_flag = NOT_HELPING;
  loc_entry->rmw_id.id+= GLOBAL_SESSION_NUM;
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->rmw_id.id % GLOBAL_SESSION_NUM == loc_entry->glob_sess_id);
    assert(glob_ses_id_to_t_id((uint32_t) (loc_entry->rmw_id.id % GLOBAL_SESSION_NUM)) == t_id &&
           glob_ses_id_to_m_id((uint32_t) (loc_entry->rmw_id.id % GLOBAL_SESSION_NUM)) == machine_id);
  }
  advance_loc_entry_l_id(loc_entry, t_id);
  loc_entry->accepted_log_no = 0;
  //my_printf(yellow, "Init  RMW-id %u  \n",
  // loc_entry->rmw_id.id);
  loc_entry->help_loc_entry->state = INVALID_RMW;
}


// Activate the entry that belongs to a given key to initiate an RMW (either a local or a remote)
static inline void activate_kv_pair(uint8_t state, uint32_t new_version, mica_op_t *kv_ptr,
                                    uint8_t opcode, uint8_t new_ts_m_id, loc_entry_t *loc_entry,
                                    uint64_t rmw_id,
                                    uint32_t log_no, uint16_t t_id, const char *message)
{
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->log_no == log_no && kv_ptr->state == ACCEPTED && state != ACCEPTED) {
      printf("%s \n", message);
      assert(false);
    }
    assert(kv_ptr->log_no <= log_no);
  }
  // pass the new base_ts!
  kv_ptr->opcode = opcode;
  kv_ptr->prop_ts.m_id = new_ts_m_id;
  //if (ENABLE_ASSERTIONS) assert(new_version >= kv_ptr->prop_ts.version);
  kv_ptr->prop_ts.version = new_version;
  //my_printf(cyan, "Wrkr %u Activating from remote accept rmw_id %u \n", t_id, rmw_id);
  kv_ptr->rmw_id.id = rmw_id;
  kv_ptr->state = state;
  kv_ptr->log_no = log_no;

  if (state == ACCEPTED) {
    if (ENABLE_ASSERTIONS) {
      assert(kv_ptr->prop_ts.version == new_version);
      assert(kv_ptr->prop_ts.m_id == new_ts_m_id);
      kv_ptr->accepted_rmw_id = kv_ptr->rmw_id;
    }
    kv_ptr->accepted_ts = kv_ptr->prop_ts;
    kv_ptr->accepted_log_no = log_no;
    if (loc_entry != NULL && loc_entry->all_aboard) {
      perform_the_rmw_on_the_loc_entry(kv_ptr, loc_entry, t_id);
    }
  }
  if (ENABLE_ASSERTIONS) {
    if (committed_glob_sess_rmw_id[kv_ptr->rmw_id.id % GLOBAL_SESSION_NUM] >= kv_ptr->rmw_id.id) {
      //my_printf(red, "Wrkr %u, attempts to activate with already committed RMW id %u/%u glob_sess id %u, state %u: %s \n",
      //           t_id, kv_ptr->rmw_id.id, committed_glob_sess_rmw_id[kv_ptr->rmw_id.id % GLOBAL_SESSION_NUM],
      //           kv_ptr->rmw_id.id % GLOBAL_SESSION_NUM, state, message);
    }
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
  compare_t prop_ts_comp = compare_netw_ts_with_ts(&prop->ts, &kv_ptr->prop_ts);

  if (prop_ts_comp == GREATER) {
    assign_netw_ts_to_ts(&kv_ptr->prop_ts, &prop->ts);
    compare_t acc_ts_comp = compare_netw_ts_with_ts(&prop->ts, &kv_ptr->accepted_ts);
    if (kv_ptr->state == ACCEPTED && acc_ts_comp == GREATER) {
      if (kv_ptr->rmw_id.id == prop->t_rmw_id) {
        return_flag = RMW_ACK_ACC_SAME_RMW;
      }
      else {
        assign_ts_to_netw_ts(&rep->ts, &kv_ptr->accepted_ts);
        return_flag = SEEN_LOWER_ACC;
        rep->rmw_id = kv_ptr->rmw_id.id;
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
    compare_t ts_comp = compare_netw_ts_with_ts(&acc->ts, &kv_ptr->prop_ts);
    // Higher Ts  = Success
    if (ts_comp == EQUAL || ts_comp == GREATER) {
      return_flag = RMW_ACK;
      if (ENABLE_ASSERTIONS) {
        if (DEBUG_RMW && ts_comp == EQUAL && kv_ptr->state == ACCEPTED)
          my_printf(red, "Wrkr %u Received Accept for the same TS as already accepted, "
                      "version %u/%u m_id %u/%u, rmw_id %u/%u\n",
                    t_id, acc->ts.version, kv_ptr->prop_ts.version, acc->ts.m_id,
                    kv_ptr->prop_ts.m_id, acc->t_rmw_id, kv_ptr->rmw_id.id);
      }
    }
    else if (ts_comp == SMALLER) {
      if (kv_ptr->state == PROPOSED) {
        return_flag = SEEN_HIGHER_PROP;
      }
      else if (kv_ptr->state == ACCEPTED) {
        return_flag = SEEN_HIGHER_ACC;
      }
      else if (ENABLE_ASSERTIONS) assert(false);
      assign_ts_to_netw_ts(&rep->ts, &kv_ptr->prop_ts);
    }
    else if (ENABLE_ASSERTIONS) assert(false);
  }

  if (DEBUG_RMW)
    my_printf(yellow, "Wrkr %u: %s Accept with rmw_id %u, log_no: %u, base_ts.version: %u, ts_m_id %u,"
                "locally stored state: %u, locally stored base_ts: version %u, m_id %u \n",
              t_id, return_flag == RMW_ACK ? "Acks" : "Nacks",
              acc->t_rmw_id, acc->log_no,
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
    flag = is_prop ? propose_snoops_entry((struct propose *) prop_or_acc, kv_ptr, sender_m_id, t_id, rep) :
           accept_snoops_entry((struct accept *) prop_or_acc, kv_ptr, sender_m_id, t_id, rep);
  return flag;
}


/*--------------------------------------------------------------------------
 * --------------------ACCEPTING-------------------------------------
 * --------------------------------------------------------------------------*/


// After gathering a quorum of proposal acks, check if you can accept locally-- THIS IS STRICTLY LOCAL RMWS -- no helps
// Every RMW that gets committed must pass through this function successfully (at least one time)
static inline void attempt_local_accept(p_ops_t *p_ops, loc_entry_t *loc_entry,
                                        uint16_t t_id)
{
  mica_op_t *kv_ptr = loc_entry->kv_ptr;
  checks_preliminary_local_accept(kv_ptr, loc_entry, t_id);

  lock_seqlock(&loc_entry->kv_ptr->seqlock);
  if (if_already_committed_bcast_commits(p_ops, loc_entry, t_id)) {
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
    return;
  }

  if (same_rmw_id_same_ts_and_invalid(kv_ptr, loc_entry)) {
    checks_before_local_accept(kv_ptr, loc_entry, t_id);
    //state would be typically proposed, but may also be accepted if someone has helped

    kv_ptr->state = ACCEPTED;
    // calculate the new value depending on the type of RMW
    perform_the_rmw_on_the_loc_entry(kv_ptr, loc_entry, t_id);
    //when last_accepted_value is update also update the acc_base_ts
    kv_ptr->base_acc_ts = kv_ptr->ts;
    kv_ptr->accepted_ts = loc_entry->new_ts;
    kv_ptr->accepted_log_no = kv_ptr->log_no;
    checks_after_local_accept(kv_ptr, loc_entry, t_id);
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
    loc_entry->state = ACCEPTED;
  }
  else { // the entry stores a different rmw_id and thus our proposal has been won by another
    // Some other RMW has won the RMW we are trying to get accepted
    // If the other RMW has been committed then the last_committed_log_no will be bigger/equal than the current log no

    loc_entry->state = NEEDS_KV_PTR;
    checks_after_failure_to_locally_accept(kv_ptr, loc_entry, t_id);
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
  }
}

// After gathering a quorum of proposal reps, one of them was a lower TS accept, try and help it
static inline void attempt_local_accept_to_help(loc_entry_t *loc_entry,
                                                   uint16_t t_id)
{
  bool kv_ptr_is_the_same, kv_ptr_is_invalid_but_not_committed,
       helping_stuck_accept, propose_locally_accepted;
  mica_op_t *kv_ptr = loc_entry->kv_ptr;
  loc_entry_t* help_loc_entry = loc_entry->help_loc_entry;
  help_loc_entry->new_ts = loc_entry->new_ts;
  checks_preliminary_local_accept_help(kv_ptr, loc_entry, help_loc_entry);

  lock_seqlock(&loc_entry->kv_ptr->seqlock);

  //We don't need to check if the RMW is already registered here -- it's not wrong to do so--
  // but if the RMW has been committed, it will be in the present log_no
  // and we will not be able to accept locally anyway.

  find_out_if_can_accept_help_locally(kv_ptr, loc_entry, help_loc_entry, &kv_ptr_is_the_same,
                                      &kv_ptr_is_invalid_but_not_committed,
                                      &helping_stuck_accept, &propose_locally_accepted, t_id);

  if (kv_ptr_is_the_same   || kv_ptr_is_invalid_but_not_committed ||
      helping_stuck_accept || propose_locally_accepted) {
    kv_ptr->state = ACCEPTED;
    kv_ptr->rmw_id = help_loc_entry->rmw_id;
    kv_ptr->accepted_ts = help_loc_entry->new_ts;
    kv_ptr->accepted_log_no = kv_ptr->log_no;
    write_kv_ptr_acc_val(kv_ptr, help_loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
    kv_ptr->base_acc_ts = help_loc_entry->base_ts;// the base_ts of the RMW we are helping
    checks_after_local_accept_help(kv_ptr, loc_entry, t_id);
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
    loc_entry->state = ACCEPTED;
  }
  else {
    checks_after_failure_to_locally_accept_help(kv_ptr, loc_entry, t_id);
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
    loc_entry->state = NEEDS_KV_PTR;
    help_loc_entry->state = INVALID_RMW;
  }
}


/*--------------------------------------------------------------------------
 * --------------------COMMITING-------------------------------------
 * --------------------------------------------------------------------------*/

static inline void commit_algorithm(mica_op_t *kv_ptr,
                                    commit_info_t *com_info,
                                    uint16_t t_id)
{
  check_inputs_commit_algorithm(kv_ptr, com_info, t_id);

  lock_seqlock(&kv_ptr->seqlock);

  check_state_before_commit_algorithm(kv_ptr, com_info, t_id);

  // 0. Check if it's a commit without a value -- if it cannot be committed
  // then do not attempt to overwrite the value and timestamp, because the commit's
  // value and ts are stored in the kv_ptr->accepted_value/ts and may have been lost
  if (com_info->no_value) {
    if (!can_process_com_no_value(kv_ptr, com_info, t_id)) {
      com_info->overwrite_kv = false;
    }
  }


  // 1. Clear the kv_ptr state and advance its log-no
  if (kv_ptr->log_no <= com_info->log_no) {
    kv_ptr->log_no = com_info->log_no;
    kv_ptr->state = INVALID_RMW;
  }

  // 2. Apply the value if the carstamp is bigger
  if (com_info->overwrite_kv) {
    compare_t cart_comp = compare_carts(&com_info->base_ts, com_info->log_no,
                                        &kv_ptr->ts, kv_ptr->last_committed_log_no);
    check_on_overwriting_commit_algorithm(kv_ptr, com_info, cart_comp, t_id);
    if (cart_comp == GREATER) {
      write_kv_ptr_val(kv_ptr, com_info->value, (size_t) VALUE_SIZE, com_info->flag);
      kv_ptr->ts = com_info->base_ts;
    }
  }

  // 3. Advance the last_committed log_no and rmw_id
  check_on_updating_rmw_meta_commit_algorithm(kv_ptr, com_info, t_id);
  if (kv_ptr->last_committed_log_no < com_info->log_no) {
    kv_ptr->last_committed_log_no = com_info->log_no;
    kv_ptr->last_committed_rmw_id = com_info->rmw_id;
  }


  check_log_nos_of_kv_ptr(kv_ptr, com_info->message, t_id);

  // 4. Unconditionally attempt to register the rmw
  register_committed_global_sess_id(com_info->rmw_id.id, t_id);
  check_registered_against_kv_ptr_last_committed(kv_ptr, com_info->rmw_id.id,
                                                 com_info->message, t_id);
  unlock_seqlock(&kv_ptr->seqlock);
}


static inline void commit_rmw(mica_op_t *kv_ptr,
                              void* rmw, loc_entry_t *loc_entry,
                              uint8_t flag, uint16_t t_id)
{

  process_commit_flags(rmw, loc_entry, &flag);
  struct rmw_rep_last_committed *rep;
  struct commit *com;
  struct commit_no_val *com_no_val;
  r_info_t* r_info;
  commit_info_t com_info;
  //mica_op_t *kv_ptr = loc_entry->kv_ptr;
  struct ts_tuple base_ts = {0, 0};
  switch (flag) {
    case FROM_LOG_TOO_LOW_REP:
      rep = (struct rmw_rep_last_committed *) rmw;
      assign_netw_ts_to_ts(&base_ts, &rep->ts);
      fill_commit_info(&com_info, flag, rep->rmw_id,
                       rep->log_no_or_base_version,
                       base_ts, rep->value, true);
      break;
    case FROM_ALREADY_COMM_REP:
    case FROM_LOCAL:
      fill_commit_info(&com_info, flag, loc_entry->rmw_id.id,
                       loc_entry->accepted_log_no, loc_entry->base_ts,
                       loc_entry->value_to_write, loc_entry->rmw_is_successful);
      break;
    case FROM_ALREADY_COMM_REP_HELP:
    case FROM_LOCAL_HELP:
      fill_commit_info(&com_info, flag, loc_entry->help_loc_entry->rmw_id.id,
                       loc_entry->help_loc_entry->log_no,
                       loc_entry->help_loc_entry->base_ts,
                       loc_entry->help_loc_entry->value_to_write,
                       true);
      break;
    case FROM_REMOTE_COMMIT:
      com = (struct commit *) rmw;
      assert(com->opcode == COMMIT_OP);
      assign_netw_ts_to_ts(&base_ts, &com->base_ts);
      fill_commit_info(&com_info, flag, com->t_rmw_id,
                       com->log_no, base_ts, com->value, true);
      break;
    case FROM_REMOTE_COMMIT_NO_VAL:
      com_no_val = (struct commit_no_val *) rmw;
      fill_commit_info(&com_info, flag, com_no_val->t_rmw_id,
                       com_no_val->log_no, base_ts, NULL, true);
      com_info.no_value = true;
      break;
    case FROM_LOCAL_ACQUIRE:
    case FROM_OOE_READ:
      r_info = (r_info_t *) rmw;
      fill_commit_info(&com_info, flag, r_info->rmw_id.id,
                       r_info->log_no, r_info->ts_to_read,
                       r_info->value, true);
      break;
    default:
      if (ENABLE_ASSERTIONS) {assert(false);}
  }
  commit_algorithm(kv_ptr, &com_info, t_id);
}



// On gathering quorum of acks for commit, commit locally and signal that the session must be freed if not helping
static inline void act_on_quorum_of_commit_acks(p_ops_t *p_ops, uint32_t ack_ptr, uint16_t t_id)
{
  loc_entry_t *loc_entry = &p_ops->prop_info->entry[p_ops->w_meta[ack_ptr].sess_id];
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry != NULL);
    assert(loc_entry->state = COMMITTED);
  }

  if (loc_entry->helping_flag == HELPING &&
      rmw_ids_are_equal(&loc_entry->help_loc_entry->rmw_id, &loc_entry->rmw_id)) {
    loc_entry->helping_flag = HELPING_MYSELF;
    my_printf(red, "Helping myself, but should not\n");
  }

  if (loc_entry->helping_flag != HELP_PREV_COMMITTED_LOG_TOO_HIGH)
    commit_rmw(loc_entry->kv_ptr, NULL, loc_entry, FROM_LOCAL, t_id);
    //attempt_local_commit(p_ops, loc_entry, t_id);

  switch(loc_entry->helping_flag)
  {
    case NOT_HELPING:
    case PROPOSE_NOT_LOCALLY_ACKED:
    case HELPING_MYSELF: // Deprecated
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
      loc_entry->state = RETRY_WITH_BIGGER_TS;
      loc_entry->helping_flag = NOT_HELPING;
      break;
    default: if (ENABLE_ASSERTIONS) assert(false);
  }
}



/*--------------------------------------------------------------------------
 * --------------------HANDLE REPLIES-------------------------------------
 * --------------------------------------------------------------------------*/

// The help_loc_entry is used when receiving an already committed reply or an already accepted
static inline void store_rmw_rep_to_help_loc_entry(loc_entry_t* loc_entry,
                                                   struct rmw_rep_last_committed* prop_rep, uint16_t t_id)
{
  loc_entry_t *help_loc_entry = loc_entry->help_loc_entry;
  compare_t ts_comp = compare_netw_ts_with_ts(&prop_rep->ts, &help_loc_entry->new_ts);
  if (ENABLE_ASSERTIONS) {
    if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED) {
      assert(help_loc_entry->new_ts.version > 0);
      assert(help_loc_entry->state == ACCEPTED);
      assert(ts_comp != EQUAL); // It would have been an SAME_ACC_ACK
    }
    assert(help_loc_entry->state == INVALID_RMW || help_loc_entry->state == ACCEPTED);
  }

  if (help_loc_entry->state == INVALID_RMW ||
      ts_comp == GREATER) {
    if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED) loc_entry->helping_flag = NOT_HELPING;
    assign_netw_ts_to_ts(&help_loc_entry->new_ts, &prop_rep->ts);
    help_loc_entry->base_ts.version = prop_rep->log_no_or_base_version;
    help_loc_entry->base_ts.m_id = prop_rep->base_m_id;
    help_loc_entry->log_no = loc_entry->log_no;
    help_loc_entry->state = ACCEPTED;
    help_loc_entry->rmw_id.id = prop_rep->rmw_id;
    memcpy(help_loc_entry->value_to_write, prop_rep->value, (size_t) RMW_VALUE_SIZE);
    help_loc_entry->key = loc_entry->key;
  }
}


// Handle a proposal/accept reply
static inline void handle_prop_or_acc_rep(struct rmw_rep_message *rep_mes,
                                          struct rmw_rep_last_committed *rep,
                                          loc_entry_t *loc_entry,
                                          bool is_accept,
                                          const uint16_t t_id)
{

  rmw_rep_info_t *rep_info = &loc_entry->rmw_reps;
  checks_when_handling_prop_acc_rep(loc_entry, rep, is_accept, t_id);
  rep_info->tot_replies++;
  if (rep_info->tot_replies >= QUORUM_NUM) {
    rep_info->ready_to_inspect = true;
  }

  if (rep->opcode > RMW_ACK_BASE_TS_STALE) rep_info->nacks++;

  switch (rep->opcode) {
    case RMW_ACK_BASE_TS_STALE:
      write_kv_if_conditional_on_netw_ts(loc_entry->kv_ptr, rep->value,
                                    (size_t) VALUE_SIZE, FROM_BASE_TS_STALE, rep->ts);
      assign_netw_ts_to_ts(&loc_entry->base_ts, &rep->ts); // this is an optimization, in case we send proposes again
    case RMW_ACK:
      loc_entry->base_ts_found = true;
      rep_info->acks++;
      if (ENABLE_ASSERTIONS)
        assert(rep_mes->m_id < MACHINE_NUM && rep_mes->m_id != machine_id);
      if (DEBUG_RMW)
        my_printf(green, "Wrkr %u, the received rep is an %s ack, "
                    "total acks %u \n", t_id, is_accept ? "acc" : "prop",
                  rep_info->acks);
      break;
    case RMW_ID_COMMITTED:
      rep_info->no_need_to_bcast = true;
    case RMW_ID_COMMITTED_SAME_LOG:
      rep_info->ready_to_inspect = true;
      rep_info->rmw_id_commited++;
      commit_rmw(loc_entry->kv_ptr, NULL, loc_entry, FROM_ALREADY_COMM_REP, t_id);
      break;
    case LOG_TOO_SMALL:
      rep_info->ready_to_inspect = true;
      rep_info->log_too_small++;
      commit_rmw(loc_entry->kv_ptr, (void*) rep, loc_entry, FROM_LOG_TOO_LOW_REP, t_id);
      //attempt_local_commit_from_rep(rep, loc_entry, t_id);
      break;
    case SEEN_LOWER_ACC:
      rep_info->already_accepted++;
      if (ENABLE_ASSERTIONS) {
        assert(compare_netw_ts_with_ts(&rep->ts, &loc_entry->new_ts) == SMALLER);
        assert(!is_accept);
      }
      // Store the accepted rmw only if no higher priority reps have been seen
      if (rep_info->seen_higher_prop_acc +
          rep_info->rmw_id_commited + rep_info->log_too_small == 0) {
        store_rmw_rep_to_help_loc_entry(loc_entry, rep, t_id);
      }
      break;
    case SEEN_HIGHER_ACC:
    case SEEN_HIGHER_PROP:
      if (!is_accept) rep_info->ready_to_inspect = true;
      rep_info->seen_higher_prop_acc++;
      if (DEBUG_RMW)
        my_printf(yellow, "Wrkr %u: the %s rep is %u, %u sum of all other reps %u \n", t_id,
                  is_accept ? "acc" : "prop",rep->opcode,
                  rep_info->seen_higher_prop_acc,
                  rep_info->rmw_id_commited + rep_info->log_too_small +
                  rep_info->already_accepted);
      if (rep->ts.version > rep_info->seen_higher_prop_version) {
        rep_info->seen_higher_prop_version = rep->ts.version;
        if (DEBUG_RMW)
          my_printf(yellow, "Wrkr %u: overwriting the TS version %u \n",
                    t_id, rep_info->seen_higher_prop_version);
      }
      break;
    case LOG_TOO_HIGH:
      rep_info->log_too_high++;
      break;
    default:
      if (ENABLE_ASSERTIONS) assert(false);
  }

  if (ENABLE_ASSERTIONS) {
    if (is_accept) assert(loc_entry->state == ACCEPTED);
    if (!is_accept) assert(loc_entry->state == PROPOSED);
    check_sum_of_reps(loc_entry);
  }
}


// Handle one accept or propose reply
static inline void handle_single_rmw_rep(p_ops_t *p_ops, struct rmw_rep_last_committed *rep,
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
//    if (prop_info->l_id <= rep->l_id)
//      my_printf(red, "Wrkr %u, rep_i %u, opcode %u, is_accept %d, incoming rep l_id %u, max prop lid %u \n",
//                t_id, r_rep_i, rep->opcode, is_accept, rep->l_id, prop_info->l_id);
//
//    assert(prop_info->l_id > rep->l_id);
  }
  //my_printf(cyan, "RMW rep opcode %u, l_id %u \n", rep->opcode, rep->l_id);
  int entry_i = search_prop_entries_with_l_id(prop_info, (uint8_t) (is_accept ? ACCEPTED : PROPOSED),
                                              rep->l_id);
  if (entry_i == -1) return;
  loc_entry_t *loc_entry = &prop_info->entry[entry_i];
  if (!TURN_OFF_KITE) {
    if (unlikely(rep->opcode) > NO_OP_PROP_REP) {
      increment_epoch_id(loc_entry->epoch_id, t_id);
      rep->opcode -= FALSE_POSITIVE_OFFSET;
      loc_entry->fp_detected = true;
    }
  }
  handle_prop_or_acc_rep(rep_mes, rep, loc_entry, is_accept, t_id);
}

// Handle read replies that refer to RMWs (either replies to accepts or proposes)
static inline void handle_rmw_rep_replies(p_ops_t *p_ops, struct r_rep_message *r_rep_mes,
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
static inline void set_up_a_proposed_but_not_locally_acked_entry(p_ops_t *p_ops, mica_op_t  *kv_ptr,
                                                                 loc_entry_t *loc_entry,
                                                                 bool help_myself, uint16_t t_id)
{
  checks_and_prints_proposed_but_not_locally_acked(p_ops, kv_ptr, loc_entry, t_id);
  loc_entry_t *help_loc_entry = loc_entry->help_loc_entry;
  loc_entry->state = PROPOSED;
  help_loc_entry->state = ACCEPTED;
  if (help_myself) {
    loc_entry->helping_flag = PROPOSE_LOCALLY_ACCEPTED;
  }
  else {
    loc_entry->helping_flag = PROPOSE_NOT_LOCALLY_ACKED;
    help_loc_entry->log_no = loc_entry->log_no;
    help_loc_entry->key = loc_entry->key;
  }
  loc_entry->rmw_reps.tot_replies = 1;
  loc_entry->rmw_reps.already_accepted = 1;
  logging_proposed_but_not_locally_acked(kv_ptr, loc_entry, help_loc_entry, t_id);

}


// When inspecting an RMW that failed to grab a kv_ptr in the past
static inline bool attempt_to_grab_kv_ptr_after_waiting(p_ops_t *p_ops,
                                                        mica_op_t *kv_ptr,
                                                        loc_entry_t *loc_entry,
                                                        uint16_t sess_i, uint16_t t_id)
{
  checks_init_attempt_to_grab_kv_ptr(loc_entry, t_id);
  bool kv_ptr_was_grabbed = false;
  bool rmw_fails = false;
  uint32_t version = PAXOS_TS;

  lock_seqlock(&kv_ptr->seqlock);
  if (if_already_committed_bcast_commits(p_ops, loc_entry, t_id)) {
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
    return true;
  }
  if (kv_ptr->state == INVALID_RMW) {
    if (!rmw_fails_with_loc_entry(loc_entry, kv_ptr, &rmw_fails, t_id)) {
      loc_entry->log_no = kv_ptr->last_committed_log_no + 1;
      activate_kv_pair(PROPOSED, PAXOS_TS, kv_ptr, loc_entry->opcode,
                       (uint8_t) machine_id, NULL, loc_entry->rmw_id.id,
                       loc_entry->log_no, t_id,
                       ENABLE_ASSERTIONS ? "attempt_to_grab_kv_ptr_after_waiting" : NULL);

      kv_ptr_was_grabbed = true;
      print_when_grabbing_kv_ptr(loc_entry, t_id);
    }
  }
  else if (kv_ptr_state_has_changed(kv_ptr, loc_entry->help_rmw)) {
    print_when_state_changed_not_grabbing_kv_ptr(kv_ptr, loc_entry, t_id);
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
    check_and_print_when_rmw_fails(kv_ptr, loc_entry, t_id);
    loc_entry->state = INVALID_RMW;
    free_session_from_rmw(p_ops, loc_entry->sess_id, false, t_id);
    return true;
  }
  return kv_ptr_was_grabbed;
}

// Insert a helping accept in the write fifo after waiting on it
static inline void attempt_to_help_a_locally_accepted_value(p_ops_t *p_ops,
                                                            loc_entry_t *loc_entry,
                                                            mica_op_t *kv_ptr, uint16_t t_id)
{
  bool help = false;
  loc_entry_t *help_loc_entry = loc_entry->help_loc_entry;
  // The stat of the kv_ptr must not be downgraded from ACCEPTED
  lock_seqlock(&loc_entry->kv_ptr->seqlock);
  // check again with the lock in hand
  if (kv_ptr_state_has_not_changed(kv_ptr, loc_entry->help_rmw)) {
    loc_entry->log_no = kv_ptr->accepted_log_no;
    help_loc_entry->new_ts = kv_ptr->accepted_ts;
    help_loc_entry->rmw_id = kv_ptr->rmw_id;
    memcpy(help_loc_entry->value_to_write, kv_ptr->last_accepted_value,
           (size_t) RMW_VALUE_SIZE);
    help_loc_entry->base_ts = kv_ptr->base_acc_ts;

    // we must make it appear as if the kv_ptr has seen our propose
    // and has replied with a lower-base_ts-accept
    loc_entry->new_ts.version = kv_ptr->prop_ts.version + 1;
    loc_entry->new_ts.m_id = (uint8_t) machine_id;
    kv_ptr->prop_ts = loc_entry->new_ts;
    help = true;
    checks_attempt_to_help_locally_accepted(kv_ptr, loc_entry, t_id);
  }
  check_log_nos_of_kv_ptr(kv_ptr, "attempt_to_help_a_locally_accepted_value", t_id);
  unlock_seqlock(&loc_entry->kv_ptr->seqlock);

  loc_entry->back_off_cntr = 0;
  if (help) {
    // Helping means we are proposing, but we are not locally acked:
    // We store a reply from the local machine that says already ACCEPTED
    bool helping_myself = help_loc_entry->rmw_id.id == loc_entry->rmw_id.id;
    set_up_a_proposed_but_not_locally_acked_entry(p_ops, kv_ptr, loc_entry, helping_myself, t_id);
  }
}

// After backing off waiting on a PROPOSED kv_ptr try to steal it
static inline void attempt_to_steal_a_proposed_kv_ptr(p_ops_t *p_ops,
                                                      loc_entry_t *loc_entry,
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
                     (uint8_t) machine_id, NULL, loc_entry->rmw_id.id,
                     loc_entry->log_no, t_id,
                     ENABLE_ASSERTIONS ? "attempt_to_steal_a_proposed_kv_ptr" : NULL);
    loc_entry->base_ts = kv_ptr->ts;
    kv_ptr_was_grabbed = true;
  }
  else if (kv_ptr_state_has_changed(kv_ptr, loc_entry->help_rmw)) {
    print_when_state_changed_steal_proposed(kv_ptr, loc_entry, t_id);
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
    print_after_stealing_proposed(kv_ptr, loc_entry, t_id);
    fill_loc_rmw_entry_on_grabbing_kv_ptr(p_ops, loc_entry, new_version,
                                          PROPOSED, sess_i, t_id);
  }
}


//------------------------------ALREADY-COMMITTED------------------------------------------

//When inspecting an accept/propose and have received already-committed Response
static inline void handle_already_committed_rmw(p_ops_t *p_ops,
                                                loc_entry_t *loc_entry,
                                                uint16_t t_id)
{
  // Broadcast commits iff you got back you own RMW
  if (!loc_entry->rmw_reps.no_need_to_bcast &&
      (loc_entry->rmw_reps.rmw_id_commited < REMOTE_QUORUM)) {
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
}




//------------------------------ACKS------------------------------------------
// If a quorum of proposal acks have been gathered, try to broadcast accepts
static inline void act_on_quorum_of_prop_acks(p_ops_t *p_ops, loc_entry_t *loc_entry,
                                              uint16_t t_id)
{
  // first we need to accept locally,
  attempt_local_accept(p_ops, loc_entry, t_id);
  check_state_with_allowed_flags(4, loc_entry->state, ACCEPTED, NEEDS_KV_PTR, MUST_BCAST_COMMITS);
  checks_acting_on_quorum_of_prop_ack(loc_entry, t_id);

  if (loc_entry->state == ACCEPTED) {
    zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
    local_rmw_ack(loc_entry);
    check_loc_entry_metadata_is_reset(loc_entry, "act_on_quorum_of_prop_acks", t_id);
    insert_accept_in_writes_message_fifo(p_ops, loc_entry, false, t_id);
        loc_entry->killable = false;
  }
}





//------------------------------SEEN-LOWER_ACCEPT------------------------------------------
// When a quorum of prop replies have been received, and one of the replies says it has accepted an RMW with lower TS
static inline void act_on_receiving_already_accepted_rep_to_prop(p_ops_t *p_ops,
                                                                 loc_entry_t* loc_entry,
                                                                 uint16_t t_id)
{
  checks_acting_on_already_accepted_rep(loc_entry, t_id);
  attempt_local_accept_to_help(loc_entry, t_id);
  if (loc_entry->state == ACCEPTED) {
    loc_entry->helping_flag = HELPING;
    zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
    local_rmw_ack(loc_entry);
    insert_accept_in_writes_message_fifo(p_ops, loc_entry->help_loc_entry, true, t_id);
  }

}


//------------------------------LOG-TOO_HIGH------------------------------------------

static inline void react_on_log_too_high_for_prop(loc_entry_t *loc_entry,
                                                  uint16_t t_id)
{
  loc_entry->state = RETRY_WITH_BIGGER_TS;
  loc_entry->log_too_high_cntr++;
  if (loc_entry->log_too_high_cntr == LOG_TOO_HIGH_TIME_OUT) {
    if (ENABLE_ASSERTIONS) {
      my_printf(red, "Timed out on log_too-high\n",
                t_id, loc_entry->sess_id);
      print_loc_entry(loc_entry, yellow, t_id);
    }
    mica_op_t *kv_ptr = loc_entry->kv_ptr;
    lock_seqlock(&kv_ptr->seqlock);
    if (kv_ptr->last_committed_log_no + 1 == loc_entry->log_no) {
      loc_entry->state = MUST_BCAST_COMMITS_FROM_HELP;
      loc_entry_t *help_loc_entry = loc_entry->help_loc_entry;
      memcpy(help_loc_entry->value_to_write, kv_ptr->value, (size_t) VALUE_SIZE);
      help_loc_entry->rmw_id = kv_ptr->last_committed_rmw_id;
      help_loc_entry->base_ts = kv_ptr->ts;
    }
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);

    if (unlikely(loc_entry->state == MUST_BCAST_COMMITS_FROM_HELP)) {
      loc_entry->helping_flag = HELP_PREV_COMMITTED_LOG_TOO_HIGH;
      loc_entry->help_loc_entry->log_no = loc_entry->log_no - 1;
      loc_entry->help_loc_entry->key = loc_entry->key;
    }

    loc_entry->log_too_high_cntr = 0;
  }
}


//------------------------------CLEAN-UP------------------------------------------

static inline void clean_up_after_retrying(p_ops_t *p_ops, mica_op_t *kv_ptr,
                                           loc_entry_t *loc_entry,
                                           bool kv_ptr_was_grabbed,
                                           bool help_locally_acced,
                                           bool rmw_fails,
                                           uint16_t t_id)
{
  if (kv_ptr_was_grabbed) {
    if (DEBUG_RMW)
      my_printf(cyan, "Wrkr %u: session %u gets/regains the kv_ptr log %u to do its propose \n",
                t_id, loc_entry->sess_id, kv_ptr->log_no);
    loc_entry->state = PROPOSED;
    if (help_locally_acced)
      set_up_a_proposed_but_not_locally_acked_entry(p_ops, kv_ptr, loc_entry, true, t_id);
    else local_rmw_ack(loc_entry);
  }
  else if (rmw_fails) {
    if (ENABLE_ASSERTIONS) {
      assert(loc_entry->accepted_log_no == 0);
      assert(loc_entry->killable);
      assert(!help_locally_acced);
    }
    loc_entry->state = INVALID_RMW;
    //printf("Cancelling on needing kv_ptr Wrkr%u, sess %u, entry %u rmw_failing \n",
    //     t_id, loc_entry->sess_id, loc_entry->index_to_rmw);
    assert(ENABLE_CAS_CANCELLING);
    free_session_from_rmw(p_ops, loc_entry->sess_id, false, t_id);
  }
  else loc_entry->state = NEEDS_KV_PTR;
}



static inline void clean_up_after_inspecting_props(p_ops_t *p_ops,
                                                   loc_entry_t *loc_entry,
                                                   bool zero_out_log_too_high_cntr,
                                                   uint16_t t_id)
{

  /* ------------------CHECK STATE---------------------------*/
  check_state_with_allowed_flags(7, (int) loc_entry->state,
                                 INVALID_RMW,          // Already-committed, no need to send commits
                                 RETRY_WITH_BIGGER_TS, // Log-too-high
                                 ACCEPTED,             // Acks or prop_locally_accepted or helping
                                 NEEDS_KV_PTR,         // log-too-small, failed to accept or failed to help because kv_ptr is taken
                                 MUST_BCAST_COMMITS,   //  already-committed, accept attempt found it's already committd
                                 MUST_BCAST_COMMITS_FROM_HELP // log-too-hig timeout
  );
  check_state_with_allowed_flags(7, (int) loc_entry->helping_flag,
                                 NOT_HELPING,
                                 HELPING,
                                 PROPOSE_NOT_LOCALLY_ACKED,
                                 PROPOSE_LOCALLY_ACCEPTED,
                                 HELP_PREV_COMMITTED_LOG_TOO_HIGH,
                                 HELPING_MYSELF);

  if (loc_entry->helping_flag == PROPOSE_NOT_LOCALLY_ACKED ||
      loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED)
    loc_entry->helping_flag = NOT_HELPING;


  /* -------------------RESET---------------------------*/
  if (loc_entry->rmw_reps.ready_to_inspect)
    zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
  else if (ENABLE_ASSERTIONS) assert(loc_entry->rmw_reps.tot_replies == 1);

  if (zero_out_log_too_high_cntr) loc_entry->log_too_high_cntr = 0;

  if (ENABLE_CAS_CANCELLING) {
    loc_entry->killable = (loc_entry->state == RETRY_WITH_BIGGER_TS ||
                           loc_entry->state == NEEDS_KV_PTR) &&
                          loc_entry->accepted_log_no == 0 &&
                          loc_entry->opcode == COMPARE_AND_SWAP_WEAK;

  }
}



static inline void clean_up_after_inspecting_accept(p_ops_t *p_ops,
                                                    loc_entry_t *loc_entry,
                                                    uint16_t t_id)
{
  /* ------------------CHECK STATE---------------------------*/
  check_state_with_allowed_flags(6, (int) loc_entry->state,
                                 INVALID_RMW,          // already committed -- no broadcasts
                                 RETRY_WITH_BIGGER_TS, //log-too-high
                                 NEEDS_KV_PTR,         // log-too-small
                                 MUST_BCAST_COMMITS_FROM_HELP, // ack-quorum for help
                                 MUST_BCAST_COMMITS            // ack-quorum or already committed
  );

  check_state_with_allowed_flags(4, (int) loc_entry->helping_flag,
                                 NOT_HELPING,
                                 HELPING);

  /* -------------------RESET---------------------------*/
  advance_loc_entry_l_id(loc_entry, t_id);
  zero_out_the_rmw_reply_loc_entry_metadata(loc_entry);
  // All-aboard
  if (ENABLE_ALL_ABOARD && loc_entry->all_aboard) {
    if (ENABLE_STAT_COUNTING) {
      t_stats[t_id].all_aboard_rmws++;
    }
    loc_entry->all_aboard = false;
  }
}


//------------------------------REGULAR INSPECTIONS------------------------------------------

// local_entry->state == RETRY_WITH_BIGGER_TS
static inline void take_kv_ptr_with_higher_TS(p_ops_t *p_ops,
                                              loc_entry_t *loc_entry,
                                              bool from_propose,
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
      if (rmw_fails_with_loc_entry(loc_entry, kv_ptr, &rmw_fails, t_id)) {
        if (ENABLE_ASSERTIONS) assert(kv_ptr->state != ACCEPTED);
        kv_ptr->state = INVALID_RMW;
      }
      else {
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
        loc_entry->new_ts.version = MAX(loc_entry->new_ts.version, kv_ptr->prop_ts.version) + 1;
        loc_entry->base_ts = kv_ptr->ts; // Minimize the possibility for RMW_ACK_BASE_TS_STALE
        if (ENABLE_ASSERTIONS) {
          assert(loc_entry->new_ts.version > kv_ptr->prop_ts.version);
        }
        loc_entry->new_ts.m_id = (uint8_t) machine_id;
        kv_ptr->prop_ts = loc_entry->new_ts;
        if (!is_still_accepted) {
          if (ENABLE_ASSERTIONS) assert(kv_ptr->state != ACCEPTED);
          kv_ptr->state = PROPOSED;
        }
          //PROPOSE_LOCALLY_ACCEPTED
        else {
          // Attention: when retrying an RMW that has been locally accepted,
          // you need to start from Proposes, but the kv_ptr can NOT be downgraded to proposed
          help = true;
          //loc_entry->helping_flag = PROPOSE_LOCALLY_ACCEPTED;
          loc_entry->help_loc_entry->new_ts = kv_ptr->accepted_ts;
        }
        kv_ptr_was_grabbed = true;
      }
    } else {
      if (DEBUG_RMW)
        my_printf(yellow, "Wrkr %u, session %u  failed when attempting to get/regain the kv_ptr, "
                    "waiting: waited for %u cycles for "
                    "now waiting on rmw_id %, state %u\n",
                  t_id, loc_entry->sess_id,
                  kv_ptr->rmw_id.id, kv_ptr->state);
    }
    check_log_nos_of_kv_ptr(kv_ptr, "take_kv_ptr_with_higher_TS", t_id);
  }
  unlock_seqlock(&loc_entry->kv_ptr->seqlock);

  if (ENABLE_ASSERTIONS) if (is_still_accepted) assert(help);
  clean_up_after_retrying(p_ops, kv_ptr, loc_entry,
                          kv_ptr_was_grabbed, is_still_accepted,
                          rmw_fails, t_id);


}


// local_entry->state = NEEDS_KV_PTR
static inline void handle_needs_kv_ptr_state(p_ops_t *p_ops,
                                             loc_entry_t *loc_entry,
                                             uint16_t sess_i,
                                             uint16_t t_id)
{
  mica_op_t *kv_ptr = loc_entry->kv_ptr;

  // If this fails to grab a kv_ptr it will try to update
  // the (rmw_id + state) that is being waited on.
  // If it updates it will zero the back-off counter
  if (!attempt_to_grab_kv_ptr_after_waiting(p_ops, kv_ptr, loc_entry,
                                            sess_i, t_id)) {
    if (ENABLE_ASSERTIONS) assert(p_ops->sess_info[sess_i].stalled);
    loc_entry->back_off_cntr++;
    if (loc_entry->back_off_cntr == RMW_BACK_OFF_TIMEOUT) {
       //   my_printf(yellow, "Wrkr %u  sess %u waiting for an rmw on key %u on log %u, back_of cntr %u waiting on rmw_id %u state %u \n",
       //                 t_id, sess_i,loc_entry->key.bkt, loc_entry->help_rmw->log_no, loc_entry->back_off_cntr,
       //                 loc_entry->help_rmw->rmw_id.id,
       //                 loc_entry->help_rmw->state);

      // This is failure-related help/stealing it should not be that we are being held up by the local machine
      // However we may wait on a "local" glob sess id, because it is being helped
      // if have accepted a value help it
      if (loc_entry->help_rmw->state == ACCEPTED)
        attempt_to_help_a_locally_accepted_value(p_ops, loc_entry, kv_ptr, t_id); // zeroes the back-off counter
        // if have received a proposal, send your own proposal
      else  if (loc_entry->help_rmw->state == PROPOSED) {
        attempt_to_steal_a_proposed_kv_ptr(p_ops, loc_entry, kv_ptr, sess_i, t_id); // zeroes the back-off counter
      }
    }
  }
  if (loc_entry->state == PROPOSED) {
    loc_entry->back_off_cntr = 0;
    insert_prop_to_read_fifo(p_ops, loc_entry, t_id);
  }

}

// Inspect each propose that has gathered a quorum of replies
static inline void inspect_proposes(p_ops_t *p_ops,
                                    loc_entry_t *loc_entry,
                                    uint16_t t_id)
{
  bool zero_out_log_too_high_cntr = true;
  struct rmw_rep_info *rep_info = &loc_entry->rmw_reps;
  uint8_t remote_quorum = QUORUM_NUM;

  // RMW_ID COMMITTED
  if (rep_info->rmw_id_commited > 0) {
    debug_fail_help(loc_entry, " rmw id committed", t_id);
    // as an optimization clear the kv_ptr entry if it is still in proposed state
    if (loc_entry->accepted_log_no != loc_entry->log_no)
      free_kv_ptr_if_rmw_failed(loc_entry, PROPOSED, t_id);
    handle_already_committed_rmw(p_ops, loc_entry, t_id);
    check_state_with_allowed_flags(3, (int) loc_entry->state, INVALID_RMW,
                                   MUST_BCAST_COMMITS);
  }
  // LOG_NO TOO SMALL
  else if (rep_info->log_too_small > 0) {
    debug_fail_help(loc_entry, " log too small", t_id);
    //It is impossible for this RMW to still hold the kv_ptr
    loc_entry->state = NEEDS_KV_PTR;
  }
  // SEEN HIGHER-TS PROPOSE OR ACCEPT
  else if (rep_info->seen_higher_prop_acc > 0) {
    debug_fail_help(loc_entry, " seen higher prop", t_id);
    // retry by incrementing the highest base_ts seen
    loc_entry->state = RETRY_WITH_BIGGER_TS;
    loc_entry->new_ts.version = rep_info->seen_higher_prop_version;
  }
  // ACK QUORUM
  else if (rep_info->acks >= remote_quorum &&
           loc_entry->helping_flag != PROPOSE_NOT_LOCALLY_ACKED) {
    debug_fail_help(loc_entry, " quorum", t_id);
    // Quorum of prop acks gathered: send an accept
    act_on_quorum_of_prop_acks(p_ops, loc_entry, t_id);
    check_state_with_allowed_flags(4, (int) loc_entry->state,
                                   ACCEPTED, NEEDS_KV_PTR, MUST_BCAST_COMMITS);
  }
  // ALREADY ACCEPTED AN RMW WITH LOWER_TS
  else if (rep_info->already_accepted > 0) {
    debug_fail_help(loc_entry, " already accepted", t_id);
    if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED)
      act_on_quorum_of_prop_acks(p_ops, loc_entry, t_id);
    else act_on_receiving_already_accepted_rep_to_prop(p_ops, loc_entry, t_id);
    check_state_with_allowed_flags(4, (int) loc_entry->state, ACCEPTED,
                                   NEEDS_KV_PTR, MUST_BCAST_COMMITS);
  }
  // LOG TOO HIGH
  else if (rep_info->log_too_high > 0) {
    react_on_log_too_high_for_prop(loc_entry, t_id);
    loc_entry->new_ts.version = loc_entry->new_ts.version;
    zero_out_log_too_high_cntr = false;
  }
  else if (ENABLE_ASSERTIONS) assert(false);

  clean_up_after_inspecting_props(p_ops, loc_entry, zero_out_log_too_high_cntr, t_id);
}


// Inspect each propose that has gathered a quorum of replies
static inline void inspect_accepts(p_ops_t *p_ops,
                                   loc_entry_t *loc_entry,
                                   uint16_t t_id)
{
  struct rmw_rep_info *rep_info = &loc_entry->rmw_reps;
  uint8_t remote_quorum = (uint8_t) (loc_entry->all_aboard ?
                                     MACHINE_NUM : QUORUM_NUM);
  //uint32_t new_version = 0;
  if (loc_entry->helping_flag != NOT_HELPING &&
      rep_info->rmw_id_commited  + rep_info->log_too_small +
      rep_info->already_accepted + rep_info->seen_higher_prop_acc +
      rep_info->log_too_high > 0)
  {
      if (ENABLE_ASSERTIONS) assert(loc_entry->helping_flag == HELPING);
      reinstate_loc_entry_after_helping(loc_entry, t_id);

  }
  // RMW_ID COMMITTED
  else if (rep_info->rmw_id_commited > 0) {
    handle_already_committed_rmw(p_ops, loc_entry, t_id);
    check_state_with_allowed_flags(3, (int) loc_entry->state, INVALID_RMW,
                                   MUST_BCAST_COMMITS);
    if (ENABLE_ASSERTIONS) assert(loc_entry->helping_flag == NOT_HELPING);
  }
    // LOG_NO TOO SMALL
  else if (rep_info->log_too_small > 0) {
    //It is impossible for this RMW to still hold the kv_ptr
    loc_entry->state = NEEDS_KV_PTR;
    if (ENABLE_ASSERTIONS) assert(loc_entry->helping_flag == NOT_HELPING);
  }
    // ACK QUORUM
  else if (rep_info->acks >= remote_quorum) {
    bookkeeping_after_gathering_accept_acks(loc_entry, t_id);
    loc_entry->state = (uint8_t) (loc_entry->helping_flag == HELPING ?
                                  MUST_BCAST_COMMITS_FROM_HELP : MUST_BCAST_COMMITS);
  }
  // SEEN HIGHER-TS PROPOSE
  else if (rep_info->seen_higher_prop_acc > 0) {
    // retry by incrementing the highest base_ts seen
    loc_entry->state = RETRY_WITH_BIGGER_TS;
    loc_entry->new_ts.version = rep_info->seen_higher_prop_version;
    if (ENABLE_ASSERTIONS) assert(loc_entry->helping_flag == NOT_HELPING);
  }
    // LOG TOO HIGH
  else if (rep_info->log_too_high > 0) {
    loc_entry->state = RETRY_WITH_BIGGER_TS;
  }
  // if a quorum of messages have been received but
  // we are waiting for more, then we are doing all aboard
  else if (ENABLE_ALL_ABOARD) {
    if (ENABLE_ASSERTIONS) assert(loc_entry->all_aboard);
    loc_entry->all_aboard_time_out++;
    if (ENABLE_ASSERTIONS) assert(loc_entry->new_ts.version == ALL_ABOARD_TS);
    if (loc_entry->all_aboard_time_out > ALL_ABOARD_TIMEOUT_CNT) {
      // printf("Wrkr %u, Timing out on key %u \n", t_id, loc_entry->key.bkt);
      loc_entry->state = RETRY_WITH_BIGGER_TS;
      loc_entry->all_aboard_time_out = 0;
      loc_entry->new_ts.version = PAXOS_TS;
    }
    else return; // avoid zeroing out the responses
  }
  else if (ENABLE_ASSERTIONS) assert(false);


  clean_up_after_inspecting_accept(p_ops, loc_entry, t_id);

}



/*--------------------------------------------------------------------------
 * --------------------INIT RMW-------------------------------------
 * --------------------------------------------------------------------------*/

// Insert an RMW in the local RMW structs
static inline void insert_rmw(p_ops_t *p_ops, trace_op_t *op,
                              uint16_t t_id)
{
  uint16_t session_id = op->session_id;
  loc_entry_t *loc_entry = &p_ops->prop_info->entry[session_id];
  if (loc_entry->state == CAS_FAILED) {
    //printf("Wrkr%u, sess %u, entry %u rmw_failing \n", t_id, session_id, resp->rmw_entry);
    signal_completion_to_client(session_id, op->index_to_req_array, t_id);
    p_ops->sess_info[session_id].stalled = false;
    p_ops->all_sessions_stalled = false;
    loc_entry->state = INVALID_RMW;
    return;
  }
  if (ENABLE_ASSERTIONS) {
    assert(session_id < SESSIONS_PER_THREAD);
  }

  // my_printf(green, "Session %u starts an rmw \n", loc_entry->glob_sess_id);
  uint8_t state = (uint8_t) (ENABLE_ALL_ABOARD && op->attempt_all_aboard ? ACCEPTED: PROPOSED);
  // if the kv_ptr was occupied, put in the next op to try next round
  if (loc_entry->state == state) { // the RMW has gotten an entry and is to be sent
    fill_loc_rmw_entry_on_grabbing_kv_ptr(p_ops, loc_entry, op->ts.version,
                                          state, session_id, t_id);
    if (state == ACCEPTED) {
      if (ENABLE_ASSERTIONS) {
        assert(op->ts.version == ALL_ABOARD_TS);
      }
      loc_entry->accepted_log_no = loc_entry->log_no;
      insert_accept_in_writes_message_fifo(p_ops, loc_entry, false, t_id);
      loc_entry->killable = false;
      loc_entry->all_aboard = true;
      loc_entry->all_aboard_time_out = 0;
    }
    else {
      if (ENABLE_ASSERTIONS) assert(op->ts.version == PAXOS_TS);
      insert_prop_to_read_fifo(p_ops, loc_entry, t_id);
    }
  }
  else if (loc_entry->state == NEEDS_KV_PTR) {
    if (ENABLE_ALL_ABOARD) loc_entry->all_aboard = false;
  }
  else my_assert(false, "Wrong loc_entry in RMW");
}


#endif //KITE_PAXOS_UTIL_H

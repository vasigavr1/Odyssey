//
// Created by vasilis on 22/05/20.
//

#ifndef KITE_PAXOS_GENERIC_UTILITY_H
#define KITE_PAXOS_GENERIC_UTILITY_H

#include <config.h>
#include "main.h"
#include "kite_debug_util.h"
#include "client_if_util.h"
#include "kite_config_util.h"
#include "reserve_stations_util.h"



/* ---------------------------------------------------------------------------
//------------------------------ GENERIC UTILITY------------------------------------------
//---------------------------------------------------------------------------*/
static inline void zero_out_the_rmw_reply_loc_entry_metadata(loc_entry_t* loc_entry)
{
  if (ENABLE_ASSERTIONS) { // make sure the loc_entry is correctly set-up
    if (loc_entry->help_loc_entry == NULL) {
      my_printf(red, "When Zeroing: The help_loc_ptr is NULL. The reason is typically that "
        "help_loc_entry was passed to the function "
        "instead of loc entry to check \n");
      assert(false);
    }
  }
  //assert(loc_entry->state != ACCEPTED);
  loc_entry->help_loc_entry->state = INVALID_RMW;
  assert(loc_entry->rmw_reps.ready_to_inspect);
  assert(loc_entry->rmw_reps.inspected);
  memset(&loc_entry->rmw_reps, 0, sizeof(struct rmw_rep_info));
  if (ENABLE_ASSERTIONS) assert(!loc_entry->rmw_reps.ready_to_inspect);
  loc_entry->back_off_cntr = 0;
  if (ENABLE_ALL_ABOARD) loc_entry->all_aboard_time_out = 0;
}


// After having helped another RMW, bring your own RMW back into the local entry
static inline void reinstate_loc_entry_after_helping(loc_entry_t *loc_entry, uint16_t t_id)
{
  loc_entry->state = NEEDS_KV_PTR;
  loc_entry->helping_flag = NOT_HELPING;
  if (DEBUG_RMW)
    my_printf(yellow, "Wrkr %u, sess %u reinstates its RMW id %u after helping \n",
              t_id, loc_entry->sess_id, loc_entry->rmw_id.id);
  if (ENABLE_ASSERTIONS)
    assert(glob_ses_id_to_m_id((uint32_t) (loc_entry->rmw_id.id % GLOBAL_SESSION_NUM)) == (uint8_t) machine_id);

}


// Perform the operation of the RMW and store the result in the local entry, call on locally accepting
static inline void perform_the_rmw_on_the_loc_entry(mica_op_t *kv_ptr,
                                                    loc_entry_t *loc_entry,
                                                    uint16_t t_id)
{
  loc_entry->rmw_is_successful = true;
  loc_entry->base_ts = kv_ptr->ts;
  loc_entry->accepted_log_no = kv_ptr->log_no;

  switch (loc_entry->opcode) {
    case RMW_PLAIN_WRITE:
      break;
    case FETCH_AND_ADD:
      memcpy(loc_entry->value_to_read, kv_ptr->value, loc_entry->rmw_val_len);
      *(uint64_t *)loc_entry->value_to_write = (*(uint64_t *)loc_entry->value_to_read) + (*(uint64_t *)loc_entry->compare_val);
      if (ENABLE_ASSERTIONS && !ENABLE_CLIENTS && RMW_RATIO >= 1000)
        assert((*(uint64_t *)loc_entry->compare_val == 1));
      //printf("%u %lu \n", loc_entry->log_no, *(uint64_t *)loc_entry->value_to_write);
      break;
    case COMPARE_AND_SWAP_WEAK:
    case COMPARE_AND_SWAP_STRONG:
      // if are equal
      loc_entry->rmw_is_successful = memcmp(loc_entry->compare_val,
                                            kv_ptr->value,
                                            loc_entry->rmw_val_len) == 0;
      if (!loc_entry->rmw_is_successful) {
        memcpy(loc_entry->value_to_read, kv_ptr->value, loc_entry->rmw_val_len);
      }
      break;
    default:
      if (ENABLE_ASSERTIONS) assert(false);
  }
    // we need to remember the last accepted value
  if (loc_entry->rmw_is_successful) {
    write_kv_ptr_acc_val(kv_ptr, loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
  }
  else {
    write_kv_ptr_acc_val(kv_ptr, loc_entry->value_to_read, (size_t) RMW_VALUE_SIZE);
  }

}


// free a session held by an RMW
static inline void free_session_from_rmw(p_ops_t *p_ops, uint16_t sess_id, bool allow_paxos_log,
                                         uint16_t t_id)
{
  loc_entry_t *loc_entry = &p_ops->prop_info->entry[sess_id];
  if (ENABLE_ASSERTIONS) {
    assert(sess_id < SESSIONS_PER_THREAD);
    assert(loc_entry->state == INVALID_RMW);
    if(!p_ops->sess_info[sess_id].stalled) {
      my_printf(red, "Wrkr %u sess %u should be stalled \n", t_id, sess_id);
      assert(false);
    }
  }
  fill_req_array_when_after_rmw(loc_entry->sess_id, loc_entry->index_to_req_array, loc_entry->opcode,
                                loc_entry->value_to_read, loc_entry->rmw_is_successful, t_id);
  if (VERIFY_PAXOS && allow_paxos_log) verify_paxos(loc_entry, t_id);
  // my_printf(cyan, "Session %u completing \n", loc_entry->glob_sess_id);
  signal_completion_to_client(sess_id, loc_entry->index_to_req_array, t_id);
  p_ops->sess_info[sess_id].stalled = false;
  p_ops->all_sessions_stalled = false;
}


static inline void local_rmw_ack(loc_entry_t *loc_entry)
{
  loc_entry->rmw_reps.tot_replies = 1;
  loc_entry->rmw_reps.acks = 1;
}


// when committing register global_sess id as committed
static inline void register_committed_global_sess_id (uint64_t rmw_id, uint16_t t_id)
{
  uint64_t glob_sess_id = rmw_id % GLOBAL_SESSION_NUM;
  uint64_t tmp_rmw_id, debug_cntr = 0;
  if (ENABLE_ASSERTIONS) assert(glob_sess_id < GLOBAL_SESSION_NUM);
  tmp_rmw_id = committed_glob_sess_rmw_id[glob_sess_id];
  do {
    if (ENABLE_ASSERTIONS) {
      debug_cntr++;
      if (debug_cntr > 100) {
        my_printf(red, "stuck on registering glob sess id %u \n", debug_cntr);
        debug_cntr = 0;
      }
    }
    if (rmw_id <= tmp_rmw_id) return;
  } while (!atomic_compare_exchange_strong(&committed_glob_sess_rmw_id[glob_sess_id], &tmp_rmw_id, rmw_id));
  MY_ASSERT(rmw_id <= committed_glob_sess_rmw_id[glob_sess_id], "After registering: rmw_id/registered %u/%u glob sess_id %u \n",
            rmw_id, committed_glob_sess_rmw_id[glob_sess_id], glob_sess_id);
}

/* ---------------------------------------------------------------------------
//------------------------------ACCEPTING------------------------------------------
//---------------------------------------------------------------------------*/
static inline void find_out_if_can_accept_help_locally(mica_op_t *kv_ptr,
                                                       loc_entry_t *loc_entry,
                                                       loc_entry_t* help_loc_entry,
                                                       bool *kv_ptr_is_the_same,
                                                       bool *kv_ptr_is_invalid_but_not_committed,
                                                       bool *helping_stuck_accept,
                                                       bool *propose_locally_accepted,
                                                       uint16_t t_id)
{

  compare_t comp = compare_ts(&kv_ptr->prop_ts, &loc_entry->new_ts);
  bool same_rmw_id_log = same_rmw_id_same_log(kv_ptr, help_loc_entry);
  bool entry_still_mine = help_loc_entry->log_no == kv_ptr->log_no &&
                          comp == EQUAL &&
                          rmw_ids_are_equal(&kv_ptr->rmw_id, &loc_entry->rmw_id);

  *kv_ptr_is_the_same = kv_ptr->state == PROPOSED  && entry_still_mine;

  *kv_ptr_is_invalid_but_not_committed = kv_ptr->state == INVALID_RMW &&
                                         kv_ptr->last_committed_log_no < help_loc_entry->log_no;

  *helping_stuck_accept = loc_entry->helping_flag == PROPOSE_NOT_LOCALLY_ACKED &&
                          same_rmw_id_log &&
                          kv_ptr->state == ACCEPTED &&
                          comp != GREATER;
  // When retrying after accepts fail, i must first send proposes but if the local state is still accepted,
  // i can't downgrade it to proposed, so if i am deemed to help another RMW, i may come back to find
  // my original Accept still here
  *propose_locally_accepted = kv_ptr->state == ACCEPTED  & entry_still_mine;
  if (ENABLE_ASSERTIONS) {
    if (*kv_ptr_is_the_same   || *kv_ptr_is_invalid_but_not_committed ||
        *helping_stuck_accept || *propose_locally_accepted)
    checks_and_prints_local_accept_help(loc_entry, help_loc_entry, kv_ptr, *kv_ptr_is_the_same,
                                        *kv_ptr_is_invalid_but_not_committed,
                                        *helping_stuck_accept, *propose_locally_accepted, t_id);
  }
}


/* ---------------------------------------------------------------------------
//------------------------------COMMITTING------------------------------------
//---------------------------------------------------------------------------*/
static inline void process_commit_flags(void* rmw, loc_entry_t *loc_entry, uint8_t *flag)
{
  struct commit *com = (struct commit *) rmw;

  switch (*flag) {
    case FROM_ALREADY_COMM_REP:
      if (loc_entry->helping_flag == HELPING) {
        *flag = FROM_ALREADY_COMM_REP_HELP;
      }
      break;
    case FROM_LOCAL:
      if (loc_entry->helping_flag == HELPING)
        *flag = FROM_LOCAL_HELP;
      else if (ENABLE_ASSERTIONS)
        assert(loc_entry->log_no == loc_entry->accepted_log_no);
      break;
    case FROM_REMOTE_COMMIT:
      if (com->opcode == COMMIT_OP_NO_VAL)
        *flag = FROM_REMOTE_COMMIT_NO_VAL;
      break;
    case FROM_LOCAL_ACQUIRE:
    case FROM_OOE_READ:
    case FROM_LOG_TOO_LOW_REP:
    break;
    default:
      if (ENABLE_ASSERTIONS) {printf("%u \n", *flag); assert(false);}
  }
}

static inline void fill_commit_info(commit_info_t *com_info, uint8_t flag,
                                    uint64_t rmw_id,
                                    uint32_t log_no, struct ts_tuple base_ts,
                                    uint8_t *value, bool overwrite_kv)
{
  com_info->rmw_id.id = rmw_id;
  com_info->log_no = log_no;
  com_info->base_ts = base_ts;
  com_info->value = value;
  com_info->overwrite_kv = overwrite_kv;
  com_info->message = committing_flag_to_str(flag);
  com_info->no_value = false;
  com_info->flag = flag;
}


static inline bool can_process_com_no_value(mica_op_t *kv_ptr,
                                            commit_info_t *com_info,
                                            uint16_t t_id)
{

  if (kv_ptr->last_committed_log_no < com_info->log_no) {
    com_info->base_ts = kv_ptr->base_acc_ts;
    com_info->value = kv_ptr->last_accepted_value;
    return true;
  }

  return false;
}
/*--------------------------------------------------------------------------
 * --------------------CAS EARLY FAILURE--------------------------
 * --------------------------------------------------------------------------*/

// Returns true if the CAS has to be cut short
static inline bool rmw_compare_fails(uint8_t opcode, uint8_t *compare_val,
                                     uint8_t *kv_ptr_value, uint32_t val_len, uint16_t t_id)
{
  if (!opcode_is_compare_rmw(opcode) || (!ENABLE_CAS_CANCELLING)) return false; // there is nothing to fail
  if (ENABLE_ASSERTIONS) {
    assert(compare_val != NULL);
    assert(kv_ptr_value != NULL);
  }
  // memcmp() returns 0 if regions are equal. Thus the CAS fails if the result is not zero
  bool rmw_fails = memcmp(compare_val, kv_ptr_value, val_len) != 0;
  if (ENABLE_STAT_COUNTING && rmw_fails) {
    t_stats[t_id].cancelled_rmws++;
  }
  return rmw_fails;

}

// returns true if the RMW can be failed before allocating a local entry
static inline bool does_rmw_fail_early(trace_op_t *op, mica_op_t *kv_ptr,
                                       uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) assert(op->real_val_len <= RMW_VALUE_SIZE);
  if (op->opcode == COMPARE_AND_SWAP_WEAK &&
      rmw_compare_fails(op->opcode, op->value_to_read,
                        kv_ptr->value, op->real_val_len, t_id)) {
    //my_printf(red, "CAS fails returns val %u/%u \n", kv_ptr->value[RMW_BYTE_OFFSET], op->value_to_read[0]);

    fill_req_array_on_rmw_early_fail(op->session_id, kv_ptr->value,
                                     op->index_to_req_array, t_id);
    return true;
  }
  else return  false;
}

//
static inline bool rmw_fails_with_loc_entry(loc_entry_t *loc_entry, mica_op_t *kv_ptr,
                                            bool *rmw_fails, uint16_t t_id)
{
  if (ENABLE_CAS_CANCELLING) {
    if (loc_entry->killable) {
      if (rmw_compare_fails(loc_entry->opcode, loc_entry->compare_val,
                            kv_ptr->value, loc_entry->rmw_val_len, t_id)) {
        (*rmw_fails) = true;
        if (ENABLE_ASSERTIONS) {
          assert(!loc_entry->rmw_is_successful);
          assert(loc_entry->rmw_val_len <= RMW_VALUE_SIZE);
          assert(loc_entry->helping_flag != HELPING);
        }
        memcpy(loc_entry->value_to_read, kv_ptr->value,
               loc_entry->rmw_val_len);
        return true;
      }
    }
  }
  return false;
}

/*--------------------------------------------------------------------------
 * --------------------BACK-OFF UTILITY-------------------------------------
 * --------------------------------------------------------------------------*/

// Check if the kv_ptr state that is blocking a local RMW is persisting
static inline bool kv_ptr_state_has_not_changed(mica_op_t *kv_ptr,
                                                struct rmw_help_entry *help_rmw)
{
  return kv_ptr->state == help_rmw->state &&
         rmw_ids_are_equal(&help_rmw->rmw_id, &kv_ptr->rmw_id) &&
         (compare_ts(&kv_ptr->prop_ts, &help_rmw->ts) == EQUAL);
}

// Check if the kv_ptr state that is blocking a local RMW is persisting
static inline bool kv_ptr_state_has_changed(mica_op_t *kv_ptr,
                                            struct rmw_help_entry *help_rmw)
{
  return kv_ptr->state != help_rmw->state ||
         (!rmw_ids_are_equal(&help_rmw->rmw_id, &kv_ptr->rmw_id)) ||
         (compare_ts(&kv_ptr->prop_ts, &help_rmw->ts) != EQUAL);
}


/*--------------------------------------------------------------------------
 * --------------------FSM-UTILITY-------------------------------------
 * --------------------------------------------------------------------------*/

// When a propose/accept has inspected the responses (after they have reached at least a quorum),
// advance the entry's l_id such that previous responses are disregarded
static inline void advance_loc_entry_l_id(loc_entry_t *loc_entry,
                                          uint16_t t_id)
{
  loc_entry->l_id += SESSIONS_PER_THREAD; //p_ops->prop_info->l_id;
  loc_entry->help_loc_entry->l_id = loc_entry->l_id;// p_ops->prop_info->l_id;
  if (ENABLE_ASSERTIONS) assert(loc_entry->l_id % SESSIONS_PER_THREAD == loc_entry->sess_id);
  // p_ops->prop_info->l_id++;
}

//
static inline bool if_already_committed_bcast_commits(p_ops_t *p_ops,
                                                      loc_entry_t *loc_entry,
                                                      uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->state != INVALID_RMW);
    assert(loc_entry == &p_ops->prop_info->entry[loc_entry->sess_id]);
    assert(loc_entry->helping_flag != HELPING);
  }
  if (loc_entry->rmw_id.id <= committed_glob_sess_rmw_id[loc_entry->glob_sess_id]) {
    //my_printf(yellow, "Wrkr %u, sess: %u Bcast rmws %u \n", t_id, loc_entry->sess_id);
    loc_entry->log_no = loc_entry->accepted_log_no;
    loc_entry->state = MUST_BCAST_COMMITS;
    return true;
  }
  return false;
}

// Potentially useful (for performance only) when a propose receives already_committed
// responses and still is holding the kv_ptr
static inline void free_kv_ptr_if_rmw_failed(loc_entry_t *loc_entry,
                                             uint8_t state, uint16_t t_id)
{
  mica_op_t *kv_ptr = loc_entry->kv_ptr;
  if (kv_ptr->state == state &&
      kv_ptr->log_no == loc_entry->log_no &&
      rmw_ids_are_equal(&kv_ptr->rmw_id, &loc_entry->rmw_id) &&
      compare_ts(&kv_ptr->prop_ts, &loc_entry->new_ts) == EQUAL) {
//    my_printf(cyan, "Wrkr %u, kv_ptr NEEDS TO BE FREED: session %u RMW id %u/%u glob_sess_id %u/%u with version %u/%u,"
//                  " m_id %u/%u,"
//                  " kv_ptr log/help log %u/%u kv_ptr committed log %u , biggest committed rmw_id %u for glob sess %u"
//                  " \n",
//                t_id, loc_entry->sess_id, loc_entry->rmw_id.id, kv_ptr->rmw_id.id,
//                loc_entry->rmw_id.glob_sess_id, kv_ptr->rmw_id.glob_sess_id,
//                loc_entry->new_ts.version, kv_ptr->new_ts.version,
//                loc_entry->new_ts.m_id, kv_ptr->new_ts.m_id,
//                kv_ptr->log_no, loc_entry->log_no, kv_ptr->last_committed_log_no,
//                committed_glob_sess_rmw_id[kv_ptr->rmw_id.glob_sess_id], kv_ptr->rmw_id.glob_sess_id);

    lock_seqlock(&loc_entry->kv_ptr->seqlock);
    if (kv_ptr->state == state &&
        kv_ptr->log_no == loc_entry->log_no &&
        rmw_ids_are_equal(&kv_ptr->rmw_id, &loc_entry->rmw_id)) {
      if (state == PROPOSED && compare_ts(&kv_ptr->prop_ts, &loc_entry->new_ts) == EQUAL) {
        printf("Free_kv_ptr_if_prop_already_committed\n");
        print_rmw_rep_info(loc_entry, t_id);
        //assert(false);
        kv_ptr->state = INVALID_RMW;
      }
      else if (state == ACCEPTED && compare_ts(&kv_ptr->accepted_ts, &loc_entry->new_ts) == EQUAL)
        if (ENABLE_ASSERTIONS) assert(false);
    }
    check_log_nos_of_kv_ptr(kv_ptr, "free_kv_ptr_if_prop_failed", t_id);
    unlock_seqlock(&loc_entry->kv_ptr->seqlock);
  }
}


static inline void bookkeeping_after_gathering_accept_acks(loc_entry_t *loc_entry, uint16_t t_id)
{

  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->state != COMMITTED);
    if (loc_entry->helping_flag == HELPING) assert(!loc_entry->all_aboard);
    assert(!loc_entry->avoid_val_in_com);
    assert(!loc_entry->avoid_val_in_com);
    assert(!loc_entry->help_loc_entry->avoid_val_in_com);
  }
  if (!ENABLE_COMMITS_WITH_NO_VAL) return;
   // Should we send commits without value?
  if (loc_entry->rmw_reps.acks == MACHINE_NUM) {
    if (loc_entry->helping_flag == HELPING)
      loc_entry->help_loc_entry->avoid_val_in_com = true;
    else  loc_entry->avoid_val_in_com = true;
  }
}

/*--------------------------------------------------------------------------
 * -------------------SENDING MESSAGES-----------------------------------
 * --------------------------------------------------------------------------*/

// Fill a write message with a commit
static inline void fill_commit_message_from_l_entry(struct commit *com, loc_entry_t *loc_entry,
                                                    uint8_t broadcast_state, uint16_t t_id)
{

  memcpy(&com->key, &loc_entry->key, KEY_SIZE);
  com->t_rmw_id = loc_entry->rmw_id.id;
  com->base_ts.m_id = loc_entry->base_ts.m_id;
  if (loc_entry->avoid_val_in_com) {
    com->opcode = COMMIT_OP_NO_VAL;
    loc_entry->avoid_val_in_com = false;
    com->base_ts.version = loc_entry->log_no;
  }
  else {
    com->opcode = COMMIT_OP;
    com->log_no = loc_entry->log_no;
    com->base_ts.version = loc_entry->base_ts.version;
    if (broadcast_state == MUST_BCAST_COMMITS && !loc_entry->rmw_is_successful) {
      memcpy(com->value, loc_entry->value_to_read, (size_t) RMW_VALUE_SIZE);
    } else {
      memcpy(com->value, loc_entry->value_to_write, (size_t) RMW_VALUE_SIZE);
    }
    //print_treiber_top((struct top *) com->value, "Sending commit", cyan);
    if (ENABLE_ASSERTIONS) {
      assert(com->log_no > 0);
      assert(com->t_rmw_id > 0);
    }
  }
}

// Fill a write message with a commit from read info, after an rmw acquire
static inline void fill_commit_message_from_r_info(struct commit *com,
                                                   r_info_t* r_info, uint16_t t_id)
{
  com->base_ts.m_id = r_info->ts_to_read.m_id;
  com->base_ts.version = r_info->ts_to_read.version;
  memcpy(&com->key, &r_info->key, KEY_SIZE);
  assert(r_info->key.bkt != 0);
  assert(com->key.bkt != 0);
  com->opcode = RMW_ACQ_COMMIT_OP;
  memcpy(com->value, r_info->value, r_info->val_len);
  com->t_rmw_id = r_info->rmw_id.id;
  com->log_no = r_info->log_no;
  //my_printf(yellow, "Broadcasting commit log %u, rmw_id %u, key %u \n", com->log_no, com->t_rmw_id, com->key.bkt);
  if (ENABLE_ASSERTIONS) {
    //assert(com->log_no > 0);
    //assert(com->t_rmw_id > 0);
  }
}

//fill the reply entry with last_committed RMW-id, TS, value and log number
static inline void fill_reply_entry_with_committed_RMW (mica_op_t *kv_ptr,
                                                        struct rmw_rep_last_committed *rep,
                                                        uint16_t t_id)
{
  rep->ts.m_id = kv_ptr->ts.m_id; // Here we reply with the base TS
  rep->ts.version = kv_ptr->ts.version;
  memcpy(rep->value, kv_ptr->value, (size_t) RMW_VALUE_SIZE);
  rep->log_no_or_base_version = kv_ptr->last_committed_log_no;
  rep->rmw_id = kv_ptr->last_committed_rmw_id.id;
  //if (rep->base_ts.version == 0)
  //  my_printf(yellow, "Wrkr %u replies with flag %u Log_no %u, rmw_id %lu glob_sess id %u\n",
  //         t_id, rep->opcode, rep->log_no, rep->rmw_id, rep->glob_sess_id);
}




#endif //KITE_PAXOS_GENERIC_UTILITY_H

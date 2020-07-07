//
// Created by vasilis on 11/05/20.
//

#ifndef KITE_GENERIC_UTILITY_H
#define KITE_GENERIC_UTILITY_H


#include <stdint.h>
#include <config.h>
#include "main.h"
#include "generic_inline_util.h"



/*----------------------------------------------------------------
 * ----------------FLAG TO STRING FUNCTIONS-----------------------
 * ----------------------------------------------------------------
 * */


static inline const char* committing_flag_to_str(uint8_t state)
{
  switch (state)
  {
    case FROM_LOG_TOO_LOW_REP:
      return "FROM_LOG_TOO_LOW_REP";
    case FROM_ALREADY_COMM_REP:
      return "FROM_ALREADY_COMM_REP";
    case FROM_LOCAL:
      return "FROM_LOCAL";
    case FROM_ALREADY_COMM_REP_HELP:
      return "FROM_ALREADY_COMM_REP_HELP";
    case FROM_LOCAL_HELP:
      return "FROM_LOCAL_HELP";
    case FROM_REMOTE_COMMIT:
      return "FROM_REMOTE_COMMIT";
    case FROM_REMOTE_COMMIT_NO_VAL:
      return "FROM_REMOTE_COMMIT_NO_VAL";
    case FROM_LOCAL_ACQUIRE:
      return "FROM_LOCAL_ACQUIRE";
    case FROM_OOE_READ:
      return "FROM_OOE_READ";
    case FROM_TRACE_WRITE:
      return "FROM_TRACE_WRITE";
    case FROM_BASE_TS_STALE:
      return "FROM_BASE_TS_STALE";
    case FROM_ISOLATED_OP:
      return "FROM_ISOLATED_OP";
    case FROM_REMOTE_WRITE_RELEASE:
      return "FROM_REMOTE_WRITE_RELEASE";
    case FROM_OOE_LOCAL_WRITE:
      return "FROM_OOE_LOCAL_WRITE";
    default: return "Unknown";
  }
}

static inline const char* state_to_str(uint8_t state)
{
  switch (state)
  {
    case INVALID_RMW:
      return "INVALID_RMW";
    case PROPOSED:
      return "PROPOSED";
    case ACCEPTED:
      return "ACCEPTED";
    case NEEDS_KV_PTR:
      return "NEEDS_KV_PTR";
    case RETRY_WITH_BIGGER_TS:
      return "RETRY_WITH_BIGGER_TS";
    case MUST_BCAST_COMMITS:
      return "MUST_BCAST_COMMITS";
    case MUST_BCAST_COMMITS_FROM_HELP:
      return "MUST_BCAST_COMMITS_FROM_HELP";
    case COMMITTED:
      return "COMMITTED";
    case CAS_FAILED:
      return "CAS_FAILED";
    default: return "Unknown";
  }
}

static inline const char* help_state_to_str(uint8_t state)
{
  switch (state)
  {
    case NOT_HELPING:
      return "NOT_HELPING";
    case PROPOSE_NOT_LOCALLY_ACKED:
      return "PROPOSE_NOT_LOCALLY_ACKED";
    case HELPING:
      return "HELPING";
    case PROPOSE_LOCALLY_ACCEPTED:
      return "PROPOSE_LOCALLY_ACCEPTED";
    case HELP_PREV_COMMITTED_LOG_TOO_HIGH:
      return "HELP_PREV_COMMITTED_LOG_TOO_HIGH";
    case HELPING_MYSELF:
      return "HELPING_MYSELF";
    case IS_HELPER:
      return "IS_HELPER";
    default: return "Unknown";
  }
}


// Generic CAS
static inline bool cas_a_state(atomic_uint_fast8_t * state, uint8_t old_state, uint8_t new_state, uint16_t t_id)
{
  return atomic_compare_exchange_strong(state, (atomic_uint_fast8_t *) &old_state,
                                        (atomic_uint_fast8_t) new_state);
}

static inline bool rmw_ids_are_equal(struct rmw_id *id1, struct rmw_id *id2)
{
  return id1->id == id2->id;
}


static inline void assign_second_rmw_id_to_first(struct rmw_id* rmw_id1, struct rmw_id* rmw_id2)
{
  rmw_id1->id = rmw_id2->id;
}


static inline void swap_rmw_ids(struct rmw_id* rmw_id1, struct rmw_id* rmw_id2)
{
  struct rmw_id  tmp = *rmw_id1;
  assign_second_rmw_id_to_first(rmw_id1, rmw_id2);
  assign_second_rmw_id_to_first(rmw_id2, &tmp);
}

static inline uint8_t sum_of_reps(struct rmw_rep_info* rmw_reps)
{
  return rmw_reps->acks + rmw_reps->rmw_id_commited +
         rmw_reps->log_too_small + rmw_reps->already_accepted +
         rmw_reps->seen_higher_prop_acc + rmw_reps->log_too_high;
}


// Increment the per-request counters
static inline void increment_per_req_counters(uint8_t opcode, uint16_t t_id)
{
  if (ENABLE_STAT_COUNTING) {
    if (opcode == KVS_OP_PUT) t_stats[t_id].writes_per_thread++;
    else if (opcode == KVS_OP_GET) t_stats[t_id].reads_per_thread++;
    else if (opcode == OP_ACQUIRE) t_stats[t_id].acquires_per_thread++;
    else if (opcode == OP_RELEASE) t_stats[t_id].releases_per_thread++;
    else  t_stats[t_id].rmws_completed++;
  }
}


/* ---------------------------------------------------------------------------
//------------------------------ OPCODE HANDLING----------------------------
//---------------------------------------------------------------------------*/

static inline bool opcode_is_rmw(uint8_t opcode)
{
  return opcode == FETCH_AND_ADD || opcode == COMPARE_AND_SWAP_WEAK ||
         opcode == COMPARE_AND_SWAP_STRONG || opcode == RMW_PLAIN_WRITE;
}

static inline bool opcode_is_compare_rmw(uint8_t opcode)
{
  return opcode == COMPARE_AND_SWAP_WEAK || opcode == COMPARE_AND_SWAP_STRONG;
}

static inline bool opcode_is_rmw_rep(uint8_t opcode)
{
  return (opcode >= RMW_ACK && opcode <= NO_OP_PROP_REP) ||
         (opcode >= RMW_ACK + FALSE_POSITIVE_OFFSET &&
          opcode <= NO_OP_PROP_REP + FALSE_POSITIVE_OFFSET);
}


// Give an opcode to get the size of the read rep messages
static inline uint16_t get_size_from_opcode(uint8_t opcode)
{
  if (opcode > CARTS_EQUAL) opcode -= FALSE_POSITIVE_OFFSET;
  switch(opcode) {
    // ----RMWS-----
    case LOG_TOO_SMALL:
      return PROP_REP_LOG_TOO_LOW_SIZE;
    case SEEN_LOWER_ACC:
      return PROP_REP_ACCEPTED_SIZE;
    case SEEN_HIGHER_PROP:
    case SEEN_HIGHER_ACC:
      return PROP_REP_ONLY_TS_SIZE;
    case RMW_ACK_BASE_TS_STALE:
      return PROP_REP_BASE_TS_STALE_SIZE;
    case RMW_ID_COMMITTED:
    case RMW_ID_COMMITTED_SAME_LOG:
    case RMW_ACK:
    case LOG_TOO_HIGH:
    case NO_OP_PROP_REP:
      return PROP_REP_SMALL_SIZE;
      //---- RMW ACQUIRES--------
    case CARTS_TOO_HIGH:
    case CARTS_EQUAL:
      return R_REP_SMALL_SIZE;
    case CARTS_TOO_SMALL:
      return ACQ_REP_SIZE;
      // -----REGULAR READS/ACQUIRES----
    case TS_TOO_HIGH:
    case TS_EQUAL:
      return R_REP_SMALL_SIZE;
    case TS_TOO_SMALL:
      return R_REP_ONLY_TS_SIZE;
    default: if (ENABLE_ASSERTIONS) {
        my_printf(red, "Opcode %u \n", opcode);
        assert(false);
      }
  }
}

// Returns the size of a write request given an opcode -- Accepts, commits, writes, releases
static inline uint16_t get_write_size_from_opcode(uint8_t opcode) {
  switch(opcode) {
    case OP_RELEASE:
    case OP_ACQUIRE:
    case KVS_OP_PUT:
    case OP_RELEASE_BIT_VECTOR:
    case OP_RELEASE_SECOND_ROUND:
    case NO_OP_RELEASE:
      return W_SIZE;
    case ACCEPT_OP:
    case ACCEPT_OP_BIT_VECTOR:
      return ACCEPT_SIZE;
    case COMMIT_OP:
    case RMW_ACQ_COMMIT_OP:
      return COMMIT_SIZE;
    case COMMIT_OP_NO_VAL:
      return COMMIT_NO_VAL_SIZE;
    default: if (ENABLE_ASSERTIONS) assert(false);
  }
}


/* ---------------------------------------------------------------------------
//------------------------------ TREIBER DEBUGGING-----------------------------
//---------------------------------------------------------------------------
static inline void print_treiber_top(struct top* top, const char *generic_message,
                                     const char *special_message, color_t color)
{
  my_printf(red, "%s\n", generic_message);
  my_printf(color, "%s: key_id %u push/pop %u, %u \n", special_message,  top->key_id, top->push_counter, top->pop_counter);
}

static inline bool check_value_is_tr_top(uint8_t *val, const char *message)
{
  struct top *top = (struct top *) val;
  if (ENABLE_TR_ASSERTIONS) {

    //assert(top->push_counter >= top->pop_counter);
    if (top->push_counter == 0) {
      print_treiber_top(top, message, "Zero push counter", yellow);
    }

    if (top->push_counter == top->pop_counter) {
      if (top->key_id != 0) { // Stack must be empty
        print_treiber_top(top, message, "Stack must be empty", yellow);
        assert(false);
      }
    } else if (top->push_counter > top->pop_counter) {
      if (top->key_id < TR_KEY_OFFSET) { // Stack cannot be empty
        print_treiber_top(top, message, "Stack cannot be empty", yellow);
        assert(false);
      }
    }
  }
  return true;
}
*/


/* ---------------------------------------------------------------------------
//------------------------------ PRINTS---------------------------------------
//---------------------------------------------------------------------------*/



static inline void print_loc_entry(loc_entry_t *loc_entry, color_t color, uint16_t t_id)
{
  my_printf(color, "WORKER %u -------%s-Local Entry------------ \n", t_id,
            loc_entry->help_loc_entry == NULL ? "HELP" : "-");
  my_printf(color, "Key : %u \n", loc_entry->key.bkt);
  my_printf(color, "Session %u/%u \n", loc_entry->sess_id, loc_entry->glob_sess_id);
  my_printf(color, "State %s \n", state_to_str(loc_entry->state));
  my_printf(color, "Log no %u\n", loc_entry->log_no);
  my_printf(color, "Rmw %u\n", loc_entry->rmw_id.id);
  print_ts(loc_entry->base_ts, "Base base_ts:", color);
  print_ts(loc_entry->new_ts, "Propose base_ts:", color);
  my_printf(color, "Helping state %s \n", help_state_to_str(loc_entry->helping_flag));
}

static inline void print_kv_ptr(mica_op_t *kv_ptr, color_t color, uint16_t t_id)
{
  my_printf(color, "WORKER %u-------KV_ptr----------- \n", t_id);
  my_printf(color, "Key : %u \n", kv_ptr->key.bkt);
  my_printf(color, "*****Committed RMW***** \n");
  my_printf(color, "Last committed log %u\n", kv_ptr->last_committed_log_no);
  my_printf(color, "Last committed rmw %u\n", kv_ptr->last_committed_rmw_id.id);
  print_ts(kv_ptr->base_acc_ts, "Base base_ts:", color);

  my_printf(color, "*****Active RMW*****\n");
  my_printf(color, "State %s \n", state_to_str(kv_ptr->state));
  my_printf(color, "Log %u\n", kv_ptr->log_no);
  my_printf(color, "RMW-id %u \n", kv_ptr->rmw_id.id);
  print_ts(kv_ptr->prop_ts, "Proposed base_ts:", color);
  print_ts(kv_ptr->accepted_ts, "Accepted base_ts:", color);
}


static inline void print_commit_info(commit_info_t * com_info,
                                     color_t color, uint16_t t_id)
{
  my_printf(color, "WORKER %u -------Commit info------------ \n", t_id);
  my_printf(color, "State %s \n", committing_flag_to_str(com_info->flag));
  my_printf(color, "Log no %u\n", com_info->log_no);
  my_printf(color, "Rmw %u\n", com_info->rmw_id.id);
  print_ts(com_info->base_ts, "Base base_ts:", color);
  my_printf(color, "No-value : %u \n", com_info->no_value);
  my_printf(color, "Overwrite-kv %u/%u \n", com_info->overwrite_kv);
}

/* ---------------------------------------------------------------------------
//------------------------------ MSQ_DEBUGGING -------------------------------
//---------------------------------------------------------------------------
static inline void print_ms_ptr(struct ms_ptr *ptr)
{
  my_printf(yellow, "-----------MS_PTR-%u----------\n", ptr->my_key_id);
  my_printf(yellow, "Queue id %u \n", ptr->queue_id);
  my_printf(yellow, "Next key-id %u \n", ptr->next_key_id);
  my_printf(yellow, "Counter %u \n", ptr->counter);
  my_printf(yellow, "Pushed: %s \n", ptr->pushed ? "YES": "NO");
}


static inline void check_write_if_msq_active(mica_op_t *kv_ptr, uint8_t *new_val,
                                             uint8_t flag)
{
  if (!ENABLE_MS_ASSERTIONS) return;
  uint32_t key_id = kv_ptr->key_id;
  const char* message = committing_flag_to_str(flag);
  assert(key_id < LAST_MS_NODE_PTR ||
           (key_id >= DUMMY_KEY_ID_OFFSET && key_id <=  MS_INIT_DONE_FLAG_KEY));
  if (key_id == MS_INIT_DONE_FLAG_KEY)
    my_printf(green, "Writting ms_init_done_flag, %s\n", message);
  // MS_PTR
  if (kv_ptr->key_id < LAST_MS_NODE_PTR) {
    struct ms_ptr *kv_ms_ptr = (struct ms_ptr *) kv_ptr->value;
    struct ms_ptr *new_ms_ptr = (struct ms_ptr *) new_val;
    if (new_ms_ptr->my_key_id != key_id) {
      print_ms_ptr(new_ms_ptr);
    }

  }


}
*/

/* ---------------------------------------------------------------------------
//------------------------------ KV-PTR writes---------------------------------------
//---------------------------------------------------------------------------*/

static inline void write_kv_ptr_val(mica_op_t *kv_ptr, uint8_t *new_val,
                                    size_t val_size, uint8_t flag)
{
  //check_write_if_msq_active(kv_ptr, new_val, flag);
  memcpy(kv_ptr->value, new_val, val_size);
  //check_value_is_tr_top(kv_ptr->value, "Writing kv_ptr value");

}

static inline void write_kv_ptr_acc_val(mica_op_t *kv_ptr, uint8_t *new_val, size_t val_size)
{
  memcpy(kv_ptr->last_accepted_value, new_val, val_size);
  //check_value_is_tr_top(kv_ptr->last_accepted_value, "Writing kv_ptr accepted value");
}

static inline void write_kv_if_conditional_on_ts(mica_op_t *kv_ptr, uint8_t *new_val,
                                                 size_t val_size,
                                                 uint8_t flag, struct ts_tuple base_ts)
{
  lock_seqlock(&kv_ptr->seqlock);
  if (compare_ts(&base_ts, &kv_ptr->ts) == GREATER) {
    write_kv_ptr_val(kv_ptr, new_val, (size_t) VALUE_SIZE, flag);
    kv_ptr->ts = base_ts;
  }
  unlock_seqlock(&kv_ptr->seqlock);
}


static inline void write_kv_if_conditional_on_netw_ts(mica_op_t *kv_ptr, uint8_t *new_val,
                                                      size_t val_size, uint8_t flag,
                                                      struct network_ts_tuple netw_base_ts)
{
  struct ts_tuple base_ts = {netw_base_ts.m_id, netw_base_ts.version};
  write_kv_if_conditional_on_ts(kv_ptr, new_val, val_size, flag, base_ts);

}

static inline bool same_rmw_id_same_ts_and_invalid(mica_op_t *kv_ptr, loc_entry_t *loc_entry)
{
  return rmw_ids_are_equal(&loc_entry->rmw_id, &kv_ptr->rmw_id) &&
         kv_ptr->state != INVALID_RMW &&
         compare_ts(&loc_entry->new_ts, &kv_ptr->prop_ts) == EQUAL;
}

static inline bool same_rmw_id_same_log_same_ts(mica_op_t *kv_ptr, loc_entry_t *loc_entry)
{
  return rmw_ids_are_equal(&loc_entry->rmw_id, &kv_ptr->rmw_id) &&
         loc_entry->log_no == kv_ptr->log_no &&
         compare_ts(&loc_entry->new_ts, &kv_ptr->prop_ts) == EQUAL;
}

static inline bool same_rmw_id_same_log(mica_op_t *kv_ptr, loc_entry_t *loc_entry)
{
  return rmw_ids_are_equal(&loc_entry->rmw_id, &kv_ptr->rmw_id) &&
         loc_entry->log_no == kv_ptr->log_no;
}

#endif //KITE_GENERIC_UTILITY_H

//
// Created by vasilis on 11/05/20.
//

#ifndef KITE_CLIENT_IF_UTIL_H
#define KITE_CLIENT_IF_UTIL_H

#include "debug_util.h"

/*-------------------------------- CLIENT REQUEST ARRAY ----------------------------------------*/
// signal completion of a request to the client
static inline void signal_completion_to_client(uint32_t sess_id,
                                               uint32_t req_array_i, uint16_t t_id)
{
  if (ENABLE_CLIENTS) {
    client_op_t *req_array = &interface[t_id].req_array[sess_id][req_array_i];
    check_session_id_and_req_array_index((uint16_t) sess_id, (uint16_t) req_array_i, t_id);

    //my_printf(yellow, "Wrkr %u/%u completing poll ptr %u for req %u at state %u \n", t_id,
    //       sess_id, req_array_i, req_array->opcode, req_array->state);

    if (ENABLE_ASSERTIONS) {
      if (req_array->state != IN_PROGRESS_REQ)
        printf("op %u, state %u slot %u/%u \n", req_array->opcode, req_array->state, sess_id, req_array_i);
    }
    check_state_with_allowed_flags(2, req_array->state, IN_PROGRESS_REQ);

    atomic_store_explicit(&req_array->state, (uint8_t) COMPLETED_REQ, memory_order_release);
    if (CLIENT_DEBUG)
      my_printf(green, "Releasing sess %u, ptr_to_req %u ptr %p \n", sess_id,
                req_array_i, &req_array->state);
  }
}

// signal that the request is being processed to tne client
static inline void signal_in_progress_to_client(uint32_t sess_id,
                                                uint32_t req_array_i, uint16_t t_id)
{
  if (ENABLE_CLIENTS) {
    client_op_t *req_array = &interface[t_id].req_array[sess_id][req_array_i];
    //my_printf(cyan, "Wrkr %u/%u signals in progress for  poll ptr %u for req %u at state %u \n", t_id,
    //       sess_id, req_array_i,req_array->opcode, req_array->state);
    check_session_id_and_req_array_index((uint16_t) sess_id, (uint16_t) req_array_i, t_id);
    if (ENABLE_ASSERTIONS) memset(&req_array->key, 0, KEY_SIZE);
    check_state_with_allowed_flags(2, req_array->state, ACTIVE_REQ);
    atomic_store_explicit(&req_array->state, (uint8_t) IN_PROGRESS_REQ, memory_order_release);
  }
}

// Returns whether a certain request is active, i.e. if the client has issued a request in a slot
static inline bool is_client_req_active(uint32_t sess_id,
                                        uint32_t req_array_i, uint16_t t_id)
{
  client_op_t * req_array = &interface[t_id].req_array[sess_id][req_array_i];
  check_session_id_and_req_array_index((uint16_t) sess_id, (uint16_t) req_array_i, t_id);
  return req_array->state == ACTIVE_REQ;
}

// is any request of the client request array active
static inline bool any_request_active(uint16_t sess_id, uint32_t req_array_i, uint16_t t_id)
{
  for (uint32_t i = 0; i < PER_SESSION_REQ_NUM; i++) {
    if (is_client_req_active(sess_id, i, t_id)) {
      my_printf(red, "session %u slot %u, state %u pull ptr %u\n",
                sess_id, i, interface[t_id].req_array[sess_id][req_array_i].state, req_array_i);
      if (i == req_array_i) return false;
      return true;
    }
  }
  return false;
}

//
static inline void fill_req_array_when_after_rmw(uint16_t sess_id, uint32_t req_array_i, uint8_t  opcode,
                                                 uint8_t* value_to_read, bool rmw_is_successful, uint16_t t_id)
{

  if (ENABLE_CLIENTS) {
    client_op_t *cl_op = &interface[t_id].req_array[sess_id][req_array_i];



    switch (opcode) {
      case RMW_PLAIN_WRITE:
        // This is really a write so no need to read anything
        break;
      case FETCH_AND_ADD:
        if (ENABLE_ASSERTIONS) {
          assert(value_to_read != NULL);
          assert(cl_op->value_to_read != NULL);
        }
        memcpy(cl_op->value_to_read, value_to_read, cl_op->val_len);
        //*cl_op->rmw_is_successful = true; // that will segfault, no bool pointer is passed in the FAA
        break;
      case COMPARE_AND_SWAP_WEAK:
      case COMPARE_AND_SWAP_STRONG:
        if (ENABLE_ASSERTIONS) {
          assert(value_to_read != NULL);
          assert(cl_op->value_to_read != NULL);
          assert(cl_op->rmw_is_successful != NULL);
        }
        *(cl_op->rmw_is_successful) = rmw_is_successful;
        // the value_to_read of loc_entry is valid, because the RMW can only get
        // committed iff it has been accepted once
        if (!rmw_is_successful)
          memcpy(cl_op->value_to_read, value_to_read, cl_op->val_len);

        break;
      default:
        if (ENABLE_ASSERTIONS) assert(false);
    }
  }
}

static inline void fill_req_array_on_rmw_early_fail(uint32_t sess_id, uint8_t* value_to_read,
                                                    uint32_t req_array_i, uint16_t t_id)
{
  if (ENABLE_CLIENTS) {
    if (ENABLE_ASSERTIONS) {
      assert(value_to_read != NULL);
      check_session_id_and_req_array_index((uint16_t) sess_id, (uint16_t) req_array_i, t_id);
    }
    client_op_t *cl_op = &interface[t_id].req_array[sess_id][req_array_i];
    *(cl_op->rmw_is_successful) = false;
    memcpy(cl_op->value_to_read, value_to_read, cl_op->val_len);
  }
}


// Returns true if it's valid to pull a request for that session
static inline bool pull_request_from_this_session(bool stalled, uint16_t sess_i,
                                                  uint16_t t_id)
{
  uint32_t pull_ptr = interface[t_id].wrkr_pull_ptr[sess_i];
  if (ENABLE_ASSERTIONS) {
    assert(sess_i < SESSIONS_PER_THREAD);
    if (ENABLE_CLIENTS) {
      assert(pull_ptr < PER_SESSION_REQ_NUM);
    }
  }
  if (ENABLE_CLIENTS)
    return (!stalled) && is_client_req_active(sess_i, pull_ptr, t_id);
  else
    return (!stalled);
}





#endif //KITE_CLIENT_IF_UTIL_H

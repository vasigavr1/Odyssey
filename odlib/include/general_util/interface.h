//
// Created by vasilis on 11/05/20.
//

#ifndef KITE_INTERFACE_H
#define KITE_INTERFACE_H

#include <city.h>
#include "top.h"

// API_OPCODE
#define RLXD_READ_BLOCKING 1
#define RLXD_WRITE_BLOCKING 2
#define ACQUIRE_BLOCKING 3
#define RELEASE_BLOCKING 4
#define CAS_BLOCKING 5
#define FAA_BLOCKING 6
// RETURN CODES
#define NO_SLOT_TO_ISSUE_REQUEST 0
#define ERROR_NO_SLOTS_FOR_BLOCKING_FUNCTION (-1)
#define ERROR_PUSH_PULL_PTR_MUST_MATCH (-2)
#define ERROR_SESSION_T0O_BIG (-3)
#define ERROR_KEY_IS_NOT_RMWABLE (-4)
#define ERROR_KEY_IS_RMWABLE (-5)
#define ERROR_NULL_READ_VALUE_PTR (-6)
#define ERROR_NULL_WRITE_VALUE_PTR (-7)
#define ERROR_WRONG_REQ_TYPE (-8)
#define ERROR_RMW_VAL_LEN_TOO_BIG (-9)
#define ERROR_VAL_LEN_TOO_BIG (-10)
#define ERROR_KEY_ID_DOES_NOT_EXIST (-11)


/* --------------------------------------------------------------------------------------
 * ----------------------------------API UTILITY-----------------------------------------------
 * --------------------------------------------------------------------------------------*/

//
static inline int check_inputs(uint16_t session_id, uint32_t key_id, uint8_t * value_to_read,
                               uint8_t * value_to_write, uint32_t val_len, uint8_t opcode) {
  if (session_id >= SESSIONS_PER_MACHINE)
    return ERROR_SESSION_T0O_BIG;

  switch (opcode) {
    case ACQUIRE_BLOCKING:
    case RLXD_READ_BLOCKING: // read
      if (value_to_read == NULL) return ERROR_NULL_READ_VALUE_PTR;
      break;
    case RELEASE_BLOCKING:
    case RLXD_WRITE_BLOCKING:
      if (value_to_write == NULL) return ERROR_NULL_WRITE_VALUE_PTR;
      break;
    case CAS_BLOCKING:
    case FAA_BLOCKING:
      // if (key_id >= NUM_OF_RMW_KEYS) return ERROR_KEY_IS_NOT_RMWABLE;
      if (value_to_read == NULL) return ERROR_NULL_READ_VALUE_PTR;
      if (value_to_write == NULL) return ERROR_NULL_WRITE_VALUE_PTR;
      break;
    default: return ERROR_WRONG_REQ_TYPE;
  }
  if (key_id >= KVS_NUM_KEYS)
    return ERROR_KEY_ID_DOES_NOT_EXIST;

// if (key_id < NUM_OF_RMW_KEYS) {
//   if (val_len > RMW_VALUE_SIZE) return ERROR_RMW_VAL_LEN_TOO_BIG;
// }
//  else
  if (val_len > VALUE_SIZE) return ERROR_VAL_LEN_TOO_BIG;

  return 1;
}

static inline void check_push_pull_ptrs(uint16_t session_id)
{
  if (ENABLE_ASSERTIONS)
    assert(last_pushed_req[session_id] - last_pulled_req[session_id] == PER_SESSION_REQ_NUM);
}
//
static inline void fill_client_op(client_op_t *cl_op, uint32_t key_id, uint8_t type,
                                  uint8_t *value_to_read, uint8_t *value_to_write, uint32_t val_len,
                                  bool *cas_result, bool weak)

{
  uint8_t  *expected_val = value_to_read, *desired_val = value_to_write;
  switch (type) {
    case RLXD_READ_BLOCKING:
      cl_op->value_to_read = value_to_read;
      cl_op->opcode = (uint8_t) KVS_OP_GET;
      break;
    case ACQUIRE_BLOCKING:
      cl_op->value_to_read = value_to_read;
      cl_op->opcode = (uint8_t) OP_ACQUIRE;
      break;
    case RLXD_WRITE_BLOCKING:
      cl_op->opcode = (uint8_t) KVS_OP_PUT;
      memcpy(cl_op->value_to_write, value_to_write, val_len);
      break;
    case RELEASE_BLOCKING:
      cl_op->opcode = (uint8_t) OP_RELEASE;
      memcpy(cl_op->value_to_write, value_to_write, val_len);
      break;
    case CAS_BLOCKING:
      cl_op->opcode = (uint8_t) (weak ? COMPARE_AND_SWAP_WEAK : COMPARE_AND_SWAP_STRONG);
      memcpy(cl_op->value_to_write, desired_val, (size_t) val_len);
      cl_op->value_to_read = expected_val;
      cl_op->rmw_is_successful = cas_result;
      //memcpy(cl_op->value_to_read, expected_val, (size_t) RMW_VALUE_SIZE);
      break;
    case FAA_BLOCKING:
      cl_op->opcode = (uint8_t) FETCH_AND_ADD;
      cl_op->value_to_read = value_to_read;
      //memcpy(cl_op->value_to_read, value_to_read, (size_t) RMW_VALUE_SIZE);
      memcpy(cl_op->value_to_write, value_to_write, (size_t) val_len);
      break;
    default : assert(false);
  }
  cl_op->val_len = val_len;
  uint64_t key_hash = CityHash128((char *) &(key_id), 4).second;
  memcpy(&cl_op->key, &key_hash, KEY_SIZE);
}

// fill the replies // TODO Probably needs to be DEPRICATED
static inline void check_return_values(client_op_t *cl_op)
{
  switch (cl_op->opcode) {
    case KVS_OP_PUT:
      if (ENABLE_ASSERTIONS) {
        assert(cl_op->val_len <= VALUE_SIZE);
        memset(cl_op->value_to_write, 255, cl_op->val_len);
      }
    default : return;
  }
}


/* ----------------------------------POLLING API-----------------------------------------------*/

//
static inline uint64_t poll(uint16_t session_id)
{
  uint16_t wrkr = (uint16_t) (session_id / SESSIONS_PER_THREAD);
  uint16_t s_i = (uint16_t) (session_id % SESSIONS_PER_THREAD);
  uint16_t pull_ptr = interface[wrkr].clt_pull_ptr[s_i];
  client_op_t *pull_clt_op = &interface[wrkr].req_array[s_i][pull_ptr];
  client_op_t *push_clt_op = &interface[wrkr].req_array[s_i][interface[wrkr].clt_push_ptr[s_i]];
  while (pull_clt_op->state == COMPLETED_REQ) {
    // get the result
    if (CLIENT_DEBUG)
      my_printf(green, "Client  pulling req from worker %u for session %u, slot %u, last_pulled %u \n",
                wrkr, s_i, pull_ptr, last_pulled_req[session_id]);

    check_return_values(pull_clt_op);
    atomic_store_explicit(&pull_clt_op->state, (uint8_t) INVALID_REQ, memory_order_relaxed);
    check_state_with_allowed_flags(2, push_clt_op->state, INVALID_REQ);
    MOD_INCR(interface[wrkr].clt_pull_ptr[s_i], PER_SESSION_REQ_NUM);
    last_pulled_req[session_id]++; // no races across clients
  }
  return last_pulled_req[session_id];
}

// Blocking call
static inline void poll_all_reqs(uint16_t session_id)
{
  while(poll(session_id) < last_pushed_req[session_id]);
}

// returns whether it managed to poll a request
static inline bool poll_a_req_async(uint16_t session_id, uint64_t target)
{
  return poll(session_id) >= target;
}

// returns after it managed to poll a request
static inline void poll_a_req_blocking(uint16_t session_id, uint64_t target)
{
  if (last_pulled_req[session_id] >= target) return;
  while (poll(session_id) < target);
}

// Blocks until it can poll one request. Useful when you need to issue a request
static inline void poll_one_req_blocking(uint16_t session_id)
{
  uint64_t largest_polled = last_pulled_req[session_id];
  while (poll(session_id) <= largest_polled);
  if (ENABLE_ASSERTIONS) {
    uint16_t wrkr = (uint16_t) (session_id / SESSIONS_PER_THREAD);
    uint16_t s_i = (uint16_t) (session_id % SESSIONS_PER_THREAD);
    uint16_t push_ptr = interface[wrkr].clt_push_ptr[s_i];
    client_op_t *push_clt_op = &interface[wrkr].req_array[s_i][push_ptr];
    check_state_with_allowed_flags(2, push_clt_op->state, INVALID_REQ);
  }
}

static inline bool is_polled(uint16_t session_id, uint64_t target)
{
  return  last_pulled_req[session_id] >= target;
}

/* ----------------------------------SYNC & ASYNC-----------------------------------------------*/
//
static inline int access_blocking(uint32_t key_id, uint8_t *value_to_read,
                                  uint8_t *value_to_write, uint32_t val_len, bool *cas_result,
                                  bool rmw_is_weak, uint16_t session_id, uint8_t type)
{
  int return_int = check_inputs(session_id, key_id, value_to_read, value_to_write, val_len, type);
  if (return_int < 0) return return_int;
  uint16_t wrkr = (uint16_t) (session_id / SESSIONS_PER_THREAD);
  uint16_t s_i = (uint16_t) (session_id % SESSIONS_PER_THREAD);
  uint16_t push_ptr = interface[wrkr].clt_push_ptr[s_i];


  // let's poll for the slot first
  if (interface[wrkr].req_array[s_i][push_ptr].state != INVALID_REQ) {
    poll(session_id);
    if (interface[wrkr].req_array[s_i][push_ptr].state != INVALID_REQ) {
      poll_one_req_blocking(session_id);
      assert(interface[wrkr].req_array[s_i][push_ptr].state == INVALID_REQ);
    }
  }

  // Issuing the request
  client_op_t *cl_op = &interface[wrkr].req_array[s_i][push_ptr];
  fill_client_op(cl_op, key_id, type, value_to_read, value_to_write, val_len, cas_result, rmw_is_weak);

  // Implicit assumption: other client threads are not racing for this slot
  atomic_store_explicit(&cl_op->state, (uint8_t) ACTIVE_REQ, memory_order_release);
  MOD_INCR(interface[wrkr].clt_push_ptr[s_i], PER_SESSION_REQ_NUM);
  last_pushed_req[session_id]++;

  // Polling for completion
  poll_all_reqs(session_id);

  if (ENABLE_ASSERTIONS)
    assert(interface[wrkr].clt_push_ptr[s_i] == interface[wrkr].clt_pull_ptr[s_i]);
  return return_int;
}

//
static inline int access_async(uint32_t key_id, uint8_t *value_to_read,
                               uint8_t *value_to_write, uint32_t val_len, bool *cas_result,
                               bool rmw_is_weak, bool strong,
                               uint16_t session_id, uint8_t type)
{
  int return_int = check_inputs(session_id, key_id, value_to_read, value_to_write, val_len,  type);
  if (return_int < 0) {
    if (ENABLE_ASSERTIONS) {
      my_printf(red, "Error %d, when checking req type %u, for key_id %u, session %u \n",
                return_int, type, key_id, session_id);
      assert(false);
    }
    return return_int;
  }

  uint16_t wrkr = (uint16_t) (session_id / SESSIONS_PER_THREAD);
  uint16_t s_i = (uint16_t) (session_id % SESSIONS_PER_THREAD);
  uint16_t push_ptr = interface[wrkr].clt_push_ptr[s_i];
  client_op_t *push_clt_op = &interface[wrkr].req_array[s_i][push_ptr];

  // let's poll for the slot first
  if (push_clt_op->state != INVALID_REQ) {
    check_push_pull_ptrs(session_id);
    // try to do some polling
    poll(session_id);
    if (push_clt_op->state != INVALID_REQ) {
      //check_push_pull_ptrs(session_id);
      if (strong) {
        poll_one_req_blocking(session_id);
        check_state_with_allowed_flags (2, push_clt_op->state, INVALID_REQ);
      }
      else {
        assert(false);
        return NO_SLOT_TO_ISSUE_REQUEST;
      }
    }
  }
  // Issuing the request
  client_op_t *cl_op = &interface[wrkr].req_array[s_i][push_ptr];
  fill_client_op(cl_op, key_id, type, value_to_read, value_to_write, val_len, cas_result, rmw_is_weak);

  // Implicit assumption: other client threads are not racing for this slot
  check_state_with_allowed_flags (2, cl_op->state, INVALID_REQ);

  //my_printf(green, "Sess %u Activating poll ptr %u for req %u at state %u \n",
  //             s_i, w_push_ptr, cl_op->opcode, cl_op->state);
  atomic_store_explicit(&cl_op->state, (uint8_t) ACTIVE_REQ, memory_order_release);
  MOD_INCR(interface[wrkr].clt_push_ptr[s_i], PER_SESSION_REQ_NUM);
  last_pushed_req[session_id]++;
  return (int)last_pushed_req[session_id];
}


/* --------------------------------------------------------------------------------------
 * ----------------------------------ASYNC API----------------------------------------
 * --------------------------------------------------------------------------------------*/

/*------------------------STRONG------------------------------------------------------*/
// Async strong functions will block until the request is *issued* (i.e. block polling for a free slot)

//
static inline int async_read_strong(uint32_t key_id, uint8_t *value_to_read,
                                    uint32_t val_len, uint16_t session_id)
{
  return access_async((uint32_t) key_id, value_to_read, NULL, val_len, NULL,
                      false, true, session_id, RLXD_READ_BLOCKING);
}

//
static inline int async_write_strong(uint32_t key_id, uint8_t *value_to_write,
                                     uint32_t val_len, uint16_t session_id)
{

  return access_async((uint32_t) key_id, NULL, value_to_write, val_len,
                      NULL, false, true, session_id, RLXD_WRITE_BLOCKING);
}

//
static inline int async_acquire_strong(uint32_t key_id, uint8_t *value_to_read,
                                       uint32_t val_len, uint16_t session_id)
{
  return access_async((uint32_t) key_id, value_to_read, NULL, val_len,
                      NULL, false, true, session_id, ACQUIRE_BLOCKING);
}

//
static inline int async_release_strong(uint32_t key_id, uint8_t *value_to_write,
                                       uint32_t val_len, uint16_t session_id)
{

  return access_async((uint32_t) key_id, NULL, value_to_write, val_len,
                      NULL, false, true, session_id, RELEASE_BLOCKING);
}

//
static inline int async_cas_strong(uint32_t key_id, uint8_t *expected_val,
                                   uint8_t *desired_val, uint32_t val_len,
                                   bool *cas_result, bool rmw_is_weak,
                                   uint16_t session_id)
{
  return access_async((uint32_t) key_id, expected_val, desired_val, val_len,
                      cas_result, rmw_is_weak, true, session_id, CAS_BLOCKING);
}

//
static inline int async_faa_strong(uint32_t key_id, uint8_t *value_to_read,
                                   uint8_t *argument_val, uint32_t val_len,
                                   uint16_t session_id)
{
  return access_async((uint32_t) key_id, value_to_read, argument_val, val_len,
                      NULL, false, true, session_id, FAA_BLOCKING);
}
/*------------------------WEAK------------------------------------------------------*/
// Async weak functions will not block, but may return that the request was not issued

//
static inline int async_read_weak(uint32_t key_id, uint8_t *value_to_read,
                                  uint32_t val_len, uint16_t session_id)
{
  return access_async((uint32_t) key_id, value_to_read, NULL, val_len,
                      NULL, false, false, session_id, RLXD_READ_BLOCKING);
}

//
static inline int async_write_weak(uint32_t key_id, uint8_t *value_to_write,
                                   uint32_t val_len, uint16_t session_id)
{

  return access_async((uint32_t) key_id, NULL, value_to_write, val_len,
                      NULL, false, false, session_id, RLXD_WRITE_BLOCKING);
}

//
static inline int async_acquire_weak(uint32_t key_id, uint8_t *value_to_read,
                                     uint32_t val_len, uint16_t session_id)
{
  return access_async((uint32_t) key_id, value_to_read, NULL, val_len,
                      NULL, false, false, session_id, ACQUIRE_BLOCKING);
}

//
static inline int async_release_weak(uint32_t key_id, uint8_t *value_to_write,
                                     uint32_t val_len, uint16_t session_id)
{

  return access_async((uint32_t) key_id, NULL, value_to_write, val_len,
                      NULL, false, false, session_id, RELEASE_BLOCKING);
}

//
static inline int async_cas_weak(uint32_t key_id, uint8_t *expected_val,
                                 uint8_t *desired_val, uint32_t val_len,
                                 bool *cas_result, bool rmw_is_weak,
                                 uint16_t session_id)
{
  return access_async((uint32_t) key_id, expected_val, desired_val, val_len,
                      cas_result, rmw_is_weak, false, session_id, CAS_BLOCKING);
}

//
static inline int async_faa_weak(uint32_t key_id, uint8_t *value_to_read,
                                 uint8_t *argument_val, uint32_t val_len,
                                 uint16_t session_id)
{
  return access_async((uint32_t) key_id, value_to_read, argument_val, val_len,
                      NULL, false, false, session_id, FAA_BLOCKING);
}



/* --------------------------------------------------------------------------------------
 * ----------------------------------BLOCKING API----------------------------------------
 * --------------------------------------------------------------------------------------*/

//
static inline int blocking_read(uint32_t key_id, uint8_t *value_to_read,
                                uint32_t val_len, uint16_t session_id)
{
  return access_blocking((uint32_t) key_id, value_to_read, NULL, val_len,
                         NULL, false, session_id, RLXD_READ_BLOCKING);
}

//
static inline int blocking_write(uint32_t key_id, uint8_t *value_to_write,
                                 uint32_t val_len, uint16_t session_id)
{

  return access_blocking((uint32_t) key_id, NULL, value_to_write, val_len,
                         NULL, false, session_id, RLXD_WRITE_BLOCKING);
}

//
static inline int blocking_acquire(uint32_t key_id, uint8_t *value_to_read,
                                   uint32_t val_len, uint16_t session_id)
{
  return access_blocking((uint32_t) key_id, value_to_read, NULL, val_len,
                         NULL, false, session_id, ACQUIRE_BLOCKING);
}

//
static inline int blocking_release(uint32_t key_id, uint8_t *value_to_write,
                                   uint32_t val_len, uint16_t session_id)
{

  return access_blocking((uint32_t) key_id, NULL, value_to_write, val_len,
                         NULL, false, session_id, RELEASE_BLOCKING);
}

//
static inline int blocking_cas(uint32_t key_id, uint8_t *expected_val,
                               uint8_t *desired_val, uint32_t val_len,
                               bool *cas_result, bool rmw_is_weak,
                               uint16_t session_id)
{
  return access_blocking((uint32_t) key_id, expected_val, desired_val, val_len,
                         cas_result, rmw_is_weak, session_id, CAS_BLOCKING);
}

//
static inline int blocking_faa(uint32_t key_id, uint8_t *value_to_read,
                               uint8_t *argument_val, uint32_t val_len,
                               uint16_t session_id)
{
  return access_blocking((uint32_t) key_id, value_to_read, argument_val, val_len,
                         NULL, false, session_id, FAA_BLOCKING);
}


/* ------------------------------------------------------------------------------------------------------------------- */
/* ------------------------------------USER INTERFACE----------------------------------------------------------------- */
/* ------------------------------------------------------------------------------------------------------------------- */
// allow the user to check the API from the console
static inline void user_interface()
{
  uint8_t expected_val[VALUE_SIZE] = {0};
  uint8_t desired_val[VALUE_SIZE] = {0};
  uint8_t value[VALUE_SIZE] = {0};
  int key_id = 0;
  int expected_int, desired_int;
  uint16_t session_id = 10;
  bool cas_result;
  sleep(3);
  int req_no = 0;
  do {
    my_printf(cyan, "Pick a request number: read: 1, write 2, acquire 3, release 4, CAS 5, FAA 6 \n");
  } while (scanf("%d", &req_no) != 1);

  switch (req_no) {
    case RLXD_READ_BLOCKING: // read
      do {
        my_printf(yellow, "READ: Please input key_id \n");
      } while (scanf("%d", &key_id) != 1);
      assert(key_id >= 0);
      printf ("key_id %u \n", key_id);
      int ret = blocking_read((uint32_t) key_id, value, 1, session_id);
      my_printf(green, "Return code %d, value read %u \n", ret, value[0]);
      assert(ret > 0);
      break;
    case RLXD_WRITE_BLOCKING: // write
      do {
        my_printf(yellow, "WRITE: Please input key_id, value_to_write \n");
      } while (scanf("%d %d", &key_id, &desired_int) != 2);
      assert(key_id >= 0 && desired_int < 256);
      printf ("key_id %u,  desired %u \n", key_id, desired_int);
      value[0] = (uint8_t) desired_int;
      ret = blocking_write((uint32_t) key_id, value, 1,  session_id);
      my_printf(green, "Return code %d \n", ret);
      assert(ret > 0);
      break;
    case ACQUIRE_BLOCKING:
      do {
        my_printf(yellow, "ACQUIRE: Please input key_id \n");
      } while (scanf("%d", &key_id) != 1);
      assert(key_id >= 0);
      printf ("key_id %u \n", key_id);
      ret = blocking_acquire((uint32_t) key_id, value, 1,  session_id);
      my_printf(green, "Return code %d, value read  %u \n", ret, value[0]);
      assert(ret > 0);
      break;
    case RELEASE_BLOCKING:
      do {
        my_printf(yellow, "RELEASE: Please input key_id, value_to_write \n");
      } while (scanf("%d %d", &key_id, &desired_int) != 2);
      assert(key_id >= 0 && desired_int < 256);
      printf ("key_id %u,  desired %u \n", key_id, desired_int);
      value[0] = (uint8_t) desired_int;
      ret = blocking_release((uint32_t) key_id, value, 1,  session_id);
      my_printf(green, "Return code %d \n", ret);
      assert(ret > 0);
      break;
    case CAS_BLOCKING:
      do {
        my_printf(yellow, "Please input key_id, expected value , desired value \n");
      } while (scanf("%d %d %d:  ", &key_id, &expected_int, &desired_int) != 3);
      assert(expected_int < 256 && desired_int < 256 && key_id >= 0);
      expected_val[0] = (uint8_t) expected_int;
      desired_val[0] = (uint8_t) desired_int;
      printf ("key_id %u, expected %u, desired %u \n", key_id, expected_val[0], desired_val[0]);
      ret = blocking_cas((uint32_t) key_id, expected_val, desired_val,  1,
                         &cas_result, true, session_id);
      my_printf(green, "%s, return code %d, expected val %u, desired val %u\n",
                cas_result ? "Success" : "Failure", ret, expected_val[0], desired_val[0]);
      assert(ret > 0);
      break;
    case FAA_BLOCKING:
      do {
        my_printf(yellow, "Please input key_id, value to add \n");
      } while (scanf("%d %d:  ", &key_id, &desired_int) != 2);
      assert(desired_int < 256 && key_id >= 0);
      desired_val[0] = (uint8_t) desired_int;
      printf ("key_id %u, desired %u \n", key_id, desired_val[0]);
      ret = blocking_faa((uint32_t) key_id, value, desired_val, 1, session_id);
      my_printf(green, "Return code %d, value read: %u\n",
                ret, value[0], desired_val[0]);
      assert(ret > 0);
      break;
    default:
      my_printf(red, "No request corresponds to input number %d \n", req_no);
      assert(false);

  }
}




#endif //KITE_INTERFACE_H

//
// Created by vasilis on 05/02/19.
//

#include "util.h"
#include "inline_util.h"

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
 * ----------------------------------TRACE-----------------------------------------------
 * --------------------------------------------------------------------------------------*/

// Use a trace - can be either manufactured or from text
static inline  uint32_t  send_reqs_from_trace(uint16_t worker_num, uint16_t first_worker,
                                          struct trace_command *trace, uint32_t trace_ptr,
                                          uint32_t *dbg_cntr_, uint16_t t_id)
{
  uint32_t dbg_cntr = *dbg_cntr_;
  uint16_t w_i = 0, s_i = 0;
  bool polled = false;
  // poll requests
  for (w_i = 0; w_i < worker_num; w_i++) {
    uint16_t wrkr = w_i + first_worker;
    for (s_i = 0; s_i < SESSIONS_PER_THREAD; s_i++) {
      uint16_t pull_ptr = interface[wrkr].clt_pull_ptr[s_i];
      while (interface[wrkr].req_array[s_i][pull_ptr].state == COMPLETED_REQ) {
        // get the result
        polled = true;
        if (CLIENT_DEBUG)
          green_printf("Client %u pulling req from worker %u for session %u, slot %u\n",
                       t_id, wrkr, s_i, pull_ptr);
        atomic_store_explicit(&interface[wrkr].req_array[s_i][pull_ptr].state, INVALID_REQ, memory_order_relaxed);
        MOD_ADD(interface[wrkr].clt_pull_ptr[s_i], PER_SESSION_REQ_NUM);
      }
    }
  }
  if (!polled) dbg_cntr++;
  else dbg_cntr = 0;
  if (dbg_cntr == BILLION) {
    printf("Failed to poll \n");
    //interface[wrkr].clt_pull_ptr[s_i], (void *)&req_array[w_i][s_i][interface[wrkr].clt_pull_ptr[s_i]].state, req_array[w_i][s_i][pull_ptr[w_i][s_i]].state);
    dbg_cntr = 0;
  }
  // issue requests
  for (w_i = 0; w_i < worker_num; w_i++) {
    uint16_t wrkr = w_i + first_worker;
    for (s_i = 0; s_i < SESSIONS_PER_THREAD; s_i++) {
      uint16_t push_ptr = interface[wrkr].clt_push_ptr[s_i];
      while (interface[wrkr].req_array[s_i][push_ptr].state == INVALID_REQ) {
        if (CLIENT_DEBUG)
          yellow_printf("Client %u inserting req to worker %u for session %u, in slot %u from trace slot %u ptr %p\n",
                        t_id, wrkr, s_i, push_ptr, trace_ptr, &interface[wrkr].req_array[s_i][push_ptr].state);
        interface[wrkr].req_array[s_i][push_ptr].opcode = trace[trace_ptr].opcode;
        memcpy(&interface[wrkr].req_array[s_i][push_ptr].key, trace[trace_ptr].key_hash, TRUE_KEY_SIZE);
        atomic_store_explicit(&interface[wrkr].req_array[s_i][push_ptr].state, (uint8_t) ACTIVE_REQ, memory_order_release);
        MOD_ADD(interface[wrkr].clt_push_ptr[s_i], PER_SESSION_REQ_NUM);
        trace_ptr++;
        if (trace[trace_ptr].opcode == NOP) trace_ptr = 0;
      }
    }
  }
  *dbg_cntr_ = dbg_cntr;
  return trace_ptr;
}


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
      if (key_id >= NUM_OF_RMW_KEYS) return ERROR_KEY_IS_NOT_RMWABLE;
      if (value_to_read == NULL) return ERROR_NULL_READ_VALUE_PTR;
      if (value_to_write == NULL) return ERROR_NULL_WRITE_VALUE_PTR;
      break;
    default: return ERROR_WRONG_REQ_TYPE;
  }
 if (key_id >= CACHE_NUM_KEYS)
   return ERROR_KEY_ID_DOES_NOT_EXIST;

 if (key_id < NUM_OF_RMW_KEYS) {
   if (val_len > RMW_VALUE_SIZE) return ERROR_RMW_VAL_LEN_TOO_BIG;
 }
  else if (val_len > VALUE_SIZE) return ERROR_VAL_LEN_TOO_BIG;

 return 1;
}

static inline void check_push_pull_ptrs(uint16_t session_id)
{
  if (ENABLE_ASSERTIONS)
    assert(last_pushed_req[session_id] - last_pulled_req[session_id] == PER_SESSION_REQ_NUM);
}
//
static inline void fill_client_op(struct client_op *cl_op, uint32_t key_id, uint8_t type,
                                  uint8_t *value_to_read, uint8_t *value_to_write, uint32_t val_len,
                                  bool *cas_result, bool weak)

{
  uint8_t  *expected_val = value_to_read, *desired_val = value_to_write;
  switch (type) {
    case RLXD_READ_BLOCKING:
      cl_op->value_to_read = value_to_read;
      cl_op->opcode = (uint8_t) CACHE_OP_GET;
      break;
    case ACQUIRE_BLOCKING:
      cl_op->value_to_read = value_to_read;
      cl_op->opcode = (uint8_t) OP_ACQUIRE;
      break;
    case RLXD_WRITE_BLOCKING:
      cl_op->opcode = (uint8_t) (key_id >= NUM_OF_RMW_KEYS ? CACHE_OP_PUT : RMW_PLAIN_WRITE);
      memcpy(cl_op->value_to_write, value_to_write, val_len);
      break;
    case RELEASE_BLOCKING:
      cl_op->opcode = (uint8_t) (key_id >= NUM_OF_RMW_KEYS ? OP_RELEASE : RMW_PLAIN_WRITE);
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
  memcpy(&cl_op->key, &key_hash, TRUE_KEY_SIZE);
}

// fill the replies // TODO Probably needs to be DEPRICATED
static inline void check_return_values(struct client_op *cl_op)
{
  switch (cl_op->opcode) {
    case CACHE_OP_PUT:
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
  struct client_op *pull_clt_op = &interface[wrkr].req_array[s_i][pull_ptr];
  struct client_op *push_clt_op = &interface[wrkr].req_array[s_i][interface[wrkr].clt_push_ptr[s_i]];
  while (pull_clt_op->state == COMPLETED_REQ) {
    // get the result
    if (CLIENT_DEBUG)
      green_printf("Client  pulling req from worker %u for session %u, slot %u, last_pulled %u \n",
                    wrkr, s_i, pull_ptr, last_pulled_req[session_id]);

    check_return_values(pull_clt_op);
    atomic_store_explicit(&pull_clt_op->state, (uint8_t) INVALID_REQ, memory_order_relaxed);
    check_state_with_allowed_flags(2, push_clt_op->state, INVALID_REQ);
    MOD_ADD(interface[wrkr].clt_pull_ptr[s_i], PER_SESSION_REQ_NUM);
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
    struct client_op *push_clt_op = &interface[wrkr].req_array[s_i][push_ptr];
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
  struct client_op *cl_op = &interface[wrkr].req_array[s_i][push_ptr];
  fill_client_op(cl_op, key_id, type, value_to_read, value_to_write, val_len, cas_result, rmw_is_weak);

  // Implicit assumption: other client threads are not racing for this slot
  atomic_store_explicit(&cl_op->state, (uint8_t) ACTIVE_REQ, memory_order_release);
  MOD_ADD(interface[wrkr].clt_push_ptr[s_i], PER_SESSION_REQ_NUM);
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
      red_printf("Error %d, when checking req type %u, for key_id %u, session %u \n",
                 return_int, type, key_id, session_id);
      assert(false);
    }
    return return_int;
  }

  uint16_t wrkr = (uint16_t) (session_id / SESSIONS_PER_THREAD);
  uint16_t s_i = (uint16_t) (session_id % SESSIONS_PER_THREAD);
  uint16_t push_ptr = interface[wrkr].clt_push_ptr[s_i];
  struct client_op *push_clt_op = &interface[wrkr].req_array[s_i][push_ptr];

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
  struct client_op *cl_op = &interface[wrkr].req_array[s_i][push_ptr];
  fill_client_op(cl_op, key_id, type, value_to_read, value_to_write, val_len, cas_result, rmw_is_weak);

  // Implicit assumption: other client threads are not racing for this slot
  atomic_store_explicit(&cl_op->state, (uint8_t) ACTIVE_REQ, memory_order_release);
  MOD_ADD(interface[wrkr].clt_push_ptr[s_i], PER_SESSION_REQ_NUM);
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
    cyan_printf("Pick a request number: read: 1, write 2, acquire 3, release 4, CAS 5, FAA 6 \n");
  } while (scanf("%d", &req_no) != 1);

  switch (req_no) {
    case RLXD_READ_BLOCKING: // read
      do {
        yellow_printf("READ: Please input key_id \n");
      } while (scanf("%d", &key_id) != 1);
      assert(key_id >= 0);
      printf ("key_id %u \n", key_id);
      int ret = blocking_read((uint32_t) key_id, value, 1, session_id);
      green_printf("Return code %d, value read %u \n", ret, value[0]);
      assert(ret > 0);
      break;
    case RLXD_WRITE_BLOCKING: // write
      do {
        yellow_printf("WRITE: Please input key_id, value_to_write \n");
      } while (scanf("%d %d", &key_id, &desired_int) != 2);
      assert(key_id >= 0 && desired_int < 256);
      printf ("key_id %u,  desired %u \n", key_id, desired_int);
      value[0] = (uint8_t) desired_int;
      ret = blocking_write((uint32_t) key_id, value, 1,  session_id);
      green_printf("Return code %d \n", ret);
      assert(ret > 0);
      break;
    case ACQUIRE_BLOCKING:
      do {
        yellow_printf("ACQUIRE: Please input key_id \n");
      } while (scanf("%d", &key_id) != 1);
      assert(key_id >= 0);
      printf ("key_id %u \n", key_id);
      ret = blocking_acquire((uint32_t) key_id, value, 1,  session_id);
      green_printf("Return code %d, value read  %u \n", ret, value[0]);
      assert(ret > 0);
      break;
    case RELEASE_BLOCKING:
      do {
        yellow_printf("RELEASE: Please input key_id, value_to_write \n");
      } while (scanf("%d %d", &key_id, &desired_int) != 2);
      assert(key_id >= 0 && desired_int < 256);
      printf ("key_id %u,  desired %u \n", key_id, desired_int);
      value[0] = (uint8_t) desired_int;
      ret = blocking_release((uint32_t) key_id, value, 1,  session_id);
      green_printf("Return code %d \n", ret);
      assert(ret > 0);
      break;
    case CAS_BLOCKING:
      do {
        yellow_printf("Please input key_id, expected value , desired value \n");
      } while (scanf("%d %d %d:  ", &key_id, &expected_int, &desired_int) != 3);
      assert(expected_int < 256 && desired_int < 256 && key_id >= 0);
      expected_val[0] = (uint8_t) expected_int;
      desired_val[0] = (uint8_t) desired_int;
      printf ("key_id %u, expected %u, desired %u \n", key_id, expected_val[0], desired_val[0]);
      ret = blocking_cas((uint32_t) key_id, expected_val, desired_val,  1,
                         &cas_result, true, session_id);
      green_printf("%s, return code %d, expected val %u, desired val %u\n",
                   cas_result ? "Success" : "Failure", ret, expected_val[0], desired_val[0]);
      assert(ret > 0);
      break;
    case FAA_BLOCKING:
      do {
        yellow_printf("Please input key_id, value to add \n");
      } while (scanf("%d %d:  ", &key_id, &desired_int) != 2);
      assert(desired_int < 256 && key_id >= 0);
      desired_val[0] = (uint8_t) desired_int;
      printf ("key_id %u, desired %u \n", key_id, desired_val[0]);
      ret = blocking_faa((uint32_t) key_id, value, desired_val, 1, session_id);
      green_printf("Return code %d, value read: %u\n",
                   ret, value[0], desired_val[0]);
      assert(ret > 0);
      break;
    default:
      red_printf("No request corresponds to input number %d \n", req_no);
      assert(false);

  }
}

#define RELAXED_WRITES 5
static inline void rel_acq_circular_blocking() {
  int i = 0;
  uint8_t expected_val[VALUE_SIZE] = {0};
  uint8_t desired_val[VALUE_SIZE] = {0};
  uint8_t value[VALUE_SIZE] = {0};
  int key_id = 0;
  int expected_int, desired_int;
  uint16_t session_id = 0;
  bool cas_result;
  sleep(3);
  uint32_t flag_offset = 2 * NUM_OF_RMW_KEYS + (MACHINE_NUM * 100);
  uint32_t key_flags[MACHINE_NUM];
  for (i = 0; i < MACHINE_NUM; i++) key_flags[i] = flag_offset + i;
  uint32_t key_offset = (uint32_t) (NUM_OF_RMW_KEYS);
  uint16_t relaxed_writes = 5;
  uint32_t val_len = 20;

  // machine 0 kicks things off
  if (machine_id == 0) {
    for (i = 0; i < relaxed_writes; i++) {
      desired_val[0] = (uint8_t) (10 * machine_id + i);
      blocking_write(key_offset + i, desired_val, val_len, session_id);
      yellow_printf("Writing key %u, iteration %u, value %u \n", key_offset + i, i, desired_val[0]);
    }
    desired_val[0] = 1;
    blocking_release(key_flags[machine_id], desired_val, val_len, session_id);
  }

  uint8_t prev_machine_id = (uint8_t) ((MACHINE_NUM + machine_id - 1) % MACHINE_NUM);
  while (true) {
    // First acquire the previous machine flag
    yellow_printf("Machine %d Acquiring key_flag %u  from machine %u\n",
                  machine_id, key_flags[prev_machine_id], prev_machine_id);
    do {
      int ret = blocking_acquire(key_flags[prev_machine_id], value, val_len, session_id);
      assert(ret > 0);
    } while (value[0] != 1);

    yellow_printf("Machine %d Acquired key_flag %u  from machine %u\n",
                  machine_id, key_flags[prev_machine_id], prev_machine_id);
    // Then read all values to make sure they are what they are supposed to be
    for (i = 0; i < relaxed_writes; i++) {
      blocking_read(key_offset + i, value, val_len, session_id);
      cyan_printf("Reading key %u, iteration %u, value %u \n", key_offset + i, i, value[0]);
      assert(value[0] == 10 * prev_machine_id + i);
    }

    // Write the same values but with different values
    for (i = 0; i < relaxed_writes; i++) {
      desired_val[0] = (uint8_t) (10 * machine_id + i);
      blocking_write(key_offset + i, desired_val, val_len, session_id);
    }

    // reset the flag of the previous machine
    desired_val[0] = 0;
    blocking_write(key_flags[prev_machine_id], desired_val, val_len, session_id);

    // release your flag
    desired_val[0] = 1;
    yellow_printf("Releasing key_flag %u \n", key_flags[machine_id]);
    blocking_release(key_flags[machine_id], desired_val, val_len, session_id);
  }
}

static inline void rel_acq_circular_async() {
  int i = 0;
  uint8_t expected_val[VALUE_SIZE] = {0};
  uint8_t desired_val[VALUE_SIZE] = {0};
  uint8_t value[RELAXED_WRITES][VALUE_SIZE] = {0};
  int key_id = 0;
  int expected_int, desired_int;
  uint16_t session_id = 0;
  bool cas_result;
  sleep(3);
  uint32_t flag_offset = 2 * NUM_OF_RMW_KEYS + (MACHINE_NUM * 100);
  uint32_t key_flags[MACHINE_NUM];
  for (i = 0; i < MACHINE_NUM; i++) key_flags[i] = flag_offset + i;
  //uint32_t key_offset = (uint32_t) (NUM_OF_RMW_KEYS);
  uint8_t prev_machine_id = (uint8_t) ((MACHINE_NUM + machine_id - 1) % MACHINE_NUM);
  // Write keys depending on my machine_id
  //uint32_t write_key_offset = (uint32_t) (NUM_OF_RMW_KEYS + machine_id * RELAXED_WRITES);
  //uint32_t read_key_offset = (uint32_t) (NUM_OF_RMW_KEYS + prev_machine_id * RELAXED_WRITES);
  uint32_t write_key_offset = (uint32_t) (NUM_OF_RMW_KEYS);
  uint32_t read_key_offset = write_key_offset;
  uint16_t relaxed_writes = RELAXED_WRITES;
  uint64_t last_issued_req = 0;
  int ret;
  uint val_len = 1;

  // machine 0 kicks things off
  if (machine_id == 0) {
    for (i = 0; i < relaxed_writes; i++) {
      desired_val[0] = (uint8_t) (10 * machine_id + i);
      async_write_strong(write_key_offset + i, desired_val, val_len, session_id);
      //yellow_printf("Writing key %u, iteration %u, value %u \n", write_key_offset + i, i, desired_val[0]);
    }
    desired_val[0] = 1;
    async_release_strong(key_flags[machine_id], desired_val, val_len, session_id);
  }
  while (true) {
    // First acquire the previous machine flag
    //yellow_printf("Machine %d Acquiring key_flag %u  from machine %u\n",
    //              machine_id, key_flags[prev_machine_id], prev_machine_id);
    do {
      ret = blocking_acquire(key_flags[prev_machine_id], value[0], val_len, session_id);
      assert(ret >= 0);
    } while (value[0][0] != 1);

    //yellow_printf("Machine %d Acquired key_flag %u  from machine %u\n",
    //              machine_id, key_flags[prev_machine_id], prev_machine_id);


    // Issue asynchronous reads
    for (i = 0; i < relaxed_writes; i++) {
      last_issued_req = (uint64_t) async_read_strong(read_key_offset + i, value[i], val_len, session_id);
    }


    // Write different values but with different values
    for (i = 0; i < relaxed_writes; i++) {
      desired_val[0] = (uint8_t) (10 * machine_id + i);
      ret = async_write_strong(write_key_offset + i, desired_val, val_len, session_id);
      assert(ret > 0);
    }

    // reset the flag of the previous machine
    desired_val[0] = 0;
    ret = async_write_strong(key_flags[prev_machine_id], desired_val, val_len, session_id);
    assert(ret > 0);

    // release your flag
    desired_val[0] = 1;
    //yellow_printf("Releasing key_flag %u \n", key_flags[machine_id]);
    ret = async_release_strong(key_flags[machine_id], desired_val, val_len, session_id);
    assert(ret > 0);

    // Do the actual reads
    poll_a_req_blocking(session_id, last_issued_req);
    for (i = 0; i < relaxed_writes; i++) {
      //cyan_printf("Reading key %u, iteration %u, value %u \n", read_key_offset + i, i, value[i][0]);
      assert(value[i][0] == 10 * prev_machine_id + i);
    }

  }
}

/*-------------------------------------- TREIBER STACK---------------------------------------------------------------- */
struct tr_sess_info {
  uint8_t state;
  uint8_t push_or_pull_state;
  bool success;
  bool valid_key_to_write;
  bool done_cas;
  uint16_t s_i;
  uint16_t real_sess_i;
  uint16_t glob_sess_i;
  uint16_t wrkr;
  uint32_t last_req_id;
  uint32_t stack_id;
  uint32_t owned_key;
  uint32_t pop_dbg;
  uint32_t push_dbg;
  struct top top;
  struct top new_top;
  struct top pop_top;
  struct top pop_new_top;
  struct node *new_node;
  struct node *owned_node;
};


static inline void update_file(uint16_t t_id, uint32_t key_id, struct tr_sess_info *info, bool push,
                               struct top *top, struct top * new_top)
{
  if (CLIENT_LOGS) {
    uint64_t key_hash = CityHash128((char *) &(key_id), 4).second;
    struct key key;
    memcpy(&key, &key_hash, TRUE_KEY_SIZE);

//    fprintf(client_log[t_id],
//            "%s: Stack: %u key: %u/%u, new_top key_id %u  push counter: %u pop counter: %u \n",
//            push ? "Push" : "Pull", info->stack_id, key.bkt, key_id, new_top->key_id,
//            new_top->push_counter,
//            new_top->pop_counter);
    uint16_t glob_sess = (uint16_t) (machine_id * SESSIONS_PER_MACHINE + info->real_sess_i);

//    fprintf(client_log[t_id],
//            "%u %u %s %u %u %u %u\n",
//            key.bkt, glob_sess, push ? "Push" : "Pull", info->stack_id,  new_top->push_counter,
//            new_top->pop_counter, key_id);
    fprintf(client_log[t_id],
            "%u %u \n",
            info->stack_id, new_top->pop_counter);

  }

  //fprintf(client_log[t_id], "Push: Stack: %u key: %u  push counter: %u pop counter: %u \n",
  //        info->stack_id, new_top->key_id, new_top->push_counter, new_top->pop_counter);

}




static inline void treiber_push_blocking(uint16_t session_id, uint32_t stack_id, int key_id_to_push)
{
  if (key_id_to_push < 0) {
    red_printf("Tried to push a negative key id %d \n", key_id_to_push);
    return;
  }
  uint32_t new_node_key = (uint32_t) key_id_to_push;
  uint32_t top_key_id = stack_id;
  assert(top_key_id < NUM_OF_RMW_KEYS);
  assert(key_id_to_push >= NUM_OF_RMW_KEYS);
  assert(key_id_to_push < CACHE_NUM_KEYS);
  assert(session_id < SESSIONS_PER_MACHINE);
  assert(sizeof(struct top) <= RMW_VALUE_SIZE);
  assert(sizeof(struct node) == VALUE_SIZE);

  struct top top, new_top;
  struct node new_node;
  bool success = false;
  blocking_read(top_key_id, (uint8_t *)&top, sizeof(struct top), session_id);
  //green_printf("Read top_key_id %u, points to key %u top counter %u\n",
  //              stack_id, top.key_id, top.counter);

  do {
    new_node.next_key_id = top.key_id;
    async_write_strong(new_node_key, (uint8_t *)&new_node, sizeof(struct node), session_id);
    new_top.pop_counter = top.pop_counter;
    new_top.key_id = new_node_key;
    blocking_cas(top_key_id, (uint8_t *)&top, (uint8_t *)&new_top, sizeof(struct top),
                 &success, true, session_id);
//    if (!success) {
//      printf("top.key %u, top.counter %lu success %d \n", top.key_id, top.counter, success);
//      assert(false);
//    }
  } while(!success);

//  green_printf("Session %u successfully pushed key %u to stack %u, previous top key %u top counter %u\n",
//               session_id, new_node_key, stack_id, top.key_id, top.counter);
}

static inline int treiber_pop_blocking(uint16_t session_id, uint32_t stack_id)
{
  uint32_t top_key_id = stack_id;
  assert(top_key_id < NUM_OF_RMW_KEYS);
  assert(session_id < SESSIONS_PER_MACHINE);
  assert(sizeof(struct top) <= RMW_VALUE_SIZE);
  assert(sizeof(struct node) == VALUE_SIZE);

  struct top top, new_top;
  struct node first_node;
  bool success = false;
  blocking_read(top_key_id, (uint8_t *)&top, sizeof(struct top), session_id);
  //green_printf("Read top_key_id %u, points to key %u top counter %u\n",
  //              stack_id, top.key_id, top.counter);

  do {
    if (top.key_id == 0) return - 1;
    blocking_read(top.key_id, (uint8_t *)&first_node, sizeof(struct node), session_id);
    //async_write_strong(new_node_key, (uint8_t *)&new_node, sizeof(struct node), session_id);
    new_top.pop_counter = top.pop_counter + 1;
    new_top.key_id = first_node.next_key_id;
    blocking_cas(top_key_id, (uint8_t *)&top, (uint8_t *)&new_top, sizeof(struct top),
                 &success, true, session_id);
//    if (!success) {
//      printf("top.key %u, top.counter %lu success %d \n", top.key_id, top.counter, success);
//      assert(false);
//    }
  } while(!success);


//  green_printf("Session %u successfully popped key %u from stack %u, top counter %u\n",
//               session_id, top.key_id, stack_id, top.counter);
  return top.key_id;
}

#define INIT 0
// TREIBER PULL STATES
#define READ_TOP 1
#define READ_FIRST 2
// TREIBER PUSH or PULL STATE
#define PUSHING 0
#define POPPING 1



#define NUMBER_OF_STACKS 10000
#define DEBUG_MAX 200







static inline void treiber_pop_multi_session(uint16_t t_id)
{

  assert(sizeof(struct top) <= RMW_VALUE_SIZE);
  assert(sizeof(struct node) == VALUE_SIZE);
  uint16_t s_i = 0;
  uint16_t sess_offset = (uint16_t) (t_id * SESSIONS_PER_CLIENT);
  assert(sess_offset + SESSIONS_PER_CLIENT <= SESSIONS_PER_MACHINE);
  //uint32_t stack_ids[SESSIONS_PER_MACHINE];
  struct top top[SESSIONS_PER_CLIENT], new_top[SESSIONS_PER_CLIENT];
  struct node first_node[SESSIONS_PER_CLIENT];
  bool success[SESSIONS_PER_CLIENT] = {0};
  uint8_t state[SESSIONS_PER_CLIENT] = {0};
  uint32_t last_req_id[SESSIONS_PER_CLIENT];


  uint32_t stack_id[SESSIONS_PER_CLIENT] = {0};
  uint32_t stack_id_cntr = (uint32_t) machine_id * 100;
  int ret;

  for (s_i = 0; s_i < SESSIONS_PER_CLIENT; s_i++) {
    uint16_t real_sess_i = sess_offset + s_i;
    if (ENABLE_ASSERTIONS) assert(real_sess_i < SESSIONS_PER_MACHINE);
    stack_id[s_i] = stack_id_cntr;
    MOD_ADD(stack_id_cntr, NUM_OF_RMW_KEYS);
    last_req_id[s_i] = (uint32_t)  async_read_strong(stack_id[s_i], (uint8_t *) &top[s_i],
                                                    sizeof(struct top), real_sess_i);
    state[s_i] = READ_TOP;
  }
  s_i = 0;
  while(true) {
    uint16_t real_sess_i = sess_offset + s_i;
    switch (state[s_i]) {
      case READ_TOP:
        poll_a_req_blocking(real_sess_i, last_req_id[s_i]);
        if (top[s_i].key_id == 0) {
          //printf("poping nothing \n");
          success[s_i] = true;
        }
        else {
          if (top[s_i].key_id < NUM_OF_RMW_KEYS) {
            printf("Will pop_something %u, stack_id %u \n", top[s_i].key_id, stack_id[s_i]);
            assert(false);
          }
        }
        if (success[s_i]) {
          stack_id[s_i] = stack_id_cntr;
          MOD_ADD(stack_id_cntr, NUM_OF_RMW_KEYS);
          last_req_id[s_i] = (uint32_t) async_read_strong(stack_id[s_i], (uint8_t *) &top[s_i],
                                                          sizeof(struct top), real_sess_i);
          success[s_i] = false;
          state[s_i] = READ_TOP;
          c_stats[t_id].treiber_pops++;
        }
        else {
          assert(top[s_i].key_id > 0);
          if (ENABLE_ASSERTIONS) assert(top[s_i].key_id >= NUM_OF_RMW_KEYS);
          for (uint16_t i = 0; i < TREIBER_WRITES_NUM; i++) {
            last_req_id[s_i] =
              (uint32_t) async_read_strong(top[s_i].key_id + i, (uint8_t *) &first_node[s_i],
                                           sizeof(struct node), real_sess_i);
          }
          state[s_i] = READ_FIRST;
        }
        break;
      case READ_FIRST:
        poll_a_req_blocking(real_sess_i, last_req_id[s_i]);
        new_top[s_i].pop_counter = top[s_i].pop_counter + 1;
        new_top[s_i].key_id = first_node[s_i].next_key_id;
        last_req_id[s_i] =
          (uint32_t) async_cas_strong(stack_id[s_i], (uint8_t *) &top[s_i], (uint8_t *) &new_top[s_i],
                                      sizeof(struct top), &success[s_i], true, real_sess_i);
        state[s_i] = READ_TOP;
        break;
      default: assert(false);
    }
    MOD_ADD(s_i, SESSIONS_PER_CLIENT);
  }

  //printf("Completed %u pushes in %u stacks \n", SESSIONS_PER_MACHINE, SESSIONS_PER_MACHINE);
}


static inline void treiber_push_multi_session(uint16_t t_id)
{

  assert(sizeof(struct top) <= RMW_VALUE_SIZE);
  assert(sizeof(struct node) == VALUE_SIZE);
  uint16_t s_i = 0;
  uint32_t key_offset = (uint32_t) (NUM_OF_RMW_KEYS + (NUM_OF_RMW_KEYS * machine_id));
  uint16_t sess_offset = (uint16_t) (t_id * SESSIONS_PER_CLIENT);
  assert(sess_offset + SESSIONS_PER_CLIENT <= SESSIONS_PER_MACHINE);
  //uint32_t stack_ids[SESSIONS_PER_MACHINE];
  struct top top[SESSIONS_PER_CLIENT], new_top[SESSIONS_PER_CLIENT];
  struct node new_node[SESSIONS_PER_CLIENT][TREIBER_WRITES_NUM];
  bool success[SESSIONS_PER_CLIENT] = {0};
  uint32_t last_req_id[SESSIONS_PER_CLIENT];
  uint8_t state[SESSIONS_PER_CLIENT] = {0};
  uint32_t stack_id[SESSIONS_PER_CLIENT] = {0};
  uint32_t stack_id_cntr = (uint32_t) machine_id * 100;
  //green_printf("Read top_key_id %u, points to key %u top counter %u\n",
  //              stack_id, top.key_id, top.counter);
  //uint32_t success_cntr = 0;
  for (s_i = 0; s_i < SESSIONS_PER_CLIENT; s_i++) {
    uint16_t real_sess_i = sess_offset + s_i;
    if (ENABLE_ASSERTIONS) assert(real_sess_i < SESSIONS_PER_MACHINE);
    stack_id[s_i] = stack_id_cntr;
    MOD_ADD(stack_id_cntr, NUM_OF_RMW_KEYS);
    last_req_id[s_i] = (uint32_t) async_read_strong(stack_id[s_i], (uint8_t *) &top[s_i],
                                                    sizeof(struct top), real_sess_i);
    for (uint16_t i = 1; i < TREIBER_WRITES_NUM; i++) {
      uint32_t key_to_write = key_offset + (s_i * TREIBER_WRITES_NUM) + i;
      async_write_strong(key_to_write, (uint8_t *) &new_node[s_i][i],
                         sizeof(struct node), real_sess_i);
    }
    state[s_i] = READ_TOP;
  }
  s_i = 0;
  while(true) {
    uint16_t real_sess_i = sess_offset + s_i;
    switch (state[s_i]) {
      case READ_TOP:
        if (ENABLE_ASSERTIONS) assert(real_sess_i < SESSIONS_PER_MACHINE);
        poll_a_req_blocking(real_sess_i, last_req_id[s_i]);
        if (top[s_i].key_id != 0 && top[s_i].key_id < NUM_OF_RMW_KEYS) {
          printf("Stack id %u points to key %u \n", stack_id[s_i], top[s_i].key_id);
          assert(false);
        }
        if (success[s_i]) {
          stack_id[s_i] = stack_id_cntr;
          MOD_ADD(stack_id_cntr, NUM_OF_RMW_KEYS);
          last_req_id[s_i] = (uint32_t) async_read_strong(stack_id[s_i], (uint8_t *) &top[s_i],
                                                          sizeof(struct top), real_sess_i);
          for (uint16_t i = 1; i < TREIBER_WRITES_NUM; i++) {
            uint32_t key_to_write = key_offset + (s_i * TREIBER_WRITES_NUM) + i;
            async_write_strong(key_to_write, (uint8_t *) &new_node[s_i][i],
                               sizeof(struct node), real_sess_i);
          }
          success[s_i] = false;
          c_stats[t_id].treiber_pushes++;
          break;
        }

        // Do only one write here: the one that needs to point to what top used to point
        // the rest of the writes need not happen in the conflict path, and need not wait
        // for the previous read to compelte
        new_node[s_i][0].next_key_id = top[s_i].key_id;
        uint32_t key_to_write = key_offset + s_i;
        async_write_strong(key_to_write, (uint8_t *) &new_node[s_i][0],
                           sizeof(struct node), real_sess_i);


        new_top[s_i].pop_counter = top[s_i].pop_counter;
        new_top[s_i].key_id = key_to_write;
        assert(new_top[s_i].key_id >= NUM_OF_RMW_KEYS);
        last_req_id[s_i] = (uint32_t) async_cas_strong(stack_id[s_i], (uint8_t *) &top[s_i], (uint8_t *) &new_top[s_i],
                                                       sizeof(struct top), &success[s_i], true, real_sess_i);
        break;
      default: assert(false);
    }
    MOD_ADD(s_i, SESSIONS_PER_CLIENT);
  }

}

static inline void treiber_pop_state_machine(struct tr_sess_info *info,
                                             uint32_t* stack_id_cntr,
                                             uint16_t t_id)
{
  struct top *top = &info->pop_top;
  struct top *new_top = &info->pop_new_top;
  struct node *first_node = info->new_node;
  uint16_t real_sess_i = info->real_sess_i;
  uint16_t s_i = info->s_i;
  if (ENABLE_ASSERTIONS) assert(real_sess_i < SESSIONS_PER_MACHINE);
  uint32_t key_to_write;
  //printf("Session %u %p %p \n", s_i, info, top);

  //green_printf("Session %u state %u, top key_id %u\n", s_i, info->state, top->key_id);
  switch (info->state) {
    case INIT:
      assert(!info->success);
      info->last_req_id = (uint32_t)  async_read_strong(info->stack_id, (uint8_t *) top,
                                                       sizeof(struct top), real_sess_i);
      info->state = READ_TOP;
      break;
    case READ_TOP:
      poll_a_req_blocking(real_sess_i, info->last_req_id);
      check_top(top, "Pop-after reading top ", info->stack_id);
      if (top->key_id == 0) {
        printf("Popping from empty stack %u success %d push counter/pop counter %u/%u\n",
               info->stack_id, info->success, top->push_counter, top->pop_counter);

        info->success = true;
      }
      if (!info->success) { //either CAS failed, or has not been attempted yet
        if (ENABLE_ASSERTIONS) assert(top->key_id >= NUM_OF_RMW_KEYS);
        assert(top->key_id <= MAX_TR_NODE_KEY);
        for (uint16_t i = 0; i < TREIBER_WRITES_NUM; i++) {
          info->last_req_id =
            //(uint32_t) async_acquire_strong(top->key_id + i, (uint8_t *) &first_node[i],
            //                                sizeof(struct node), real_sess_i);
            (uint32_t) async_read_strong(top->key_id + i, (uint8_t *) &first_node[i],
                                         sizeof(struct node), real_sess_i);
        }
        info->state = READ_FIRST;
      }
      else if (info->success) {
        //assert(first_node[0].pushed);
        uint32_t dbg_count = 0;
        while  (first_node[0].stack_id != (uint16_t) info->stack_id) {
          if (dbg_count == 0) {
            struct key key = create_key(info->stack_id);
            yellow_printf("Sess %u/%u,  Popping stack %u -- Read node %u stack id %u, pushed %u, owner %u,"
                            "bkt %u  key id %u push/pop %u/%u \n",
                          info->glob_sess_i, t_id, info->stack_id, top->key_id,
                          first_node[0].stack_id, first_node[0].pushed, first_node[0].owner, key.bkt,
            new_top->key_id, new_top->push_counter, new_top->pop_counter);
          }
          blocking_read(top->key_id, (uint8_t *) &first_node[0],
                            sizeof(struct node), real_sess_i);
          MOD_ADD(dbg_count, M_4);
          if (first_node[0].stack_id == (uint16_t) info->stack_id)
            green_printf("Repeat: Sess %u Popping stack %u -- Read node %u stack id %u \n",
                        info->glob_sess_i,
                        info->stack_id, top->key_id, first_node[0].stack_id);
          //assert(false);
        }

        if (top->sec_key_id != 1) {
          while (top->sec_key_id != first_node[0].next_key_id) {
            struct key key = create_key(info->stack_id);
            if (dbg_count == 0)
              printf("sess %u/%u, after popping stack %u/%u (bkt %u ), "
                       "old top %u/%u/%u, new_top %u/%u/%u"
                       " got key %u/%u "
                       "left first node: key %u/%d/%u"
                       " -- it should have been  %u/%d/%u \n",
                info->glob_sess_i, t_id, info->stack_id, first_node[0].stack_id, key.bkt,
                     top->key_id, top->push_counter, top->pop_counter,
                     new_top->key_id, new_top->push_counter, new_top->pop_counter,
                     top->key_id, create_key(top->key_id).bkt,
                     first_node[0].next_key_id, first_node[0].pushed, first_node[0].push_counter,
                     top->sec_key_id, true, top->push_counter);
            blocking_read(top->key_id, (uint8_t *) &first_node[0],
                          sizeof(struct node), real_sess_i);
            if (top->sec_key_id == first_node[0].next_key_id)
              green_printf("sess %u/%u, Read key becomes correct %u/%u/%u/%u \n",
                           info->glob_sess_i, t_id, first_node[0].next_key_id,
                           first_node[0].pushed, first_node[0].stack_id, first_node[0].push_counter);
            MOD_ADD(dbg_count, M_4);
          }

        }

        info->owned_key = top->key_id;
        first_node[0].pushed = false;
        first_node[0].stack_id = (uint16_t) (NUMBER_OF_STACKS + info->stack_id);
        first_node[0].owner = info->glob_sess_i;
        async_write_strong(info->owned_key, (uint8_t *) &first_node[0],
                           sizeof(struct node), real_sess_i);


        check_top(new_top, "Pop-new_top on success ", info->stack_id);
//        cyan_printf("Session %u Popped key %u from stack %u pushed/pulled %u/%u \n",
//                    info->real_sess_i, top->key_id, info->stack_id, new_top->push_counter, new_top->pop_counter);
        update_file(t_id, top->key_id, info, false, top, new_top);
        info->success = false;
        info->stack_id = (top->key_id + new_top->push_counter + (uint32_t)(&info->stack_id) +
          (uint32_t)(c_stats[t_id].treiber_pops * c_stats[t_id].treiber_pushes) ) % NUMBER_OF_STACKS;
        c_stats[t_id].treiber_pops++;

        info->valid_key_to_write = true;
        // Transition to pushing
        info->push_or_pull_state = PUSHING;
        info->state = INIT;
      }
      break;
    case READ_FIRST:
      poll_a_req_blocking(real_sess_i, info->last_req_id);
      new_top->pop_counter = top->pop_counter + 1;
      new_top->push_counter = top->push_counter;
      new_top->key_id = first_node[0].next_key_id;
      new_top->sec_key_id = top->third_key_id;
      new_top->third_key_id = top->fourth_key_id; // we concede that we do not know that
      new_top->fourth_key_id = 1;
      //assert(first_node[0].stack_id = (uint16_t) info->stack_id);
      //assert(first_node[0].push_counter <= top->push_counter);


      check_top(top, "Pop-already checked ", info->stack_id);
      if (!check_top(new_top, "Pop-new_top before CAS ", info->stack_id)) {
        //cyan_printf("Before popping stack %u, new_top key/top key %u/%u, new pushed/pulled %u/%u\n",
        //            info->stack_id, new_top->key_id, top->key_id, new_top->push_counter, new_top->pop_counter);
        if (info->pop_dbg <= DEBUG_MAX) info->pop_dbg++;
        if (info->pop_dbg == DEBUG_MAX) {
          printf("Sess %u stack when popping for stack %u, push/pop %u/%u next key id %u, current key_id %u \n",
                 info->real_sess_i, info->stack_id, new_top->push_counter,
                 new_top->pop_counter, new_top->key_id, top->key_id);
          //info->pop_dbg = 0;
        }
        info->state = INIT;
        break;
        // TOP MUST HAVE BEEN UPDATED FOR THAT KEY TO BE ZERO
      }
      if (info->pop_dbg >= DEBUG_MAX) {
        green_printf("Sess %u Unstack when popping for stack %u, push/pop %u/%u next key id %u, current key_id %u \n",
                     info->real_sess_i, info->stack_id, new_top->push_counter,
                     new_top->pop_counter, new_top->key_id, top->key_id);
      }
      info->pop_dbg = 0;
      //check_top(new_top, "Pop-new_top before CAS ", info->stack_id);
      info->last_req_id =
        (uint32_t) async_cas_strong(info->stack_id, (uint8_t *) top, (uint8_t *) new_top,
                                    sizeof(struct top), &info->success, true, real_sess_i);
      info->state = READ_TOP;
      break;
    default: assert(false);
  }
}


static inline void treiber_push_state_machine(struct tr_sess_info *info,
                                              uint32_t* stack_id_cntr, uint16_t t_id)
{
  struct top *top = &info->top;
  struct top *new_top = &info->new_top;
  struct node *new_node = info->new_node;
  uint16_t real_sess_i = info->real_sess_i;
  uint16_t s_i = info->s_i;
  if (ENABLE_ASSERTIONS) assert(real_sess_i < SESSIONS_PER_MACHINE);
  //uint32_t key_to_write;
  //printf("Session %u %p %p \n", s_i, info, top);

  //green_printf("Session %u state %u, top key_id %u\n", s_i, info->state, top->key_id);
  switch (info->state) {
    case INIT: // INITIAL phase of a push: come here in the first push ever, and after every successful push
      //MOD_ADD(*stack_id_cntr, NUMBER_OF_STACKS);
      info->done_cas = false;
      assert(!info->success);
      async_read_strong(info->owned_key, (uint8_t *) info->owned_node,
                        sizeof(struct node), real_sess_i);
      info->last_req_id = (uint32_t) async_read_strong(info->stack_id, (uint8_t *) top,
                                                       sizeof(struct top), real_sess_i);

      if (ENABLE_ASSERTIONS) t_stats[info->wrkr].writes_asked_by_clients+= (TREIBER_WRITES_NUM - 1);
//      if (info->wrkr == 13)
//        yellow_printf("Clt %u have Asked worker %u for %lu writes \n",
//                      t_id, info->wrkr, t_stats[info->wrkr].writes_asked_by_clients);
      for (uint16_t i = 1; i < TREIBER_WRITES_NUM; i++) {
        assert(false);

        async_write_strong(info->owned_key + i, (uint8_t *) &new_node[i],
                           sizeof(struct node), real_sess_i);
      }
      info->state = READ_TOP;
      break;
    case READ_TOP:
      poll_a_req_blocking(real_sess_i, info->last_req_id);
      if (!info->done_cas) {
        if (memcmp(info->owned_node, &new_node[0], sizeof(struct node)) != 0) {
          printf("Sess %u, On pushing checking owned key %u, stack_ids %u/%u "
                   "owned/new key id %u/%u, owner %u/%u/ pushed %u/%u \n", info->glob_sess_i,
                 info->owned_key, info->owned_node->stack_id, new_node[0].stack_id,
                 info->owned_node->next_key_id, new_node[0].next_key_id,
                  info->owned_node->owner, new_node[0].owner,
                 info->owned_node->pushed, new_node[0].pushed);
          //assert(false);
        }
      }
      assert(check_top(top, "Push-after reading top ", info->stack_id));
      if (!info->success) {
        // Do only one write here: the one that needs to point to what top used to point
        // the rest of the writes need not happen in the conflict path, and need not wait
        // for the previous read to complete
        if (info->done_cas) {
          uint32_t dbg_count = 0;
          while ( new_top->push_counter - 1 == top->push_counter &&
            new_top->pop_counter == top->pop_counter) {
            if (dbg_count == 0) {
              red_printf("Sess %u/%u Stack_id %u, CAS failed, top %u/%u/%u, prev top %u/%u/%u \n",
            info->glob_sess_i, t_id, info->stack_id, top->key_id, top->push_counter, top->pop_counter,
                       new_node[0].next_key_id, new_top->push_counter - 1, new_top->pop_counter);
            }

            blocking_read(info->stack_id, (uint8_t *) top,
                          sizeof(struct top), real_sess_i);

            if (dbg_count == 0) {
              red_printf("Sess %u/%u Stack_id %u, CAS failed-blocking read, top %u/%u/%u, prev top %u/%u/%u \n",
                         info->glob_sess_i, t_id, info->stack_id, top->key_id, top->push_counter, top->pop_counter,
                         new_node[0].next_key_id, new_top->push_counter - 1, new_top->pop_counter);
            }

            MOD_ADD(dbg_count, M_4);
          }
        }

        new_node[0].next_key_id = top->key_id;
        new_node[0].owner = info->glob_sess_i;
        new_node[0].stack_id = (uint16_t) info->stack_id;
        new_node[0].push_counter = top->push_counter + 1;
        new_node[0].pushed = true;
        new_node[0].key_id = info->owned_key;

        assert(info->valid_key_to_write);
        new_top->pop_counter = top->pop_counter;
        new_top->push_counter = top->push_counter + 1;
        new_top->key_id = info->owned_key;
        new_top->sec_key_id = top->key_id;
        new_top->third_key_id = top->sec_key_id;
        new_top->fourth_key_id = top->third_key_id;
        assert(new_top->key_id >= NUM_OF_RMW_KEYS);
        assert(info->owned_key  <= MAX_TR_NODE_KEY);
        //check_top(new_top, "Push-new_top before CAS ", info->stack_id);
//        if (!check_top(new_top, "before pushing", info->stack_id)) {
//          info->push_dbg++;
//          if (info->push_dbg == DEBUG_MAX) {
//            printf("Sess %u stack when pushing for stack %u, push/pop %u/%u next key id %u current key_id %u \n",
//                   info->real_sess_i, info->stack_id, new_top->push_counter,
//                   new_top->pop_counter, new_top->key_id, top->key_id);
//            info->push_dbg = 0;
//          }
//          info->state = INIT;
//          break;
//        }
        check_node((uint8_t *)&new_node[0], "client before pushing", info->stack_id);
        //printf("Sess %u Trying to push %u  to stack %u \n", real_sess_i, key_to_write, info->stack_id);

        if (ENABLE_ASSERTIONS) t_stats[info->wrkr].writes_asked_by_clients++;

        async_write_strong(info->owned_key, (uint8_t *) &new_node[0],
                           sizeof(struct node), real_sess_i);
        //async_release_strong(MAX_TR_NODE_KEY + info->glob_sess_i, (uint8_t *) &new_node[0],
         //                    sizeof(struct node), real_sess_i);
        info->last_req_id = (uint32_t) async_cas_strong(info->stack_id, (uint8_t *) top, (uint8_t *) new_top,
                                                        sizeof(struct top), &info->success, true, real_sess_i);
        assert(info->last_req_id > 0);
        info->done_cas = true;
      }
      else if (info->success) { // If push succeeded, do a pull
//        yellow_printf("Session %u Pushed key %u to stack %u : %u/%u \n",
//                      info->real_sess_i, new_top->key_id, info->stack_id,
//                      new_top->push_counter, new_top->pop_counter);
        info->valid_key_to_write = false;
        check_top(new_top, "Push-new_top after success ", info->stack_id);
        //update_file(t_id, info->key_to_write, info, true, top, new_top);

        info->success = false;
        c_stats[t_id].treiber_pushes++;
        assert(c_stats[t_id].treiber_pushes > c_stats[t_id].treiber_pops);
        info->push_or_pull_state = POPPING;
        info->state = INIT;
      }
      break;
    default: assert(false);
  }
}


// Each session picks a stack, pushes one element and then pops one element
static inline void treiber_push_pull_multi_session(uint16_t t_id)
{

  assert(sizeof(struct top) <= RMW_VALUE_SIZE);
  assert(sizeof(struct node) == VALUE_SIZE);
  assert(TREIBER_WRITES_NUM > 0);
  uint16_t s_i = 0;
  uint32_t key_offset = (uint32_t) (NUM_OF_RMW_KEYS + (SESSIONS_PER_MACHINE * machine_id));
  uint16_t sess_offset = (uint16_t) (t_id * SESSIONS_PER_CLIENT);
  assert(sess_offset + SESSIONS_PER_CLIENT <= SESSIONS_PER_MACHINE);
  uint32_t stack_id_cntr = 0;//(uint32_t) machine_id * 100;
  struct tr_sess_info *info = calloc(SESSIONS_PER_CLIENT, sizeof(struct tr_sess_info));
  uint32_t first_key = key_offset + sess_offset * TREIBER_WRITES_NUM;
  uint32_t last_key = key_offset + (sess_offset + (SESSIONS_PER_CLIENT -1)) * TREIBER_WRITES_NUM;
  uint16_t min_sess = sess_offset, max_sess = (uint16_t) (sess_offset + (SESSIONS_PER_CLIENT -1));
  uint16_t min_wrkr = (uint16_t) (min_sess / SESSIONS_PER_THREAD),
    max_wrkr = (uint16_t) (max_sess / SESSIONS_PER_THREAD);
  printf("Client %u: sessions %u to %u, workers %u to %u, keys %u to %u, globally max key %u \n",
         t_id, min_sess, max_sess, min_wrkr, max_wrkr,
         first_key, last_key, MAX_TR_NODE_KEY);

  for (s_i = 0; s_i < SESSIONS_PER_CLIENT; s_i++) {
    info[s_i].state = INIT;
    info[s_i].push_or_pull_state = PUSHING;
    info[s_i].s_i = s_i;
    info[s_i].real_sess_i = sess_offset + s_i;
    info[s_i].glob_sess_i = (uint16_t) (SESSIONS_PER_MACHINE * machine_id + info[s_i].real_sess_i);
    info[s_i].owned_key = key_offset + info[s_i].real_sess_i * TREIBER_WRITES_NUM;
    //printf("Session %u key %u \n", info[s_i].real_sess_i, info[s_i].key_to_write);
    if (ENABLE_ASSERTIONS) assert(info[s_i].real_sess_i < SESSIONS_PER_MACHINE);
    info[s_i].new_node = calloc(TREIBER_WRITES_NUM, (sizeof(struct node)));
    info[s_i].new_node[0].next_key_id = 0;
    info[s_i].stack_id =  (info[s_i].owned_key - NUM_OF_RMW_KEYS) % NUMBER_OF_STACKS;
    assert(info[s_i].stack_id < NUMBER_OF_STACKS);
    info[s_i].valid_key_to_write = true;
    info[s_i].wrkr = (uint16_t) (info[s_i].real_sess_i / SESSIONS_PER_THREAD);
    info[s_i].owned_node = calloc(1,(sizeof(struct node)));
  }
  s_i = 0;
  while(true) {
    switch (info[s_i].push_or_pull_state) {
      case PUSHING:
        treiber_push_state_machine(&info[s_i], &stack_id_cntr, t_id);
        break;
      case POPPING:
        treiber_pop_state_machine(&info[s_i], &stack_id_cntr, t_id);
        break;
      default: if (ENABLE_ASSERTIONS) assert(false);
    }
    MOD_ADD(s_i, SESSIONS_PER_CLIENT);
  }

}


/*------------------------------M&S QUEUE---------------------------------------------------*/
#define MS_NODE_SIZE (VALUE_SIZE - 5)

#define MS_QUEUES_NUM 5000
// all head/tail ptrs point in 0 when starting
// the dummy node has no body, it's just a ptr
// there is 1 Dummy per Queue and the dummy keys range from
// NUM_OF_RMW_KEYS to NUM_OF_RMW_KEYS + MS_QUEUES_NUM
// (Dummy nodes don't need to be RMWed)
#define DUMMY_KEY_ID_OFFSET (NUM_OF_RMW_KEYS)
#define TAIL_KEY_ID_OFFSET 0
#define HEAD_KEY_ID_OFFSET MS_QUEUES_NUM

struct ms_ptr {
  uint32_t key_id;
  uint64_t counter;
};
struct ms_node {
  uint8_t value[MS_NODE_SIZE];
  uint8_t node_id;
  uint32_t node_ptr_key_id;
};

//MS_STATES

static inline void ms_set_up_tail_and_head(uint16_t t_id)
{
  assert(NUM_OF_RMW_KEYS > 2 * MS_QUEUES_NUM);
  struct ms_ptr tail, head;
  tail.counter = 1;
  head.counter = 1;
  uint16_t sess_offset = (uint16_t) (t_id * SESSIONS_PER_CLIENT);
  uint16_t s_i = 0;
  uint32_t dummy_key = DUMMY_KEY_ID_OFFSET,
    tail_key_id = TAIL_KEY_ID_OFFSET,
    head_key_id = HEAD_KEY_ID_OFFSET;
  printf("Client %u uses sessions from %u to %u \n", t_id, sess_offset,  sess_offset + SESSIONS_PER_CLIENT -1);
  for(uint32_t q_i = 0; q_i < MS_QUEUES_NUM; q_i++) {
    tail.key_id = dummy_key;
    head.key_id = dummy_key;
    uint16_t real_sess_i = sess_offset + s_i;
    //printf("pushing a req for key %u \n", dummy_key);
    async_write_strong(dummy_key, (uint8_t *) &tail,
                       sizeof(struct ms_ptr), real_sess_i);

    dummy_key++; tail_key_id++; head_key_id++;
    MOD_ADD(s_i, SESSIONS_PER_CLIENT);
  }
  printf("CLient %u initialiazed %u queues \n", t_id, MS_QUEUES_NUM);

}

//
static inline void ms_enqueue_blocking(uint16_t t_id)
{
  assert(sizeof(struct ms_ptr) <= RMW_VALUE_SIZE);
  assert(sizeof(struct ms_node) == VALUE_SIZE);
  uint16_t s_i = 0;
  uint32_t key_offset = (uint32_t) (NUM_OF_RMW_KEYS + (NUM_OF_RMW_KEYS * machine_id));
  uint16_t sess_offset = (uint16_t) (t_id * SESSIONS_PER_CLIENT);
  assert(sess_offset + SESSIONS_PER_CLIENT <= SESSIONS_PER_MACHINE);
  uint32_t last_req_id[SESSIONS_PER_CLIENT];
  uint8_t state[SESSIONS_PER_CLIENT] = {0};

  assert(NUM_OF_RMW_KEYS > 2 * MS_QUEUES_NUM);
  struct ms_ptr tail[SESSIONS_PER_CLIENT], new_tail[SESSIONS_PER_CLIENT];
  struct ms_node new_node[SESSIONS_PER_CLIENT][MS_WRITES_NUM];
  struct ms_node last_node[SESSIONS_PER_CLIENT];
  bool success[SESSIONS_PER_CLIENT] = {0};

  for (s_i = 0; s_i < SESSIONS_PER_CLIENT; s_i++) {
    for (uint8_t i =0; i < MS_WRITES_NUM; i++) {
      new_node[s_i][i].node_id = i;
    }
  }

  while(true);
  uint32_t queue_id[SESSIONS_PER_CLIENT] = {0}; // Which queue is the session working on
  uint32_t q_id_cntr = (uint32_t) machine_id * 100;

  // Start the enqueuing
  s_i = 0;
  uint16_t real_sess_i = sess_offset + s_i;
  uint32_t ptr_key = MS_QUEUES_NUM;
  new_node[s_i][0].node_ptr_key_id = ptr_key;
  for (uint8_t i =0; i < MS_WRITES_NUM; i++) {
    uint32_t key_to_write = key_offset + (s_i * MS_WRITES_NUM) + i;
    async_write_strong(key_to_write, (uint8_t *)&new_node[s_i][i],
                       sizeof(struct ms_node), real_sess_i);
  }
  while(true) {
    last_req_id[s_i] =
      (uint32_t) async_read_strong(queue_id[s_i], (uint8_t *) &tail[s_i],
                                   sizeof(struct top), real_sess_i);

    printf("S_i %u reading on pull ptr %u \n", s_i, last_req_id[s_i]);
    poll_a_req_blocking(real_sess_i, last_req_id[s_i]);
    printf("polled a req \n");
    async_read_strong(tail[s_i].key_id, (uint8_t *) &last_node[s_i],
                      sizeof(struct top), real_sess_i);

  }
}

/* --------------------------------------------------------------------------------------
 * ----------------------------------CLIENT THREAD----------------------------------------
 * --------------------------------------------------------------------------------------*/

void *client(void *arg) {
  //uint32_t i = 0, j = 0;
  struct thread_params params = *(struct thread_params *) arg;
  uint16_t t_id = (uint16_t) params.id;

  const uint16_t worker_num = (uint16_t)(WORKERS_PER_MACHINE / CLIENTS_PER_MACHINE_);

  uint16_t first_worker = worker_num * t_id;
  uint16_t last_worker = (uint16_t) (first_worker + worker_num - 1);
  uint32_t trace_ptr = 0;
  bool done = false;
  struct trace_command *trace;
  if (CLIENT_USE_TRACE)  trace_init((void **)&trace, t_id);
  uint32_t dbg_cntr = 0;
  green_printf("Client %u reached loop \n", t_id);
  //sleep(10);
  while (true) {
    if (CLIENT_USE_TRACE)
      trace_ptr = send_reqs_from_trace(worker_num, first_worker, trace,  trace_ptr, &dbg_cntr,  t_id);
    else if (CLIENT_UI) {
      user_interface();
    }
    else if (CLIENT_TEST_CASES) {
      if (BLOCKING_TEST_CASE) rel_acq_circular_blocking();
      else if (ASYNC_TEST_CASE) rel_acq_circular_async();
      else if (TREIBER_BLOCKING) {
          uint32_t key_offset = (uint32_t) (NUM_OF_RMW_KEYS + (NUM_OF_RMW_KEYS * machine_id));
          uint32_t stack_id =  0;//machine_id;
          for (int i = 0; i < 10; i++) {
            treiber_push_blocking(0, stack_id, key_offset + i);
          }
        while (true) {
          for (int i = 0; i < NUM_OF_RMW_KEYS; i++) {
            uint32_t key_to_push = (uint32_t) treiber_pop_blocking(0, stack_id);
            treiber_push_blocking(0, stack_id, key_to_push);
          }
        }
      }
      else if (TREIBER_ASYNC) {
        treiber_push_pull_multi_session(t_id);
//        while (true) {
//
//          if (t_id % 1 == 0)
//            treiber_push_multi_session(t_id);
//          else {
//            sleep(5);
//            treiber_pop_multi_session(t_id);
//          }
//          //c_stats[t_id].treiber_pushes += (uint64_t)SESSIONS_PER_CLIENT;
//        }
      }
      else if (MSQ_ASYNC) {
        ms_set_up_tail_and_head(t_id);
        ms_enqueue_blocking(t_id);
      }
    }
    else assert(false);
  } // while(true) loop
}
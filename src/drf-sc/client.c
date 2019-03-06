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
#define ERROR_NO_SLOTS_FOR_BLOCKING_FUNCTION (-1)
#define ERROR_PUSH_PULL_PTR_MUST_MATCH (-2)
#define ERROR_SESSION_T0O_BIG (-3)
#define ERROR_KEY_IS_NOT_RMWABLE (-4)
#define ERROR_KEY_IS_RMWABLE (-5)
#define ERROR_NULL_READ_VALUE_PTR (-6)
#define ERROR_NULL_WRITE_VALUE_PTR (-7)
#define ERROR_WRONG_REQ_TYPE (-8)

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
        atomic_store_explicit(&interface[wrkr].req_array[s_i][push_ptr].state, ACTIVE_REQ, memory_order_release);
        MOD_ADD(interface[wrkr].clt_push_ptr[s_i], PER_SESSION_REQ_NUM);
        trace_ptr++;
        if (trace[trace_ptr].opcode == NOP) trace_ptr = 0;
      }
    }
  }
  *dbg_cntr_ = dbg_cntr;
  return trace_ptr;
}

//
static inline int check_inputs(uint16_t session_id, uint32_t key_id, uint8_t * value_to_read,
                               uint8_t * value_to_write,  uint8_t opcode) {
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
 return 1;
}


//
static inline void fill_client_op(struct client_op *cl_op, uint32_t key_id, uint8_t type,
                                  uint8_t *value_to_read, uint8_t *value_to_write, bool weak)

{
  uint8_t  *expected_val = value_to_read, *desired_val = value_to_write;
  switch (type) {
    case RLXD_READ_BLOCKING:
      cl_op->opcode = (uint8_t) CACHE_OP_GET;
      break;
    case ACQUIRE_BLOCKING:
      cl_op->opcode = (uint8_t) OP_ACQUIRE;
      break;
    case RLXD_WRITE_BLOCKING:
      cl_op->opcode = (uint8_t) (key_id >= NUM_OF_RMW_KEYS ? CACHE_OP_PUT : RMW_PLAIN_WRITE);
      memcpy(cl_op->value_to_write, value_to_write, VALUE_SIZE);
      break;
    case RELEASE_BLOCKING:
      cl_op->opcode = (uint8_t) (key_id >= NUM_OF_RMW_KEYS ? OP_RELEASE : RMW_PLAIN_WRITE);
      memcpy(cl_op->value_to_write, value_to_write, VALUE_SIZE);
      break;
    case CAS_BLOCKING:
      cl_op->opcode = (uint8_t) (weak ? COMPARE_AND_SWAP_WEAK : COMPARE_AND_SWAP_STRONG);
      memcpy(cl_op->value_to_write, desired_val, (size_t) RMW_VALUE_SIZE);
      memcpy(cl_op->value_to_read, expected_val, (size_t) RMW_VALUE_SIZE);
      break;
    case FAA_BLOCKING:
      cl_op->opcode = (uint8_t) FETCH_AND_ADD;
      memcpy(cl_op->value_to_read, value_to_read, (size_t) RMW_VALUE_SIZE);
      memcpy(cl_op->value_to_write, value_to_write, (size_t) RMW_VALUE_SIZE);
      break;
    default : assert(false);
  }
  uint64_t key_hash = CityHash128((char *) &(key_id), 4).second;
  memcpy(&cl_op->key, &key_hash, TRUE_KEY_SIZE);
}

// fill the replies
static inline void fill_return_values(struct client_op *cl_op, uint8_t type, uint8_t *value_to_read,
                                 bool *cas_result)
{
  uint8_t  *expected_val = value_to_read;
  switch (type) {
    case RLXD_READ_BLOCKING:
    case ACQUIRE_BLOCKING:
      memcpy(value_to_read, cl_op->value_to_read, VALUE_SIZE);
      break;
    case RLXD_WRITE_BLOCKING:
    case RELEASE_BLOCKING:
      // nothing to do
      break;
    case CAS_BLOCKING:
      *cas_result = cl_op->rmw_is_successful;
      if (!cl_op->rmw_is_successful)
        memcpy(expected_val, cl_op->value_to_read, (size_t) RMW_VALUE_SIZE);
      break;
    case FAA_BLOCKING:
      memcpy(value_to_read, cl_op->value_to_read, (size_t) RMW_VALUE_SIZE);
      break;
    default : assert(false);
  }
}


//
static inline int access_blocking(uint32_t key_id, uint8_t *value_to_read,
                                  uint8_t *value_to_write, bool *cas_result, bool rmw_is_weak,
                                  uint16_t session_id, uint8_t type)
{
  int return_int = check_inputs(session_id, key_id, value_to_read, value_to_write, type);
  if (return_int < 0) return return_int;
  uint16_t wrkr = (uint16_t) (session_id / SESSIONS_PER_THREAD);
  uint16_t s_i = (uint16_t) (session_id % SESSIONS_PER_THREAD);
  uint16_t push_ptr = interface[wrkr].clt_push_ptr[s_i];

  if (interface[wrkr].clt_push_ptr[s_i] != interface[wrkr].clt_pull_ptr[s_i])
    return ERROR_PUSH_PULL_PTR_MUST_MATCH;

  // let's poll for the slot first
  if (interface[wrkr].req_array[s_i][push_ptr].state != INVALID_REQ) {
    printf("Deadlock: calling a blocking function, without slots \n");
    return ERROR_NO_SLOTS_FOR_BLOCKING_FUNCTION;
    //assert(false);
  }
  // Issuing the request
  struct client_op *cl_op = &interface[wrkr].req_array[s_i][push_ptr];
  fill_client_op(cl_op, key_id, type, value_to_read, value_to_write, rmw_is_weak);

  atomic_store_explicit(&cl_op->state, ACTIVE_REQ, memory_order_release);
  MOD_ADD(interface[wrkr].clt_push_ptr[s_i], PER_SESSION_REQ_NUM);

  // Polling for completion
  uint16_t pull_ptr = interface[wrkr].clt_pull_ptr[s_i];
  cl_op = &interface[wrkr].req_array[s_i][pull_ptr]; // this is the same as above
  while (cl_op->state != COMPLETED_REQ);
  atomic_store_explicit(&cl_op->state, INVALID_REQ, memory_order_relaxed);

  fill_return_values(cl_op, type, value_to_read, cas_result);
  MOD_ADD(interface[wrkr].clt_pull_ptr[s_i], PER_SESSION_REQ_NUM);
  return return_int;
}


/* --------------------------------------------------------------------------------------
 * ----------------------------------BLOCKING API----------------------------------------
 * --------------------------------------------------------------------------------------*/

//
static inline int blocking_read(uint32_t key_id, uint8_t *value_to_read,
                                uint16_t session_id)
{
  return access_blocking((uint32_t) key_id, value_to_read, NULL, NULL, false, session_id, RLXD_READ_BLOCKING);
}

//
static inline int blocking_write(uint32_t key_id, uint8_t *value_to_write,
                                uint16_t session_id)
{

  return access_blocking((uint32_t) key_id, NULL, value_to_write, NULL, false, session_id, RLXD_WRITE_BLOCKING);
}

//
static inline int blocking_acquire(uint32_t key_id, uint8_t *value_to_read,
                                   uint16_t session_id)
{
  return access_blocking((uint32_t) key_id, value_to_read, NULL, NULL, false, session_id, ACQUIRE_BLOCKING);
}

//
static inline int blocking_release(uint32_t key_id, uint8_t *value_to_write,
                                   uint16_t session_id)
{

  return access_blocking((uint32_t) key_id, NULL, value_to_write, NULL, false, session_id, RELEASE_BLOCKING);
}

//
static inline int blocking_cas(uint32_t key_id, uint8_t *expected_val,
                               uint8_t *desired_val, bool *cas_result, bool rmw_is_weak,
                               uint16_t session_id)
{
  return access_blocking((uint32_t) key_id, expected_val, desired_val, cas_result, rmw_is_weak, session_id, CAS_BLOCKING);
}

//
static inline int blocking_faa(uint32_t key_id, uint8_t *value_to_read,
                               uint8_t *argument_val, uint16_t session_id)
{
  return access_blocking((uint32_t) key_id, value_to_read, argument_val, NULL, false, session_id, FAA_BLOCKING);
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
      int ret = blocking_read((uint32_t) key_id, value, session_id);
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
      ret = blocking_write((uint32_t) key_id, value, session_id);
      green_printf("Return code %d \n", ret);
      assert(ret > 0);
      break;
    case ACQUIRE_BLOCKING:
      do {
        yellow_printf("ACQUIRE: Please input key_id \n");
      } while (scanf("%d", &key_id) != 1);
      assert(key_id >= 0);
      printf ("key_id %u \n", key_id);
      ret = blocking_acquire((uint32_t) key_id, value, session_id);
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
      ret = blocking_release((uint32_t) key_id, value, session_id);
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
      ret = blocking_cas((uint32_t) key_id, expected_val, desired_val, &cas_result, true, session_id);
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
      ret = blocking_faa((uint32_t) key_id, value, desired_val, session_id);
      green_printf("Return code %d, value read: %u\n",
                   ret, value[0], desired_val[0]);
      assert(ret > 0);
      break;
    default:
      red_printf("No request corresponds to input number %d \n", req_no);
      assert(false);

  }
}



/* --------------------------------------------------------------------------------------
 * ----------------------------------CLIENT THREAD----------------------------------------
 * --------------------------------------------------------------------------------------*/

void *client(void *arg) {
  //uint32_t i = 0, j = 0;
  struct thread_params params = *(struct thread_params *) arg;
  uint16_t t_id = (uint16_t) params.id;

  const uint16_t worker_num = (uint16_t)(WORKERS_PER_MACHINE / CLIENTS_PER_MACHINE);

  uint16_t first_worker = worker_num * t_id;
  uint16_t last_worker = (uint16_t) (first_worker + worker_num - 1);
  uint32_t trace_ptr = 0;

  struct trace_command *trace;
  trace_init((void **)&trace, t_id);
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

      // machine 0 kicks things off
      if (machine_id == 0) {
          for (i = 0; i < relaxed_writes; i++) {
            desired_val[0] = (uint8_t) (10 * machine_id + i);
            blocking_write(key_offset + i, desired_val, session_id);
            yellow_printf("Writing key %u, iteration %u, value %u \n", key_offset + i, i, desired_val[0]);
          }
        desired_val[0] = 1;
        blocking_release(key_flags[machine_id], desired_val, session_id);
      }

      uint8_t prev_machine_id = (uint8_t) ((MACHINE_NUM + machine_id -1) % MACHINE_NUM);
      while(true) {
        // First acquire the previous machine flag
        yellow_printf("Machine %d Acquiring key_flag %u  from machine %u\n",
                      machine_id, key_flags[prev_machine_id], prev_machine_id);
        do {
          int ret = blocking_acquire(key_flags[prev_machine_id], value, session_id);
          assert(ret > 0);
        } while(value[0] != 1);

        yellow_printf("Machine %d Acquired key_flag %u  from machine %u\n",
                      machine_id, key_flags[prev_machine_id], prev_machine_id);
        // Then read all values to make sure they are what they are supposed to be
        for (i = 0; i < relaxed_writes; i++) {
          blocking_read(key_offset + i, value, session_id);
          cyan_printf("Reading key %u, iteration %u, value %u \n", key_offset + i, i, value[0]);
          assert(value[0] == 10 * prev_machine_id + i);
        }

        // Write the same values but with different values
        for (i = 0; i < relaxed_writes; i++) {
          desired_val[0] = (uint8_t) (10 * machine_id + i);
          blocking_write(key_offset + i, desired_val, session_id);
        }

        // reset the flag of the previous machine
        desired_val[0] = 0;
        blocking_write(key_flags[prev_machine_id], value, session_id);

        // release your flag
        desired_val[0] = 1;
        yellow_printf("Releasing key_flag %u \n", key_flags[machine_id]);
        blocking_release(key_flags[machine_id], desired_val, session_id);

      }
    }

    else assert(false);
  } // while(true) loop
}
//
// Created by vasilis on 05/02/19.
//

#include "util.h"
#include "inline_util.h"

#define ERROR_NO_SLOTS_FOR_BLOCKING_FUNCTION (-1)
#define ERROR_PUSH_PULL_PTR_MUST_MATCH (-2)
#define ERROR_SESSION_T0O_BIG (-3)

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

static inline int check_inputs(uint16_t session_id) {
  if (session_id >= SESSIONS_PER_MACHINE)
    return ERROR_SESSION_T0O_BIG;
 return 1;
}
static inline int cas_blocking(uint64_t key_id, uint8_t  *expected_val,
                                uint8_t *desired_val, bool weak, uint16_t session_id,
                                bool *cas_result)
{
  int return_int = 1;
  check_inputs(session_id);
  uint16_t wrkr = (uint16_t) (session_id / SESSIONS_PER_THREAD);
  uint16_t s_i = (uint16_t) (session_id % SESSIONS_PER_THREAD);
  uint16_t push_ptr = interface[wrkr].clt_push_ptr[s_i];


  // let's poll for the slot first
  if (interface[wrkr].req_array[s_i][push_ptr].state != INVALID_REQ) {
    printf("Deadlock: calling a blocking function, without slots \n");
    return ERROR_NO_SLOTS_FOR_BLOCKING_FUNCTION;
    //assert(false);
  }
  if (interface[wrkr].clt_push_ptr[s_i] != interface[wrkr].clt_pull_ptr[s_i])
    return ERROR_PUSH_PULL_PTR_MUST_MATCH;

  // Issuing the request
  struct client_op *cl_op = &interface[wrkr].req_array[s_i][push_ptr];
  uint64_t key_hash = CityHash128((char *) &(key_id), 4).second;
  cl_op->opcode = (uint8_t) (weak ? COMPARE_AND_SWAP_WEAK : COMPARE_AND_SWAP_STRONG);
  memcpy(&cl_op->key, &key_hash, TRUE_KEY_SIZE);
  memcpy(cl_op->value_to_write, desired_val, (size_t) RMW_VALUE_SIZE);
  memcpy(cl_op->value_to_read, expected_val, (size_t) RMW_VALUE_SIZE);
  atomic_store_explicit(&cl_op->state, ACTIVE_REQ, memory_order_release);
  MOD_ADD(interface[wrkr].clt_push_ptr[s_i], PER_SESSION_REQ_NUM);

  // Polling for completion
  uint16_t pull_ptr = interface[wrkr].clt_pull_ptr[s_i];
  cl_op = &interface[wrkr].req_array[s_i][pull_ptr]; // this is the same as above
  while (cl_op->state != COMPLETED_REQ);
  atomic_store_explicit(&cl_op->state, INVALID_REQ, memory_order_relaxed);

  MOD_ADD(interface[wrkr].clt_pull_ptr[s_i], PER_SESSION_REQ_NUM);

  *cas_result = cl_op->rmw_is_successful;
  if (!cl_op->rmw_is_successful)
    memcpy(expected_val, cl_op->value_to_read, (size_t) RMW_VALUE_SIZE);

  return return_int;
}


void *client(void *arg) {
  //uint32_t i = 0, j = 0;
  uint16_t w_i = 0;
  uint16_t s_i = 0, r_i = 0;
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
      uint8_t expected_val[RMW_VALUE_SIZE] = {0};
      uint8_t desired_val[RMW_VALUE_SIZE] = {0};
      int key_id = 0;
      int expected_int, desired_int;
      uint16_t session_id = 10;
      bool cas_result;
      sleep(3);
      printf("Pick a request number: read: 1, write 2, acquire 3, release 4, CAS 5, FAA 6 \n");
      int req_no = 0;
      int sc = scanf("%d", &req_no);
      switch (req_no) {
        case 1:
        case 2:
        case 3:
        case 4:
          assert(false);
        case 5:
          do {
            printf("Please input key_id, expected value , desired value \n");
          } while (scanf("%d %d %d:  ", &key_id, &expected_int, &desired_int) != 3);
          assert(expected_int < 256 && desired_int < 256 && key_id >= 0);
          printf ("key_id %u, expected %u, desired %u \n", key_id, expected_int, desired_int);
          expected_val[0] = (uint8_t) expected_int;
          desired_val[0] = (uint8_t) desired_int;
          printf ("key_id %u, expected %u, desired %u \n", key_id, expected_val[0], desired_val[0]);
          int ret = cas_blocking((uint32_t)key_id, expected_val, desired_val, true, session_id, &cas_result);
          green_printf("%s, return code %d, expected val %u, desired val %u\n",
                       cas_result ? "Success" : "Failure", ret, expected_val[0], desired_val[0]);
          assert(ret > 0);
          break;
        case 6:
        default: assert(false);

      }
    }
    else assert(false);
  } // while(true) loop
}
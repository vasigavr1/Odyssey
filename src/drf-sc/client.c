//
// Created by vasilis on 05/02/19.
//

#include "util.h"
#include "inline_util.h"

void *client(void *arg) {
  uint32_t i = 0, j = 0;
  struct thread_params params = *(struct thread_params *) arg;
  uint16_t t_id = (uint16_t) params.id;
  uint16_t gid = (uint16_t)((machine_id * WORKERS_PER_MACHINE) + t_id);


  for (i = 0; i < SESSIONS_PER_THREAD; i++) {
    for (j = 0; j < PER_SESSION_REQ_NUM; j++)
      req_array[i][j].state = INVALID_REQ;
  }

  uint32_t pull_ptr[SESSIONS_PER_THREAD] = {0},
           push_ptr[SESSIONS_PER_THREAD] = {0},
           size[SESSIONS_PER_THREAD] = {0},
           trace_ptr = 0;

  struct trace_command *trace;
  trace_init((void **)&trace, t_id);
  uint32_t dbg_cntr = 0;
  green_printf("Client %u reached loop \n", t_id);
  //sleep(10);
  while (true) {
    bool polled = false;

    // poll requests
    for (i = 0; i < SESSIONS_PER_THREAD; i++) {
      while (req_array[i][pull_ptr[i]].state == COMPLETED_REQ) {
        // get the result
        polled = true;
        if (CLIENT_DEBUG)
          green_printf("Client %u pulling req for session %u, slot %u\n",
                      t_id, i, pull_ptr[i]);
        atomic_store_explicit(&req_array[i][pull_ptr[i]].state, INVALID_REQ, memory_order_relaxed);
        MOD_ADD(pull_ptr[i], PER_SESSION_REQ_NUM);
      }
    }


    if (!polled) dbg_cntr++;
    else dbg_cntr = 0;
    if (dbg_cntr == BILLION) {
      printf("Trying to pull from pull_ptr %u %p state %u \n",
             pull_ptr[0], (void *)&req_array[i][pull_ptr[i]].state, req_array[i][pull_ptr[i]].state);
      dbg_cntr = 0;
    }

    // issue requests
    for (i = 0; i < SESSIONS_PER_THREAD; i++) {
      while (req_array[i][push_ptr[i]].state == INVALID_REQ) {
        if (CLIENT_DEBUG)
          yellow_printf("Client %u inserting req for session %u, in slot %u from trace slot %u ptr %p\n",
                        t_id, i, push_ptr[i], trace_ptr, &req_array[i][push_ptr[i]].state);
        req_array[i][push_ptr[i]].opcode = trace[trace_ptr].opcode;
        memcpy(&req_array[i][push_ptr[i]].key, trace[trace_ptr].key_hash, TRUE_KEY_SIZE);
        atomic_store_explicit(&req_array[i][push_ptr[i]].state, ACTIVE_REQ, memory_order_release);
        MOD_ADD(push_ptr[i], PER_SESSION_REQ_NUM);
        trace_ptr++;
        if (trace[trace_ptr].opcode == NOP) trace_ptr = 0;
      }
    }
  } // while(true) loop
}
//
// Created by vasilis on 26/06/20.
//

#ifndef DEBUG_UTIL_H
#define DEBUG_UTIL_H

#include "top.h"

// called when failing see an even version -- I.E. called by reads
static inline void debug_stalling_on_lock(uint32_t *debug_cntr, const char *message, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    (*debug_cntr)++;
    if ((*debug_cntr) == M_4) {
      printf("Worker %u stuck on %s \n", t_id, message);
      (*debug_cntr) = 0;
    }
  }
}

static inline void check_session_id_and_req_array_index(uint16_t sess_id, uint16_t req_array_i, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(sess_id < SESSIONS_PER_THREAD);
    assert(req_array_i < PER_SESSION_REQ_NUM);
    assert(t_id < WORKERS_PER_MACHINE);
  }
}

#endif //DEBUG_UTIL_H

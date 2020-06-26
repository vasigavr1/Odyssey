//
// Created by vasilis on 26/06/20.
//

#ifndef KITE_DEBUG_UTIL_H
#define KITE_DEBUG_UTIL_H

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

#endif //KITE_DEBUG_UTIL_H

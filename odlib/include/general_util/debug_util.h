//
// Created by vasilis on 26/06/20.
//

#ifndef DEBUG_UTIL_H
#define DEBUG_UTIL_H

#include "top.h"

static inline bool is_client_req_active(uint32_t sess_id, uint32_t req_array_i, uint16_t t_id);

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

// Prints out information about the participants
static inline void print_q_info(quorum_info_t *q_info)
{
  my_printf(yellow, "-----QUORUM INFO----- \n");
  my_printf(green, "Active m_ids: \n");
  for (uint8_t i = 0; i < q_info->active_num; i++) {
    my_printf(green, "%u) %u \n", i, q_info->active_ids[i]);
  }
  my_printf(red, "Missing m_ids: \n");
  for (uint8_t i = 0; i < q_info->missing_num; i++) {
    my_printf(red, "%u) %u \n", i, q_info->missing_ids[i]);
  }
  my_printf(yellow, "Send vector : ");
  for (uint8_t i = 0; i < REM_MACH_NUM; i++) {
    my_printf(yellow, "%d ", q_info->send_vector[i]);
  }
  my_printf(yellow, "\n First rm_id: %u, Last rm_id: %u \n",
            q_info->first_active_rm_id, q_info->last_active_rm_id);
}

static inline void check_client_req_state_when_filling_op(int working_session,
                                                          uint32_t pull_ptr,
                                                          uint32_t index_to_req_array,
                                                          uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(is_client_req_active((uint32_t) working_session, pull_ptr, t_id));
    uint32_t next_pull_ptr = (pull_ptr + 1) % PER_SESSION_REQ_NUM;
    uint32_t prev_pull_ptr = (PER_SESSION_REQ_NUM + pull_ptr - 1) % PER_SESSION_REQ_NUM;
    // if the next req is not active, no request can be active, so check the previous
    if (!is_client_req_active((uint32_t) working_session, next_pull_ptr, t_id) &&
        next_pull_ptr != prev_pull_ptr && prev_pull_ptr != pull_ptr)
      assert(!is_client_req_active((uint32_t) working_session, prev_pull_ptr, t_id));
  }
  //printf("Wrkr %u sess %u saves poll ptr %u for req at state %u \n", t_id,
  //       working_session,
  //       index_to_req_array,
  //       interface[t_id].req_array[working_session][index_to_req_array].state);
}

#endif //DEBUG_UTIL_H

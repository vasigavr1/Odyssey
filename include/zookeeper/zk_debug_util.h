//
// Created by vasilis on 30/06/20.
//

#ifndef KITE_ZK_DEBUG_UTIL_H
#define KITE_ZK_DEBUG_UTIL_H

#include "zk_main.h"
/* ---------------------------------------------------------------------------
//------------------------------ ZOOKEEPER DEBUGGING -----------------------------
//---------------------------------------------------------------------------*/


static inline void print_ldr_stats (uint16_t t_id)
{

  my_printf(yellow, "Prepares sent %ld/%ld \n", t_stats[t_id].preps_sent_mes_num, t_stats[t_id].preps_sent );
  my_printf(yellow, "Acks Received %ld/%ld \n", t_stats[t_id].received_acks_mes_num, t_stats[t_id].received_acks );
  my_printf(yellow, "Commits sent %ld/%ld \n", t_stats[t_id].coms_sent_mes_num, t_stats[t_id].coms_sent );
}

static inline void print_flr_stats (uint16_t t_id)
{

  my_printf(yellow, "Prepares received %ld/%ld \n", t_stats[t_id].received_preps_mes_num, t_stats[t_id].received_preps );
  my_printf(yellow, "Acks sent %ld/%ld \n", t_stats[t_id].acks_sent_mes_num, t_stats[t_id].acks_sent );
  my_printf(yellow, "Commits received %ld/%ld \n", t_stats[t_id].received_coms_mes_num, t_stats[t_id].received_coms );
}

// Leader checks its debug counters
static inline void ldr_check_debug_cntrs(uint32_t *credit_debug_cnt, uint32_t *wait_for_acks_dbg_counter,
                                         uint32_t *wait_for_gid_dbg_counter,p_writes_t *p_writes,
                                         uint16_t t_id)
{
  if (unlikely((*wait_for_gid_dbg_counter) > M_16)) {
    my_printf(red, "Leader %d waits for the g_id, committed g_id %lu \n", t_id, committed_global_w_id);
    print_ldr_stats(t_id);
    (*wait_for_gid_dbg_counter) = 0;
  }
  if (unlikely((*wait_for_acks_dbg_counter) > M_16)) {
    my_printf(red, "Leader %d waits for acks, committed g_id %lu \n", t_id, committed_global_w_id);
    my_printf(cyan, "Sent lid %u and state %d\n", p_writes->local_w_id, p_writes->w_state[p_writes->pull_ptr]);
    print_ldr_stats(t_id);
    (*wait_for_acks_dbg_counter) = 0;
    exit(0);
  }
  if (unlikely(credit_debug_cnt[PREP_VC] > M_16)) {
    my_printf(red, "Leader %d lacks prep credits, committed g_id %lu \n", t_id, committed_global_w_id);
    print_ldr_stats(t_id);
    credit_debug_cnt[PREP_VC] = 0;
  }
  if (unlikely(credit_debug_cnt[COMM_VC] > M_16)) {
    my_printf(red, "Leader %d lacks comm credits, committed g_id %lu \n", t_id, committed_global_w_id);
    print_ldr_stats(t_id);
    credit_debug_cnt[COMM_VC] = 0;
  }
}

// Follower checks its debug counters
static inline void flr_check_debug_cntrs(uint32_t *credit_debug_cnt, uint32_t *wait_for_coms_dbg_counter,
                                         uint32_t *wait_for_preps_dbg_counter,
                                         uint32_t *wait_for_gid_dbg_counter, volatile struct prep_message_ud_req *prep_buf,
                                         uint32_t pull_ptr, p_writes_t *p_writes, uint16_t t_id)
{

  if (unlikely((*wait_for_preps_dbg_counter) > M_16)) {
    my_printf(red, "Follower %d waits for preps, committed g_id %lu \n", t_id, committed_global_w_id);
    struct prepare *prep = (struct prepare *)&prep_buf[pull_ptr].prepare.prepare;
    uint32_t l_id = *(uint32_t *)prep_buf[pull_ptr].prepare.l_id;
    uint32_t g_id = *(uint32_t *)prep->g_id;
    uint8_t message_opc = prep_buf[pull_ptr].prepare.opcode;
    my_printf(cyan, "Flr %d, polling on index %u,polled opc %u, 1st write opcode: %u, l_id %u, first g_id %u, expected l_id %u\n",
              t_id, pull_ptr, message_opc, prep->opcode, l_id, g_id, p_writes->local_w_id);
    MOD_ADD(pull_ptr, FLR_PREP_BUF_SLOTS);
    prep = (struct prepare *)&prep_buf[pull_ptr].prepare.prepare;
    l_id = *(uint32_t *)prep_buf[pull_ptr].prepare.l_id;
    g_id = *(uint32_t *)prep->g_id;
    message_opc = prep_buf[pull_ptr].prepare.opcode;
    my_printf(cyan, "Next index %u,polled opc %u, 1st write opcode: %u, l_id %u, first g_id %u, expected l_id %u\n",
              pull_ptr, message_opc, prep->opcode, l_id, g_id, p_writes->local_w_id);
    for (int i = 0; i < FLR_PREP_BUF_SLOTS; ++i) {
      if (prep_buf[i].prepare.opcode == KVS_OP_PUT) {
        my_printf(green, "GOOD OPCODE in index %d, l_id %u \n", i, *(uint32_t *)prep_buf[i].prepare.l_id);
      }
      else my_printf(red, "BAD OPCODE in index %d, l_id %u \n", i, *(uint32_t *)prep_buf[i].prepare.l_id);

    }

    print_flr_stats(t_id);
    (*wait_for_preps_dbg_counter) = 0;
//    exit(0);
  }
  if (unlikely((*wait_for_gid_dbg_counter) > M_16)) {
    my_printf(red, "Follower %d waits for the g_id, committed g_id %lu \n", t_id, committed_global_w_id);
    print_flr_stats(t_id);
    (*wait_for_gid_dbg_counter) = 0;
  }
  if (unlikely((*wait_for_coms_dbg_counter) > M_16)) {
    my_printf(red, "Follower %d waits for coms, committed g_id %lu \n", t_id, committed_global_w_id);
    print_flr_stats(t_id);
    (*wait_for_coms_dbg_counter) = 0;
  }
  if (unlikely((*credit_debug_cnt) > M_16)) {
    my_printf(red, "Follower %d lacks write credits, committed g_id %lu \n", t_id, committed_global_w_id);
    print_flr_stats(t_id);
    (*credit_debug_cnt) = 0;
  }
}

// Check the states of pending writes
static inline void check_ldr_p_states(p_writes_t *p_writes, uint16_t t_id)
{
  assert(p_writes->size <= LEADER_PENDING_WRITES);
  for (uint16_t w_i = 0; w_i < LEADER_PENDING_WRITES - p_writes->size; w_i++) {
    uint16_t ptr = (p_writes->push_ptr + w_i) % LEADER_PENDING_WRITES;
    if (p_writes->w_state[ptr] != INVALID) {
      my_printf(red, "LDR %d push ptr %u, pull ptr %u, size %u, state %d at ptr %u \n",
                t_id, p_writes->push_ptr, p_writes->pull_ptr, p_writes->size, p_writes->w_state[ptr], ptr);
      print_ldr_stats(t_id);
      exit(0);
    }
  }
}


#endif //KITE_ZK_DEBUG_UTIL_H

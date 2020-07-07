//
// Created by vasilis on 11/05/20.
//

#ifndef KITE_DEBUG_UTIL_H
#define KITE_DEBUG_UTIL_H

#include <config.h>
#include "config.h"
#include "main.h"
#include "generic_util.h"
#include "debug_util.h"


/* ---------------------------------------------------------------------------
//------------------------------DEBUGGING-------------------------------------
//---------------------------------------------------------------------------*/

static inline void update_commit_logs(uint16_t t_id, uint32_t bkt, uint32_t log_no, uint8_t *old_value,
                                      uint8_t *value, const char* message, uint8_t flag)
{
  if (COMMIT_LOGS) { /*
    if (flag == LOG_COMS) {
      struct top *top = (struct top *) old_value;
      struct top *new_top = (struct top *) value;
      bool pushing = new_top->push_counter == top->push_counter + 1;
      bool popping = new_top->pop_counter == top->pop_counter + 1;
      fprintf(rmw_verify_fp[t_id], "Key: %u, log %u: %s: push/pop poitner %u/%u, "
                "key_ptr %u/%u/%u/%u %s - t = %lu\n",
              bkt, log_no, pushing ? "Pushing" : "Pulling",
              new_top->push_counter, new_top->pop_counter, new_top->key_id,
              new_top->sec_key_id, new_top->third_key_id, new_top->fourth_key_id, message,
              time_approx);
    }
    else if (flag == LOG_WS){
      struct node *node = (struct node *) old_value;
      struct node *new_node = (struct node *) value;
      fprintf(rmw_verify_fp[t_id], "Key: %u, %u/%u/%u/%u, "
                "old: %u/%u/%u/%u version %u -- %s - t = %lu\n",
              bkt, new_node->key_id,
              new_node->stack_id, new_node->push_counter, new_node->next_key_id,
              node->key_id,
              node->stack_id, node->push_counter, node->next_key_id, log_no, message,
              time_approx);
    }*/
  }
}

static inline void check_version(uint32_t version, const char *message) {
  if (ENABLE_ASSERTIONS) {


//    if (version == 0 || version % 2 != 0) {
//      my_printf(red, "Version %u %s\n", version, message);
//    }
    assert(version >= ALL_ABOARD_TS);
//    assert(version % 2 == 0);
  }
}

static inline void print_wrkr_stats (uint16_t t_id)
{
  my_printf(green, "WORKER %u SENT MESSAGES \n", t_id);
  my_printf(yellow, "Writes sent %ld/%ld \n", t_stats[t_id].writes_sent_mes_num, t_stats[t_id].writes_sent);
  my_printf(yellow, "Acks sent %ld/%ld \n", t_stats[t_id].acks_sent_mes_num, t_stats[t_id].acks_sent);
  my_printf(yellow, "Reads sent %ld/%ld \n", t_stats[t_id].reads_sent_mes_num, t_stats[t_id].reads_sent);
  my_printf(yellow, "R_reps sent %ld/%ld \n", t_stats[t_id].r_reps_sent_mes_num, t_stats[t_id].r_reps_sent);
  my_printf(green, "WORKER %u RECEIVED MESSAGES \n", t_id);
  //my_printf(yellow, "Writes sent %ld/%ld \n", t_stats[g_id].writes_sent_mes_num, t_stats[g_id].writes_sent);
  //my_printf(yellow, "Acks sent %ld/%ld \n", t_stats[g_id].acks_sent_mes_num, t_stats[g_id].acks_sent);
  my_printf(yellow, "Reads received %ld/%ld \n", t_stats[t_id].received_reads_mes_num, t_stats[t_id].received_reads);
  my_printf(yellow, "R_reps received %ld/%ld \n", t_stats[t_id].received_r_reps_mes_num, t_stats[t_id].received_r_reps);

  for (uint8_t i = 0; i < MACHINE_NUM; i++) {
    if (i == machine_id) continue;
    my_printf(cyan, "FROM/ TO MACHINE %u \n", i);
    my_printf(yellow, "Acks Received %lu/%lu from machine id %u \n", t_stats[t_id].per_worker_acks_mes_received[i],
              t_stats[t_id].per_worker_acks_received[i], i);
    my_printf(yellow, "Writes Received %lu from machine id %u\n", t_stats[t_id].per_worker_writes_received[i], i);
    my_printf(yellow, "Acks Sent %lu/%lu to machine id %u \n", t_stats[t_id].per_worker_acks_mes_sent[i],
              t_stats[t_id].per_worker_acks_sent[i], i);

  }
//  my_printf(yellow, "Reads sent %ld/%ld \n", t_stats[g_id].r_reps_sent_mes_num, t_stats[g_id].r_reps_sent );
}

// Print the rep info received for a propose or an accept
static inline void print_rmw_rep_info(loc_entry_t *loc_entry, uint16_t t_id) {
  struct rmw_rep_info *rmw_rep = &loc_entry->rmw_reps;
  my_printf(yellow, "Wrkr %u Printing rmw_rep for sess %u state %u helping flag %u \n"
              "Tot_replies %u \n acks: %u \n rmw_id_committed: %u \n log_too_small %u\n"
              "already_accepted : %u\n seen_higher_prop : %u\n "
              "log_too_high: %u \n",
            t_id, loc_entry->sess_id, loc_entry->state, loc_entry->helping_flag,
            rmw_rep->tot_replies,
            rmw_rep->acks, rmw_rep->rmw_id_commited, rmw_rep->log_too_small,
            rmw_rep->already_accepted,
            rmw_rep->seen_higher_prop_acc, rmw_rep->log_too_high);
}

// Leader checks its debug counters
static inline void check_debug_cntrs(uint32_t *credit_debug_cnt, uint32_t *wait_dbg_counter,
                                     p_ops_t *p_ops, void *buf,
                                     uint32_t r_pull_ptr, uint32_t w_pull_ptr,
                                     uint32_t ack_pull_ptr, uint32_t r_rep_pull_ptr,
                                     uint16_t t_id)
{

//  volatile struct  w_message_ud_req *w_buffer =
//    (volatile w_mes_ud_t *)(buf + ACK_BUF_SIZE);
//  volatile struct  r_message_ud_req *r_buffer =
//    (volatile r_mes_ud_t *)(cb->dgram_buf + ACK_BUF_SIZE + W_BUF_SIZE);

  // ACKS
  if (unlikely(wait_dbg_counter[ACK_QP_ID] > M_512)) {
    my_printf(red, "Worker %d waits for acks \n", t_id);
    if (VERBOSE_DBG_COUNTER) {
      ack_mes_ud_t *ack_buf = (ack_mes_ud_t *) (buf);
      ack_mes_t *ack = &ack_buf[ack_pull_ptr].ack;
      uint64_t l_id = ack->l_id;
      uint8_t message_opc = ack->opcode;
      my_printf(cyan, "Wrkr %d, polling on index %u, polled opc %u, 1st ack opcode: %u, l_id %lu, expected l_id %lu\n",
                t_id, ack_pull_ptr, message_opc, ack->opcode, l_id, p_ops->local_w_id);
      MOD_INCR(ack_pull_ptr, ACK_BUF_SLOTS);
      ack = &ack_buf[ack_pull_ptr].ack;
      l_id = ack->l_id;
      message_opc = ack->opcode;
      my_printf(cyan, "Next index %u,polled opc %u, 1st ack opcode: %u, l_id %lu, expected l_id %lu\n",
                ack_pull_ptr, message_opc, ack->opcode, l_id, p_ops->local_w_id);
      for (int i = 0; i < ACK_BUF_SLOTS; ++i) {
        if (ack_buf[i].ack.opcode == OP_ACK) {
          my_printf(green, "GOOD OPCODE in index %d, l_id %u \n", i, ack_buf[i].ack.l_id);
        } else
          my_printf(red, "BAD OPCODE in index %d, l_id %u, from machine: %u  \n", i, ack_buf[i].ack.l_id,
                    ack_buf[i].ack.m_id);

      }
    }
    print_wrkr_stats(t_id);
    wait_dbg_counter[ACK_QP_ID] = 0;
    //exit(0);
  }
  // R_REPS
  if (unlikely(wait_dbg_counter[R_REP_QP_ID] > M_512)) {
    my_printf(red, "Worker %d waits for r_reps \n", t_id);
    if (VERBOSE_DBG_COUNTER) {
      r_rep_mes_ud_t *r_rep_buf =
        (r_rep_mes_ud_t *) (buf + ACK_BUF_SIZE + W_BUF_SIZE + R_BUF_SIZE);
      struct r_rep_message *r_rep_mes = (struct r_rep_message *)&r_rep_buf[r_rep_pull_ptr].r_rep_mes;
      uint64_t l_id = r_rep_mes->l_id;
      uint8_t message_opc = r_rep_mes->opcode;
      my_printf(cyan, "Wrkr %d, polling on index %u, polled opc %u, 1st r_rep opcode: %u, l_id %lu, expected l_id %lu\n",
                t_id, r_rep_pull_ptr, message_opc, r_rep_mes->opcode, l_id, p_ops->local_r_id);
      MOD_INCR(r_rep_pull_ptr, R_REP_BUF_SLOTS);
      r_rep_mes = (struct r_rep_message *)&r_rep_buf[r_rep_pull_ptr].r_rep_mes;
      l_id = r_rep_mes->l_id;
      message_opc = r_rep_mes->opcode;
      my_printf(cyan, "Next index %u,polled opc %u, 1st r_rep opcode: %u, l_id %lu, expected l_id %lu\n",
                r_rep_pull_ptr, message_opc, r_rep_mes->opcode, l_id, p_ops->local_r_id);
      for (int i = 0; i < R_REP_BUF_SLOTS; ++i) {
        if (r_rep_mes->opcode == READ_REPLY) {
          my_printf(green, "GOOD OPCODE in index %d, l_id %u \n", i, r_rep_mes->l_id);
        } else
          my_printf(red, "BAD OPCODE in index %d, l_id %u, from machine: %u  \n", i,
                    r_rep_mes->l_id,
                    r_rep_mes->m_id);

      }
    }
    print_wrkr_stats(t_id);
    wait_dbg_counter[R_REP_QP_ID] = 0;
    //exit(0);
  }
  if (unlikely(wait_dbg_counter[R_QP_ID] > M_512)) {
    my_printf(red, "Worker %d waits for reads \n", t_id);
    print_wrkr_stats(t_id);
    wait_dbg_counter[R_QP_ID] = 0;
  }
  if (unlikely(credit_debug_cnt[W_VC] > M_512)) {
    my_printf(red, "Worker %d lacks write credits \n", t_id);
    print_wrkr_stats(t_id);
    credit_debug_cnt[W_VC] = 0;
  }
  if (unlikely(credit_debug_cnt[R_VC] > M_512)) {
    my_printf(red, "Worker %d lacks read credits \n", t_id);
    print_wrkr_stats(t_id);
    credit_debug_cnt[R_VC] = 0;
  }
}

// When pulling a n ew req from the trace, check the req and the working session
static inline void check_trace_req(p_ops_t *p_ops, trace_t *trace, trace_op_t *op,
                                   int working_session, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(trace->opcode != NOP);
    check_state_with_allowed_flags(8, trace->opcode, OP_RELEASE, KVS_OP_PUT,
                                   OP_ACQUIRE, KVS_OP_GET, FETCH_AND_ADD, COMPARE_AND_SWAP_WEAK,
                                   COMPARE_AND_SWAP_STRONG);
    assert(op->opcode == trace->opcode);
    assert(!p_ops->sess_info[working_session].stalled);
    if (ENABLE_RMWS && p_ops->prop_info->entry[working_session].state != INVALID_RMW) {
      my_printf(cyan, "wrk %u  Session %u has loc_entry state %u , helping flag %u\n", t_id,
                working_session, p_ops->prop_info->entry[working_session].state,
                p_ops->prop_info->entry[working_session].helping_flag);
      assert(false);
    }
  }
}


static inline void debug_and_count_stats_when_broadcasting_writes
  (p_ops_t *p_ops, uint32_t bcast_pull_ptr,
   uint8_t coalesce_num, uint16_t t_id, uint64_t* expected_l_id_to_send,
   uint16_t br_i, uint32_t *outstanding_writes)
{
  //bool is_accept = p_ops->w_fifo->w_message[bcast_pull_ptr].write[0].opcode == ACCEPT_OP;
  if (ENABLE_ASSERTIONS) {
//    if (!is_accept) {
//      uint64_t lid_to_send = p_ops->w_fifo->w_message[bcast_pull_ptr].l_id;
//      if (lid_to_send != (*expected_l_id_to_send)) {
//        my_printf(red, "Wrkr %u, expected l_id %lu lid_to send %u, opcode %u \n",
//                   t_id, (*expected_l_id_to_send), lid_to_send,
//                   p_ops->w_fifo->w_message[bcast_pull_ptr].write[0].opcode );
//        assert(false);
//      }
//      (*expected_l_id_to_send) = lid_to_send + coalesce_num;
//    }
    struct w_message *w_mes = (struct w_message *) &p_ops->w_fifo->w_message[bcast_pull_ptr];
    if (coalesce_num == 0) {
      my_printf(red, "Wrkr %u coalesce_num is %u, bcast_size %u, w_size %u, push_ptr %u, pull_ptr %u"
                  " mes fifo push_ptr %u, mes fifo pull ptr %u l_id %lu"
                  " bcast_pull_ptr %u, br_i %u\n",
                t_id, coalesce_num, p_ops->w_fifo->bcast_size,
                p_ops->w_size,
                p_ops->w_push_ptr, p_ops->w_pull_ptr,
                p_ops->w_fifo->push_ptr, p_ops->w_fifo->bcast_pull_ptr,
                w_mes->l_id,
                bcast_pull_ptr, br_i);
    }
    assert(coalesce_num > 0);
    assert(p_ops->w_fifo->bcast_size >= coalesce_num);
    (*outstanding_writes) += coalesce_num;
  }
  if (ENABLE_STAT_COUNTING) {
    //bool is_commit = p_ops->w_fifo->w_message[bcast_pull_ptr].write[0].opcode == COMMIT_OP;
//    if (is_accept) t_stats[t_id].accepts_sent++;
//    else if (is_commit) t_stats[t_id].commits_sent++;
//    else {
//      t_stats[t_id].writes_sent += coalesce_num;
//      t_stats[t_id].writes_sent_mes_num++;
//    }
  }
}


// Perform some basic checks when inserting a write to a fresh message
static inline void debug_checks_when_inserting_a_write
  (const uint8_t source, write_t *write, const uint32_t w_mes_ptr,
   const uint64_t message_l_id, p_ops_t *p_ops,
   const uint32_t w_ptr, const uint16_t t_id)
{

  if (ENABLE_ASSERTIONS) {
    w_mes_info_t *info = &p_ops->w_fifo->info[w_mes_ptr];
    if (message_l_id > MAX_MES_IN_WRITE && info->valid_header_l_id) {
      uint32_t prev_w_mes_ptr = (w_mes_ptr + W_FIFO_SIZE - 1) % W_FIFO_SIZE;


      struct w_message *prev_w_mes = (struct w_message *) &p_ops->w_fifo->w_message[prev_w_mes_ptr];
      w_mes_info_t *prev_info = &p_ops->w_fifo->info[prev_w_mes_ptr];

      bool prev_mes_valid_l_id = prev_info->valid_header_l_id;
      uint64_t prev_l_id = prev_w_mes->l_id;
      uint8_t prev_write_num = prev_info->writes_num;

      if ((prev_mes_valid_l_id) && (message_l_id != prev_l_id + prev_write_num)) {
        my_printf(red, "Worker %u: Current message l_id %u ptr %u, previous message l_id %u , prev_write_num %u ptr %u\n",
                  t_id, message_l_id, w_mes_ptr, prev_l_id, prev_write_num, prev_w_mes_ptr);
      }
    }
    // Check the versions
    assert (write->version < B_4_EXACT);
//    if (write->version % 2 != 0) {
//      my_printf(red, "Worker %u: Version to insert %u, comes from read %u \n", t_id,
//                 write->version, source);
//      assert (false);
//    }
    // Check that the buffer is not occupied
    if (p_ops->virt_w_size > MAX_ALLOWED_W_SIZE  || p_ops->w_size >= MAX_ALLOWED_W_SIZE)
      my_printf(red, "Worker %u w_state %d at w_ptr %u, kvs hits %lu, w_size/virt_w_size %u/%u, source %u\n",
                t_id, p_ops->w_meta[w_ptr].w_state, w_ptr,
                t_stats[t_id].cache_hits_per_thread, p_ops->w_size, p_ops->virt_w_size, source);
    if (unlikely(p_ops->w_meta[w_ptr].w_state != INVALID))
      my_printf(red, "Worker %u w_state %d at w_ptr %u, kvs hits %lu, w_size %u \n",
                t_id, p_ops->w_meta[w_ptr].w_state, w_ptr,
                t_stats[t_id].cache_hits_per_thread, p_ops->w_size);
    //					printf("Sent %d, Valid %d, Ready %d \n", SENT, VALID, READY);
    assert(p_ops->w_meta[w_ptr].w_state == INVALID);
  }

}

// When forging a write (which the accept hijack)
static inline void checks_when_forging_an_accept(struct accept* acc, struct ibv_sge *send_sgl,
                                                 uint16_t br_i, uint8_t w_i, uint8_t coalesce_num, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(coalesce_num > 0);
    if (DEBUG_RMW)
      printf("Worker: %u, Accept in position %d, val-len %u, message w_size %d\n", t_id, w_i, acc->val_len,
             send_sgl[br_i].length);
    check_state_with_allowed_flags(3, acc->opcode, ACCEPT_OP, ACCEPT_OP_BIT_VECTOR);
    //assert(acc->val_len == VALUE_SIZE >> SHIFT_BITS);

  }
}

// When forging a write (which the accept hijack)
static inline void checks_when_forging_a_commit(struct commit *com, struct ibv_sge *send_sgl,
                                                uint16_t br_i, uint8_t w_i, uint8_t coalesce_num, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(coalesce_num > 0);
    if (DEBUG_RMW)
      printf("Worker: %u, Commit %d, val-len %u, message w_size %d\n", t_id, w_i, com->val_len,
             send_sgl[br_i].length);
    //assert(com->val_len == VALUE_SIZE >> SHIFT_BITS);
    assert(com->opcode == COMMIT_OP || com->opcode == RMW_ACQ_COMMIT_OP ||
           com->opcode == COMMIT_OP_NO_VAL);
  }
}


static inline void checks_when_forging_a_write(write_t* write, struct ibv_sge *send_sgl,
                                               uint16_t br_i, uint8_t w_i, uint8_t coalesce_num, uint16_t t_id) {

  if (DEBUG_WRITES)
    printf("Worker: %u, Write %d, val-len %u, message w_size %d\n", t_id, w_i, write->val_len,
           send_sgl[br_i].length);
  if (ENABLE_ASSERTIONS) {
    assert(write->val_len == VALUE_SIZE >> SHIFT_BITS);
    check_state_with_allowed_flags(6, write->opcode, KVS_OP_PUT, OP_RELEASE,
                                   OP_ACQUIRE, OP_RELEASE_SECOND_ROUND, OP_RELEASE_BIT_VECTOR);
    if (write->opcode == OP_RELEASE_SECOND_ROUND)
      if (DEBUG_QUORUM) my_printf(green, "Thread %u Changing the op of the second round of a release \n", t_id);
  }

}


static inline void print_thread_stats(uint16_t t_id) {

  my_printf(yellow, "Cache hits: %u \nReads: %lu \nWrites: %lu \nReleases: %lu \nAcquires: %lu \nQ Reads: %lu "
              "\nRectified keys %lu\n",
            t_stats[t_id].cache_hits_per_thread, t_stats[t_id].reads_per_thread,
            t_stats[t_id].writes_per_thread, t_stats[t_id].releases_per_thread,
            t_stats[t_id].acquires_per_thread, t_stats[t_id].quorum_reads,
            t_stats[t_id].rectified_keys);
}


static inline void print_verbouse_debug_info(p_ops_t *p_ops, uint16_t t_id, uint16_t credits[][MACHINE_NUM])
{
  uint16_t i;
  my_printf(green, "---DEBUG INFO---------\n");
  my_printf(yellow, "1. ---SESSIONS--- \n");
  if (p_ops->all_sessions_stalled) my_printf(yellow, "All sessions are stalled \n");
  else my_printf(yellow, "There are available sessions \n");
  for (i = 0; i < SESSIONS_PER_THREAD; i++)
    printf("S%u: %d ", i, p_ops->sess_info[i].stalled);
  printf("\n");
  my_printf(cyan, "2. ---CREDITS--- \n");
  for (i = 0; i < MACHINE_NUM; i++)
    my_printf(cyan, "Credits for machine %u: %u R and %u W \n", i, credits[R_VC][i], credits[W_VC][i]);
  printf("\n");
  my_printf(green, "3. ---FIFOS--- \n");
  my_printf(green, "W_size: %u \nw_push_ptr %u \nw_pull_ptr %u\n", p_ops->w_size, p_ops->w_push_ptr, p_ops->w_pull_ptr);
  my_printf(green, "R_size: %u \nr_push_ptr %u \nr_pull_ptr %u\n", p_ops->r_size, p_ops->r_push_ptr, p_ops->r_pull_ptr);

  my_printf(yellow, "Cache hits: %u \nReads: %u \nWrites: %u \nReleases: %u \nAcquires: %u \n",
            t_stats[t_id].cache_hits_per_thread, t_stats[t_id].reads_per_thread,
            t_stats[t_id].writes_per_thread, t_stats[t_id].releases_per_thread,
            t_stats[t_id].acquires_per_thread);
  print_for_debug = false;
}





//The purpose of this is to save some space in function that polls read replies
static inline void print_and_check_mes_when_polling_r_reps(struct r_rep_message *r_rep_mes,
                                                           uint32_t index, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    check_state_with_allowed_flags(7, r_rep_mes->opcode, READ_REPLY, PROP_REPLY, ACCEPT_REPLY,
                                   READ_PROP_REPLY, ACCEPT_REPLY_NO_CREDITS);
    my_assert(r_rep_mes->m_id < MACHINE_NUM, "Received r_rep with m_id >= Machine_NUM");
    my_assert(r_rep_mes->coalesce_num > 0, "Received r_rep with coalesce num = 0");
  }

  if ((DEBUG_READ_REPS && (r_rep_mes->opcode == READ_REPLY)) ||
      (DEBUG_RMW   && (r_rep_mes->opcode == PROP_REPLY || r_rep_mes->opcode == ACCEPT_REPLY ||
                       r_rep_mes->opcode == ACCEPT_REPLY_NO_CREDITS))) {
    my_printf(yellow, "Worker %u sees a READ REPLY: %d at offset %d, l_id %lu, from machine "
                "%u with %u replies first opc %u\n",
              t_id, r_rep_mes->opcode, index,
              r_rep_mes->l_id,
              r_rep_mes->m_id, r_rep_mes->coalesce_num, r_rep_mes->r_rep[0].opcode);
  }
}

static inline void increase_credits_when_polling_r_reps(uint16_t credits[][MACHINE_NUM],
                                                        bool increase_w_credits,
                                                        uint8_t rem_m_id, uint16_t t_id)
{
  if (!increase_w_credits) credits[R_VC][rem_m_id]++;
  else credits[W_VC][rem_m_id]++;
  if (ENABLE_ASSERTIONS) {
    assert(credits[R_VC][rem_m_id] <= R_CREDITS);
    assert(credits[W_VC][rem_m_id] <= W_CREDITS);
  }
}

// Debug session
static inline void debug_sessions(struct session_dbg *ses_dbg, p_ops_t *p_ops,
                                  uint32_t sess_id, uint16_t t_id)
{
  if (DEBUG_SESSIONS && ENABLE_ASSERTIONS) {
    //assert(p_ops->prop_info->entry[sess_id].state != INVALID_RMW);
    ses_dbg->dbg_cnt[sess_id]++;
    if (ses_dbg->dbg_cnt[sess_id] == DEBUG_SESS_COUNTER) {
//      if (sess_id == 0)
      my_printf(red, "Wrkr %u Session %u seems to be stuck \n", t_id, sess_id);
      ses_dbg->dbg_cnt[sess_id] = 0;
    }
  }
}

// Debug all the session
static inline void debug_all_sessions(struct session_dbg *ses_dbg, p_ops_t *p_ops,
                                      uint16_t t_id)
{
  if (DEBUG_SESSIONS && ENABLE_ASSERTIONS) {
    for (uint16_t sess_id = 0; sess_id < SESSIONS_PER_THREAD; sess_id++) {
      assert(p_ops->sess_info[sess_id].stalled);
      ses_dbg->dbg_cnt[sess_id]++;
      //assert(p_ops->prop_info->entry[sess_id].state != INVALID_RMW);
      if (ses_dbg->dbg_cnt[sess_id] == DEBUG_SESS_COUNTER) {
        if (sess_id == 0) {
          per_write_meta_t *w_meta = &p_ops->w_meta[p_ops->w_pull_ptr];
          my_printf(red, "Wrkr %u Session %u seems to be stuck-- all stuck \n", t_id, sess_id);
          my_printf(yellow, "Wrkr %u w_size %u, w_state %u, acks seen/expected %u/%u \n",
                    t_id, p_ops->w_size, w_meta->w_state, w_meta->acks_seen, w_meta->acks_expected);
//          for (uint16_t i = 0; i < SESSIONS_PER_THREAD; i++)
//            printf("%u - %u- %u - %u, ", ses_dbg->dbg_cnt[i],
//                   p_ops->prop_info->entry[i].state, p_ops->prop_info->entry[i].back_off_cntr,
//                   p_ops->prop_info->entry[i].index_to_rmw);
//          printf ("\n");
        }
        ses_dbg->dbg_cnt[sess_id] = 0;
      }
    }
  }
}

static inline void print_all_stalled_sessions(p_ops_t *p_ops, uint16_t t_id)
{
  uint32_t count = 0;
  for (uint16_t sess_i = 0; sess_i < SESSIONS_PER_THREAD; sess_i++) {
    uint32_t glob_sess_i = get_glob_sess_id((uint8_t) machine_id, t_id, sess_i);

    if (p_ops->sess_info[sess_i].stalled) {
      loc_entry_t *loc_entry = &p_ops->prop_info->entry[sess_i];
      if (!count) {
        my_printf(yellow, "----------------------------------------\n");
        my_printf(yellow, "WORKER %u STALLED SESSIONS\n", t_id);
        my_printf(yellow, "----------------------------------------\n");
      }
      count++;
      my_printf(magenta, "Session %u state %u reason %u \n",
                glob_sess_i, loc_entry->state, loc_entry->stalled_reason);
      if (loc_entry->stalled_reason == STALLED_BECAUSE_NOT_ENOUGH_REPS) {
        rmw_rep_info_t *reps = &loc_entry->rmw_reps;
        my_printf(red, "Session %u, reps %u\n", glob_sess_i, reps->tot_replies);
      }
    }
  }
  if (count) {
    my_printf(yellow, "----------------------------------------\n");
    my_printf(yellow, "%u STALLED SESSIONS FOUND FOR WORKER %u \n", count, t_id);
    my_printf(yellow, "----------------------------------------\n");
  }
}


// From commit reads
static inline void checks_when_committing_a_read(p_ops_t *p_ops, uint32_t pull_ptr,
                                                 bool acq_second_round_to_flip_bit, bool insert_write_flag,
                                                 bool write_local_kvs, bool signal_completion,
                                                 bool signal_completion_after_kvs_write,
                                                 uint16_t t_id)
{
  r_info_t *read_info = &p_ops->read_info[pull_ptr];
  if (ENABLE_ASSERTIONS) {
    if (acq_second_round_to_flip_bit) assert(p_ops->virt_r_size < MAX_ALLOWED_R_SIZE);
    check_state_with_allowed_flags(6, read_info->opcode, OP_ACQUIRE, OP_ACQUIRE_FLIP_BIT,
                                   KVS_OP_GET, OP_RELEASE, KVS_OP_PUT);
    assert(!(signal_completion && signal_completion_after_kvs_write));
    if (read_info->is_read) {
      assert(read_info->opcode == OP_ACQUIRE || read_info->opcode == KVS_OP_GET);
    }
    if (read_info->opcode == OP_ACQUIRE_FLIP_BIT) {
      //printf("%d, %d, %d, %d, %d, %d \n", acq_second_round_to_flip_bit, insert_write_flag, write_local_kvs, insert_commit_flag,
      //                               signal_completion, signal_completion_after_kvs_write);
      assert(!acq_second_round_to_flip_bit && !insert_write_flag && !write_local_kvs &&
             !signal_completion && !signal_completion_after_kvs_write);
    }
    if (read_info->opcode == KVS_OP_GET) {
      assert(epoch_id > 0);
      assert(!acq_second_round_to_flip_bit && !insert_write_flag &&
             !signal_completion && signal_completion_after_kvs_write && write_local_kvs);
    }
    if (read_info->opcode == OP_RELEASE)
      assert(!acq_second_round_to_flip_bit && insert_write_flag && !write_local_kvs &&
             !signal_completion && !signal_completion_after_kvs_write);
    if (read_info->opcode == KVS_OP_PUT)
      assert(!acq_second_round_to_flip_bit && insert_write_flag && write_local_kvs &&
             !signal_completion && signal_completion_after_kvs_write);
    if (read_info->opcode == OP_ACQUIRE) {
      if (insert_write_flag) assert(!signal_completion && !signal_completion_after_kvs_write);
      else if (write_local_kvs) assert(signal_completion_after_kvs_write);
      else assert(signal_completion);
    }
  }
  if (DEBUG_READS || DEBUG_TS)
    my_printf(green, "Committing read at index %u, it has seen %u times the same timestamp\n",
              pull_ptr, read_info->times_seen_ts);

}

//
static inline void check_read_fifo_metadata(p_ops_t *p_ops, struct r_message *r_mes,
                                            uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(p_ops->virt_r_size <= MAX_ALLOWED_R_SIZE); // this may need to be MAX_ALLOWED_R_SIZE + 1
    assert(p_ops->r_size <= p_ops->virt_r_size);
    assert(r_mes->coalesce_num <= MAX_READ_COALESCE);
    assert(p_ops->r_session_id[p_ops->r_push_ptr] <= SESSIONS_PER_THREAD);
  }
}

static inline void check_global_sess_id(uint8_t machine_id, uint16_t t_id,
                                        uint16_t session_id, uint64_t rmw_id)
{
  uint32_t glob_sess_id = (uint32_t) (rmw_id % GLOBAL_SESSION_NUM);
  assert(glob_ses_id_to_m_id(glob_sess_id) == machine_id);
  assert(glob_ses_id_to_t_id(glob_sess_id) == t_id);
  assert(glob_ses_id_to_sess_id(glob_sess_id) == session_id);
}


// Do some preliminary checks for the write message,
// the while loop is there to wait for the entire message to be written (currently not useful)
static inline void check_the_polled_write_message(struct w_message *w_mes,
                                                  uint32_t index, uint32_t polled_writes, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
//    assert(w_mes->m_id < MACHINE_NUM);
    assert(w_mes->coalesce_num <= MAX_MES_IN_WRITE);
    uint32_t debug_cntr = 0;
    if (w_mes->coalesce_num == 0 || w_mes->m_id >= MACHINE_NUM) {
      my_printf(red, "Wrkr %u received a write with coalesce_num %u, op %u from machine %u with lid %lu \n",
                t_id, w_mes->coalesce_num, w_mes->write[0].opcode, w_mes->m_id, w_mes->l_id);
      assert(false);
    }
    if (polled_writes + w_mes->coalesce_num > MAX_INCOMING_W) {
      assert(false);
    }
    return;
  }
}

// When polling for writes
static inline void check_a_polled_write(write_t* write, uint16_t w_i,
                                        uint16_t w_num, uint8_t mes_opcode, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(write->m_id < MACHINE_NUM);
    if (mes_opcode == ONLY_WRITES)
      assert(write->opcode != ACCEPT_OP && write->opcode != ACCEPT_OP_BIT_VECTOR);
    else if (mes_opcode == ONLY_ACCEPTS)
      assert(write->opcode == ACCEPT_OP || write->opcode == ACCEPT_OP_BIT_VECTOR);
    if (write->opcode != KVS_OP_PUT && write->opcode != OP_RELEASE &&
        write->opcode != ACCEPT_OP &&
        write->opcode != OP_RELEASE_BIT_VECTOR && write->opcode != COMMIT_OP &&
        write->opcode != COMMIT_OP_NO_VAL &&
        write->opcode != NO_OP_RELEASE && write->opcode != ACCEPT_OP_BIT_VECTOR)
      my_printf(red, "Wrkr %u Receiving write : Opcode %u, i %u/%u \n", t_id, write->opcode, w_i, w_num);
//    if (write->version % 2 != 0) {
//      my_printf(red, "Wrkr %u :Odd version %u, m_id %u \n", t_id, write->version, write->m_id);
//    }
  }
}


// When polling for writes
static inline void print_polled_write_message_info(struct w_message *w_mes, uint32_t index, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    struct write * write = &w_mes->write[0];
    if (DEBUG_WRITES && write->opcode != ACCEPT_OP)
      printf("Worker %u sees a write Opcode %d at offset %d, l_id %lu  \n",
             t_id, write->opcode, index, w_mes->l_id);
    else if (DEBUG_RMW &&
             (write->opcode == ACCEPT_OP || write->opcode == ACCEPT_OP_BIT_VECTOR)) {
      struct accept *acc = (struct accept*) write;
      my_printf(yellow, "Worker %u sees an Accept: opcode %d at offset %d, rmw_id %lu, "
               "log_no %u, coalesce_num %u \n",
             t_id, acc->opcode, index, acc->t_rmw_id,
             acc->log_no,
             w_mes->coalesce_num);
    }
    else if (DEBUG_RMW && write->opcode == COMMIT_OP) {
      struct commit *com = (struct commit *) write;
      printf("Worker %u sees a Commit: opcode %d at offset %d, l_id %lu, "
               "glob_sess_id %u, log_no %u, coalesce_num %u \n",
             t_id, com->opcode, index, com->t_rmw_id,
             (uint32_t) com->t_rmw_id % GLOBAL_SESSION_NUM, com->log_no,
             w_mes->coalesce_num);
    }
  }
}


static inline void count_stats_on_receiving_w_mes_reset_w_num(struct w_message *w_mes,
                                                              uint8_t w_num, uint16_t t_id)
{
  if (ENABLE_STAT_COUNTING) {
    if (ENABLE_ASSERTIONS) t_stats[t_id].per_worker_writes_received[w_mes->m_id] += w_num;
    t_stats[t_id].received_writes += w_num;
    t_stats[t_id].received_writes_mes_num++;
  }
  if (ENABLE_ASSERTIONS) w_mes->coalesce_num = 0;
}

static inline void check_accept_mes(struct w_message *acc_mes)
{
  if (ENABLE_ASSERTIONS) {
    check_state_with_allowed_flags(3, acc_mes->opcode, WRITES_AND_ACCEPTS,
                                   ONLY_ACCEPTS);
    assert(acc_mes->coalesce_num == 0); // the coalesce_num gets reset after polling a write
    assert(acc_mes->m_id < MACHINE_NUM);
  }
}

// Called when forging a read reply work request
static inline void checks_and_prints_when_forging_r_rep_wr(uint8_t coalesce_num, uint16_t mes_i,
                                                           struct ibv_sge *send_sgl, uint32_t r_rep_i,
                                                           struct r_rep_message *r_rep_mes,
                                                           struct r_rep_fifo *r_rep_fifo,
                                                           uint16_t t_id)
{
  if (DEBUG_READ_REPS) {
    for (uint16_t i = 0; i < coalesce_num; i++)
      my_printf(yellow, "Wrkr: %u, Read Reply no %d, opcode :%u message mes_size %d \n",
                t_id, i, r_rep_mes->opcode, send_sgl[mes_i].length);
    my_printf(green, "Wrkr %d : I send a READ REPLY message of %u r reps with mes_size %u, with lid: %u to machine %u \n",
              t_id, coalesce_num, send_sgl[mes_i].length,
              r_rep_mes->l_id, r_rep_fifo->rem_m_id[r_rep_i]);
  }
  if (ENABLE_ASSERTIONS) {
    assert(send_sgl[mes_i].length < MTU);
    assert(send_sgl[mes_i].length <= R_REP_SEND_SIZE);
    assert(r_rep_fifo->rem_m_id[r_rep_i] < MACHINE_NUM);
    assert(coalesce_num > 0);
  }
}



// check the local id of a read reply
static inline void check_r_rep_l_id(uint64_t l_id, uint8_t r_rep_num, uint64_t pull_lid,
                                    uint32_t r_size, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(l_id + r_rep_num <= pull_lid + r_size);
  }
}


static inline void check_a_polled_r_rep(struct r_rep_big *r_rep,
                                        struct r_rep_message *r_rep_mes,
                                        uint16_t r_rep_i, uint8_t r_rep_num,
                                        uint16_t t_id) {
  if (ENABLE_ASSERTIONS) {
    uint8_t opcode = r_rep->opcode;
    if (opcode > CARTS_EQUAL) opcode -= FALSE_POSITIVE_OFFSET;
    //check_state_with_allowed_flags(8, opcode, TS_TOO_HIGH, TS_EQUAL, TS_GREATER_TS_ONLY, TS_GREATER,
    //                              LOG_TOO_HIGH, LOG_TOO_SMALL, LOG_EQUAL);

    if ((r_rep->opcode < TS_TOO_HIGH || r_rep->opcode > CARTS_EQUAL) &&
        (r_rep->opcode < TS_TOO_HIGH + FALSE_POSITIVE_OFFSET ||
         r_rep->opcode > CARTS_EQUAL + FALSE_POSITIVE_OFFSET)) {
      my_printf(red, "Receiving r_rep: Opcode %u, i %u/%u \n", r_rep->opcode, r_rep_i, r_rep_num);
      assert(false);
    }
  }
}



// Check when inserting a read
static inline void check_previous_read_lid(uint8_t source, uint8_t opcode, uint64_t message_l_id,
                                           struct r_message *r_mes, uint32_t r_mes_ptr, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (source == FROM_TRACE) assert(opcode != PROPOSE_OP);
    if (message_l_id > MAX_READ_COALESCE) {
      uint32_t prev_r_mes_ptr = (r_mes_ptr + R_FIFO_SIZE - 1) % R_FIFO_SIZE;
      if (r_mes[prev_r_mes_ptr].read[0].opcode != PROPOSE_OP) {
        uint64_t prev_l_id = r_mes[prev_r_mes_ptr].l_id;
        uint8_t prev_coalesce = r_mes[prev_r_mes_ptr].coalesce_num;
        if (message_l_id != prev_l_id + prev_coalesce) {
          my_printf(red, "Wrkr: %u Read: Current message l_id %u, previous message l_id %u , "
                      "prev push ptr %u, current push ptr %u ,previous coalesce %u, previous opcode %u \n",
                    t_id, message_l_id, prev_l_id, prev_r_mes_ptr, r_mes_ptr,
                    prev_coalesce, r_mes[prev_r_mes_ptr].read[0].opcode);
        }
      }
    }
  }
}

// Check when inserting a read
static inline void check_read_state_and_key(p_ops_t *p_ops, uint32_t r_ptr, uint8_t source, struct r_message *r_mes,
                                            r_info_t *r_info, uint32_t r_mes_ptr, struct read *read, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (p_ops->r_state[r_ptr] != INVALID)
      my_printf(red, "Worker %u r_state %d at r_ptr %u, kvs hits %lu, r_size %u \n",
                t_id, p_ops->r_state[r_ptr], r_ptr,
                t_stats[t_id].cache_hits_per_thread, p_ops->r_size);
    //printf("Sent %d, Valid %d, Ready %d \n", SENT, VALID, READY);
    assert(p_ops->r_state[r_ptr] == INVALID);
    //struct read *read = &r_mes[r_mes_ptr].read[inside_r_ptr];
//    if (read->opcode == 0) my_printf(red, "R_mes_ptr %u, inside_r_ptr %u, first read opcode %u source %u \n",
//      r_mes_ptr, inside_r_ptr, r_mes[r_mes_ptr].read[0].opcode);
    check_state_with_allowed_flags(5, read->opcode, KVS_OP_GET, OP_GET_TS,
                                   OP_ACQUIRE, OP_ACQUIRE_FLIP_BIT);
    if (source == FROM_TRACE) {
      assert(keys_are_equal(&read->key, &r_info->key));
      assert(compare_netw_ts_with_ts(&read->ts, &r_info->ts_to_read) == EQUAL);
    }
  }
}


// Returns true if the incoming key and the entry key are equal
static inline bool check_entry_validity_with_key(struct key *incoming_key, mica_op_t * kv_ptr)
{
  if (ENABLE_ASSERTIONS) {
    struct key *entry_key = &kv_ptr->key;
    return keys_are_equal(incoming_key, entry_key);
  }
  return true;
}

// When polling an ack message
static inline void check_ack_message_count_stats(p_ops_t* p_ops, ack_mes_t* ack,
                                                 uint32_t index, uint16_t ack_num, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(ack_num > 0);
    assert(ack->opcode == OP_ACK);
    //      wait_for_the_entire_ack((volatile ack_mes_t *)ack, t_id, index);
    assert(ack->m_id < MACHINE_NUM);
    uint64_t l_id = ack->l_id;
    uint64_t pull_lid = p_ops->local_w_id;
    assert(l_id + ack_num <= pull_lid + p_ops->w_size);
    if (DEBUG_ACKS)
      my_printf(yellow,
                "Wrkr %d  polled ack opcode %d with %d acks for l_id %lu, oldest lid %lu, at offset %d from machine %u \n",
                t_id, ack->opcode, ack_num, l_id, pull_lid, index, ack->m_id);
  }
  if (ENABLE_STAT_COUNTING) {
    if (ENABLE_ASSERTIONS) {
      t_stats[t_id].per_worker_acks_received[ack->m_id] += ack_num;
      t_stats[t_id].per_worker_acks_mes_received[ack->m_id]++;
    }
    t_stats[t_id].received_acks += ack_num;
    t_stats[t_id].received_acks_mes_num++;
  }
}


// When polling acks: more precisely when inspecting each l_id acked
static inline void  check_ack_and_print(p_ops_t* p_ops, uint16_t ack_i, uint32_t ack_ptr,
                                        uint16_t  ack_num, uint64_t l_id, uint64_t pull_lid, uint16_t t_id) {
  if (ENABLE_ASSERTIONS) {
    if (DEBUG_WRITES && (ack_ptr == p_ops->w_push_ptr)) {
      uint32_t origin_ack_ptr = (ack_ptr - ack_i + PENDING_WRITES) % PENDING_WRITES;
      my_printf(red, "Worker %u: Origin ack_ptr %u/%u, acks %u/%u, w_pull_ptr %u, w_push_ptr % u, w_size %u \n",
                t_id, origin_ack_ptr, (p_ops->w_pull_ptr + (l_id - pull_lid)) % PENDING_WRITES,
                ack_i, ack_num, p_ops->w_pull_ptr, p_ops->w_push_ptr, p_ops->w_size);

    }
    if (p_ops->w_meta[ack_ptr].acks_seen >= REM_MACH_NUM) {
      uint32_t origin_ack_ptr = (ack_ptr - ack_i + PENDING_WRITES) % PENDING_WRITES;
      my_printf(red, "Worker %u: acks seen %u/%d, w_state %u, max_pending _writes %d,"
                  "Origin ack_ptr %u/%u, acks %u/%u, w_pull_ptr %u, w_push_ptr % u, w_size %u \n",
                t_id, p_ops->w_meta[ack_ptr].acks_seen, REM_MACH_NUM,
                p_ops->w_meta[ack_ptr].w_state,
                PENDING_WRITES,
                origin_ack_ptr, (p_ops->w_pull_ptr + (l_id - pull_lid)) % PENDING_WRITES,
                ack_i, ack_num, p_ops->w_pull_ptr, p_ops->w_push_ptr, p_ops->w_size);
    }
    assert(p_ops->w_meta[ack_ptr].acks_seen < REM_MACH_NUM);
  }
}

// Check the key of the trace_op and the KVS
static inline void check_trace_op_key_vs_kv_ptr(trace_op_t* op, mica_op_t* kv_ptr)
{
  if (ENABLE_ASSERTIONS) {
    struct key *op_key = &op->key;
    struct key *kv_key = &kv_ptr->key;
    if (!keys_are_equal(kv_key, op_key)) {
      print_key(kv_key);
      print_key(op_key);
      assert(false);
    }
  }
}

// Check the key of the cache_op and  the KV
static inline void check_keys_with_one_trace_op(struct key *com_key, mica_op_t *kv_ptr)
{
  if (ENABLE_ASSERTIONS) {
    struct key *kv_key = &kv_ptr->key;
    if (!keys_are_equal(kv_key, com_key)) {
      print_key(kv_key);
      print_key(com_key);
      assert(false);
    }
  }
}

// When removing writes
static inline void check_after_removing_writes(p_ops_t* p_ops, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (p_ops->w_meta[p_ops->w_pull_ptr].w_state >= READY_RELEASE) {
      my_printf(red, "W state = %u at ptr  %u, size: %u \n",
                p_ops->w_meta[p_ops->w_pull_ptr].w_state, p_ops->w_pull_ptr, p_ops->w_size);
      assert(false);
    }
//    if (p_ops->w_meta[(p_ops->w_pull_ptr + 1) % PENDING_WRITES].w_state >= READY_RELEASE) {
//      my_printf(red, "W state = %u at ptr %u, push ptr %u , size: %u \n",
//                 p_ops->w_meta[(p_ops->w_pull_ptr + 1) % PENDING_WRITES].w_state,
//                 (p_ops->w_pull_ptr + 1) % PENDING_WRITES,
//                 p_ops->w_push_ptr, p_ops->w_size);
//      my_printf(red, "W state = %u at ptr %u, size: %u \n",
//                 p_ops->w_meta[p_ops->w_pull_ptr].w_state, p_ops->w_pull_ptr, p_ops->w_size);
//    }
  }
}

// Check that the counter for propose replies add up(SAME FOR ACCEPTS AND PROPS)
static inline void check_sum_of_reps(loc_entry_t *loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->rmw_reps.tot_replies == sum_of_reps(&loc_entry->rmw_reps));
    assert(loc_entry->rmw_reps.tot_replies <= MACHINE_NUM);
  }
}



static inline void check_loc_entry_metadata_is_reset(loc_entry_t* loc_entry,
                                                     const char *message, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) { // make sure the loc_entry is correctly set-up
    if (loc_entry->help_loc_entry == NULL) {
      //my_printf(red, "The help_loc_ptr is NULL. The reason is typically that help_loc_entry was passed to the function "
      //           "instead of loc entry to check \n");
      assert(loc_entry->state == INVALID_RMW);
    }
    else {
      if (loc_entry->help_loc_entry->state != INVALID_RMW) {
        my_printf(red, "Wrkr %u: %s \n", t_id, message);
        assert(false);
      }
      assert(loc_entry->rmw_reps.tot_replies == 1);
      assert(loc_entry->back_off_cntr == 0);
    }
  }
}


// When going to ack an accept/propose because the log it refers to is higher than what we are working on
static inline void check_that_log_is_high_enough(mica_op_t *kv_ptr, uint32_t log_no)
{
  if (ENABLE_ASSERTIONS) {
    assert(log_no > kv_ptr->last_committed_log_no);
    if (log_no == kv_ptr->last_committed_log_no + 1) {
      if (kv_ptr->state != INVALID_RMW) {
        my_printf(red, "Checking_that_log_is to high: log_no %u/%u, kv_ptr committed last_log %u, state %u \n",
                  log_no, kv_ptr->log_no, kv_ptr->last_committed_log_no, kv_ptr->state);
        assert(false);
      }
    } else if (kv_ptr->state != INVALID_RMW)
      assert(kv_ptr->last_committed_log_no + 1);
  }
}

//
static inline void check_log_nos_of_kv_ptr(mica_op_t *kv_ptr, const char *message, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->state != INVALID_RMW) {
      if (kv_ptr->last_committed_rmw_id.id == kv_ptr->rmw_id.id) {
        my_printf(red, "Wrkr %u Last committed rmw id is equal to current, kv_ptr state %u, com log/log %u/%u "
                    "rmw id %u/%u,  %s \n",
                  t_id, kv_ptr->state, kv_ptr->last_committed_log_no, kv_ptr->log_no,
                  kv_ptr->last_committed_rmw_id.id, kv_ptr->rmw_id.id,
                   message);
        assert(false);
      }

      if (kv_ptr->last_committed_log_no >= kv_ptr->log_no) {
        my_printf(red, "Wrkr %u t_id, kv_ptr state %u, com log/log %u/%u : %s \n",
                  t_id, kv_ptr->state, kv_ptr->last_committed_log_no, kv_ptr->log_no, message);
        assert(false);
      }
    }
  }
}

//
static inline void check_for_same_ts_as_already_proposed(mica_op_t *kv_ptr, struct propose *prop, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->state == PROPOSED) {
      if (compare_netw_ts_with_ts(&prop->ts, &kv_ptr->prop_ts) == EQUAL) {
        my_printf(red, "Wrkr %u Received a proposal with same TS as an already acked proposal, "
                    " prop log/kv_ptr log %u/%u, , rmw_id %u/%u, version %u/%u, m_id %u/%u \n",
                  t_id, prop->log_no, kv_ptr->log_no,
                  prop->t_rmw_id, kv_ptr->rmw_id.id,
                  prop->ts.version, kv_ptr->prop_ts.version, prop->ts.m_id, kv_ptr->prop_ts.m_id);
        assert(false);
      }
    }
  }
}

static inline void verify_paxos(loc_entry_t *loc_entry, uint16_t t_id)
{
  if (VERIFY_PAXOS && is_global_ses_id_local((uint32_t)loc_entry->rmw_id.id % GLOBAL_SESSION_NUM, t_id)) {
    //if (committed_log_no != *(uint32_t *)loc_entry->value_to_write)
    //  red_printf ("vale_to write/log no %u/%u",
    //             *(uint32_t *)loc_entry->value_to_write, committed_log_no );
    uint64_t val = *(uint64_t *)loc_entry->value_to_read;
    //assert(val == loc_entry->accepted_log_no - 1);
    fprintf(rmw_verify_fp[t_id], "%u %lu %u \n", loc_entry->key.bkt, val, loc_entry->accepted_log_no);
  }
}


static inline void check_last_registered_rmw_id(loc_entry_t *loc_entry,
                                                mica_op_t *kv_ptr, uint8_t helping_flag, uint16_t t_id)
{
//  if (ENABLE_ASSERTIONS) {
//    if (kv_ptr->last_registered_log_no != loc_entry->log_no - 1) {
//      my_printf(red, "Last registered/last-committed/working  %u/%u/%u, key %u, helping flag %u \n",
//                kv_ptr->last_registered_log_no, kv_ptr->last_committed_log_no,
//                loc_entry->log_no, loc_entry->key.bkt, helping_flag);
//      sleep(2);
//      assert(false);
//    }
//    if (loc_entry->log_no == kv_ptr->last_committed_log_no + 1) {
//      if (!rmw_ids_are_equal(&kv_ptr->last_registered_rmw_id, &kv_ptr->last_committed_rmw_id)) {
//        my_printf(red,
//                  "Wrkr %u, filling help loc entry last registered rmw id, help log no/ kv_ptr last committed log no %u/%u,"
//                    "glob rmw ids: last committed/last registered %lu/%lu \n", t_id,
//                  loc_entry->log_no, kv_ptr->last_committed_log_no,
//                  kv_ptr->last_registered_rmw_id.id, kv_ptr->last_committed_rmw_id.id);
//      }
//      assert(rmw_ids_are_equal(&kv_ptr->last_registered_rmw_id, &kv_ptr->last_committed_rmw_id));
//    }
//      // If I am helping log_no X, without having committed log_no X-1, the i better have the correct last registered RMW-id
//    else if (loc_entry->log_no > kv_ptr->last_committed_log_no + 1) {
//      assert(!rmw_ids_are_equal(&kv_ptr->last_registered_rmw_id, &kv_ptr->last_committed_rmw_id));
//    } else
//      assert(false);
//  }
}


static inline void check_that_the_rmw_ids_match(mica_op_t *kv_ptr, uint64_t rmw_id,
                                                 uint32_t log_no, uint32_t version,
                                                uint8_t m_id, const char *message, uint16_t t_id)
{
  uint64_t glob_sess_id = rmw_id % GLOBAL_SESSION_NUM;
  if (kv_ptr->last_committed_rmw_id.id != rmw_id) {
    my_printf(red, "~~~~~~~~COMMIT MISSMATCH Worker %u key: %u, Log %u %s ~~~~~~~~ \n", t_id, kv_ptr->key.bkt, log_no, message);
    my_printf(green, "GLOBAL ENTRY COMMITTED log %u: rmw_id %lu glob_sess-id- %u\n",
              kv_ptr->last_committed_log_no, kv_ptr->last_committed_rmw_id.id,
              kv_ptr->last_committed_rmw_id.id % GLOBAL_SESSION_NUM);
    my_printf(yellow, "COMMIT log %u: rmw_id %lu glob_sess-id-%u version %u m_id %u \n",
              log_no, rmw_id, glob_sess_id, version, m_id);
    /*if (ENABLE_DEBUG_RMW_KV_PTR) {
      my_printf(green, "GLOBAL ENTRY COMMITTED log %u: rmw_id %lu glob_sess-id- %u, FLAG %u\n",
                   kv_ptr->last_committed_log_no, kv_ptr->last_committed_rmw_id.id,
                   kv_ptr->last_committed_rmw_id.glob_sess_id, kv_ptr->dbg->last_committed_flag);
      my_printf(yellow, "COMMIT log %u: rmw_id %lu glob_sess-id-%u version %u m_id %u \n",
                    log_no, rmw_id, glob_sess_id, version, m_id);
      if (kv_ptr->dbg->last_committed_flag <= 1) {
        my_printf(cyan, "PROPOSED log %u: rmw_id %lu glob_sess-id-%u version %u m_id %u \n",
                    kv_ptr->dbg->proposed_log_no, kv_ptr->dbg->proposed_rmw_id.id,
                    kv_ptr->dbg->proposed_rmw_id.glob_sess_id,
                    kv_ptr->dbg->proposed_ts.version, kv_ptr->dbg->proposed_ts.m_id);


        my_printf(cyan, "LAST COMMIT log %u: rmw_id %lu glob_sess-id-%u version %u m_id %u \n",
                    kv_ptr->dbg->last_committed_log_no, kv_ptr->dbg->last_committed_rmw_id.id,
                    kv_ptr->dbg->last_committed_rmw_id.glob_sess_id,
                    kv_ptr->dbg->last_committed_ts.version, kv_ptr->dbg->last_committed_ts.m_id);

      }
    }*/
    assert(false);
  }
}


// After registering, make sure the registered is bigger/equal to what is saved as registered
static inline void check_registered_against_kv_ptr_last_committed(mica_op_t *kv_ptr,
                                                                  uint64_t committed_id,
                                                                  const char *message, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    uint32_t committed_glob_ses_id = (uint32_t)(committed_id % GLOBAL_SESSION_NUM);
    uint32_t glob_sess_id = (uint32_t)(kv_ptr->last_committed_rmw_id.id % GLOBAL_SESSION_NUM);
    uint64_t id = kv_ptr->last_committed_rmw_id.id;
    assert(glob_sess_id < GLOBAL_SESSION_NUM);
    if (committed_glob_sess_rmw_id[glob_sess_id] < id) {
      my_printf(yellow, "Committing %s rmw_id: %u glob_sess_id: %u \n", message, committed_id, committed_glob_ses_id);
      my_printf(red, "Wrkr %u: %s rmw_id: kv_ptr last committed %lu, "
                  "glob_sess_id :kv_ptr last committed %u,"
                  "committed_glob_sess_rmw_id %lu,   \n", t_id, message,
                kv_ptr->last_committed_rmw_id.id,
                glob_sess_id,
                committed_glob_sess_rmw_id[glob_sess_id]);
      //assert(false);
    }
  }
}

// Perofrm checks after receiving a rep to commit an RMW
static inline void check_local_commit_from_rep(mica_op_t *kv_ptr, loc_entry_t *loc_entry,
                                               struct rmw_rep_last_committed *rmw_rep, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->state != INVALID_RMW) {
      loc_entry_t *working_entry = loc_entry->helping_flag == HELPING ?
                                              loc_entry->help_loc_entry : loc_entry;
      if (kv_ptr->rmw_id.id == working_entry->rmw_id.id &&
          kv_ptr->log_no == working_entry->log_no) {
        my_printf(red, "Wrkr: %u Received a rep opcode %u for rmw id %lu , log no %u "
                    "received highest committed log %u with rmw_id id %u, "
                    "but kv_ptr is in state %u, for rmw_id %u, on log no %u\n",
                  t_id, rmw_rep->opcode, working_entry->rmw_id.id,
                  working_entry->log_no,
                  rmw_rep->log_no_or_base_version, rmw_rep->rmw_id,
                  kv_ptr->state, kv_ptr->rmw_id.id,
                  kv_ptr->log_no);
        assert(rmw_rep->opcode == RMW_ID_COMMITTED);
      }
      assert(kv_ptr->rmw_id.id !=rmw_rep->rmw_id);
    }
  }
}

static inline void check_when_polling_for_reads(struct r_message *r_mes, uint32_t index,
                                                uint32_t polled_reads, uint16_t t_id)
{
  uint8_t r_num = r_mes->coalesce_num;
  if (ENABLE_ASSERTIONS) {
    //struct prop_message *p_mes = (struct prop_message *)r_mes;
    struct read *read = &r_mes->read[0];
    struct propose *prop =(struct propose *)&r_mes->read[0];
    assert(r_mes->coalesce_num > 0);
    if (DEBUG_READ_REPS)
      printf("Worker %u sees a read Opcode %d at offset %d, l_id %lu  \n", t_id,
             read->opcode, index, r_mes->l_id);
    else if (DEBUG_RMW && r_mes->read[0].opcode == PROPOSE_OP) {
      //struct prop_message *prop_mes = (struct prop_message *) r_mes;
      my_printf(cyan, "Worker %u sees a Propose from m_id %u: opcode %d at offset %d, rmw_id %lu, "
                  "log_no %u, coalesce_num %u version %u \n",
                t_id, r_mes->m_id, prop->opcode, index, prop->t_rmw_id,
                prop->log_no,
                r_mes->coalesce_num, prop->ts.version);
    }
    if (polled_reads + r_num > MAX_INCOMING_R) assert(false);
  }
  if (ENABLE_STAT_COUNTING) {
    if (ENABLE_ASSERTIONS) t_stats[t_id].per_worker_reads_received[r_mes->m_id] += r_num;
    t_stats[t_id].received_reads += r_num;
    t_stats[t_id].received_reads_mes_num++;
  }
}

static inline void check_read_opcode_when_polling_for_reads(struct read *read, uint16_t read_i,
                                                            uint16_t r_num, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    //assert(MAX_PROP_COALESCE == 1); // this function won't work otherwise
    check_state_with_allowed_flags(5, read->opcode,
                                   OP_GET_TS, OP_ACQUIRE,
                                   KVS_OP_GET, OP_ACQUIRE_FLIP_BIT);
    if (read->opcode != KVS_OP_GET && read->opcode != OP_ACQUIRE &&
        read->opcode != OP_GET_TS &&
        read->opcode != OP_ACQUIRE_FLIP_BIT)
      my_printf(red, "Receiving read: Opcode %u, i %u/%u \n", read->opcode, read_i, r_num);
  }
}




static inline void debug_fail_help(loc_entry_t *loc_entry, const char *message, uint16_t t_id)
{
  if (DEBUG_RMW) {
    if (loc_entry->helping_flag == PROPOSE_NOT_LOCALLY_ACKED && t_id == 0)
      my_printf(cyan, "Sess %u %s \n", loc_entry->sess_id, message);
  }
}

// When stealing kv_ptr from a stuck proposal, check that the proposal was referring to a valid log no
static inline void check_the_proposed_log_no(mica_op_t *kv_ptr, loc_entry_t *loc_entry,
                                             uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->log_no > kv_ptr->last_committed_log_no + 1) {
      my_printf(red, "Key %u Last committed//accepted/active %u/%u/%u \n", loc_entry->key.bkt,
                kv_ptr->last_committed_log_no,
                kv_ptr->accepted_log_no,
                kv_ptr->log_no);
      assert(false);
    }
  }
}



static inline void debug_set_version_of_op_to_one(trace_op_t *op, uint8_t opcode,
                                                  uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    bool is_update = (opcode == (uint8_t) KVS_OP_PUT ||
                      opcode == (uint8_t) OP_RELEASE);
    assert(WRITE_RATIO > 0 || is_update == 0);
    if (is_update) assert(op->val_len > 0);
    op->ts.version = 1;
  }
}




static inline void check_all_w_meta(p_ops_t* p_ops, uint16_t t_id, const char* message)
{
  if (ENABLE_ASSERTIONS) ;
  for (uint16_t i = 0; i < PENDING_WRITES; i++) {
    per_write_meta_t *w_meta = &p_ops->w_meta[i];
    uint8_t w_state = w_meta->w_state;
    check_state_with_allowed_flags(5, w_state, INVALID, VALID, SENT_COMMIT, READY_COMMIT);
    uint8_t id_to_check = (uint8_t) (machine_id == 0 ? 0 : 1);
    if (w_state == SENT_COMMIT) {
      if (w_meta->acks_expected == (MACHINE_NUM - 2) && (w_meta->expected_ids[id_to_check] == MACHINE_THAT_SLEEPS))
        my_printf(red, "Wrkr %d, ptr %u, expected %u, expected_id[%d]= %d | %s \n", t_id, i,
                  w_meta->acks_expected, id_to_check, w_meta->expected_ids[id_to_check],
                  message);
    }
  }
}




// called when sending read replies
static inline void print_check_count_stats_when_sending_r_rep(struct r_rep_fifo *r_rep_fifo,
                                                              uint8_t coalesce_num,
                                                              uint16_t mes_i, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    uint32_t pull_ptr = r_rep_fifo->pull_ptr;
    struct r_rep_message *r_rep_mes = (struct r_rep_message *) &r_rep_fifo->r_rep_message[pull_ptr];
    check_state_with_allowed_flags(6, r_rep_mes->opcode, ACCEPT_REPLY_NO_CREDITS, ACCEPT_REPLY,
                                   PROP_REPLY, READ_REPLY, READ_PROP_REPLY);
    uint16_t byte_ptr = R_REP_MES_HEADER;
    struct r_rep_big *r_rep;
    struct rmw_rep_last_committed *rmw_rep;
    assert(r_rep_mes->coalesce_num > 0 && r_rep_mes->coalesce_num <= MAX_R_REP_COALESCE);
    for (uint8_t i = 0; i < r_rep_mes->coalesce_num; i++) {
      r_rep = (struct r_rep_big *)(((void *) r_rep_mes) + byte_ptr);
      uint8_t opcode = r_rep->opcode;
      //if (byte_ptr > 505)
      //printf("%u/%u \n", byte_ptr, r_rep_fifo->message_sizes[pull_ptr]);
      if (opcode > CARTS_EQUAL) opcode -= FALSE_POSITIVE_OFFSET;
      if (opcode < TS_TOO_HIGH || opcode > CARTS_EQUAL)
        printf("R_rep %u/%u, byte ptr %u/%u opcode %u/%u \n",
               i, r_rep_mes->coalesce_num, byte_ptr, r_rep_fifo->message_sizes[pull_ptr],
               opcode, r_rep_mes->opcode);


      assert(opcode >= TS_TOO_HIGH && opcode <= CARTS_EQUAL);
      bool is_rmw = false, is_rmw_acquire = false;
      if (opcode >= RMW_ACK && opcode <= NO_OP_PROP_REP)
        is_rmw = true;
      else if (opcode > NO_OP_PROP_REP)
        is_rmw_acquire = true;

      if (is_rmw) {
        check_state_with_allowed_flags(6, r_rep_mes->opcode, ACCEPT_REPLY_NO_CREDITS, ACCEPT_REPLY,
                                       PROP_REPLY, READ_PROP_REPLY);
        rmw_rep = (struct rmw_rep_last_committed *) r_rep;
        assert(opcode_is_rmw_rep(rmw_rep->opcode));
      }
      byte_ptr += get_size_from_opcode(r_rep->opcode);

    }
    //if (r_rep->opcode > CARTS_EQUAL) printf("big opcode comes \n");
    //check_a_polled_r_rep(r_rep, r_rep_mes, i, r_rep_num, t_id);
    if (DEBUG_READ_REPS)
      printf("Wrkr %d has %u read replies to send \n", t_id, r_rep_fifo->total_size);
    if (ENABLE_ASSERTIONS) {
      assert(r_rep_fifo->total_size >= coalesce_num);
      assert(mes_i < MAX_R_REP_WRS);
    }
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[t_id].r_reps_sent += coalesce_num;
    t_stats[t_id].r_reps_sent_mes_num++;
  }
}

static inline void checks_when_handling_prop_acc_rep(loc_entry_t *loc_entry,
                                                     struct rmw_rep_last_committed *rep,
                                                     bool is_accept, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    rmw_rep_info_t *rep_info = &loc_entry->rmw_reps;
    assert(rep_info->tot_replies > 0);
    if (is_accept) assert(loc_entry->state == ACCEPTED);
    else {
      assert(loc_entry->state == PROPOSED);
      // this checks that the performance optimization of NO-op reps is valid
      assert(rep->opcode != NO_OP_PROP_REP);
      check_state_with_allowed_flags(4, loc_entry->helping_flag, NOT_HELPING,
                                     PROPOSE_NOT_LOCALLY_ACKED, PROPOSE_LOCALLY_ACCEPTED);
      if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED ||
          loc_entry->helping_flag == PROPOSE_NOT_LOCALLY_ACKED)
        assert(rep_info->already_accepted > 0);
    }
  }
}
/*--------------------------------------------------------------------------
 * --------------------ACCEPTING-------------------------------------
 * --------------------------------------------------------------------------*/

static inline void checks_preliminary_local_accept(mica_op_t *kv_ptr,
                                                   loc_entry_t *loc_entry,
                                                   uint16_t t_id)
{
  my_assert(keys_are_equal(&loc_entry->key, &kv_ptr->key),
            "Attempt local accept: Local entry does not contain the same key as kv_ptr");

  if (ENABLE_ASSERTIONS) assert(loc_entry->glob_sess_id < GLOBAL_SESSION_NUM);
}

static inline void checks_before_local_accept(mica_op_t *kv_ptr,
                                              loc_entry_t *loc_entry,
                                              uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    //assert(compare_ts(&loc_entry->new_ts, &kv_ptr->prop_ts) == EQUAL);
    assert(kv_ptr->log_no == loc_entry->log_no);
    assert(kv_ptr->last_committed_log_no == loc_entry->log_no - 1);
  }

  if (DEBUG_RMW)
    my_printf(green, "Wrkr %u got rmw id %u, accepted locally \n",
              t_id, loc_entry->rmw_id.id);
}


static inline void checks_after_local_accept(mica_op_t *kv_ptr,
                                              loc_entry_t *loc_entry,
                                              uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->accepted_log_no == loc_entry->log_no);
    assert(loc_entry->log_no == kv_ptr->last_committed_log_no + 1);
    assert(compare_ts(&kv_ptr->prop_ts, &kv_ptr->accepted_ts) != SMALLER);
    kv_ptr->accepted_rmw_id = kv_ptr->rmw_id;
  }
  if (ENABLE_DEBUG_RMW_KV_PTR) {
    //kv_ptr->dbg->proposed_ts = loc_entry->new_ts;
    //kv_ptr->dbg->proposed_log_no = loc_entry->log_no;
    //kv_ptr->dbg->proposed_rmw_id = loc_entry->rmw_id;
  }
  check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept and succeed", t_id);
}


static inline void checks_after_failure_to_locally_accept(mica_op_t *kv_ptr,
                                                          loc_entry_t *loc_entry,
                                                          uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(green, "Wrkr %u failed to get rmw id %u, accepted locally "
                "kv_ptr rmw id %u, state %u \n",
              t_id, loc_entry->rmw_id.id,
              kv_ptr->rmw_id.id, kv_ptr->state);
  // --CHECKS--
  if (ENABLE_ASSERTIONS) {
    if (kv_ptr->state == PROPOSED || kv_ptr->state == ACCEPTED) {
      if(!(compare_ts(&kv_ptr->prop_ts, &loc_entry->new_ts) == GREATER ||
           kv_ptr->log_no > loc_entry->log_no)) {
        my_printf(red, "State: %s,  loc-entry-helping %d, Kv prop/base_ts %u/%u -- loc-entry base_ts %u/%u, "
                    "kv-log/loc-log %u/%u kv-rmw_id/loc-rmw-id %u/%u\n",
                  kv_ptr->state == ACCEPTED ? "ACCEPTED" : "PROPOSED",
                  loc_entry->helping_flag,
                  kv_ptr->prop_ts.version, kv_ptr->prop_ts.m_id,
                  loc_entry->new_ts.version, loc_entry->new_ts.m_id,
                  kv_ptr->log_no, loc_entry->log_no,
                  kv_ptr->rmw_id.id, loc_entry->rmw_id.id);
        assert(false);
      }
    }
    else if (kv_ptr->state == INVALID_RMW) // some other rmw committed
      // with cancelling it is possible for some other RMW to stole and then cancelled itself
      if (!ENABLE_CAS_CANCELLING) assert(kv_ptr->last_committed_log_no >= loc_entry->log_no);
  }


  check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept and fail", t_id);
}


static inline void checks_acting_on_quorum_of_prop_ack(loc_entry_t *loc_entry, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (loc_entry->helping_flag == PROPOSE_LOCALLY_ACCEPTED) {
      assert(loc_entry->rmw_reps.tot_replies >= QUORUM_NUM);
      assert(loc_entry->rmw_reps.already_accepted >= 0);
      assert(loc_entry->rmw_reps.seen_higher_prop_acc == 0);
      assert(glob_ses_id_to_t_id((uint32_t) (loc_entry->rmw_id.id % GLOBAL_SESSION_NUM)) == t_id &&
             glob_ses_id_to_m_id((uint32_t) (loc_entry->rmw_id.id % GLOBAL_SESSION_NUM)) == machine_id);

    }
  }
}

static inline void checks_preliminary_local_accept_help(mica_op_t *kv_ptr,
                                                        loc_entry_t *loc_entry,
                                                        loc_entry_t *help_loc_entry)
{
  if (ENABLE_ASSERTIONS) {
    my_assert(keys_are_equal(&help_loc_entry->key, &kv_ptr->key),
              "Attempt local accpet to help: Local entry does not contain the same key as kv_ptr");
    my_assert(loc_entry->help_loc_entry->log_no == loc_entry->log_no,
              " the help entry and the regular have not the same log nos");
    assert(help_loc_entry->glob_sess_id < GLOBAL_SESSION_NUM);
    assert(loc_entry->log_no == help_loc_entry->log_no);
  }
}

static inline void checks_and_prints_local_accept_help(loc_entry_t *loc_entry,
                                                       loc_entry_t* help_loc_entry,
                                                       mica_op_t *kv_ptr, bool kv_ptr_is_the_same,
                                                       bool kv_ptr_is_invalid_but_not_committed,
                                                       bool helping_stuck_accept,
                                                       bool propose_locally_accepted,
                                                       uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(compare_ts(&kv_ptr->prop_ts, &help_loc_entry->new_ts) != SMALLER);
    assert(kv_ptr->last_committed_log_no == help_loc_entry->log_no - 1);
    if (kv_ptr_is_invalid_but_not_committed) {
      printf("last com/log/help-log/loc-log %u/%u/%u/%u \n",
             kv_ptr->last_committed_log_no, kv_ptr->log_no,
             help_loc_entry->log_no, loc_entry->log_no);
      assert(false);
    }
    // if the TS are equal it better be that it is because it remembers the proposed request
    if (kv_ptr->state != INVALID_RMW &&
        compare_ts(&kv_ptr->prop_ts, &loc_entry->new_ts) == EQUAL && !helping_stuck_accept &&
        !helping_stuck_accept && !propose_locally_accepted) {
      assert(kv_ptr->rmw_id.id == loc_entry->rmw_id.id);
      if (kv_ptr->state != PROPOSED) {
        my_printf(red, "Wrkr: %u, state %u \n", t_id, kv_ptr->state);
        assert(false);
      }
    }
    if (propose_locally_accepted)
      assert(compare_ts(&help_loc_entry->new_ts, &kv_ptr->accepted_ts) == GREATER);
  }
  if (DEBUG_RMW)
    my_printf(green, "Wrkr %u on attempting to locally accept to help "
                "got rmw id %u, accepted locally \n",
              t_id, help_loc_entry->rmw_id.id);
}

static inline void checks_after_local_accept_help(mica_op_t *kv_ptr,
                                                  loc_entry_t *loc_entry,
                                                  uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(compare_ts(&kv_ptr->prop_ts, &kv_ptr->accepted_ts) != SMALLER);
    kv_ptr->accepted_rmw_id = kv_ptr->rmw_id;
    check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept_to_help and succeed", t_id);
  }
}


static inline void checks_after_failure_to_locally_accept_help(mica_op_t *kv_ptr,
                                                               loc_entry_t *loc_entry,
                                                               uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(green, "Wrkr %u sess %u failed to get rmw id %u, accepted locally "
                "kv_ptr rmw id %u, state %u \n",
              t_id, loc_entry->sess_id, loc_entry->rmw_id.id,
              kv_ptr->rmw_id.id, kv_ptr->state);


  check_log_nos_of_kv_ptr(kv_ptr, "attempt_local_accept_to_help and fail", t_id);
}

static inline void checks_acting_on_already_accepted_rep(loc_entry_t *loc_entry, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    loc_entry_t* help_loc_entry = loc_entry->help_loc_entry;
    assert(loc_entry->log_no == help_loc_entry->log_no);
    assert(loc_entry->help_loc_entry->state == ACCEPTED);
    assert(compare_ts(&help_loc_entry->new_ts, &loc_entry->new_ts) == SMALLER);
  }
}

/*--------------------------------------------------------------------------
 * --------------------COMMITS-------------------------------------
 * --------------------------------------------------------------------------*/
static inline void check_inputs_commit_algorithm(mica_op_t *kv_ptr,
                                                 commit_info_t *com_info,
                                                 uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(kv_ptr != NULL);
    if (com_info->value == NULL) assert(com_info->no_value);
    if (com_info->log_no == 0) assert(com_info->rmw_id.id == 0);
    if (com_info->rmw_id.id == 0) assert(com_info->log_no == 0);
    if (!com_info->overwrite_kv)
      assert(com_info->flag == FROM_LOCAL ||
             com_info->flag == FROM_ALREADY_COMM_REP);
  }
}

static inline void check_on_overwriting_commit_algorithm(mica_op_t *kv_ptr,
                                                         commit_info_t *com_info,
                                                         compare_t cart_comp,
                                                         uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (cart_comp == EQUAL) {
      assert(kv_ptr->last_committed_log_no == com_info->log_no);
      assert(kv_ptr->last_committed_rmw_id.id == com_info->rmw_id.id);
      assert(memcmp(kv_ptr->value, com_info->value, (size_t) 8) == 0);
    }
    else if (cart_comp == SMALLER) {
      assert(compare_ts(&com_info->base_ts, &kv_ptr->ts) == SMALLER ||
             (compare_ts(&com_info->base_ts, &kv_ptr->ts) == EQUAL &&
               com_info->log_no < kv_ptr->last_committed_log_no));
    }
  }
}

static inline void check_on_updating_rmw_meta_commit_algorithm(mica_op_t *kv_ptr,
                                                               commit_info_t *com_info,
                                                               uint16_t t_id)
{
  if (kv_ptr->last_committed_log_no < com_info->log_no) {
    if (DEBUG_RMW)
      my_printf(green, "Wrkr %u commits locally rmw id %u: %s \n",
                t_id, com_info->rmw_id, com_info->message);
    update_commit_logs(t_id, kv_ptr->key.bkt, com_info->log_no, kv_ptr->value,
                       com_info->value, com_info->message, LOG_COMS);
  }
  else if (kv_ptr->last_committed_log_no == com_info->log_no) {
    check_that_the_rmw_ids_match(kv_ptr,  com_info->rmw_id.id, com_info->log_no,
    com_info->base_ts.version, com_info->base_ts.m_id,
    com_info->message, t_id);
  }
}

static inline void check_state_before_commit_algorithm(mica_op_t *kv_ptr,
                                                       commit_info_t *com_info,
                                                       uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (com_info->flag == FROM_LOCAL || com_info->flag == FROM_LOCAL_HELP) {
      // make sure that if we are on the same log
      if (kv_ptr->log_no == com_info->log_no) {
        if (!rmw_ids_are_equal(&com_info->rmw_id, &kv_ptr->rmw_id)) {
          my_printf(red, "kv_ptr is on same log as what is about to be committed but on different rmw-id \n");
          print_commit_info(com_info, yellow, t_id);
          print_kv_ptr(kv_ptr, cyan, t_id);
         // this is a hard error
          assert(false);
        }
        if (kv_ptr->state != INVALID_RMW) {
          if (kv_ptr->state != ACCEPTED) {
            my_printf(red, "Committing: Logs are equal, rmw-ids are equal "
              "but state is not accepted \n");
            print_commit_info(com_info, yellow, t_id);
            print_kv_ptr(kv_ptr, cyan, t_id);
            assert(false);
          }
        }
      }
      else {
        // if the log has moved on then the RMW has been helped,
        // it has been committed in the other machines so there is no need to change its state
        check_log_nos_of_kv_ptr(kv_ptr, "commit_helped_or_local_from_loc_entry", t_id);
        if (ENABLE_ASSERTIONS) {
          if (kv_ptr->state != INVALID_RMW)
            assert(!rmw_ids_are_equal(&kv_ptr->rmw_id, &com_info->rmw_id));
        }
      }
    }
    else if (com_info->flag == FROM_REMOTE_COMMIT_NO_VAL) {
      if (kv_ptr->last_committed_log_no < com_info->log_no) {
        if (ENABLE_ASSERTIONS) {
          assert(kv_ptr->state == ACCEPTED);
          assert(kv_ptr->log_no == com_info->log_no);
          assert(kv_ptr->accepted_rmw_id.id == com_info->rmw_id.id);
        }
      }
    }



  }
}

//------------------------------HELP STUCK RMW------------------------------------------

static inline void
checks_and_prints_proposed_but_not_locally_acked(p_ops_t *p_ops,
                                                 mica_op_t *kv_ptr,
                                                 loc_entry_t * loc_entry,
                                                 uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(cyan, "Wrkr %u, session %u helps RMW id %u with version %u, m_id %u,"
                " kv_ptr log/help log %u/%u kv_ptr committed log %u , "
                " stashed rmw_id: %u state %u \n",
              t_id, loc_entry->sess_id, loc_entry->rmw_id.id,
              loc_entry->new_ts.version, loc_entry->new_ts.m_id,
              kv_ptr->log_no, loc_entry->log_no, kv_ptr->last_committed_log_no,
              loc_entry->help_rmw->rmw_id.id, loc_entry->help_rmw->state);

  if (ENABLE_ASSERTIONS) {
    assert(p_ops->sess_info[loc_entry->sess_id].stalled);
    assert(loc_entry->rmw_reps.tot_replies == 0);
  }
}

static inline void logging_proposed_but_not_locally_acked(mica_op_t *kv_ptr,
                                                          loc_entry_t * loc_entry,
                                                          loc_entry_t *help_loc_entry,
                                                          uint16_t t_id)
{
  if (PRINT_LOGS && ENABLE_DEBUG_RMW_KV_PTR)
    fprintf(rmw_verify_fp[t_id], "Key: %u, log %u: Prop-not-locally accepted: helping rmw_id %lu, "
              "version %u, m_id: %u, From: rmw_id %lu, with version %u, m_id: %u \n",
            loc_entry->key.bkt, loc_entry->log_no, help_loc_entry->rmw_id.id,
            help_loc_entry->new_ts.version, help_loc_entry->new_ts.m_id, loc_entry->rmw_id.id,
            loc_entry->new_ts.version, loc_entry->new_ts.m_id);
}


static inline void checks_init_attempt_to_grab_kv_ptr(loc_entry_t * loc_entry,
                                                      uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->rmw_id.id % GLOBAL_SESSION_NUM == loc_entry->glob_sess_id);
    assert(!loc_entry->rmw_reps.ready_to_inspect);
    assert(loc_entry->rmw_reps.tot_replies == 0);
  }
}



static inline void print_when_grabbing_kv_ptr(loc_entry_t * loc_entry,
                                              uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(yellow, "Wrkr %u, after waiting for %u cycles, session %u  \n",
              t_id, loc_entry->back_off_cntr, loc_entry->sess_id);
}


static inline void print_when_state_changed_not_grabbing_kv_ptr(mica_op_t *kv_ptr,
                                                                loc_entry_t * loc_entry,
                                                                uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(yellow, "Wrkr %u, session %u changed who is waiting: waited for %u cycles on "
                "state %u rmw_id %u , now waiting on rmw_id %u , state %u\n",
              t_id, loc_entry->sess_id, loc_entry->back_off_cntr,
              loc_entry->help_rmw->state, loc_entry->help_rmw->rmw_id.id,
              kv_ptr->rmw_id.id, kv_ptr->state);
}


static inline void check_and_print_when_rmw_fails(mica_op_t *kv_ptr,
                                                  loc_entry_t * loc_entry,
                                                  uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(loc_entry->accepted_log_no == 0);
    assert(loc_entry->killable);
    assert(ENABLE_CAS_CANCELLING);
  }
  if (DEBUG_RMW)
    printf("Cancelling on needing kv_ptr Wrkr%u, sess %u, rmw_failing \n",
           t_id, loc_entry->sess_id);

}


static inline void checks_attempt_to_help_locally_accepted(mica_op_t *kv_ptr,
                                                           loc_entry_t * loc_entry,
                                                           uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(kv_ptr->accepted_log_no == kv_ptr->log_no);
    assert(kv_ptr->prop_ts.version > kv_ptr->accepted_ts.version);
    assert(rmw_ids_are_equal(&kv_ptr->rmw_id, &kv_ptr->accepted_rmw_id));
    assert(loc_entry->key.bkt == kv_ptr->key.bkt);
    assert(kv_ptr->state == ACCEPTED);
  }
}

static inline void print_when_state_changed_steal_proposed(mica_op_t *kv_ptr,
                                                           loc_entry_t * loc_entry,
                                                           uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(yellow, "Wrkr %u, session %u on attempting to steal the propose, changed who is "
                "waiting: waited for %u cycles for state %u "
                "rmw_id %u  state %u,  now waiting on rmw_id % , state %u\n",
              t_id, loc_entry->sess_id, loc_entry->back_off_cntr,
              loc_entry->help_rmw->state, loc_entry->help_rmw->rmw_id.id,
              kv_ptr->rmw_id.id, kv_ptr->state);
}


static inline void print_after_stealing_proposed(mica_op_t *kv_ptr,
                                                 loc_entry_t * loc_entry,
                                                 uint16_t t_id)
{
  if (DEBUG_RMW)
    my_printf(cyan, "Wrkr %u: session %u steals kv_ptr to do its propose \n",
              t_id, loc_entry->sess_id);
}


//-------------------------SENDING ACKS
static inline void checks_stats_prints_when_sending_acks(ack_mes_t *acks,
                                                         uint8_t m_i, uint16_t t_id)
{
  if (ENABLE_STAT_COUNTING) {
    t_stats[t_id].per_worker_acks_sent[m_i] += acks[m_i].ack_num;
    t_stats[t_id].per_worker_acks_mes_sent[m_i]++;
    t_stats[t_id].acks_sent += acks[m_i].ack_num;
    t_stats[t_id].acks_sent_mes_num++;
  }
  if (DEBUG_ACKS)
    my_printf(yellow, "Wrkr %d is sending an ack for lid %lu, credits %u and ack num %d and m id %d \n",
              t_id, acks[m_i].l_id, acks[m_i].credits, acks[m_i].ack_num, acks[m_i].m_id);

  if (ENABLE_ASSERTIONS) {
    assert(acks[m_i].credits <= acks[m_i].ack_num);
    if (acks[m_i].ack_num > MAX_MES_IN_WRITE) assert(acks[m_i].credits > 1);
    assert(acks[m_i].credits <= W_CREDITS);
    assert(acks[m_i].ack_num > 0);
  }
}

static inline void checks_when_posting_write_receives(recv_info_t *w_recv_info, uint32_t recvs_to_post_num,
                                                      uint8_t ack_i)
{
  if (DEBUG_RECEIVES) {
    //w_recv_info->posted_recvs += recvs_to_post_num;
    assert(w_recv_info->posted_recvs == MAX_RECV_W_WRS);
  }
  //w_recv_info->posted_recvs += recvs_to_post_num;
  // printf("Wrkr %d posting %u recvs and has a total of %u recvs for writes \n",
  //       g_id, recvs_to_post_num,  w_recv_info->posted_recvs);
  if (ENABLE_ASSERTIONS) {

    assert(recvs_to_post_num <= MAX_RECV_W_WRS);
    if (ack_i > 0) assert(recvs_to_post_num >= ack_i);
    if (W_CREDITS == 1) assert(recvs_to_post_num == ack_i);
    //assert(w_recv_info->posted_recvs <= MAX_RECV_W_WRS);
  }
}


#endif //KITE_DEBUG_UTIL_H

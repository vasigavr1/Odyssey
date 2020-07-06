//
// Created by vasilis on 30/06/20.
//

#ifndef KITE_ZK_DEBUG_UTIL_H
#define KITE_ZK_DEBUG_UTIL_H

#include "zk_main.h"
/* ---------------------------------------------------------------------------
//------------------------------ ZOOKEEPER DEBUGGING -----------------------------
//---------------------------------------------------------------------------*/
static inline const char* w_state_to_str(w_state_t state)
{
  switch (state) {

    case INVALID:return "INVALID";
    case VALID:return "VALID";
    case SENT:return "SENT";
    case READY:return "READY";
    case SEND_COMMITTS:return "SEND_COMMITTS";
    default: assert(false);
  }
}


static inline const char* prot_to_str(protocol_t protocol)
{
  switch (protocol){
    case FOLLOWER:
      return "FOLLOWER";
    case LEADER:
      return "LEADER";
    default: if (ENABLE_ASSERTIONS) assert(false);
  }
}

static inline void print_ldr_stats (uint16_t t_id)
{

  my_printf(yellow, "Prepares sent %ld/%ld \n", t_stats[t_id].preps_sent_mes_num, t_stats[t_id].preps_sent );
  my_printf(yellow, "Acks Received %ld/%ld/%ld \n",
            t_stats[t_id].received_acks_mes_num, t_stats[t_id].received_acks,
            t_stats[t_id].received_acks / FOLLOWER_MACHINE_NUM);
  my_printf(yellow, "Commits sent %ld/%ld \n", t_stats[t_id].coms_sent_mes_num, t_stats[t_id].coms_sent );
}

static inline void print_flr_stats (uint16_t t_id)
{

  my_printf(yellow, "Prepares received %ld/%ld \n", t_stats[t_id].received_preps_mes_num, t_stats[t_id].received_preps );
  my_printf(yellow, "Acks sent %ld/%ld \n", t_stats[t_id].acks_sent_mes_num, t_stats[t_id].acks_sent );
  my_printf(yellow, "Commits received %ld/%ld \n", t_stats[t_id].received_coms_mes_num, t_stats[t_id].received_coms );
}


static inline void zk_print_error_message(const char *mes, protocol_t protocol,
                                          p_writes_t *p_writes, uint16_t t_id)
{
  uint64_t anticipated_g_id = 0;

  if (p_writes->w_state[p_writes->pull_ptr] == READY)
    anticipated_g_id = p_writes->g_id[p_writes->pull_ptr];

  my_printf(red, "%s %u %s, committed g_id %lu, ",
            prot_to_str(protocol), t_id, mes,
            committed_global_w_id);
  if (protocol == LEADER)
    my_printf(red, "highest g_id %u, ",
            p_writes->highest_g_id_taken);

  if (anticipated_g_id > 0)
    my_printf(red, "anticipated g_id %u",
              anticipated_g_id);
  my_printf(red, "\n");
}

// Leader checks its debug counters
static inline void ldr_check_debug_cntrs(uint32_t *credit_debug_cnt, uint32_t *wait_for_acks_dbg_counter,
                                         uint32_t *wait_for_gid_dbg_counter, p_writes_t *p_writes,
                                         uint16_t t_id)
{
  if (unlikely((*wait_for_gid_dbg_counter) > M_16)) {
    zk_print_error_message("waits for the g_id", LEADER, p_writes, t_id);
    print_ldr_stats(t_id);
    (*wait_for_gid_dbg_counter) = 0;
  }
  if (unlikely((*wait_for_acks_dbg_counter) > M_16)) {
    zk_print_error_message("waits for acks", LEADER, p_writes, t_id);
    my_printf(cyan, "Sent lid %u and state %s \n",
              p_writes->local_w_id, w_state_to_str(p_writes->w_state[p_writes->pull_ptr]));
    print_ldr_stats(t_id);
    (*wait_for_acks_dbg_counter) = 0;
    //exit(0);
  }
  if (unlikely(credit_debug_cnt[PREP_VC] > M_16)) {
    zk_print_error_message("lacks prep credit", LEADER, p_writes, t_id);
    print_ldr_stats(t_id);
    credit_debug_cnt[PREP_VC] = 0;
  }
  if (unlikely(credit_debug_cnt[COMM_VC] > M_16)) {
    zk_print_error_message("lacks comm credits", LEADER, p_writes, t_id);
    print_ldr_stats(t_id);
    credit_debug_cnt[COMM_VC] = 0;
  }
}

// Follower checks its debug counters
static inline void flr_check_debug_cntrs(uint32_t *credit_debug_cnt, uint32_t *wait_for_coms_dbg_counter,
                                         uint32_t *wait_for_preps_dbg_counter,
                                         uint32_t *wait_for_gid_dbg_counter, volatile zk_prep_mes_ud_t *prep_buf,
                                         uint32_t pull_ptr, p_writes_t *p_writes, uint16_t t_id)
{
  uint32_t waiting_time = M_256;
  if (unlikely((*wait_for_preps_dbg_counter) > waiting_time)) {
    my_printf(red, "Follower %d waits for preps, committed g_id %lu \n", t_id, committed_global_w_id);
    zk_prepare_t *prep = (zk_prepare_t *)&prep_buf[pull_ptr].prepare.prepare;
    uint32_t l_id = prep_buf[pull_ptr].prepare.l_id;
    uint64_t g_id = prep->g_id;
    uint8_t message_opc = prep_buf[pull_ptr].prepare.opcode;
    my_printf(cyan, "Flr %d, polling on index %u,polled opc %u, 1st write opcode: %u, l_id %u, first g_id %u, expected l_id %u\n",
              t_id, pull_ptr, message_opc, prep->opcode, l_id, g_id, p_writes->local_w_id);
    MOD_INCR(pull_ptr, FLR_PREP_BUF_SLOTS);
    prep = (zk_prepare_t *)&prep_buf[pull_ptr].prepare.prepare;
    l_id = prep_buf[pull_ptr].prepare.l_id;
    g_id = prep->g_id;
    message_opc = prep_buf[pull_ptr].prepare.opcode;
    my_printf(cyan, "Next index %u,polled opc %u, 1st write opcode: %u, l_id %u, first g_id %u, expected l_id %u\n",
              pull_ptr, message_opc, prep->opcode, l_id, g_id, p_writes->local_w_id);
    for (int i = 0; i < FLR_PREP_BUF_SLOTS; ++i) {
      if (prep_buf[i].prepare.opcode == KVS_OP_PUT) {
        my_printf(green, "GOOD OPCODE in index %d, l_id %u \n", i, prep_buf[i].prepare.l_id);
      }
      else my_printf(red, "BAD OPCODE in index %d, l_id %u \n", i, prep_buf[i].prepare.l_id);

    }

    print_flr_stats(t_id);
    (*wait_for_preps_dbg_counter) = 0;
//    exit(0);
  }
  if (unlikely((*wait_for_gid_dbg_counter) > waiting_time)) {
    zk_print_error_message("waits for the g_id", FOLLOWER, p_writes, t_id);
    print_flr_stats(t_id);
    (*wait_for_gid_dbg_counter) = 0;
  }
  if (unlikely((*wait_for_coms_dbg_counter) > waiting_time)) {
    zk_print_error_message("waits for coms", FOLLOWER, p_writes, t_id);
    print_flr_stats(t_id);
    (*wait_for_coms_dbg_counter) = 0;
  }
  if (unlikely((*credit_debug_cnt) > waiting_time)) {
    zk_print_error_message("acks write credits", FOLLOWER, p_writes, t_id);
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
      assert(false);
    }
  }
}



/* ---------------------------------------------------------------------------
//------------------------------ POLLNG ACKS -----------------------------
//---------------------------------------------------------------------------*/

static inline void zk_check_polled_ack_and_print(zk_ack_mes_t *ack, uint16_t ack_num,
                                                 uint64_t pull_lid, uint32_t buf_ptr, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert (ack->opcode == KVS_OP_ACK);
    assert(ack_num > 0 && ack_num <= FLR_PENDING_WRITES);
    assert(ack->follower_id < MACHINE_NUM);
  }
  if (DEBUG_ACKS)
    my_printf(yellow, "Leader %d ack opcode %d with %d acks for l_id %lu, oldest lid %lu, at offset %d from flr %u \n",
              t_id, ack->opcode, ack_num, ack->l_id, pull_lid, buf_ptr, ack->follower_id);
  if (ENABLE_STAT_COUNTING) {
    t_stats[t_id].received_acks += ack_num;
    t_stats[t_id].received_acks_mes_num++;
  }

}

static inline void zk_check_ack_l_id_is_small_enough(uint16_t ack_num,
                                                     uint64_t l_id, p_writes_t *p_writes,
                                                     uint64_t pull_lid, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(l_id + ack_num <= pull_lid + p_writes->size);
    if ((l_id + ack_num < pull_lid) && (!USE_QUORUM)) {
      my_printf(red, "l_id %u, ack_num %u, pull_lid %u \n", l_id, ack_num, pull_lid);
      assert(false);
    }
  }
}

static inline void zk_debug_info_bookkeep(int completed_messages, int polled_messages,
                                          uint32_t *dbg_counter, recv_info_t *ack_recv_info,
                                          uint32_t *outstanding_prepares, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) assert(polled_messages == completed_messages);
  if (polled_messages > 0) {
    if (ENABLE_ASSERTIONS) (*dbg_counter) = 0;
  }
  else {
    if (ENABLE_ASSERTIONS && (*outstanding_prepares) > 0) (*dbg_counter)++;
    if (ENABLE_STAT_COUNTING && (*outstanding_prepares) > 0) t_stats[t_id].stalled_ack_prep++;
  }
  if (ENABLE_ASSERTIONS) assert(ack_recv_info->posted_recvs >= polled_messages);
}

/* ---------------------------------------------------------------------------
//------------------------------ POLLNG COMMITS -----------------------------
//---------------------------------------------------------------------------*/


static inline void zk_check_polled_commit_and_print(zk_com_mes_t *com,
                                                    p_writes_t *p_writes,
                                                    uint32_t buf_ptr,
                                                    uint64_t l_id,
                                                    uint64_t pull_lid,
                                                    uint16_t com_num,
                                                    uint16_t t_id)
{
  if (DEBUG_COMMITS)
    my_printf(yellow, "Flr %d com opcode %d with %d coms for l_id %lu, "
              "oldest lid %lu, at offset %d at address %u \n",
              t_id, com->opcode, com_num, l_id, pull_lid, buf_ptr, (void*) com);
  if (ENABLE_ASSERTIONS) {
    assert(com->opcode == KVS_OP_PUT);
    if ((pull_lid > l_id) ||
       ((l_id + com_num > pull_lid + p_writes->size) &&
       (!USE_QUORUM))) {
      my_printf(red, "Flr %d, COMMIT: received lid %lu, com_num %u, pull_lid %lu, p_writes size  %u \n",
                t_id, l_id, com_num, pull_lid, p_writes->size);
      print_ldr_stats(t_id);
      assert(false);
    }
    assert(com_num > 0 && com_num <= MAX_LIDS_IN_A_COMMIT);
  }
}


static inline void zk_checks_after_polling_commits(uint32_t *dbg_counter,
                                                   uint32_t polled_messages,
                                                   recv_info_t *com_recv_info)
{
  if (polled_messages > 0) {
    if (ENABLE_ASSERTIONS) (*dbg_counter) = 0;
  }
  if (ENABLE_ASSERTIONS) assert(com_recv_info->posted_recvs >= polled_messages);
}

/* ---------------------------------------------------------------------------
//------------------------------ POLLNG PREPARES -----------------------------
//---------------------------------------------------------------------------*/


static inline void zk_increment_wait_for_preps_cntr(p_writes_t *p_writes,
                                                    p_acks_t *p_acks,
                                                    uint32_t *wait_for_prepares_dbg_counter)
{
  if (ENABLE_ASSERTIONS && p_acks->acks_to_send == 0 && p_writes->size == 0)
    (*wait_for_prepares_dbg_counter)++;
}


static inline void zk_check_polled_prep_and_print(zk_prep_mes_t* prep_mes,
                                                  p_writes_t *p_writes,
                                                  uint8_t coalesce_num,
                                                  uint32_t buf_ptr,
                                                  uint32_t incoming_l_id,
                                                  uint64_t expected_l_id,
                                                  volatile zk_prep_mes_ud_t *incoming_preps,
                                                  uint16_t t_id)
{
  if (DEBUG_PREPARES)
    my_printf(green, "Flr %d sees a prep_mes message with %d prepares at index %u l_id %u, expected lid %lu \n",
              t_id, coalesce_num, index, incoming_l_id, expected_l_id);
  if (FLR_DISALLOW_OUT_OF_ORDER_PREPARES && ENABLE_ASSERTIONS) {
    if (expected_l_id != (uint64_t) incoming_l_id) {
      my_printf(red, "flr %u expected l_id  %lu and received %u \n",
                t_id, expected_l_id, incoming_l_id);
      uint32_t dbg = B_4_ ;
      flr_check_debug_cntrs(&dbg, &dbg, &dbg, &dbg, incoming_preps, buf_ptr, p_writes, t_id);
      //print_flr_stats(t_id);
      assert(false);
    }
  }
  if (ENABLE_ASSERTIONS) {
    assert(prep_mes->opcode == KVS_OP_PUT);
    assert(expected_l_id <= (uint64_t) incoming_l_id);
    assert(coalesce_num > 0 && coalesce_num <= MAX_PREP_COALESCE);
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[t_id].received_preps += coalesce_num;
    t_stats[t_id].received_preps_mes_num++;
  }
}


static inline void
zk_check_prepare_and_print(zk_prepare_t *prepare,
                           p_writes_t* p_writes,
                           uint8_t prep_i,
                           uint16_t t_id)
{
  uint32_t push_ptr = p_writes->push_ptr;
  if (ENABLE_ASSERTIONS) {
    assert(prepare->sess_id < SESSIONS_PER_THREAD);
    assert(prepare->flr_id <= MACHINE_NUM);
    assert(prepare->g_id > committed_global_w_id);
    assert(prepare->val_len == VALUE_SIZE >> SHIFT_BITS);
    assert(p_writes->w_state[push_ptr] == INVALID);
  }
  if (DEBUG_PREPARES)
    my_printf(green, "Flr %u, prep_i %u new write at ptr %u with g_id %lu and flr id %u, value_len %u \n",
              t_id, prep_i, push_ptr, prepare->g_id, prepare->flr_id, prepare[prep_i].val_len);
}


static inline void zk_checks_after_polling_prepares(p_writes_t *p_writes,
                                                    uint32_t *wait_for_prepares_dbg_counter,
                                                    uint32_t polled_messages,
                                                    recv_info_t *prep_recv_info,
                                                    p_acks_t *p_acks, uint16_t t_id)
{


  if (polled_messages > 0) {
    if (ENABLE_ASSERTIONS) (*wait_for_prepares_dbg_counter) = 0;
  }
  if (ENABLE_STAT_COUNTING && p_acks->acks_to_send == 0 && p_writes->size == 0) t_stats[t_id].stalled_ack_prep++;
  if (ENABLE_ASSERTIONS) assert(prep_recv_info->posted_recvs >= polled_messages);
  if (ENABLE_ASSERTIONS) assert(prep_recv_info->posted_recvs <= FLR_MAX_RECV_PREP_WRS);
}


/* ---------------------------------------------------------------------------
//------------------------------ BROADCASTS -----------------------------
//---------------------------------------------------------------------------*/

static inline void zk_checks_and_stats_on_bcasting_prepares(p_writes_t *p_writes,
                                                            uint8_t coalesce_num,
                                                            uint32_t *outstanding_prepares,
                                                            uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(p_writes->prep_fifo->bcast_size >= coalesce_num);
    (*outstanding_prepares) +=
      coalesce_num;
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[t_id].preps_sent +=
      coalesce_num;
    t_stats[t_id].preps_sent_mes_num++;
  }
}


static inline void zk_checks_and_stats_on_bcasting_commits(zk_com_fifo_t *com_fifo,
                                                           zk_com_mes_t *com_mes,
                                                           uint16_t  br_i,
                                                           uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(br_i <= COMMIT_CREDITS);
    assert(com_fifo != NULL);
    if (com_fifo->size > COMMIT_FIFO_SIZE)
      printf("com fifo size %u/%d \n", com_fifo->size, COMMIT_FIFO_SIZE);
    assert(com_fifo->size <= COMMIT_FIFO_SIZE);
    assert(com_mes->com_num > 0 && com_mes->com_num <= MAX_LIDS_IN_A_COMMIT);
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[t_id].coms_sent += com_mes->com_num;
    t_stats[t_id].coms_sent_mes_num++;
  }
}


/* ---------------------------------------------------------------------------
//------------------------------ UNICASTS------- -----------------------------
//---------------------------------------------------------------------------*/


static inline void check_stats_prints_when_sending_acks(zk_ack_mes_t *ack,
                                                        p_writes_t *p_writes,
                                                        p_acks_t *p_acks,
                                                        uint64_t l_id_to_send, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(ack->l_id == l_id_to_send);
    assert (p_acks->slots_ahead <= p_writes->size);
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[t_id].acks_sent += ack->ack_num;
    t_stats[t_id].acks_sent_mes_num++;
  }

  if (DEBUG_ACKS)
    my_printf(yellow, "Flr %d is sending an ack for lid %lu and ack num %d and flr id %d, p_writes size %u/%d \n",
              t_id, l_id_to_send, ack->ack_num, ack->follower_id, p_writes->size, FLR_PENDING_WRITES);
  if (ENABLE_ASSERTIONS) assert(ack->ack_num > 0 && ack->ack_num <= FLR_PENDING_WRITES);
}

static inline void checks_and_prints_posting_recvs_for_preps(recv_info_t *prep_recv_info,
                                                             uint32_t recvs_to_post_num,
                                                             uint16_t t_id)
{
  //printf("FLR %d posting %u recvs and has a total of %u recvs for prepares \n",
  //       t_id, recvs_to_post_num,  prep_recv_info->posted_recvs);
  if (ENABLE_ASSERTIONS) {
    assert(recvs_to_post_num <= FLR_MAX_RECV_PREP_WRS);
    assert(prep_recv_info->posted_recvs <= FLR_MAX_RECV_PREP_WRS);
  }
}


static inline void checks_and_print_when_forging_write(zk_w_mes_t *w_mes, uint16_t coalesce_num,
                                                       uint32_t length, uint16_t credits, uint16_t t_id)
{
  for (uint16_t i = 0; i < coalesce_num; i++) {
    if (DEBUG_WRITES)
      printf("Write %d, session id %u, val-len %u, message size %d\n", i,
             w_mes->write[i].sess_id,
             w_mes->write[i].val_len,
             length);
    if (ENABLE_ASSERTIONS) {
      assert(w_mes->write[i].val_len == VALUE_SIZE >> SHIFT_BITS);
      assert(w_mes->write[i].opcode == KVS_OP_PUT);
    }
  }
  if (DEBUG_WRITES)
    my_printf(green, "Follower %d : I sent a write message %d of %u writes with size %u,  with  credits: %d \n",
              t_id, w_mes->write->opcode, coalesce_num, length, credits);
}


static inline void checks_and_stats_when_sending_write(p_writes_t *p_writes,
                                                       uint16_t coalesce_num,
                                                       uint32_t *outstanding_writes,
                                                       uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(p_writes->w_fifo->size >= coalesce_num);
    (*outstanding_writes) += coalesce_num;
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[t_id].writes_sent += coalesce_num;
    t_stats[t_id].writes_sent_mes_num++;
  }
}



/* ---------------------------------------------------------------------------
//------------------------------TRACE --------------------------------
//---------------------------------------------------------------------------*/

static inline void zk_check_op(zk_trace_op_t *op)
{
  if (ENABLE_ASSERTIONS) {
    check_state_with_allowed_flags(3, op->opcode, KVS_OP_PUT, KVS_OP_GET);
    assert(op->real_val_len > 0);
    assert(op->index_to_req_array < PER_SESSION_REQ_NUM);
    assert(op->session_id < SESSIONS_PER_THREAD);
    assert(op->key.bkt > 0);
  }
}

static inline void checks_when_leader_creates_write(zk_prep_mes_t *preps, uint32_t prep_ptr,
                                                    uint32_t inside_prep_ptr, p_writes_t *p_writes,
                                                    uint32_t w_ptr, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (inside_prep_ptr == 0) {
      uint32_t message_l_id = preps[prep_ptr].l_id;
      if (message_l_id > MAX_PREP_COALESCE) {
        uint32_t prev_prep_ptr = (prep_ptr + PREP_FIFO_SIZE - 1) % PREP_FIFO_SIZE;
        uint32_t prev_l_id = preps[prev_prep_ptr].l_id;
        uint8_t prev_coalesce = preps[prev_prep_ptr].coalesce_num;
        if (message_l_id != prev_l_id + prev_coalesce) {
          my_printf(red, "Current message l_id %u, previous message l_id %u , previous coalesce %u\n",
                    message_l_id, prev_l_id, prev_coalesce);
        }
      }
    }
    if (p_writes->w_state[w_ptr] != INVALID)
      my_printf(red, "Leader %u w_state %d at w_ptr %u, g_id %lu, cache hits %lu, size %u \n",
                t_id, p_writes->w_state[w_ptr], w_ptr, p_writes->g_id[w_ptr],
                t_stats[t_id].cache_hits_per_thread, p_writes->size);
    //printf("Sent %d, Valid %d, Ready %d \n", SENT, VALID, READY);
    assert(p_writes->w_state[w_ptr] == INVALID);

  }
}


#endif //KITE_ZK_DEBUG_UTIL_H

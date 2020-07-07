#include "zk_util.h"


void zk_print_parameters_in_the_start()
{
  my_printf(green, "COMMIT: commit message %lu/%d, commit message ud req %llu/%d\n",
               sizeof(zk_com_mes_t), LDR_COM_SEND_SIZE,
               sizeof(zk_com_mes_ud_t), FLR_COM_RECV_SIZE);
  my_printf(cyan, "ACK: ack message %lu/%d, ack message ud req %llu/%d\n",
              sizeof(zk_ack_mes_t), FLR_ACK_SEND_SIZE,
              sizeof(zk_ack_mes_ud_t), LDR_ACK_RECV_SIZE);
  my_printf(yellow, "PREPARE: prepare %lu/%d, prep message %lu/%d, prep message ud req %llu/%d\n",
                sizeof(zk_prepare_t), PREP_SIZE,
                sizeof(zk_prep_mes_t), LDR_PREP_SEND_SIZE,
                sizeof(zk_prep_mes_ud_t), FLR_PREP_RECV_SIZE);
  my_printf(cyan, "Write: write %lu/%d, write message %lu/%d, write message ud req %llu/%d\n",
              sizeof(zk_write_t), W_SIZE,
              sizeof(zk_w_mes_t), FLR_W_SEND_SIZE,
              sizeof(zk_w_mes_ud_t), LDR_W_RECV_SIZE);

  my_printf(green, "LEADER PREPARE INLINING %d, LEADER PENDING WRITES %d \n",
               LEADER_PREPARE_ENABLE_INLINING, LEADER_PENDING_WRITES);
  my_printf(green, "FOLLOWER WRITE INLINING %d, FOLLOWER WRITE FIFO SIZE %d \n",
               FLR_W_ENABLE_INLINING, W_FIFO_SIZE);
  my_printf(cyan, "PREPARE CREDITS %d, FLR PREPARE BUF SLOTS %d, FLR PREPARE BUF SIZE %d\n",
              PREPARE_CREDITS, FLR_PREP_BUF_SLOTS, FLR_PREP_BUF_SIZE);

  my_printf(yellow, "Using Quorom %d , Quorum Machines %d \n", USE_QUORUM, LDR_QUORUM_OF_ACKS);
}

void zk_static_assert_compile_parameters()
{
  //static_assert(!ENABLE_CLIENTS, " ");

  if (ENABLE_MULTICAST) assert(MCAST_QP_NUM == MCAST_GROUPS_NUM);
  assert(LEADER_MACHINE < MACHINE_NUM);
  assert(LEADER_PENDING_WRITES >= SESSIONS_PER_THREAD);
  assert(sizeof(struct key) == KEY_SIZE);
  assert(LEADERS_PER_MACHINE == FOLLOWERS_PER_MACHINE); // hopefully temporary restriction
  assert((W_CREDITS % LDR_CREDIT_DIVIDER) == 0); // division better be perfect
  assert((COMMIT_CREDITS % FLR_CREDIT_DIVIDER) == 0); // division better be perfect
  assert(sizeof(zk_ack_mes_ud_t) == LDR_ACK_RECV_SIZE);
  assert(sizeof(zk_com_mes_ud_t) == FLR_COM_RECV_SIZE);
  assert(sizeof(zk_prep_mes_ud_t) == FLR_PREP_RECV_SIZE);
  assert(sizeof(zk_w_mes_ud_t) == LDR_W_RECV_SIZE);
  assert(SESSIONS_PER_THREAD < M_16);
  assert(FLR_MAX_RECV_COM_WRS >= FLR_CREDITS_IN_MESSAGE);
  if (WRITE_RATIO > 0) assert(ZK_UPDATE_BATCH >= LEADER_PENDING_WRITES);

  if (PUT_A_MACHINE_TO_SLEEP) assert(MACHINE_THAT_SLEEPS != LEADER_MACHINE);


//
//  my_printf(yellow, "WRITE: size of write recv slot %d size of w_message %lu , "
//           "value size %d, size of cache op %lu , sizeof udreq w message %lu \n",
//         LDR_W_RECV_SIZE, sizeof(zk_w_mes_t), VALUE_SIZE,
//         sizeof(struct cache_op), sizeof(zk_w_mes_ud_t));
  assert(sizeof(zk_w_mes_ud_t) == LDR_W_RECV_SIZE);
  assert(sizeof(zk_w_mes_t) == FLR_W_SEND_SIZE);
}

void zk_init_globals()
{
  global_w_id = 1; // DO not start from 0, because when checking for acks there is a non-zero test
  committed_global_w_id = 0;
}


void dump_stats_2_file(struct stats* st){
    uint8_t typeNo = LEADER;
    assert(typeNo >=0 && typeNo <=3);
    int i = 0;
    char filename[128];
    FILE *fp;
    double total_MIOPS;
    char* path = "../../results/scattered-results";

    sprintf(filename, "%s/%s_s_%d__v_%d_m_%d_l_%d_f_%d_r_%d-%d.csv", path,
            "ZK",
            SESSIONS_PER_THREAD,
            USE_BIG_OBJECTS == 1 ? ((EXTRA_CACHE_LINES * 64) + BASE_VALUE_SIZE): BASE_VALUE_SIZE,
            MACHINE_NUM, LEADERS_PER_MACHINE,
            FOLLOWERS_PER_MACHINE, WRITE_RATIO,
            machine_id);
    printf("%s\n", filename);
    fp = fopen(filename, "w"); // "w" means that we are going to write on this file
    fprintf(fp, "machine_id: %d\n", machine_id);

    fprintf(fp, "comment: thread ID, total MIOPS,"
            "preps sent, coms sent, acks sent, "
            "received preps, received coms, received acks\n");
    for(i = 0; i < WORKERS_PER_MACHINE; ++i){
        total_MIOPS = st->cache_hits_per_thread[i];
        fprintf(fp, "client: %d, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f\n",
                i, total_MIOPS, st->cache_hits_per_thread[i], st->preps_sent[i],
                st->coms_sent[i], st->acks_sent[i],
                st->received_preps[i],st->received_coms[i],
                st->received_acks[i]);
    }

    fclose(fp);
}


/* ---------------------------------------------------------------------------
------------------------------LEADER --------------------------------------
---------------------------------------------------------------------------*/
// construct a prep_message-- max_size must be in bytes
void init_fifo(struct fifo **fifo, uint32_t max_size, uint32_t fifos_num)
{
  (*fifo) = (struct fifo *) malloc(fifos_num * sizeof(struct fifo));
  memset((*fifo), 0, fifos_num *  sizeof(struct fifo));
  for (int i = 0; i < fifos_num; ++i) {
    (*fifo)[i].fifo = malloc(max_size);
    memset((*fifo)[i].fifo, 0, max_size);
  }
}

// Initialize the quorum info that contains the system configuration
quorum_info_t* set_up_q_info(struct ibv_send_wr *prep_send_wr,
                             struct ibv_send_wr *com_send_wr,
                             uint16_t credits[][MACHINE_NUM])
{
  quorum_info_t * q_info = (quorum_info_t *) calloc(1, sizeof(quorum_info_t));
  q_info->active_num = REM_MACH_NUM;
  q_info->first_active_rm_id = 0;
  q_info->last_active_rm_id = REM_MACH_NUM - 1;
  for (uint8_t i = 0; i < REM_MACH_NUM; i++) {
    uint8_t m_id = i < machine_id ? i : (uint8_t) (i + 1);
    q_info->active_ids[i] = m_id;
    q_info->send_vector[i] = true;
  }

  q_info->num_of_send_wrs = Q_INFO_NUM_SEND_WRS;
  q_info->send_wrs_ptrs = (struct ibv_send_wr **) malloc(Q_INFO_NUM_SEND_WRS * sizeof(struct ibv_send_wr *));
  q_info->send_wrs_ptrs[0] = prep_send_wr;
  q_info->send_wrs_ptrs[1] = com_send_wr;

  q_info->num_of_credit_targets = Q_INFO_CREDIT_TARGETS;
  q_info->targets = malloc (q_info->num_of_credit_targets * sizeof(uint16_t));
  q_info->targets[0] = W_CREDITS;
  q_info->targets[1] = COMMIT_CREDITS;
  q_info->credit_ptrs = malloc(q_info->num_of_credit_targets * sizeof(uint16_t*));
  q_info->credit_ptrs[0] = credits[PREP_VC];
  q_info->credit_ptrs[1] = credits[COMM_VC];
  return q_info;

}


// Set up a struct that stores pending writes
p_writes_t* set_up_pending_writes(uint32_t size, struct ibv_send_wr *prep_send_wr,
                                  struct ibv_send_wr *com_send_wr,
                                  uint16_t credits[][MACHINE_NUM],
                                  protocol_t protocol)
{
  int i;
  p_writes_t* p_writes = (p_writes_t*) calloc(1,sizeof(p_writes_t));
  p_writes->q_info = protocol == LEADER ? set_up_q_info(prep_send_wr, com_send_wr, credits) : NULL;


  p_writes->g_id = (uint64_t *) malloc(size * sizeof(uint64_t));
  p_writes->w_state = (enum write_state *) malloc(size * sizeof(enum write_state));
  p_writes->session_id = (uint32_t *) calloc(size, sizeof(uint32_t));
  p_writes->acks_seen = (uint8_t *) calloc(size, sizeof(uint8_t));
  p_writes->w_index_to_req_array = (uint32_t *) calloc(SESSIONS_PER_THREAD, sizeof(uint32_t));

  p_writes->flr_id = (uint8_t *) malloc(size * sizeof(uint8_t));
  p_writes->is_local = (bool *) malloc(size * sizeof(bool));
  p_writes->stalled = (bool *) malloc(SESSIONS_PER_THREAD * sizeof(bool));
  p_writes->ptrs_to_ops = (zk_prepare_t **) malloc(size * sizeof(zk_prepare_t *));
  if (protocol == FOLLOWER) init_fifo(&(p_writes->w_fifo), W_FIFO_SIZE * sizeof(zk_w_mes_t), 1);
  memset(p_writes->g_id, 0, size * sizeof(uint64_t));
  p_writes->prep_fifo = (zk_prep_fifo_t *) calloc(1, sizeof(zk_prep_fifo_t));
    p_writes->prep_fifo->prep_message =
    (zk_prep_mes_t *) calloc(PREP_FIFO_SIZE, sizeof(zk_prep_mes_t));
  assert(p_writes->prep_fifo != NULL);
  for (i = 0; i < SESSIONS_PER_THREAD; i++) p_writes->stalled[i] = false;
  for (i = 0; i < size; i++) {
    p_writes->w_state[i] = INVALID;
  }
  if (protocol == LEADER) {
    zk_prep_mes_t *preps = p_writes->prep_fifo->prep_message;
    for (i = 0; i < PREP_FIFO_SIZE; i++) {
      preps[i].opcode = KVS_OP_PUT;
      for (uint16_t j = 0; j < MAX_PREP_COALESCE; j++) {
        preps[i].prepare[j].opcode = KVS_OP_PUT;
        preps[i].prepare[j].val_len = VALUE_SIZE >> SHIFT_BITS;
      }
    }
  } else { // PROTOCOL == FOLLOWER
    zk_w_mes_t *writes = (zk_w_mes_t *) p_writes->w_fifo->fifo;
    for (i = 0; i < W_FIFO_SIZE; i++) {
      for (uint16_t j = 0; j < MAX_W_COALESCE; j++) {
        writes[i].write[j].opcode = KVS_OP_PUT;
        writes[i].write[j].val_len = VALUE_SIZE >> SHIFT_BITS;
      }
    }
  }
  return p_writes;
}



// set the different queue depths for client's queue pairs
void set_up_queue_depths_ldr_flr(int** recv_q_depths, int** send_q_depths, int protocol)
{
  /* -------LEADER-------------
  * 1st Dgram send Prepares -- receive ACKs
  * 2nd Dgram send Commits  -- receive Writes
  * 3rd Dgram  receive Credits
  *
    * ------FOLLOWER-----------
  * 1st Dgram receive prepares -- send Acks
  * 2nd Dgram receive Commits  -- send Writes
  * 3rd Dgram  send Credits
  * */
  if (protocol == FOLLOWER) {
    *send_q_depths = malloc(FOLLOWER_QP_NUM * sizeof(int));
    *recv_q_depths = malloc(FOLLOWER_QP_NUM * sizeof(int));
    (*recv_q_depths)[PREP_ACK_QP_ID] = ENABLE_MULTICAST == 1? 1 : FLR_RECV_PREP_Q_DEPTH;
    (*recv_q_depths)[COMMIT_W_QP_ID] = ENABLE_MULTICAST == 1? 1 : FLR_RECV_COM_Q_DEPTH;
    (*recv_q_depths)[FC_QP_ID] = FLR_RECV_CR_Q_DEPTH;
    (*send_q_depths)[PREP_ACK_QP_ID] = FLR_SEND_ACK_Q_DEPTH;
    (*send_q_depths)[COMMIT_W_QP_ID] = FLR_SEND_W_Q_DEPTH;
    (*send_q_depths)[FC_QP_ID] = FLR_SEND_CR_Q_DEPTH;
  }
  else if (protocol == LEADER) {
    *send_q_depths = malloc(LEADER_QP_NUM * sizeof(int));
    *recv_q_depths = malloc(LEADER_QP_NUM * sizeof(int));
    (*recv_q_depths)[PREP_ACK_QP_ID] = LDR_RECV_ACK_Q_DEPTH;
    (*recv_q_depths)[COMMIT_W_QP_ID] = LDR_RECV_W_Q_DEPTH;
    (*recv_q_depths)[FC_QP_ID] = LDR_RECV_CR_Q_DEPTH;
    (*send_q_depths)[PREP_ACK_QP_ID] = LDR_SEND_PREP_Q_DEPTH;
    (*send_q_depths)[COMMIT_W_QP_ID] = LDR_SEND_COM_Q_DEPTH;
    (*send_q_depths)[FC_QP_ID] = LDR_SEND_CR_Q_DEPTH;
  }
  else check_protocol(protocol);
}

// Prepost Receives on the Leader Side
// Post receives for the coherence traffic in the init phase
void pre_post_recvs(uint32_t* push_ptr, struct ibv_qp *recv_qp, uint32_t lkey, void* buf,
                    uint32_t max_reqs, uint32_t number_of_recvs, uint16_t QP_ID, uint32_t message_size)
{
  uint32_t i;//, j;
  for(i = 0; i < number_of_recvs; i++) {
        hrd_post_dgram_recv(recv_qp,	(buf + *push_ptr * message_size),
                            message_size, lkey);
      MOD_INCR(*push_ptr, max_reqs);
  }
}


// set up some basic leader buffers
zk_com_fifo_t *set_up_ldr_ops(zk_resp_t *resp,  uint16_t t_id)
{
  int i;
  assert(resp != NULL);
  zk_com_fifo_t *com_fifo = calloc(1, sizeof(zk_com_fifo_t));
  com_fifo->commits = (zk_com_mes_t *)
    calloc(COMMIT_FIFO_SIZE, sizeof(zk_com_mes_t));
  for(i = 0; i <  MAX_OP_BATCH; i++) resp[i].type = EMPTY;
  for(i = 0; i <  COMMIT_FIFO_SIZE; i++) {
      com_fifo->commits[i].opcode = KVS_OP_PUT;
  }
  assert(com_fifo->push_ptr == 0 && com_fifo->pull_ptr == 0 && com_fifo->size == 0);
 return com_fifo;
}

// Set up the memory registrations required in the leader if there is no Inlining
void set_up_ldr_mrs(struct ibv_mr **prep_mr, void *prep_buf,
                    struct ibv_mr **com_mr, void *com_buf,
                    struct hrd_ctrl_blk *cb)
{
  if (!LEADER_PREPARE_ENABLE_INLINING) {
   *prep_mr = register_buffer(cb->pd, (void*)prep_buf, PREP_FIFO_SIZE * sizeof(zk_prep_mes_t));
  }
  if (!COM_ENABLE_INLINING) *com_mr = register_buffer(cb->pd, com_buf,
                                                      COMMIT_CREDITS * sizeof(zk_com_mes_t));
}

// Set up all leader WRs
void set_up_ldr_WRs(struct ibv_send_wr *prep_send_wr, struct ibv_sge *prep_send_sgl,
                    struct ibv_send_wr *com_send_wr, struct ibv_sge *com_send_sgl,
                    uint16_t t_id, uint16_t remote_thread,
                    struct ibv_mr *prep_mr, struct ibv_mr *com_mr,
                    struct mcast_essentials *mcast) {
  uint16_t i, j;
  //BROADCAST WRs and credit Receives
  for (j = 0; j < MAX_BCAST_BATCH; j++) { // Number of Broadcasts
    //prep_send_sgl[j].addr = (uint64_t) (uintptr_t) (buf + j);
    if (LEADER_PREPARE_ENABLE_INLINING == 0) prep_send_sgl[j].lkey = prep_mr->lkey;
    if (!COM_ENABLE_INLINING) com_send_sgl[j].lkey = com_mr->lkey;

    for (i = 0; i < MESSAGES_IN_BCAST; i++) {
      uint16_t m_id = (uint16_t) (i < LEADER_MACHINE ? i : i + 1);
      assert(m_id != LEADER_MACHINE);
      assert(m_id < MACHINE_NUM);
      uint16_t index = (uint16_t) ((j * MESSAGES_IN_BCAST) + i);
      assert (index < MESSAGES_IN_BCAST_BATCH);
      assert(index < LDR_MAX_PREP_WRS);
      assert(index < LDR_MAX_COM_WRS);
      if (ENABLE_MULTICAST == 1) {
        prep_send_wr[index].wr.ud.ah = mcast->send_ah[PREP_MCAST_QP];
        prep_send_wr[index].wr.ud.remote_qpn = mcast->qpn[PREP_MCAST_QP];
        prep_send_wr[index].wr.ud.remote_qkey = mcast->qkey[PREP_MCAST_QP];
        com_send_wr[index].wr.ud.ah = mcast->send_ah[COM_MCAST_QP];
        com_send_wr[index].wr.ud.remote_qpn = mcast->qpn[COM_MCAST_QP];
        com_send_wr[index].wr.ud.remote_qkey = mcast->qkey[COM_MCAST_QP];
      } else {
        prep_send_wr[index].wr.ud.ah = rem_qp[m_id][remote_thread][PREP_ACK_QP_ID].ah;
        prep_send_wr[index].wr.ud.remote_qpn = (uint32) rem_qp[m_id][remote_thread][PREP_ACK_QP_ID].qpn;
        prep_send_wr[index].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
        com_send_wr[index].wr.ud.ah = rem_qp[m_id][remote_thread][COMMIT_W_QP_ID].ah;
        com_send_wr[index].wr.ud.remote_qpn = (uint32) rem_qp[m_id][remote_thread][COMMIT_W_QP_ID].qpn;
        com_send_wr[index].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
        assert(com_send_wr[index].wr.ud.ah != NULL);
        assert(prep_send_wr[index].wr.ud.ah != NULL);
      }
      prep_send_wr[index].opcode = IBV_WR_SEND;
      prep_send_wr[index].num_sge = 1;
      prep_send_wr[index].sg_list = &prep_send_sgl[j];
      com_send_wr[index].opcode = IBV_WR_SEND;
      com_send_wr[index].num_sge = 1;
      com_send_wr[index].sg_list = &com_send_sgl[j];
      if (LEADER_PREPARE_ENABLE_INLINING == 1) prep_send_wr[index].send_flags = IBV_SEND_INLINE;
      else prep_send_wr[index].send_flags = 0;
      if (COM_ENABLE_INLINING == 1) com_send_wr[index].send_flags = IBV_SEND_INLINE;
      prep_send_wr[index].next = (i == MESSAGES_IN_BCAST - 1) ? NULL : &prep_send_wr[index + 1];
      com_send_wr[index].next = (i == MESSAGES_IN_BCAST - 1) ? NULL : &com_send_wr[index + 1];
    }
  }
}
// The Leader sends credits to the followers when it receives their writes
// The follower sends credits to the leader when it receives commit messages
void ldr_set_up_credits_and_WRs(uint16_t credits[][MACHINE_NUM], struct ibv_recv_wr *credit_recv_wr,
                                struct ibv_sge *credit_recv_sgl, struct hrd_ctrl_blk *cb,
                                uint32_t max_credit_recvs)
{
  int i = 0;
  for (i = 0; i < MACHINE_NUM; i++) {
    credits[PREP_VC][i] = PREPARE_CREDITS;
    credits[COMM_VC][i] = COMMIT_CREDITS;
  }
  //Credit Receives
  credit_recv_sgl->length = 64;
  credit_recv_sgl->lkey = cb->dgram_buf_mr->lkey;
  credit_recv_sgl->addr = (uintptr_t) &cb->dgram_buf[0];
  for (i = 0; i < max_credit_recvs; i++) {
    credit_recv_wr[i].sg_list = credit_recv_sgl;
    credit_recv_wr[i].num_sge = 1;
  }
}

/* ---------------------------------------------------------------------------
------------------------------FOLLOWER --------------------------------------
---------------------------------------------------------------------------*/

// Set up all Follower WRs
void set_up_follower_WRs(struct ibv_send_wr *ack_send_wr, struct ibv_sge *ack_send_sgl,
                         struct ibv_recv_wr *prep_recv_wr, struct ibv_sge *prep_recv_sgl,
                         struct ibv_send_wr *w_send_wr, struct ibv_sge *w_send_sgl,
                         struct ibv_recv_wr *com_recv_wr, struct ibv_sge *com_recv_sgl,
                         uint16_t remote_thread,
                         struct hrd_ctrl_blk *cb, struct ibv_mr *w_mr,
                         struct mcast_essentials *mcast)
{
  uint16_t i;
    // ACKS
    ack_send_wr->wr.ud.ah = rem_qp[LEADER_MACHINE][remote_thread][PREP_ACK_QP_ID].ah;
    ack_send_wr->wr.ud.remote_qpn = (uint32) rem_qp[LEADER_MACHINE][remote_thread][PREP_ACK_QP_ID].qpn;
    ack_send_wr->wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
    ack_send_wr->opcode = IBV_WR_SEND;
    ack_send_wr->send_flags = IBV_SEND_INLINE;
    ack_send_sgl->length = FLR_ACK_SEND_SIZE;
    ack_send_wr->num_sge = 1;
    ack_send_wr->sg_list = ack_send_sgl;
    ack_send_wr->next = NULL;
    // WRITES
    for (i = 0; i < FLR_MAX_W_WRS; ++i) {
        w_send_wr[i].wr.ud.ah = rem_qp[LEADER_MACHINE][remote_thread][COMMIT_W_QP_ID].ah;
        w_send_wr[i].wr.ud.remote_qpn = (uint32) rem_qp[LEADER_MACHINE][remote_thread][COMMIT_W_QP_ID].qpn;
        if (FLR_W_ENABLE_INLINING) w_send_wr[i].send_flags = IBV_SEND_INLINE;
        else {
            w_send_sgl[i].lkey = w_mr->lkey;
            w_send_wr[i].send_flags = 0;
        }
        w_send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
        w_send_wr[i].opcode = IBV_WR_SEND;
        w_send_wr[i].num_sge = 1;
        w_send_wr[i].sg_list = &w_send_sgl[i];
    }
    // PREP RECVs
    for (i = 0; i < FLR_MAX_RECV_PREP_WRS; i++) {
      if (ENABLE_MULTICAST)
        prep_recv_sgl[i].lkey = mcast->recv_mr->lkey;

    }
    // COM RECVs
    for (i = 0; i < FLR_MAX_RECV_COM_WRS; i++) {
      if (ENABLE_MULTICAST)
          com_recv_sgl[i].lkey = mcast->recv_mr->lkey;

    }

}


void flr_set_up_credit_WRs(struct ibv_send_wr* credit_send_wr, struct ibv_sge* credit_send_sgl,
                           struct hrd_ctrl_blk *cb, uint8_t flr_id, uint32_t max_credt_wrs, uint16_t t_id)
{
  // Credit WRs
  for (uint32_t i = 0; i < max_credt_wrs; i++) {
    credit_send_sgl->length = 0;
    credit_send_wr[i].opcode = IBV_WR_SEND_WITH_IMM;
    credit_send_wr[i].num_sge = 0;
    credit_send_wr[i].sg_list = credit_send_sgl;
    credit_send_wr[i].imm_data = (uint32) flr_id;
    credit_send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
    credit_send_wr[i].next = NULL;
    credit_send_wr[i].send_flags = IBV_SEND_INLINE;
    credit_send_wr[i].wr.ud.ah = rem_qp[LEADER_MACHINE][t_id][FC_QP_ID].ah;
    credit_send_wr[i].wr.ud.remote_qpn = (uint32_t) rem_qp[LEADER_MACHINE][t_id][FC_QP_ID].qpn;
  }
}



/* ---------------------------------------------------------------------------
------------------------------UTILITY --------------------------------------
---------------------------------------------------------------------------*/
void check_protocol(int protocol)
{
    if (protocol != FOLLOWER && protocol != LEADER) {
        my_printf(red, "Wrong protocol specified when setting up the queue depths %d \n", protocol);
        assert(false);
    }
}

/* ---------------------------------------------------------------------------
------------------------------MULTICAST --------------------------------------
---------------------------------------------------------------------------*/


void zk_init_multicast(struct mcast_info **mcast_data, struct mcast_essentials **mcast,
                       int t_id, struct hrd_ctrl_blk *cb, int protocol)
{
  check_protocol(protocol);
  int *recv_q_depth = (int *) malloc(MCAST_QP_NUM * sizeof(int));
  recv_q_depth[0] = protocol == FOLLOWER ? FLR_RECV_PREP_Q_DEPTH : 1;
  recv_q_depth[1] = protocol == FOLLOWER ? FLR_RECV_COM_Q_DEPTH : 1;

  init_multicast(mcast_data, mcast, t_id, cb, (size_t) FLR_BUF_SIZE, recv_q_depth);
}
/*
// Initialize the mcast_essentials structure that is necessary
void init_multicast(struct mcast_info **mcast_data, struct mcast_essentials **mcast,
                    int t_id, struct hrd_ctrl_blk *cb, int protocol)
{
  check_protocol(protocol);
  int *recv_q_depth = malloc(MCAST_QP_NUM * sizeof(int));
  recv_q_depth[0] = protocol == FOLLOWER ? FLR_RECV_PREP_Q_DEPTH : 1;
  recv_q_depth[1] = protocol == FOLLOWER ? FLR_RECV_COM_Q_DEPTH : 1;
  *mcast_data = malloc(sizeof(struct mcast_info));
  (*mcast_data)->t_id = t_id;
  setup_multicast(*mcast_data, recv_q_depth);
//   char char_buf[40];
//   inet_ntop(AF_INET6, (*mcast_data)->mcast_ud_param.ah_attr.grh.dgid.raw, char_buf, 40);
//   printf("client: joined dgid: %s mlid 0x%x sl %d\n", char_buf,	(*mcast_data)->mcast_ud_param.ah_attr.dlid, (*mcast_data)->mcast_ud_param.ah_attr.sl);
  *mcast = malloc(sizeof(struct mcast_essentials));

  for (uint16_t i = 0; i < MCAST_QP_NUM; i++){
      (*mcast)->recv_cq[i] = (*mcast_data)->cm_qp[i].cq;
      (*mcast)->recv_qp[i] = (*mcast_data)->cm_qp[i].cma_id->qp;
      (*mcast)->send_ah[i] = ibv_create_ah(cb->pd, &((*mcast_data)->mcast_ud_param[i].ah_attr));
      (*mcast)->qpn[i]  =  (*mcast_data)->mcast_ud_param[i].qp_num;
      (*mcast)->qkey[i]  =  (*mcast_data)->mcast_ud_param[i].qkey;

   }
  (*mcast)->recv_mr = ibv_reg_mr((*mcast_data)->cm_qp[0].pd, (void *)cb->dgram_buf,
                                 (size_t)FLR_BUF_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                                                       IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);


  free(*mcast_data);
  if (protocol == FOLLOWER) assert((*mcast)->recv_mr != NULL);
}


// wrapper around getaddrinfo socket function
int get_addr(char *dst, struct sockaddr *addr)
{
    struct addrinfo *res;
    int ret;
    ret = getaddrinfo(dst, NULL, NULL, &res);
    if (ret) {
        printf("getaddrinfo failed - invalid hostname or IP address %s\n", dst);
        return ret;
    }
    memcpy(addr, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);
    return ret;
}

//Handle the addresses
void resolve_addresses(struct mcast_info *mcast_data)
{
    int ret, i, t_id = mcast_data->t_id;
    char mcast_addr[40];
    // Source addresses (i.e. local IPs)
    mcast_data->src_addr = (struct sockaddr*)&mcast_data->src_in;
    ret = get_addr(local_IP, ((struct sockaddr *)&mcast_data->src_in)); // to bind
    if (ret) printf("Client: failed to get src address \n");
    for (i = 0; i < MCAST_QPS; i++) {
        ret = rdma_bind_addr(mcast_data->cm_qp[i].cma_id, mcast_data->src_addr);
        if (ret) perror("Client: address bind failed");
    }
    // Destination addresses(i.e. multicast addresses)
    for (i = 0; i < MCAST_GROUPS_NUM; i ++) {
        mcast_data->dst_addr[i] = (struct sockaddr*)&mcast_data->dst_in[i];
        int m_cast_group_id = t_id * MACHINE_NUM + i;
        sprintf(mcast_addr, "224.0.%d.%d", m_cast_group_id / 256, m_cast_group_id % 256);
//        printf("mcast addr %d: %s\n", i, mcast_addr);
        ret = get_addr((char*) &mcast_addr, ((struct sockaddr *)&mcast_data->dst_in[i]));
        if (ret) printf("Client: failed to get dst address \n");
    }
}

// Set up the Send and Receive Qps for the multicast
void set_up_qp(struct cm_qps* qps, int *max_recv_q_depth)
{
    int ret, i, recv_q_depth;
    // qps[0].pd = ibv_alloc_pd(qps[0].cma_id->verbs); //new
    for (i = 0; i < MCAST_QP_NUM; i++) {
        qps[i].pd = ibv_alloc_pd(qps[i].cma_id->verbs);
        if (i > 0) qps[i].pd = qps[0].pd;
        recv_q_depth = max_recv_q_depth[i];
        qps[i].cq = ibv_create_cq(qps[i].cma_id->verbs, recv_q_depth, &qps[i], NULL, 0);
        struct ibv_qp_init_attr init_qp_attr;
        memset(&init_qp_attr, 0, sizeof init_qp_attr);
        init_qp_attr.cap.max_send_wr = 1;
        init_qp_attr.cap.max_recv_wr = (uint32) recv_q_depth;
        init_qp_attr.cap.max_send_sge = 1;
        init_qp_attr.cap.max_recv_sge = 1;
        init_qp_attr.qp_context = &qps[i];
        init_qp_attr.sq_sig_all = 0;
        init_qp_attr.qp_type = IBV_QPT_UD;
        init_qp_attr.send_cq = qps[i].cq;
        init_qp_attr.recv_cq = qps[i].cq;
        ret = rdma_create_qp(qps[i].cma_id, qps[i].pd, &init_qp_attr);
        if (ret) printf("unable to create QP \n");
    }
}

// Initial function to call to setup multicast, this calls the rest of the relevant functions
void setup_multicast(struct mcast_info *mcast_data, int *recv_q_depth)
{
    int ret, i, clt_id = mcast_data->t_id;
    static enum rdma_port_space port_space = RDMA_PS_UDP;
    // Create the channel
    mcast_data->channel = rdma_create_event_channel();
    if (!mcast_data->channel) {
        printf("Client %d :failed to create event channel\n", mcast_data->t_id);
        exit(1);
    }
    // Set up the cma_ids
    for (i = 0; i < MCAST_QPS; i++ ) {
        ret = rdma_create_id(mcast_data->channel, &mcast_data->cm_qp[i].cma_id, &mcast_data->cm_qp[i], port_space);
        if (ret) printf("Client %d :failed to create cma_id\n", mcast_data->t_id);
    }
    // deal with the addresses
    resolve_addresses(mcast_data);
    // set up the 2 qps
    set_up_qp(mcast_data->cm_qp, recv_q_depth);

    struct rdma_cm_event* event;
    for (i = 0; i < MCAST_GROUPS_NUM; i ++) {
        int qp_i = i;
        ret = rdma_resolve_addr(mcast_data->cm_qp[i].cma_id, mcast_data->src_addr, mcast_data->dst_addr[i], 20000);
        if (ret) printf("Client %d: failed to resolve address: %d, qp_i %d \n", clt_id, i, qp_i);
        if (ret) perror("Reason");
        while (rdma_get_cm_event(mcast_data->channel, &event) == 0) {
            switch (event->event) {
                case RDMA_CM_EVENT_ADDR_RESOLVED:
//                     printf("Client %d: RDMA ADDRESS RESOLVED address: %d \n", t_id, i);
                    ret = rdma_join_multicast(mcast_data->cm_qp[qp_i].cma_id, mcast_data->dst_addr[i], mcast_data);
                    if (ret) printf("unable to join multicast \n");
                    break;
                case RDMA_CM_EVENT_MULTICAST_JOIN:
                    mcast_data->mcast_ud_param[i] = event->param.ud;
//                     printf("RDMA JOIN MUlTICAST EVENT %d \n", i);
                    break;
                case RDMA_CM_EVENT_MULTICAST_ERROR:
                default:
                    break;
            }
            rdma_ack_cm_event(event);
            if (event->event == RDMA_CM_EVENT_MULTICAST_JOIN) break;
        }
        //if (i != RECV_MCAST_QP) {
            // destroying the QPs works fine but hurts performance...
            //  rdma_destroy_qp(mcast_data->cm_qp[i].cma_id);
            //  rdma_destroy_id(mcast_data->cm_qp[i].cma_id);
        //}
    }
    // rdma_destroy_event_channel(mcast_data->channel);
    // if (mcast_data->mcast_ud_param == NULL) mcast_data->mcast_ud_param = event->param.ud;
}


// call to test the multicast
void multicast_testing(struct mcast_essentials *mcast, int clt_gid, struct hrd_ctrl_blk *cb)
{

    struct ibv_wc mcast_wc;
    printf ("Client: Multicast Qkey %u and qpn %u \n", mcast->qkey[COM_MCAST_QP], mcast->qpn[COM_MCAST_QP]);


    struct ibv_sge mcast_sg;
    struct ibv_send_wr mcast_wr;
    struct ibv_send_wr *mcast_bad_wr;

    memset(&mcast_sg, 0, sizeof(mcast_sg));
    mcast_sg.addr	  = (uintptr_t)cb->dgram_buf;
    mcast_sg.length = 10;
    //mcast_sg.lkey	  = cb->dgram_buf_mr->lkey;

    memset(&mcast_wr, 0, sizeof(mcast_wr));
    mcast_wr.wr_id      = 0;
    mcast_wr.sg_list    = &mcast_sg;
    mcast_wr.num_sge    = 1;
    mcast_wr.opcode     = IBV_WR_SEND_WITH_IMM;
    mcast_wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
    mcast_wr.imm_data   = (uint32) clt_gid + 120 + (machine_id * 10);
    mcast_wr.next       = NULL;

    mcast_wr.wr.ud.ah          = mcast->send_ah[COM_MCAST_QP];
    mcast_wr.wr.ud.remote_qpn  = mcast->qpn[COM_MCAST_QP];
    mcast_wr.wr.ud.remote_qkey = mcast->qkey[COM_MCAST_QP];

    if (ibv_post_send(cb->dgram_qp[COMMIT_W_QP_ID], &mcast_wr, &mcast_bad_wr)) {
        fprintf(stderr, "Error, ibv_post_send() failed\n");
        assert(false);
    }

    printf("THe mcast was sent, I am waiting for confirmation imm data %d\n", mcast_wr.imm_data);
    hrd_poll_cq(cb->dgram_send_cq[COMMIT_W_QP_ID], 1, &mcast_wc);
    printf("The mcast was sent \n");
    hrd_poll_cq(mcast->recv_cq[COM_MCAST_QP], 1, &mcast_wc);
    printf("Client %d imm data recved %d \n", clt_gid, mcast_wc.imm_data);
    hrd_poll_cq(mcast->recv_cq[COM_MCAST_QP], 1, &mcast_wc);
    printf("Client %d imm data recved %d \n", clt_gid, mcast_wc.imm_data);
    hrd_poll_cq(mcast->recv_cq[COM_MCAST_QP], 1, &mcast_wc);
    printf("Client %d imm data recved %d \n", clt_gid, mcast_wc.imm_data);

    exit(0);
}
*/
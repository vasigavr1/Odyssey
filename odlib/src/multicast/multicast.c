//
// Created by vasilis on 13/07/20.
//
#include "multicast.h"

int max_int(int a, int b)
{
  return a > b ? a : b;
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
void resolve_addresses(mcast_context_t* mcast_cont)
{
  mcast_init_t *init = mcast_cont->init;
  int ret, i, t_id = mcast_cont->t_id;
  char mcast_addr[40];
  // Source addresses (i.e. local IPs)
  init->src_addr = (struct sockaddr*)&init->src_in;
  ret = get_addr(mcast_cont->local_ip, ((struct sockaddr *)&init->src_in)); // to bind
  if (ret) printf("Client: failed to get src address \n");
  for (i = 0; i < mcast_cont->groups_num; i++) {
    ret = rdma_bind_addr(init->cm_qp->cma_id[i], init->src_addr);
    if (ret) perror("Client: address bind failed");
  }
  // Destination addresses(i.e. multicast addresses)
  for (i = 0; i < mcast_cont->groups_num; i ++) {
    init->dst_addr[i] = (struct sockaddr*)&init->dst_in[i];
    int m_cast_group_id = (t_id * mcast_cont->groups_num + i);
    sprintf(mcast_addr, "224.0.%d.%d", m_cast_group_id / 256, m_cast_group_id % 256);
    //printf("Thread %u mcast addr %d: %s\n", t_id, i, mcast_addr);
    ret = get_addr((char*) &mcast_addr, ((struct sockaddr *)&init->dst_in[i]));
    if (ret) printf("Client: failed to get dst address \n");
  }
}

// Set up the Send and Receive Qps for the multicast
void set_up_mcast_qps(mcast_context_t* mcast_cont)
{
  struct cm_qps *qps = mcast_cont->init->cm_qp;
  uint32_t *max_recv_q_depth = mcast_cont->recv_q_depth;

  int ret, i, recv_q_depth;
  qps->pd = ibv_alloc_pd(qps->cma_id[0]->verbs);

  // Create the recv QPs that are needed (1 for each flow we are listening to) +
  // "useless" QPs

  for (i = 0; i < mcast_cont->recv_qp_num; i++) {
    recv_q_depth = i < mcast_cont->recv_qp_num ? max_recv_q_depth[i] : 1;
    assert(qps->cma_id[i] != NULL);
    assert(&qps->cq[i] != NULL);
    assert(&qps != NULL);
    qps->cq[i] = ibv_create_cq(qps->cma_id[i]->verbs, recv_q_depth, &qps, NULL, 0);
    struct ibv_qp_init_attr init_qp_attr;
    memset(&init_qp_attr, 0, sizeof init_qp_attr);
    init_qp_attr.cap.max_send_wr = 1;
    init_qp_attr.cap.max_recv_wr = (uint32_t) recv_q_depth;
    init_qp_attr.cap.max_send_sge = 1;
    init_qp_attr.cap.max_recv_sge = 1;
    init_qp_attr.qp_context = qps;
    init_qp_attr.sq_sig_all = 0;
    init_qp_attr.qp_type = IBV_QPT_UD;
    init_qp_attr.send_cq = qps->cq[i];
    init_qp_attr.recv_cq = qps->cq[i];
    ret = rdma_create_qp(qps->cma_id[i], qps->pd, &init_qp_attr);
    if (ret) printf("unable to create QP \n");
  }
}

// Initial function to call to setup multicast, this calls the rest of the relevant functions
void setup_multicast(mcast_context_t* mcast_cont)
{
  int ret;
  uint16_t t_id = mcast_cont->t_id;
  enum rdma_port_space port_space = RDMA_PS_UDP;
  mcast_init_t *init = mcast_cont->init;
  // Create the channel
  init->channel = rdma_create_event_channel();
  if (!init->channel) {
    printf("Client %d :failed to create event channel\n", t_id);
    exit(1);
  }
  // Set up the cma_ids
  for (int i = 0; i < mcast_cont->groups_num; i++ ) {
    ret = rdma_create_id(init->channel, &init->cm_qp->cma_id[i],
                         &init->cm_qp, port_space);
    if (ret) printf("Client %d :failed to create cma_id\n", t_id);
  }
  // deal with the addresses
  resolve_addresses(mcast_cont);
  // set up the qps
  set_up_mcast_qps(mcast_cont);

  struct rdma_cm_event* event = malloc(sizeof(struct rdma_cm_event));
  int qp_i = 0, group_i = 0;
  for (int flow_i = 0; flow_i < mcast_cont->flow_num; ++flow_i) {
    for ( int sub_group_i = 0; sub_group_i < mcast_cont->groups_per_flow[flow_i]; sub_group_i++) {
      ret = rdma_resolve_addr(init->cm_qp->cma_id[group_i], init->src_addr, init->dst_addr[group_i], 20000);
      if (ret) printf("Client %d: failed to resolve address: %d, qp_i %d \n", t_id, sub_group_i, qp_i);
      if (ret) perror("Reason");

      bool is_sender = false;

      if (mcast_cont->send_qp_num > 0) {
        if (sub_group_i == mcast_cont->group_to_send_to[flow_i]) {
          is_sender = true;
        }
      }

      while (rdma_get_cm_event(init->channel, &event) == 0) {
        bool joined = false;
        switch (event->event) {
          case RDMA_CM_EVENT_ADDR_RESOLVED:
            if (mcast_cont->recv_qp_num == 0) qp_i = 0;
            else {
              // We do not want to register the recv_qp to this group, either because we are sending to it
              // or because we are not interested in the flow
              if (is_sender || (!mcast_cont->recvs_from_flow[flow_i])) {
                qp_i = mcast_cont->recv_qp_num; // this means we are not interested to receive from this flow
              }
              else { // we are registering a valid recv_qp_to this group
                assert(!is_sender); assert(mcast_cont->recvs_from_flow[flow_i]);
                qp_i = flow_i;
              }
            } // this recv_qp will be used
            ret = rdma_join_multicast(init->cm_qp->cma_id[qp_i], init->dst_addr[group_i], init);
            if (ret) printf("unable to join multicast \n");
            break;
          case RDMA_CM_EVENT_MULTICAST_JOIN:
            joined = true;
            if (is_sender) init->mcast_ud_param[flow_i] = event->param.ud;

            //printf("RDMA JOIN MUlTICAST EVENT %d \n", sub_group_i);
            break;
          case RDMA_CM_EVENT_MULTICAST_ERROR:
          default:
            break;
        }
        rdma_ack_cm_event(event);
        if (joined) break;
      }
      group_i ++;
    }
  }
}


mcast_cb_t *construct_mcast_cb(mcast_context_t* mcast_cont)
{
  mcast_cb_t* mcast_cb = (mcast_cb_t*) malloc(sizeof(mcast_cb_t));

  mcast_cb->recv_cq = calloc(mcast_cont->recv_qp_num, sizeof(struct ibv_cq*));
  mcast_cb->recv_qp = calloc(mcast_cont->recv_qp_num, sizeof(struct ibv_qp*));
  mcast_cb->send_ah = calloc(mcast_cont->send_qp_num, sizeof(struct ibv_ah*));
  mcast_cb->qpn = calloc(mcast_cont->send_qp_num, sizeof(uint32_t));
  mcast_cb->qkey = calloc(mcast_cont->send_qp_num, sizeof(uint32_t));

  return mcast_cb;
}


void check_context_inputs(uint16_t flow_num, uint16_t recv_qp_num,
                                 uint16_t send_num,
                                 uint16_t *groups_per_flow,
                                 uint32_t *recv_q_depth,
                                 uint16_t *group_to_send_to,
                                 bool *recvs_from_flow)
{
  assert(flow_num > 0);
  assert(recv_qp_num > 0 || send_num > 0);
  assert(recv_qp_num <= flow_num);
  assert(send_num <= flow_num);
  for (int i = 0; i < recv_qp_num; ++i) {

  }
  for (int i = 0; i < flow_num; ++i) {
    assert((groups_per_flow + i) != NULL);
    assert(groups_per_flow[i] > 0);
  }

  if (send_num > 0) {
    int correct_groups_found = 0;
    for (int i = 0; i < flow_num; ++i) {
      assert((group_to_send_to + i) != NULL);
      if (group_to_send_to[i] < groups_per_flow[i])
        correct_groups_found++;
    }
    assert(correct_groups_found == send_num);
  }

  if (recv_qp_num > 0) {
    int total_recv_groups = 0;
    for (int i = 0; i < flow_num; ++i) {
      assert((recv_q_depth + i) != NULL);
      assert(recv_q_depth[i] >= 1);
      assert((recvs_from_flow + i) != NULL);
      if (recvs_from_flow[i]) {
        total_recv_groups++;
        assert(recv_q_depth[i] > 1);
      }
    }
    assert(total_recv_groups == recv_qp_num);
  }

}

mcast_context_t* construct_mcast_cont(uint16_t flow_num, uint16_t recv_qp_num,
                                             uint16_t send_num,
                                             uint16_t *groups_per_flow,
                                             uint32_t *recv_q_depth,
                                             uint16_t *group_to_send_to,
                                             bool *recvs_from_flow,
                                             const char *local_ip,
                                             void *buf, size_t buff_size,
                                             uint16_t t_id)
{

  check_context_inputs(flow_num, recv_qp_num, send_num,
                       groups_per_flow,
                       recv_q_depth, group_to_send_to,
                       recvs_from_flow);
  mcast_context_t* mcast_cont = (mcast_context_t*) malloc(sizeof(mcast_context_t));
  mcast_cont->flow_num = flow_num;

  mcast_cont->recv_qp_num = recv_qp_num; // how many flows you want to receive from
  mcast_cont->send_qp_num = send_num; // in how many flows you are an active broadcaster
  mcast_cont->buf = buf;
  mcast_cont->buff_size = buff_size;
  mcast_cont->t_id = t_id;
  mcast_cont->local_ip = malloc(16 * sizeof(char));
  strcpy(mcast_cont->local_ip, local_ip);


  mcast_cont->recv_q_depth = recv_q_depth;
  mcast_cont->groups_per_flow = groups_per_flow;
  mcast_cont->group_to_send_to = group_to_send_to;
  mcast_cont->recvs_from_flow = recvs_from_flow;

  mcast_cont->groups_num = 0;
  for (int j = 0; j < flow_num; ++j) {
    mcast_cont->groups_num += mcast_cont->groups_per_flow[j];
  }

  // Set up the init structure
  mcast_cont->init = (mcast_init_t*) malloc(sizeof(mcast_init_t));
  mcast_cont->init->dst_in = calloc(mcast_cont->groups_num, sizeof(struct sockaddr_storage));
  mcast_cont->init->dst_addr = calloc(mcast_cont->groups_num, sizeof(struct sockaddr *));
  mcast_cont->init->cm_qp = calloc(1, sizeof(struct cm_qps));
  mcast_cont->init->mcast_ud_param = calloc(mcast_cont->send_qp_num, sizeof(struct rdma_ud_param));

  // Set up cm_qp
  struct cm_qps *cm = mcast_cont->init->cm_qp;

  // In the case where we do no want to receive from a group, we still join it
  // (so that we can send in it)
  // To be able to rdma_join_multicast we need to provide a cma_id
  // Since we do not care about the group, we can t give a cma_id with a recv_qp we are using
  // Therefore in the case where we do not already have redundant cma_ids, we have to allocate a redundant one
  uint16_t cma_id_num = (uint16_t) (max_int(mcast_cont->groups_num, (mcast_cont->recv_qp_num + 1)));
  cm->cma_id = calloc(cma_id_num, sizeof(struct rdma_cm_id*));
  cm->cq = calloc(mcast_cont->groups_num, sizeof(struct ibv_cq*));

  return mcast_cont;

}


void destroy_mcast_cont(mcast_context_t* mcast_cont)
{
  free(mcast_cont->init->dst_in);
  free(mcast_cont->init->dst_addr);
  free(mcast_cont->init->mcast_ud_param);

  // Set up cm_qp
  struct cm_qps *cm = mcast_cont->init->cm_qp;
  free(cm->cma_id);
  free(cm->cq);

  free(mcast_cont->init->cm_qp);
  free(mcast_cont->init);
  free(mcast_cont);
}


mcast_cb_t* create_mcast_cb(uint16_t flow_num,
                                   uint16_t recv_qp_num,
                                   uint16_t send_num,
                                   uint16_t *groups_per_flow,
                                   uint32_t *recv_q_depth,
                                   uint16_t *group_to_send_to,
                                   bool *recvs_from_flow,
                                   const char *local_ip,
                                   void *buf, size_t buff_size,
                                   uint16_t t_id)
{


  mcast_context_t* mcast_cont = construct_mcast_cont(flow_num, recv_qp_num, send_num,
                                                     groups_per_flow,
                                                     recv_q_depth, group_to_send_to,
                                                     recvs_from_flow, local_ip,
                                                     buf, buff_size, t_id);
  setup_multicast(mcast_cont);

  mcast_cb_t* mcast_cb = construct_mcast_cb(mcast_cont);
  // Fill cb from the mcast-context
  mcast_init_t* init = mcast_cont->init;
  for (uint16_t i = 0; i < mcast_cont->recv_qp_num; i++) {
    mcast_cb->recv_cq[i] = init->cm_qp->cq[i];
    mcast_cb->recv_qp[i] = init->cm_qp->cma_id[i]->qp;
  }

  for (uint16_t i = 0; i < mcast_cont->send_qp_num; i++) {
    mcast_cb->send_ah[i] = ibv_create_ah(init->cm_qp->pd, &(init->mcast_ud_param[i].ah_attr));
    mcast_cb->qpn[i] = init->mcast_ud_param[i].qp_num;
    mcast_cb->qkey[i] = init->mcast_ud_param[i].qkey;
  }
  if (recv_qp_num > 0)
    mcast_cb->recv_mr = ibv_reg_mr(init->cm_qp->pd, mcast_cont->buf,
                                   mcast_cont->buff_size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                                                          IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);

  destroy_mcast_cont(mcast_cont);


  return mcast_cb;
}

// Showcasing how to use mutlicast,
// Note how you are using an existing qp and cq to send.
// However to recv you use the qp and cq from the mcast_cb
void multicast_testing(mcast_cb_t *mcast_cb, int t_id, int m_id,
                              uint8_t mcast_qp_id, uint16_t broadcasters_num,
                              void *buf, struct ibv_qp *qp, struct ibv_cq *cq) // W_QP_ID
{

  struct ibv_wc mcast_wc;
  printf ("Client: Multicast Qkey %u and qpn %u \n", mcast_cb->qkey[mcast_qp_id], mcast_cb->qpn[mcast_qp_id]);


  struct ibv_sge mcast_sg;
  struct ibv_send_wr mcast_wr;
  struct ibv_send_wr *mcast_bad_wr;

  memset(&mcast_sg, 0, sizeof(mcast_sg));
  mcast_sg.addr	  = (uintptr_t) buf;
  mcast_sg.length = 10;
  //mcast_sg.lkey	need not be filled

  memset(&mcast_wr, 0, sizeof(mcast_wr));
  mcast_wr.wr_id      = 0;
  mcast_wr.sg_list    = &mcast_sg;
  mcast_wr.num_sge    = 1;
  mcast_wr.opcode     = IBV_WR_SEND_WITH_IMM;
  mcast_wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
  mcast_wr.imm_data   = (uint32_t) (t_id + 120 + (m_id * 10));
  mcast_wr.next       = NULL;

  /// Use the mcast_cb here to send
  mcast_wr.wr.ud.ah          = mcast_cb->send_ah[mcast_qp_id];
  mcast_wr.wr.ud.remote_qpn  = mcast_cb->qpn[mcast_qp_id];
  mcast_wr.wr.ud.remote_qkey = mcast_cb->qkey[mcast_qp_id];

  if (ibv_post_send(qp, &mcast_wr, &mcast_bad_wr)) {
    fprintf(stderr, "Error, ibv_post_send() failed\n");
    assert(false);
  }

  printf("THe mcast was sent, I am waiting for confirmation imm data %d\n", mcast_wr.imm_data);
  while (ibv_poll_cq(cq, 1, &mcast_wc) == 0);
  assert(mcast_wc.status != 0);
  printf("The mcast was sent \n");

  for (int i = 0; i < broadcasters_num; ++i) {
    while (ibv_poll_cq(mcast_cb->recv_cq[mcast_qp_id], 1, &mcast_wc) == 0);
    printf("Worker %d imm data recved %d \n", t_id, mcast_wc.imm_data);
    assert(mcast_wc.status != 0);
  }
  exit(0);
}

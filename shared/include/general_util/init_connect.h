//
// Created by vasilis on 24/06/2020.
//

#ifndef KITE_INIT_CONNECT_H
#define KITE_INIT_CONNECT_H

#include "top.h"
#include "hrd.h"

static int spawn_stats_thread() {
  pthread_t *thread_arr = (pthread_t *) malloc(sizeof(pthread_t));
  pthread_attr_t attr;
  cpu_set_t cpus_stats;
  int core = -1;
  pthread_attr_init(&attr);
  CPU_ZERO(&cpus_stats);
  if(num_threads > 17) {
    core = 39;
    CPU_SET(core, &cpus_stats);
  }
  else {
    core = 2 * (num_threads) + 2;
    CPU_SET(core, &cpus_stats);
  }
  my_printf(yellow, "Creating stats thread at core %d\n", core);
  pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus_stats);
  return pthread_create(&thread_arr[0], &attr, print_stats, NULL);
}


/* ---------------------------------------------------------------------------
------------------------------MULTICAST --------------------------------------
---------------------------------------------------------------------------*/
// wrapper around getaddrinfo socket function
static int get_addr(char *dst, struct sockaddr *addr)
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
static void resolve_addresses(struct mcast_info *mcast_data)
{
  int ret, i, t_id = mcast_data->t_id;
  char mcast_addr[40];
  // Source addresses (i.e. local IPs)
  mcast_data->src_addr = (struct sockaddr*)&mcast_data->src_in;
  ret = get_addr(local_ip, ((struct sockaddr *)&mcast_data->src_in)); // to bind
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
static void set_up_qp(struct cm_qps* qps, int *max_recv_q_depth)
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
    init_qp_attr.cap.max_recv_wr = (uint32_t) recv_q_depth;
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
static void setup_multicast(struct mcast_info *mcast_data, int *recv_q_depth)
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
//                     printf("Client %d: RDMA ADDRESS RESOLVED address: %d \n", m_id, i);
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
static void multicast_testing(mcast_essentials_t *mcast, int clt_gid, struct hrd_ctrl_blk *cb, uint8_t qp_id) // W_QP_ID
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
  mcast_wr.imm_data   = (uint32_t) (clt_gid + 120 + (machine_id * 10));
  mcast_wr.next       = NULL;

  mcast_wr.wr.ud.ah          = mcast->send_ah[COM_MCAST_QP];
  mcast_wr.wr.ud.remote_qpn  = mcast->qpn[COM_MCAST_QP];
  mcast_wr.wr.ud.remote_qkey = mcast->qkey[COM_MCAST_QP];

  if (ibv_post_send(cb->dgram_qp[qp_id], &mcast_wr, &mcast_bad_wr)) {
    fprintf(stderr, "Error, ibv_post_send() failed\n");
    assert(false);
  }

  printf("THe mcast was sent, I am waiting for confirmation imm data %d\n", mcast_wr.imm_data);
  hrd_poll_cq(cb->dgram_send_cq[qp_id], 1, &mcast_wc);
  printf("The mcast was sent \n");
  hrd_poll_cq(mcast->recv_cq[COM_MCAST_QP], 1, &mcast_wc);
  printf("Client %d imm data recved %d \n", clt_gid, mcast_wc.imm_data);
  hrd_poll_cq(mcast->recv_cq[COM_MCAST_QP], 1, &mcast_wc);
  printf("Client %d imm data recved %d \n", clt_gid, mcast_wc.imm_data);
  hrd_poll_cq(mcast->recv_cq[COM_MCAST_QP], 1, &mcast_wc);
  printf("Client %d imm data recved %d \n", clt_gid, mcast_wc.imm_data);

  exit(0);
}

// Initialize the mcast_essentials structure that is necessary
static void init_multicast(struct mcast_info **mcast_data, struct mcast_essentials **mcast,
                    int t_id, struct hrd_ctrl_blk *cb, size_t total_buf_size, int *recv_q_depth)//, size_t recv_q_depth)
{
  *mcast_data =  (mcast_info_t *) malloc(sizeof(mcast_info_t));
  (*mcast_data)->t_id = t_id;
  setup_multicast(*mcast_data, recv_q_depth);
//   char char_buf[40];
//   inet_ntop(AF_INET6, (*mcast_data)->mcast_ud_param.ah_attr.grh.dgid.raw, char_buf, 40);
//   printf("client: joined dgid: %s mlid 0x%x sl %d\n", char_buf,	(*mcast_data)->mcast_ud_param.ah_attr.dlid, (*mcast_data)->mcast_ud_param.ah_attr.sl);
  *mcast = (mcast_essentials_t *) malloc(sizeof(mcast_essentials_t));

  for (uint16_t i = 0; i < MCAST_QP_NUM; i++){
    (*mcast)->recv_cq[i] = (*mcast_data)->cm_qp[i].cq;
    (*mcast)->recv_qp[i] = (*mcast_data)->cm_qp[i].cma_id->qp;
    (*mcast)->send_ah[i] = ibv_create_ah(cb->pd, &((*mcast_data)->mcast_ud_param[i].ah_attr));
    (*mcast)->qpn[i]  =  (*mcast_data)->mcast_ud_param[i].qp_num;
    (*mcast)->qkey[i]  =  (*mcast_data)->mcast_ud_param[i].qkey;

  }
  (*mcast)->recv_mr = ibv_reg_mr((*mcast_data)->cm_qp[0].pd, (void *)cb->dgram_buf,
                                 (size_t)total_buf_size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                                                         IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);


  free(*mcast_data);
  //if (protocol == FOLLOWER) assert((*mcast)->recv_mr != NULL);
}

//--------------------------------------------------
//--------------PUBLISHING QPS---------------------
//--------------------------------------------------


// Worker calls this function to connect with all workers
static void get_qps_from_all_other_machines(struct hrd_ctrl_blk *cb)
{
  int g_i, qp_i, w_i, m_i;
  int ib_port_index = 0;
  // -- CONNECT WITH EVERYONE
  for(g_i = 0; g_i < WORKER_NUM; g_i++) {
    if (g_i / WORKERS_PER_MACHINE == machine_id) continue; // skip the local machine
    w_i = g_i % WORKERS_PER_MACHINE;
    m_i = g_i / WORKERS_PER_MACHINE;
    for (qp_i = 0; qp_i < cb->num_dgram_qps; qp_i++) {
      /* Compute the control block and physical port index for client @i */
      int local_port_i = ib_port_index;
      assert(all_qp_attr->wrkr_qp != NULL);
      assert(all_qp_attr->wrkr_qp[m_i] != NULL);
      assert(all_qp_attr->wrkr_qp[m_i][w_i] != NULL);
      assert(&all_qp_attr->wrkr_qp[m_i][w_i][qp_i] != NULL);
      //printf("Machine %u Wrkr %u, qp %u \n", m_i, w_i, qp_i);
      qp_attr_t *wrkr_qp = &all_qp_attr->wrkr_qp[m_i][w_i][qp_i];


      struct ibv_ah_attr ah_attr = {
        //-----INFINIBAND----------
        .is_global = 0,
        .dlid = (uint16_t) wrkr_qp->lid,
        .sl = (uint8_t) wrkr_qp->sl,
        .src_path_bits = 0,
        /* port_num (> 1): device-local port for responses to this worker */
        .port_num = (uint8_t) (local_port_i + 1),
      };
      // ---ROCE----------
      if (is_roce == 1) {
        ah_attr.is_global = 1;
        ah_attr.dlid = 0;
        ah_attr.grh.dgid.global.interface_id =  wrkr_qp->gid_global_interface_id;
        ah_attr.grh.dgid.global.subnet_prefix = wrkr_qp->gid_global_subnet_prefix;
        ah_attr.grh.sgid_index = 0;
        ah_attr.grh.hop_limit = 1;
      }
      rem_qp[m_i][w_i][qp_i].ah = ibv_create_ah(cb->pd, &ah_attr);
      rem_qp[m_i][w_i][qp_i].qpn = wrkr_qp->qpn;
      // printf("%d %d %d success\n", m_i, w_i, qp_i );
      assert(rem_qp[m_i][w_i][qp_i].ah != NULL);
    }
  }
}


#define BASE_SOCKET_PORT 8080

// Machines with id higher than 0 connect with machine-id 0.
// First they sent it their qps-attrs and then receive everyone's
static void set_up_qp_attr_client(int qp_num)
{
  int sock = 0, valread;
  struct sockaddr_in serv_addr;
  if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
  {
    printf("\n Socket creation error \n");
    assert(false);
  }

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons((uint16_t)(BASE_SOCKET_PORT + machine_id - 1));

  // Convert IPv4 and IPv6 addresses from text to binary form
  if(inet_pton(AF_INET, remote_ips[0], &serv_addr.sin_addr) <= 0)
  {
    printf("\nInvalid address/ Address not supported \n");
    assert(false);
  }

  while (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0);
  struct qp_attr *qp_attr_to_send = &all_qp_attr->wrkr_qp[machine_id][0][0];
  size_t send_size = WORKERS_PER_MACHINE * qp_num * sizeof(struct qp_attr);
  send(sock, qp_attr_to_send, send_size, 0);
  struct qp_attr tmp[WORKERS_PER_MACHINE][qp_num];
  memcpy(tmp, qp_attr_to_send, send_size);
  // printf("Attributes sent\n");
  size_t recv_size = sizeof(qp_attr_t) * MACHINE_NUM * WORKERS_PER_MACHINE * qp_num;
  valread = (int) recv(sock, all_qp_attr->buf, recv_size, MSG_WAITALL);
  assert(valread == recv_size);
  int cmp = memcmp(qp_attr_to_send, qp_attr_to_send, send_size);
  assert(cmp == 0);
  // printf("Received all attributes, size %ld \n", sizeof(all_qp_attr_t));
}

// Machine 0 acts as a "server"; it receives all qp attributes,
// and broadcasts them to everyone
static void set_up_qp_attr_server(int qp_num)
{
  int server_fd[REM_MACH_NUM], new_socket[REM_MACH_NUM], valread;
  struct sockaddr_in address;
  int opt = 1;
  int addrlen = sizeof(address);

  for (int rm_i = 0; rm_i < REM_MACH_NUM; rm_i++) {
    // Creating socket file descriptor
    if ((server_fd[rm_i] = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
      printf("socket failed \n");
      assert(false);
    }

    // Forcefully attaching socket to the port 8080
    if (setsockopt(server_fd[rm_i], SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT,
                   &opt, sizeof(opt))) {
      printf("setsockopt \n");
      assert(false);
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons((uint16_t)(BASE_SOCKET_PORT + rm_i));

    // Forcefully attaching socket to the port 8080
    if (bind(server_fd[rm_i], (struct sockaddr *) &address,
             sizeof(address)) < 0) {
      printf("bind failed \n");
      assert(false);
    }
    // printf("Succesful bind \n");
    if (listen(server_fd[rm_i], 3) < 0) {
      printf("listen");
      assert(false);
    }
    // printf("Succesful listen \n");
    if ((new_socket[rm_i] = accept(server_fd[rm_i], (struct sockaddr *) &address,
                                   (socklen_t *) &addrlen)) < 0) {
      printf("accept");
      assert(false);
    }
    // printf("Successful accept \n");
    size_t recv_size = WORKERS_PER_MACHINE * qp_num * sizeof(struct qp_attr);
    valread = (int) recv(new_socket[rm_i], &all_qp_attr->wrkr_qp[rm_i + 1][0][0], recv_size, MSG_WAITALL);
    assert(valread == recv_size);
    //printf("Server received qp_attributes from machine %u size %ld \n",
    //       rm_i + 1, recv_size);
  }
  size_t send_size = sizeof(qp_attr_t) * MACHINE_NUM * WORKERS_PER_MACHINE * qp_num;
  for (int rm_i = 0; rm_i < REM_MACH_NUM; rm_i++) {
    send(new_socket[rm_i], all_qp_attr->buf, send_size, 0);
  }
}


// Used by all kinds of threads to publish their QPs
static void fill_qps(int t_id, struct hrd_ctrl_blk *cb)
{
  uint32_t qp_i;
  for (qp_i = 0; qp_i < cb->num_dgram_qps; qp_i++) {
    struct qp_attr *qp_attr = &all_qp_attr->wrkr_qp[machine_id][t_id][qp_i];
    qp_attr->lid = hrd_get_local_lid(cb->dgram_qp[qp_i]->context, cb->dev_port_id);
    qp_attr->qpn = cb->dgram_qp[qp_i]->qp_num;
    qp_attr->sl = DEFAULT_SL;
    //   ---ROCE----------
    if (is_roce == 1) {
      union ibv_gid ret_gid;
      ibv_query_gid(cb->ctx, IB_PHYS_PORT, 0, &ret_gid);
      qp_attr->gid_global_interface_id = ret_gid.global.interface_id;
      qp_attr->gid_global_subnet_prefix = ret_gid.global.subnet_prefix;
    }
  }
  // Signal to other threads that you have filled your qp attributes
  atomic_fetch_add_explicit(&workers_with_filled_qp_attr, 1, memory_order_seq_cst);
}

// All workers both use this to establish connections
static void setup_connections(uint32_t g_id,
                              struct hrd_ctrl_blk *cb)
{
  int t_id = g_id % WORKERS_PER_MACHINE;
  fill_qps(t_id, cb);

  if (t_id == 0) {
    while(workers_with_filled_qp_attr != WORKERS_PER_MACHINE);
    if (machine_id == 0) set_up_qp_attr_server(cb->num_dgram_qps);
    else set_up_qp_attr_client(cb->num_dgram_qps);
    get_qps_from_all_other_machines(cb);
//    assert(!qps_are_set_up);
    // Spawn a thread that prints the stats

//    atomic_store_explicit(&qps_are_set_up, true, memory_order_release);
  }
//  else {
//    while (!atomic_load_explicit(&qps_are_set_up, memory_order_acquire));  usleep(200000);
//  }
//  assert(qps_are_set_up);
//    printf("Thread %d has all the needed ahs\n", g_id );
}

static void thread_zero_spawns_stat_thread(uint16_t t_id)
{
  if (t_id == 0) {
    if (CLIENT_MODE != CLIENT_UI) {
      if (spawn_stats_thread() != 0)
        my_printf(red, "Stats thread was not successfully spawned \n");
    }
  }
}

static void wait_for_thread_zero()
{

}


static void setup_connections_and_spawn_stats_thread(uint32_t g_id,
                                                     struct hrd_ctrl_blk *cb,
                                                     uint16_t t_id)
{
  setup_connections(g_id, cb);
  thread_zero_spawns_stat_thread(t_id);

  if (t_id == 0) atomic_store_explicit(&qps_are_set_up, true, memory_order_release);
  else {
    while (!atomic_load_explicit(&qps_are_set_up, memory_order_acquire));
    usleep(200000);
  }
}

#endif //KITE_INIT_CONNECT_H

//
// Created by vasilis on 10/07/20.
//

#ifndef ODYSSEY_MULTICAST_H
#define ODYSSEY_MULTICAST_H



// Multicast
#include <rdma/rdma_cma.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <byteswap.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdbool.h>
#include <assert.h>

// This helps us set up the necessary rdma_cm_ids for the multicast groups
struct cm_qps
{
  struct rdma_cm_id **cma_id;
  struct ibv_pd* pd;
  struct ibv_cq** cq;
};


// This helps us set up the multicasts
typedef struct mcast_init
{
  //int	t_id;
  struct rdma_event_channel *channel;
  struct sockaddr_storage *dst_in;
  struct sockaddr **dst_addr;
  struct sockaddr_storage src_in;
  struct sockaddr *src_addr;
  struct cm_qps *cm_qp;
  //Send-only stuff
  struct rdma_ud_param *mcast_ud_param;

} mcast_init_t;

typedef struct mcast_context {
  mcast_init_t *init;
  uint16_t groups_num;
  uint16_t machine_num;
  uint32_t *recv_q_depth;
  uint16_t *group_to_send_to;
  bool *recvs_from_flow;
  void *buf;
  size_t buff_size;
  uint16_t flow_num;
  uint16_t *groups_per_flow;

  char *local_ip;

  uint16_t send_qp_num;
  uint16_t recv_qp_num;
  uint16_t t_id;
} mcast_context_t;


// this contains all data we need to perform our mcasts
typedef struct mcast_cb {
  struct ibv_cq **recv_cq;
  struct ibv_qp **recv_qp;
  struct ibv_mr *recv_mr;
  struct ibv_ah **send_ah;
  uint32_t *qpn;
  uint32_t *qkey;
} mcast_cb_t;




/*------------------------MULTICAST EXPLANATION-----------------
 * We will create N multicast groups in the switch.
 * For each group we need a unique IP that will be in mcast_cont->init->dst_in.
 * We create the different addresses in resolve_addresses()
 * For each group we need one struct rdma_cm_id (i.e.mcast_cont->init->cm_qp->cma_id).
 * rdma_resolve_addr() will associate the unique dst_in with the unique rdma_cm_ids
 * ---
 * This is where it gets tricky. The expected patter is that with each rdma_cm_id, we create one
 * QP, such that we have one QP per multicast group.
 * However, this is not what we want.
 * The reason is that ALL messages that go to a multicast group are forwarded to all receive QPs that are registered
 * This means that if we were to associate one multicast group with one logical flow (e.g. broadcasting writes),
 * then we would be receiving the messages we are broadcasting.
 *
 * To avoid this we do something different:
 * For each logical flow, we create one multicast group for each active broadcaster.
 * For 5 machines all broadcasting writes we must do the following:
 * 1) Create 5 multicast groups
 * Each machine broadcasts to one group
 * Each machine receives from all groups except one -- the one it broadcasts to
 * (so that it does not receive its own messages)
 *
 * Therefore, we do not need one QP per rdma_cm_id.
 * This is essentially an anti-pattern and creates the complexity.
 *
 * To Receive from a broadcast group we must:
 * -- Create a qp (rdma_create_qp) and a cq(ibv_create_cq) and associated with an rdma_cm_id.
 * -- To register then to the multicast group we must rdma_join_multicast()
 *    with the dst_address using that rdma_cm_id
 *
 * To Send to broadcast group we need not make use of the QPs (!)
 * We need only use the parameters of the group (rdma_cm_event->param.ud) to set up our regular WRs
 * I.e. we will use whatever send QP we do for unicasts,
 * but we will simply change the parameters of the send_Wrs
 * Then the sent messages will find the multicast group in the switch
 * We get the rdma_cm_event->param.ud when rdma_cm_event->event == RDMA_CM_EVENT_MULTICAST_JOIN
 * Therefore, we need to save that, for the multicast group we are interested in
 *
 * After calling setuo_multicast(), we simply pass iny the useful attributes to mcast_cb,
 * Finally, we reigster the buffer, where received messages will arrive, this could be done
 * by the client directly, using their own protection domain (pd)
 *
 * HOW TO USE THE CB:
 * -- Receive:
 * Replace your regualar recv cq and qp with the ones int he qp
 * Use the mcast_cb->recv_mr->lkey when posting receives
 * -- Send
 * Use the send_ah, qpn and_qkey to set up your send work requests
 *
 *
 * Input explanation:
 *  --flow_num : how many distinct logical multicast flows (e.g. prepares and commits for Zookeeper)
 *  --recv_qp_num : in how many flows you want to receive from
 *  --send_num : in how many flow you want to broadcast to
 *  --groups_per_flow: how many groups to create in each flow (typically one per broadcaster in the flow)
 *  --recv_q_depth : an array with  'flow_num' entries that specifies the q_depth for each flow.
 *      Can be NULL if you are not receiving at all. For flow that are received from the depth must be bigger than 1.
 *  --group_to_send_to : an array with  'flow_num' entries, that specifies to which group of each flow we want to be broadcasting.
 *    To denote we do not want to broadcast in the flow, use a big number (bigger than the groups of that flow).
 *    It can be NUll, if you are not sending to any flow (send_num == 0).
 *  -- recv_from_flow, an array with  'flow_num' entries that specifies whether we want to receive from each flow.
 *      If true for flow i, then we the recv_qpm will be joined for all subgroups of flow = i, except the one we send to (if any)
 *      It can be NUll if recv_qp_num == 0
 *  -- local_ip: the ip of the local machine
 *  -- recv_buf: the buffer that will be used to store received messages need to be reigstered
 *  -- recv_buf_size: capacity in Bytes of recv_buf
 *  -- t_id : the thread id
 * */


mcast_cb_t* create_mcast_cb(uint16_t flow_num,
                            uint16_t recv_qp_num,
                            uint16_t send_num,
                            uint16_t *groups_per_flow,
                            uint32_t *recv_q_depth,
                            uint16_t *group_to_send_to,
                            bool *recvs_from_flow,
                            const char *local_ip,
                            void *buf, size_t buff_size,
                            uint16_t t_id);

// Showcasing how to use mutlicast,
// Note how you are using an existing qp and cq to send.
// However to recv you use the qp and cq from the mcast_cb
void multicast_testing(mcast_cb_t *mcast_cb, int t_id, int m_id,
                       uint8_t mcast_qp_id, uint16_t broadcasters_num,
                       void *buf, struct ibv_qp *qp, struct ibv_cq *cq);


#endif //ODYSSEY_MULTICAST_H

#include "hrd.h"

/*
 * If @prealloc_conn_buf != NULL, @conn_buf_size is the w_size of the preallocated
 * buffer. If @prealloc_conn_buf == NULL, @conn_buf_size is the w_size of the
 * new buffer to create.
 */
hrd_ctrl_blk_t*
hrd_ctrl_blk_init(int local_hid, int port_index,
	int numa_node_id, /* -1 means don't use hugepages */
	int num_conn_qps, int use_uc,
	volatile void *prealloc_conn_buf, int conn_buf_size, int conn_buf_shm_key,
	int num_dgram_qps, int dgram_buf_size, int dgram_buf_shm_key, int* recv_q_depth, int *send_q_depth)
{


	// my_printf(red, "HRD: creating control block %d: port %d, socket %d, "
	// 	"conn qps %d, UC %d, conn recv_buf %d bytes (key %d), "
	// 	"dgram qps %d, dgram recv_buf %d bytes (key %d)\n",
	// 	local_hid, port_index, numa_node_id,
	// 	num_conn_qps, use_uc, conn_buf_size, conn_buf_shm_key,
	// 	num_dgram_qps, dgram_buf_size, dgram_buf_shm_key);

	/*
	 * Check arguments for sanity.
	 * @local_hid can be anything: it's just control block identifier that is
	 * useful in printing debug info.
	 */
	assert(port_index >= 0 && port_index <= 16);
	assert(numa_node_id >= -1 && numa_node_id <= 8);
	assert(num_conn_qps >= 0);
	assert(use_uc == 0 || use_uc == 1);
	assert(conn_buf_size >= 0 && conn_buf_size <= M_1024);

	/* If there is no preallocated buffer, shm key can be =/>/< 0 */
	if(prealloc_conn_buf != NULL) {
		assert(conn_buf_shm_key == -1);
	}
	assert(num_dgram_qps >= 0 && num_dgram_qps <= M_2);
	assert(dgram_buf_size >= 0 && dgram_buf_size <= M_1024);

	if(num_conn_qps == 0 && num_dgram_qps == 0) {
    my_printf(red, "HRD: Control block initialization without QPs. Are you"
                   " sure you want to do this?\n");
		assert(false);
	}

	hrd_ctrl_blk_t *cb = (hrd_ctrl_blk_t *)
		malloc(sizeof(hrd_ctrl_blk_t));
	memset(cb, 0, sizeof(hrd_ctrl_blk_t));

	/* Fill in the control block */
	cb->local_hid = local_hid;
	cb->port_index = port_index;
	cb->numa_node_id = numa_node_id;
	cb->use_uc = use_uc;

	cb->num_conn_qps = num_conn_qps;
	cb->conn_buf_size = conn_buf_size;
	cb->conn_buf_shm_key = conn_buf_shm_key;

	cb->num_dgram_qps = num_dgram_qps;
	cb->dgram_buf_size = dgram_buf_size;
	cb->dgram_buf_shm_key = dgram_buf_shm_key;

//
	cb->recv_q_depth = recv_q_depth;
	cb->send_q_depth = send_q_depth;
//
	/* Get the device to use. This fills in cb->device_id and cb->dev_port_id */
	struct ibv_device *ib_dev = hrd_resolve_port_index(cb, port_index);
	CPE(!ib_dev, "HRD: IB device not found", 0);

	/* Use a single device context and PD for all QPs */
	cb->ctx = ibv_open_device(ib_dev);
	CPE(!cb->ctx, "HRD: Couldn't get context", 0);

	cb->pd = ibv_alloc_pd(cb->ctx);
	CPE(!cb->pd, "HRD: Couldn't allocate PD", 0);

	int ib_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
		IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;

	/*
	 * Create datagram QPs and transition them RTS.
	 * Create and register datagram RDMA buffer.
	 */
	if(num_dgram_qps >= 1) {
		cb->dgram_qp = (struct ibv_qp **)
			malloc(num_dgram_qps * sizeof(struct ibv_qp *));
		cb->dgram_send_cq = (struct ibv_cq **)
			malloc(num_dgram_qps * sizeof(struct ibv_cq *));
		cb->dgram_recv_cq = (struct ibv_cq **)
			malloc(num_dgram_qps * sizeof(struct ibv_cq *));

		assert(cb->dgram_qp != NULL && cb->dgram_send_cq != NULL &&
			cb->dgram_recv_cq != NULL);

		hrd_create_dgram_qps(cb);

		/* Create and register dgram_buf */
		int reg_size = 0;

		if(numa_node_id >= 0) {
			/* Hugepages */
			while(reg_size < dgram_buf_size) {
				reg_size += M_2;
			}

			/* SHM key 0 is hard to free later */
			assert(dgram_buf_shm_key >= 1 && dgram_buf_shm_key <= 128);
			cb->dgram_buf = (volatile uint8_t *) hrd_malloc_socket(
				dgram_buf_shm_key, reg_size, numa_node_id);
		} else {
			reg_size = dgram_buf_size;
			cb->dgram_buf = (volatile uint8_t *) memalign(4096, reg_size);
		}

		assert(cb->dgram_buf != NULL);
		memset((char *) cb->dgram_buf, 0, reg_size);

		cb->dgram_buf_mr = ibv_reg_mr(cb->pd,
			(char *) cb->dgram_buf, reg_size, ib_flags);
		assert(cb->dgram_buf_mr != NULL);
	}

	/*
 	 * Create connected QPs and transition them to RTS.
	 * Create and register connected QP RDMA buffer.
	 */
	if(num_conn_qps >= 1) assert(false);

	free(recv_q_depth);
	free(send_q_depth);
	return cb;
}

/* Free up the resources taken by @cb. Return -1 if something fails, else 0. */
int hrd_ctrl_blk_destroy(hrd_ctrl_blk_t *cb)
{
	int i;
  my_printf(red, "HRD: Destroying control block %d\n", cb->local_hid);

	/* Destroy QPs and CQs. QPs must be destroyed before CQs. */
	for(i = 0; i < cb->num_dgram_qps; i++) {
		assert(cb->dgram_send_cq[i] != NULL && cb->dgram_recv_cq[i] != NULL);
		assert(cb->dgram_qp[i] != NULL);

		if(ibv_destroy_qp(cb->dgram_qp[i])) {
			fprintf(stderr, "HRD: Couldn't destroy dgram QP %d\n", i);
			return -1;
		}

		if(ibv_destroy_cq(cb->dgram_send_cq[i])) {
			fprintf(stderr, "HRD: Couldn't destroy dgram SEND CQ %d\n", i);
			return -1;
		}

		if(ibv_destroy_cq(cb->dgram_recv_cq[i])) {
			fprintf(stderr, "HRD: Couldn't destroy dgram RECV CQ %d\n", i);
			return -1;
		}
	}

	for(i = 0; i < cb->num_conn_qps; i++) {
		assert(cb->conn_cq[i] != NULL && cb->conn_qp[i] != NULL);

		if(ibv_destroy_qp(cb->conn_qp[i])) {
			fprintf(stderr, "HRD: Couldn't destroy conn QP %d\n", i);
			return -1;
		}

		if(ibv_destroy_cq(cb->conn_cq[i])) {
			fprintf(stderr, "HRD: Couldn't destroy conn CQ %d\n", i);
			return -1;
		}
	}

	/* Destroy memory regions */
	if(cb->num_dgram_qps > 0) {
		assert(cb->dgram_buf_mr != NULL && cb->dgram_buf != NULL);
		if(ibv_dereg_mr(cb->dgram_buf_mr)) {
			fprintf(stderr, "HRD: Couldn't deregister dgram MR for cb %d\n",
				cb->local_hid);
			return -1;
		}

		if(cb->numa_node_id >= 0) {
			if(hrd_free(cb->dgram_buf_shm_key, (void *) cb->dgram_buf)) {
				fprintf(stderr, "HRD: Error freeing dgram hugepages for cb %d\n",
					cb->local_hid);
			}
		} else {
			free((void *) cb->dgram_buf);
		}
	}

	if(cb->num_conn_qps > 0) {
		assert(cb->conn_buf_mr != NULL);
		if(ibv_dereg_mr(cb->conn_buf_mr)) {
			fprintf(stderr, "HRD: Couldn't deregister conn MR for cb %d\n",
				cb->local_hid);
			return -1;
		}

		if(cb->numa_node_id >= 0) {
			if(hrd_free(cb->conn_buf_shm_key, (void *) cb->conn_buf)) {
				fprintf(stderr, "HRD: Error freeing conn hugepages for cb %d\n",
					cb->local_hid);
			}
		} else {
			free((void *) cb->conn_buf);
		}
	}

	/* Destroy protection domain */
	if(ibv_dealloc_pd(cb->pd)) {
		fprintf(stderr, "HRD: Couldn't dealloc PD for cb %d\n", cb->local_hid);
		return -1;
	}

	/* Destroy device context */
	if(ibv_close_device(cb->ctx)) {
		fprintf(stderr, "Couldn't release context for cb %d\n", cb->local_hid);
		return -1;
	}

  my_printf(red, "HRD: Control block %d destroyed.\n", cb->local_hid);
	return 0;
}


/* Create datagram QPs and transition them to RTS */
void hrd_create_dgram_qps(hrd_ctrl_blk_t *cb)
{
	int i;
	assert(cb->dgram_qp != NULL && cb->dgram_send_cq != NULL &&
		cb->dgram_recv_cq != NULL && cb->pd != NULL && cb->ctx != NULL);
	assert(cb->num_dgram_qps >= 1 && cb->dev_port_id >= 1);

	for(i = 0; i < cb->num_dgram_qps; i++) {
    //printf("Send q_depth %d\n",cb->send_q_depth[i]);
    assert(cb->send_q_depth + i != NULL);
		cb->dgram_send_cq[i] = ibv_create_cq(cb->ctx,
			cb->send_q_depth[i], NULL, NULL, 0);
		assert(cb->dgram_send_cq[i] != NULL);

		//printf("receive q_depth %d\n",cb->recv_q_depth[i]);
		cb->dgram_recv_cq[i] = ibv_create_cq(cb->ctx,
			cb->recv_q_depth[i], NULL, NULL, 0);
		assert(cb->dgram_recv_cq[i] != NULL);


		/* Initialize creation attributes */
		struct ibv_qp_init_attr create_attr;
		memset((void *) &create_attr, 0, sizeof(struct ibv_qp_init_attr));
		// if (i > 0) printf("The recv queue %d has w_size %d, the send queue has w_size
		// 		%d\n", i, cb->recv_q_depth[i], cb->send_q_depth[i] );
		create_attr.send_cq = cb->dgram_send_cq[i];
		create_attr.recv_cq = cb->dgram_recv_cq[i];
		create_attr.qp_type = IBV_QPT_UD;

		create_attr.cap.max_send_wr = cb->send_q_depth[i];

		//printf("Receive q depth %d\n", cb->recv_q_depth);
		create_attr.cap.max_recv_wr = cb->recv_q_depth[i];

		create_attr.cap.max_send_sge = 1;
		create_attr.cap.max_recv_sge = 1;
		create_attr.cap.max_inline_data = MAXIMUM_INLINE_SIZE;

		cb->dgram_qp[i] = ibv_create_qp(cb->pd, &create_attr);
		assert(cb->dgram_qp[i] != NULL);

		/* INIT state */
		struct ibv_qp_attr init_attr;
		memset((void *) &init_attr, 0, sizeof(struct ibv_qp_attr));
		init_attr.qp_state = IBV_QPS_INIT;
		init_attr.pkey_index = 0;
		init_attr.port_num = cb->dev_port_id;
		init_attr.qkey = HRD_DEFAULT_QKEY;

		if(ibv_modify_qp(cb->dgram_qp[i], &init_attr,
			IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_QKEY)) {
			fprintf(stderr, "Failed to modify dgram QP to INIT\n");
			return;
		}

		/* RTR state */
		struct ibv_qp_attr rtr_attr;
		memset((void *) &rtr_attr, 0, sizeof(struct ibv_qp_attr));
		rtr_attr.qp_state = IBV_QPS_RTR;

		if(ibv_modify_qp(cb->dgram_qp[i], &rtr_attr, IBV_QP_STATE)) {
			fprintf(stderr, "Failed to modify dgram QP to RTR\n");
			exit(-1);
		}

		/* Reuse rtr_attr for RTS */
		rtr_attr.qp_state = IBV_QPS_RTS;
		rtr_attr.sq_psn = HRD_DEFAULT_PSN;

		if(ibv_modify_qp(cb->dgram_qp[i], &rtr_attr,
			IBV_QP_STATE | IBV_QP_SQ_PSN)) {
			fprintf(stderr, "Failed to modify dgram QP to RTS\n");
			exit(-1);
		}
	}
}

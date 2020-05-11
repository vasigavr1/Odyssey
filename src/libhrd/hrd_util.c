//#include <zconf.h>
#include "hrd.h"


char **remote_ips, *local_ip, *dev_name;


/* Print information about all IB devices in the system */
void hrd_ibv_devinfo(void)
{
	int num_devices = 0, dev_i;
	struct ibv_device **dev_list;
	struct ibv_context *ctx;
	struct ibv_device_attr device_attr;

  my_printf(red, "HRD: printing IB dev info\n");

	dev_list = ibv_get_device_list(&num_devices);
	CPE(!dev_list, "Failed to get IB devices list", 0);

	for (dev_i = 0; dev_i < num_devices; dev_i++) {
		ctx = ibv_open_device(dev_list[dev_i]);
		CPE(!ctx, "Couldn't get context", 0);

		memset(&device_attr, 0, sizeof(device_attr));
		if (ibv_query_device(ctx, &device_attr)) {
			printf("Could not query device: %d\n", dev_i);
			assert(false);
		}

		printf("IB device %d:\n", dev_i);
		printf("    Name: %s\n", dev_list[dev_i]->name);
		printf("    Device name: %s\n", dev_list[dev_i]->dev_name);
		printf("    GUID: %016llx\n",
			(unsigned long long) ibv_get_device_guid(dev_list[dev_i]));
		printf("    Node type: %d (-1: UNKNOWN, 1: CA, 4: RNIC)\n",
			dev_list[dev_i]->node_type);
		printf("    Transport type: %d (-1: UNKNOWN, 0: IB, 1: IWARP)\n",
			dev_list[dev_i]->transport_type);

		printf("    fw: %s\n", device_attr.fw_ver);
		printf("    max_qp: %d\n", device_attr.max_qp);
		printf("    max_cq: %d\n", device_attr.max_cq);
		printf("    max_mr: %d\n", device_attr.max_mr);
		printf("    max_pd: %d\n", device_attr.max_pd);
		printf("    max_ah: %d\n", device_attr.max_ah);
		printf("    max_mcast_grp: %d\n", device_attr.max_mcast_grp);
		printf("    max_mcast_qp_attach: %d\n", device_attr.max_mcast_qp_attach);
	}
}

/*
 * Finds the port with rank `port_index` (0-based) in the list of ENABLED ports.
 * Fills its device id and device-local port id (1-based) into the supplied
 * control block.
 */
struct ibv_device*
hrd_resolve_port_index(struct hrd_ctrl_blk *cb, int port_index)
{
	struct ibv_device **dev_list;
	int num_devices = 0;

	assert(port_index >= 0);

	cb->device_id = -1;
	cb->dev_port_id = -1;

	dev_list = ibv_get_device_list(&num_devices);
	CPE(!dev_list, "HRD: Failed to get IB devices list", 0);

	int ports_to_discover = port_index;
	int dev_i;
  bool device_given_found = false;
  for(dev_i = 0; dev_i < num_devices; dev_i++) {
    if (strcmp(dev_list[dev_i]->name, dev_name) == 0)
      device_given_found = true;
  }
  if (!device_given_found) {
    my_printf(red, "DEVICE NAME GIVEN NOT FOUND (%s)\n", dev_name);
  }

	for(dev_i = 0; dev_i < num_devices; dev_i++) {
		if (strcmp(dev_list[dev_i]->name, dev_name) != 0) continue;

		//printf("%d/%d: Device name: %s \n", dev_i, num_devices, dev_list[dev_i]->name);
		struct ibv_context *ctx = ibv_open_device(dev_list[dev_i]);
		CPE(!ctx, "HRD: Couldn't open device", 0);

		struct ibv_device_attr device_attr;
		memset(&device_attr, 0, sizeof(device_attr));
		if(ibv_query_device(ctx, &device_attr)) {
			printf("HRD: Could not query device: %d\n", dev_i);
			exit(-1);
		}

		uint8_t port_i;
		for(port_i = 1; port_i <= device_attr.phys_port_cnt; port_i++) {

			/* Count this port only if it is enabled */
			struct ibv_port_attr port_attr;
			if(ibv_query_port(ctx, port_i, &port_attr) != 0) {
				printf("HRD: Could not query port %d of device %d\n",
					port_i, dev_i);
				exit(-1);
			}

			if(port_attr.phys_state != IBV_PORT_ACTIVE &&
				port_attr.phys_state != IBV_PORT_ACTIVE_DEFER) {
#ifndef __cplusplus
        ;
				//printf("HRD: Ignoring port %d of device %d. State is %s\n",
				//	port_i, dev_i, ibv_port_state_str(port_attr.phys_state));
#else
				printf("HRD: Ignoring port %d of device %d. State is %s\n",
					port_i, dev_i,
					ibv_port_state_str((ibv_port_state) port_attr.phys_state));
#endif
        printf("Device %s is has no available ports \n", dev_list[dev_i]->name);
				continue;
			}

			if(ports_to_discover == 0) {
				// printf("HRD: port index %d resolved to device %s, port %d\n",
				// 	port_index, dev_list[dev_i]->name, port_i);

				/* Fill the device ID and device-local port ID */
				cb->device_id = dev_i;
				cb->dev_port_id = port_i;

				if (ibv_close_device(ctx)) {
					fprintf(stderr, "HRD: Couldn't release context\n");
					assert(false);
				}

				return dev_list[cb->device_id];
			}

			ports_to_discover--;
		}

		if (ibv_close_device(ctx)) {
			fprintf(stderr, "HRD: Couldn't release context\n");
			assert(false);
		}
	}

	/* If we come here, port resolution failed */
	assert(cb->device_id == -1 && cb->dev_port_id == -1);
	fprintf(stderr, "HRD: Invalid port index %d. Exiting.\n", port_index);
	exit(-1);
}

/* Allocate SHM with @shm_key, and save the shmid into @shm_id_ret */
void* hrd_malloc_socket(int shm_key, int size, int socket_id)
{
	int shmid;
	if(USE_HUGE_PAGES == 1)
		shmid = shmget(shm_key,
			size, IPC_CREAT | IPC_EXCL | 0666 | SHM_HUGETLB);
	else
		shmid = shmget(shm_key,
			size, IPC_CREAT | IPC_EXCL | 0666);

	if(shmid == -1) {
		switch(errno) {
			case EACCES:
        my_printf(red, "HRD: SHM malloc error: Insufficient permissions."
                       " (SHM key = %d)\n", shm_key);
				break;
			case EEXIST:
        my_printf(red, "HRD: SHM malloc error: Already exists."
                       " (SHM key = %d)\n", shm_key);
				break;
			case EINVAL:
        my_printf(red, "HRD: SHM malloc error: SHMMAX/SHMIN mismatch."
                       " (SHM key = %d, w_size = %d)\n", shm_key, size);
				break;
			case ENOMEM:
        my_printf(red, "HRD: SHM malloc error: Insufficient memory."
                       " (SHM key = %d, w_size = %d)\n", shm_key, size);
				break;
			case ENOENT:
        my_printf(red, "HRD: SHM malloc error: No segment exists for the given key, and IPC_CREAT was not specified."
                       " (SHM key = %d, w_size = %d)\n", shm_key, size);
				break;
			case ENOSPC:
        my_printf(red,
            "HRD: SHM malloc error: All possible shared memory IDs have been taken or the limit of shared memory is exceeded."
                " (SHM key = %d, w_size = %d)\n", shm_key, size);
				break;
			case EPERM:
        my_printf(red, "HRD: SHM malloc error: The SHM_HUGETLB flag was specified, but the caller was not privileged"
                       " (SHM key = %d, w_size = %d)\n", shm_key, size);
				break;
			case ENFILE:
        my_printf(red, "HRD: SHM malloc error: The system-wide limit on the total number of open files has been reached."
                       " (SHM key = %d, w_size = %d)\n", shm_key, size);
				break;
			default:
        my_printf(red, "HRD: SHM malloc error: A wild SHM error: %s.\n",
                   strerror(errno));
				break;
		}
		assert(false);
	}

	void *buf = shmat(shmid, NULL, 0);
	if(buf == NULL) {
		printf("HRD: SHM malloc error: shmat() failed for key %d\n", shm_key);
		exit(-1);
	}

	/* Bind the buffer to this socket */
	const unsigned long nodemask = (1 << socket_id);
	int ret = mbind(buf, size, MPOL_BIND, &nodemask, 32, 0);
	if(ret != 0) {
		printf("HRD: SHM malloc error. mbind() failed for key %d\n", shm_key);
		exit(-1);
	}

	// try to take advantage of TLB coalescing, if it is there
	if (LEVERAGE_TLB_COALESCING) {
		int page_no = CEILING(size, HUGE_PAGE_SIZE);
		int i;
		for (i = 0; i < page_no; i++)
			memset(buf + i * HUGE_PAGE_SIZE, 0, 1);
	}

	return buf;
}

/* Free shm @shm_key and @shm_buf. Return 0 on success, else -1. */
int hrd_free(int shm_key, void *shm_buf)
{
	int ret;
	int shmid = shmget(shm_key, 0, 0);
	if(shmid == -1) {
		switch(errno) {
			case EACCES:
				printf("HRD: SHM free error: Insufficient permissions."
					" (SHM key = %d)\n", shm_key);
				break;
			case ENOENT:
				printf("HRD: SHM free error: No such SHM key. (SHM key = %d)\n",
					shm_key);
				break;
			default:
				printf("HRD: SHM free error: A wild SHM error: %s\n",
					strerror(errno));
				break;
		}
		return -1;
	}

	ret = shmctl(shmid, IPC_RMID, NULL);
	if(ret != 0) {
		printf("HRD: SHM free error: shmctl() failed for key %d\n", shm_key);
		exit(-1);
	}

	ret = shmdt(shm_buf);
	if(ret != 0) {
		printf("HRD: SHM free error: shmdt() failed for key %d\n", shm_key);
		exit(-1);
	}

	return 0;
}

void hrd_nano_sleep(int ns)
{
	long long start = hrd_get_cycles();
	long long end = start;
	int upp = (int) (2.1 * ns);
	while(end - start < upp) {
		end = hrd_get_cycles();
	}
}

/* Get the LID of a port on the device specified by @ctx */
uint16_t hrd_get_local_lid(struct ibv_context *ctx, int dev_port_id)
{
	assert(ctx != NULL && dev_port_id >= 1);

	struct ibv_port_attr attr;
	if(ibv_query_port(ctx, dev_port_id, &attr)) {
		printf("HRD: ibv_query_port on port %d of device %s failed! Exiting.\n",
			dev_port_id, ibv_get_device_name(ctx->device));
		assert(false);
	}

	return attr.lid;
}

/* Return the environment variable @name if it is set. Exit if not. */
char* hrd_getenv(const char *name)
{
	char *env = getenv(name);
	if(env == NULL) {
		fprintf(stderr, "Environment variable %s not set\n", name);
		assert(false);
	}

	return env;
}

/* Record the current time in @timebuf. @timebuf must have at least 50 bytes. */
void hrd_get_formatted_time(char *timebuf)
{
	assert(timebuf != NULL);
	time_t timer;
	struct tm* tm_info;

	time(&timer);
	tm_info = localtime(&timer);

	strftime(timebuf, 26, "%Y:%m:%d %H:%M:%S", tm_info);
}

void hrd_post_dgram_recv(struct ibv_qp *qp, void *buf_addr, int len, int lkey)
{
	int ret;
	struct ibv_recv_wr *bad_wr;

	struct ibv_sge list;
	memset(&list, 0, sizeof(struct ibv_sge));
	list.length = len;
	list.lkey = lkey;

	struct ibv_recv_wr recv_wr;
	memset(&recv_wr, 0, sizeof(struct ibv_recv_wr));
	recv_wr.sg_list = &list;
	recv_wr.num_sge = 1;
	recv_wr.next = NULL;

	recv_wr.sg_list->addr = (uintptr_t) buf_addr;

	ret = ibv_post_recv(qp, &recv_wr, &bad_wr);
	if(ret) {
		fprintf(stderr, "HRD: Error %d posting datagram recv.\n", ret);
		exit(-1);
	}
}

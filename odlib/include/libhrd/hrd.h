/*
*
* Adapted from https://github.com/efficient/rdma_bench
*/

#ifndef HRD_H
#define HRD_H


#include "top.h"


#define HRD_DEFAULT_PSN 3185	/* PSN for all queues */ // starting Packet Sequence Number
#define HRD_DEFAULT_QKEY 0x11111111
#define HRD_MAX_LID 256
#define HUGE_PAGE_SIZE 2097152
#define LEVERAGE_TLB_COALESCING 1
#define IB_PHYS_PORT 1
#define USE_HUGE_PAGES 1


typedef struct hrd_ctrl_blk {

	int local_hid;	/* Local ID on the machine this process runs on */

	/* Info about the device/port to use for this control block */
	struct ibv_context *ctx;
	int port_index;	/* User-supplied. 0-based across all devices */
	int device_id;	/* Resovled by libhrd from @port_index */
	int dev_port_id;	/* 1-based within dev @device_id. Resolved by libhrd */
	int numa_node_id;	/* NUMA node id */

	struct ibv_pd *pd;	/* A protection domain for this control block */

	/* Connected QPs */
	int use_uc;
	int num_conn_qps;
	struct ibv_qp **conn_qp;
	struct ibv_cq **conn_cq;
	volatile uint8_t *conn_buf;	/* A buffer for RDMA over RC/UC QPs */
	int conn_buf_size;
	int conn_buf_shm_key;
	struct ibv_mr *conn_buf_mr;

	/* Datagram QPs */
	int num_dgram_qps;
	struct ibv_qp **dgram_qp;
	struct ibv_cq **dgram_send_cq, **dgram_recv_cq;
	volatile uint8_t *dgram_buf;	/* A buffer for RECVs on dgram QPs */
	int dgram_buf_size;
	int *recv_q_depth;
	int *send_q_depth;
	int dgram_buf_shm_key;
	struct ibv_mr *dgram_buf_mr;

	//struct ibv_wc *wc;	/* Array of work completions */
} hrd_ctrl_blk_t;

/* Major initialization functions */
hrd_ctrl_blk_t* hrd_ctrl_blk_init(int local_hid,
									   int port_index, int numa_node_id,
									   int num_conn_qps, int use_uc,
									   volatile void *prealloc_conn_buf, int conn_buf_size, int conn_buf_shm_key,
									   int num_dgram_qps, int dgram_buf_size, int dgram_buf_shm_key,
									   int *recv_q_depth, int *send_q_depth);

int hrd_ctrl_blk_destroy(hrd_ctrl_blk_t *cb);

/* Debug */
void hrd_ibv_devinfo(void);

/* RDMA resolution functions */
struct ibv_device* hrd_resolve_port_index(hrd_ctrl_blk_t *cb,
										  int port_index);

uint16_t hrd_get_local_lid(struct ibv_context *ctx, int port_id);

void hrd_create_dgram_qps(hrd_ctrl_blk_t *cb);


/* Post 1 RECV for this queue pair for this buffer. Low performance. */
void hrd_post_dgram_recv(struct ibv_qp *qp, void *buf_addr, int len, int lkey);

/* Fill @wc with @num_comps comps from this @cq. Exit on error. */
static inline uint32_t
hrd_poll_cq(struct ibv_cq *cq, int num_comps, struct ibv_wc *wc)
{
	int comps = 0;
	uint32_t debug_cnt = 0;
	while(comps < num_comps) {
		if (debug_cnt > M_256) {
			printf("Someone is stuck waiting for a completion %d / %d  \n", comps, num_comps );
			debug_cnt = 0;
		}
		int new_comps = ibv_poll_cq(cq, num_comps - comps, &wc[comps]);
		if(new_comps != 0) {
//			 printf("I see completions %d\n", new_comps);
			/* Ideally, we should check from comps -> new_comps - 1 */
			if(wc[comps].status != 0) {
				fprintf(stderr, "Bad wc status %d\n", wc[comps].status);
				exit(0);
			}
			comps += new_comps;
		}
		debug_cnt++;
	}
	return debug_cnt;
}

static inline struct ibv_mr* register_buffer(struct ibv_pd *pd, void* buf, uint32_t size)
{
	int ib_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
				   IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
	struct ibv_mr* mr = ibv_reg_mr(pd,(char *)buf, size, ib_flags);
	assert(mr != NULL);
	return mr;
}
/* Fill @wc with @num_comps comps from this @cq. Return -1 on error, else 0. */
static inline int
hrd_poll_cq_ret(struct ibv_cq *cq, int num_comps, struct ibv_wc *wc)
{
	int comps = 0;

	while(comps < num_comps) {
		int new_comps = ibv_poll_cq(cq, num_comps - comps, &wc[comps]);
		if(new_comps != 0) {
			/* Ideally, we should check from comps -> new_comps - 1 */
			if(wc[comps].status != 0) {
				fprintf(stderr, "Bad wc status %d\n", wc[comps].status);
				return -1;	/* Return an error so the caller can clean up */
			}

			comps += new_comps;
		}
	}

	return 0;	/* Success */
}


/* Utility functions */
static inline uint32_t hrd_fastrand(uint64_t *seed)
{
	*seed = *seed * 1103515245 + 12345;
	return (uint32_t) (*seed >> 32);
}

static inline long long hrd_get_cycles()
{
	unsigned low, high;
	unsigned long long val;
	asm volatile ("rdtsc" : "=a" (low), "=d" (high));
	val = high;
	val = (val << 32) | low;
	return val;
}

static inline int hrd_is_power_of_2(uint32_t n)
{
	return n && !(n & (n - 1));
}

void *hrd_malloc_socket(int shm_key, int size, int socket_id);
int hrd_free(int shm_key, void *shm_buf);
void red_printf(const char *format, ...);
void yellow_printf(const char *format, ...);
void green_printf(const char *format, ...);
void cyan_printf(const char *format, ...);
void hrd_get_formatted_time(char *timebuf);
void hrd_nano_sleep(int ns);
char *hrd_getenv(const char *name);

#endif /* HRD_H */

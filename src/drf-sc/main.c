#include "cache.h"
#include "util.h"



//Global Vars
uint32_t* latency_counters;
struct latency_counters latency_count;
struct thread_stats t_stats[WORKERS_PER_MACHINE];
struct remote_qp remote_qp[MACHINE_NUM][WORKERS_PER_MACHINE][QP_NUM];
atomic_bool config_vector[MACHINE_NUM];
atomic_char qps_are_set_up;
atomic_uint_fast16_t epoch_id;




int main(int argc, char *argv[])
{

  green_printf("READ REPLY: r_rep message %lu/%d, r_rep message ud req %llu/%d,"
                 "read info %llu/%d\n",
               sizeof(struct r_rep_message), R_REP_SEND_SIZE,
               sizeof(struct r_rep_message_ud_req), R_REP_RECV_SIZE,
               sizeof (struct read_info), READ_INFO_SIZE);
  cyan_printf("ACK: ack message %lu/%d, ack message ud req %llu/%d\n",
               sizeof(struct ack_message), ACK_SIZE,
               sizeof(struct ack_message_ud_req), ACK_RECV_SIZE);
  yellow_printf("READ: read %lu/%d, read message %lu/%d, read message ud req %lu/%d\n",
                sizeof(struct read), R_SIZE,
                sizeof(struct r_message), R_SEND_SIZE,
                sizeof(struct r_message_ud_req), R_RECV_SIZE);
  cyan_printf("Write: write %lu/%d, write message %lu/%d, write message ud req %llu/%d\n",
              sizeof(struct write), W_SIZE,
              sizeof(struct w_message), W_MES_SIZE,
              sizeof(struct w_message_ud_req), W_RECV_SIZE);

  green_printf("W INLINING %d, PENDING WRITES %d \n",
							 W_ENABLE_INLINING, PENDING_WRITES);
  green_printf("R INLINING %d, PENDING_READS %d \n",
               R_ENABLE_INLINING, PENDING_READS);
  green_printf("R_REP INLINING %d \n",
               R_REP_ENABLE_INLINING);
  cyan_printf("W CREDITS %d, W BUF SLOTS %d, W BUF SIZE %d\n",
              W_CREDITS, W_BUF_SLOTS, W_BUF_SIZE);

  yellow_printf("Using Quorom %d , Quorum Machines %d \n", USE_QUORUM, REMOTE_QUORUM);
  green_printf("SEND W DEPTH %d, MESSAGES_IN_BCAST_BATCH %d, W_BCAST_SS_BATCH %d \n",
               SEND_W_Q_DEPTH, MESSAGES_IN_BCAST_BATCH, W_BCAST_SS_BATCH);
//  if (ENABLE_MULTICAST) assert(MCAST_QP_NUM == MCAST_GROUPS_NUM);
//	assert(LEADER_MACHINE < MACHINE_NUM);
//	assert(LEADER_PENDING_WRITES >= SESSIONS_PER_THREAD);
	assert(sizeof(struct key) == TRUE_KEY_SIZE);
//  assert(LEADERS_PER_MACHINE == FOLLOWERS_PER_MACHINE); // hopefully temporary restriction
//  assert((W_CREDITS % LDR_CREDIT_DIVIDER) == 0); // division better be perfect
//  assert((COMMIT_CREDITS % FLR_CREDIT_DIVIDER) == 0); // division better be perfect
  assert(CACHE_BATCH_SIZE > MAX_INCOMING_W);
  assert(CACHE_BATCH_SIZE > MAX_INCOMING_R);
  assert(sizeof(struct ack_message_ud_req) == ACK_RECV_SIZE);
  assert(sizeof(struct r_rep_message_ud_req) == R_REP_RECV_SIZE);
  assert(sizeof(struct r_message_ud_req) == R_RECV_SIZE);
  assert(sizeof(struct w_message_ud_req) == W_RECV_SIZE);
  assert(SESSIONS_PER_THREAD < M_16);
  assert(MAX_W_COALESCE < 256);
  assert(MAX_R_COALESCE < 256);
  assert(MAX_R_REP_COALESCE < 256);

  assert(MAX_R_REP_COALESCE == MAX_R_COALESCE);
  assert(SESSIONS_PER_THREAD > 0);
  assert(MAX_OP_BATCH < CACHE_BATCH_SIZE);
  assert(ENABLE_LIN == 0); // Lin is not implemented
//  assert(FLR_MAX_RECV_COM_WRS >= FLR_CREDITS_IN_MESSAGE);
//  assert(CACHE_BATCH_SIZE > LEADER_PENDING_WRITES);


//
//  yellow_printf("WRITE: w_size of write recv slot %d w_size of w_message %lu , "
//           "value w_size %d, w_size of cache op %lu , sizeof udreq w message %lu \n",
//         LDR_W_RECV_SIZE, sizeof(struct w_message), VALUE_SIZE,
//         sizeof(struct cache_op), sizeof(struct w_message_ud_req));
  assert(sizeof(struct w_message_ud_req) == W_RECV_SIZE);
  assert(sizeof(struct w_message) == W_MES_SIZE);



	assert(sizeof(cache_meta) == 8);
  assert(MACHINE_NUM <= 255); // cache meta has 1 B for machine id


	int i, c;
	num_threads = -1;
	is_roce = -1; machine_id = -1;
	remote_IP = (char *)malloc(16 * sizeof(char));
  atomic_store_explicit(&epoch_id, 0, memory_order_relaxed);
  for (i = 0; i < MACHINE_NUM; i++) config_vector[i] = true;


	struct thread_params *param_arr;
	pthread_t *thread_arr;

	static struct option opts[] = {
			{ .name = "machine-id",			.has_arg = 1, .val = 'm' },
			{ .name = "is-roce",			.has_arg = 1, .val = 'r' },
			{ .name = "remote-ips",			.has_arg = 1, .val = 'i' },
			{ .name = "local-ip",			.has_arg = 1, .val = 'l' },
			{ 0 }
	};

	/* Parse and check arguments */
	while(1) {
		c = getopt_long(argc, argv, "M:t:b:N:n:c:u:m:p:r:i:l:x", opts, NULL);
		if(c == -1) {
			break;
		}
		switch (c) {
			case 'm':
				machine_id = atoi(optarg);
				break;
			case 'r':
				is_roce = atoi(optarg);
				break;
			case 'i':
				remote_IP = optarg;
				break;
			case 'l':
				local_IP = optarg;
				break;
			default:
				printf("Invalid argument %d\n", c);
				assert(false);
		}
	}

	/* Launch  threads */
	assert(machine_id < MACHINE_NUM && machine_id >=0);
  assert(!(is_roce == 1 && ENABLE_MULTICAST));
	num_threads =  WORKERS_PER_MACHINE;

	param_arr = malloc(num_threads * sizeof(struct thread_params));
	thread_arr = malloc((WORKERS_PER_MACHINE + 1) * sizeof(pthread_t));
	memset((struct thread_stats*) t_stats, 0, WORKERS_PER_MACHINE * sizeof(struct thread_stats));

	qps_are_set_up = 0;

	cache_init(0, WORKERS_PER_MACHINE); // the first ids are taken by the workers

#if MEASURE_LATENCY == 1
	latency_count.hot_writes  = (uint32_t*) malloc(sizeof(uint32_t) * (LATENCY_BUCKETS + 1)); // the last latency bucket is to capture possible outliers (> than LATENCY_MAX)
	latency_count.hot_reads   = (uint32_t*) malloc(sizeof(uint32_t) * (LATENCY_BUCKETS + 1)); // the last latency bucket is to capture possible outliers (> than LATENCY_MAX)
	latency_count.local_reqs  = (uint32_t*) malloc(sizeof(uint32_t) * (LATENCY_BUCKETS + 1)); // the last latency bucket is to capture possible outliers (> than LATENCY_MAX)
	latency_count.remote_reqs = (uint32_t*) malloc(sizeof(uint32_t) * (LATENCY_BUCKETS + 1)); // the last latency bucket is to capture possible outliers (> than LATENCY_MAX)
  latency_count.total_measurements = 0;
#endif
	pthread_attr_t attr;
	cpu_set_t pinned_hw_threads;
	pthread_attr_init(&attr);
	int occupied_cores[TOTAL_CORES] = { 0 };
  char node_purpose[15];
  sprintf(node_purpose, "Worker");
	for(i = 0; i < num_threads; i++) {
		param_arr[i].id = i;
		int core = pin_thread(i);
		yellow_printf("Creating %s thread %d at core %d \n", node_purpose, param_arr[i].id, core);
		CPU_ZERO(&pinned_hw_threads);
		CPU_SET(core, &pinned_hw_threads);
		pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &pinned_hw_threads);
		pthread_create(&thread_arr[i], &attr, worker, &param_arr[i]);
		occupied_cores[core] = 1;
	}


	for(i = 0; i < num_threads; i++)
		pthread_join(thread_arr[i], NULL);

	return 0;
}

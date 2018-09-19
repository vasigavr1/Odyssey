#include "cache.h"
#include "util.h"



//Global Vars
struct latency_counters latency_count;
struct thread_stats t_stats[WORKERS_PER_MACHINE];
struct remote_qp remote_qp[MACHINE_NUM][WORKERS_PER_MACHINE][QP_NUM];

struct bit_vector send_bit_vector;
//struct bit_vector conf_bit_vector;
struct multiple_owner_bit conf_bit_vec[MACHINE_NUM];

atomic_char qps_are_set_up;
atomic_uint_fast16_t epoch_id;
atomic_bool print_for_debug;
const uint16_t machine_bit_id[SEND_CONF_VEC_SIZE * 8] = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512,
																		 1024, 2048, 4096, 8192, 16384, 32768};
struct rmw_info rmw;
atomic_uint_fast32_t next_rmw_entry_available;



int main(int argc, char *argv[])
{

  green_printf("READ REPLY: r_rep message %lu/%d, r_rep message ud req %llu,"
                 "read info %llu\n",
               sizeof(struct r_rep_message), R_REP_SEND_SIZE,
               sizeof(struct r_rep_message_ud_req), R_REP_RECV_SIZE,
               sizeof (struct read_info));
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

//  red_printf("MAX allowed pending RMWs per machine %d and total %d \n", RMW_ENTRIES_PER_MACHINE, RMW_ENTRIES_NUM);
//  if (ENABLE_MULTICAST) assert(MCAST_QP_NUM == MCAST_GROUPS_NUM);
//	assert(LEADER_MACHINE < MACHINE_NUM);
//	assert(LEADER_PENDING_WRITES >= SESSIONS_PER_THREAD);
	static_assert(sizeof(struct key) == TRUE_KEY_SIZE, " ");
//  assert(LEADERS_PER_MACHINE == FOLLOWERS_PER_MACHINE); // hopefully temporary restriction
//  assert((W_CREDITS % LDR_CREDIT_DIVIDER) == 0); // division better be perfect
//  assert((COMMIT_CREDITS % FLR_CREDIT_DIVIDER) == 0); // division better be perfect
  static_assert(CACHE_BATCH_SIZE > MAX_INCOMING_W, "");
  static_assert(CACHE_BATCH_SIZE > MAX_INCOMING_R, "");
  static_assert(sizeof(struct ack_message_ud_req) == ACK_RECV_SIZE, "");
  static_assert(sizeof(struct r_rep_message_ud_req) == R_REP_RECV_SIZE, "");
  static_assert(sizeof(struct r_message_ud_req) == R_RECV_SIZE, "");
  static_assert(sizeof(struct w_message_ud_req) == W_RECV_SIZE, "");
  static_assert(SESSIONS_PER_THREAD < M_16, "");
  static_assert(MAX_W_COALESCE < 256, "");
  static_assert(MAX_R_COALESCE < 256, "");
  static_assert(MAX_R_REP_COALESCE < 256, "");

  static_assert(MAX_R_REP_COALESCE == MAX_R_COALESCE, "");
  static_assert(SESSIONS_PER_THREAD > 0, "");
  static_assert(MAX_OP_BATCH < CACHE_BATCH_SIZE, "");
  static_assert(ENABLE_LIN == 0, "Lin is not implemented");
  static_assert(MACHINE_NUM < 16, "the bit_vec vector is 16 bits-- can be extended");
	static_assert(VALUE_SIZE % 8 == 0 || !USE_BIG_OBJECTS, "Big objects are enabled but the value size is not a multiple of 8");
  static_assert(VALUE_SIZE >= 2, "first round of release can overload the first 2 bytes of value");
  static_assert(sizeof(struct cache_key) ==  KEY_SIZE, "");

  static_assert(VALUE_SIZE >= (RMW_VALUE_SIZE + BYTES_OVERRIDEN_IN_KVS_VALUE), "RMW requires the value to be at least this many bytes");


  static_assert(sizeof(struct w_message_ud_req) == W_RECV_SIZE, "");
  static_assert(sizeof(struct w_message) == W_MES_SIZE, "");

  // RMWs
  static_assert(LOCAL_PREP_NUM >= SESSIONS_PER_THREAD, "");



  static_assert(sizeof(cache_meta) == 8, "");
  static_assert(MACHINE_NUM <= 255, ""); // cache meta has 1 B for machine id


	int i, c;
	num_threads = -1;
	is_roce = -1; machine_id = -1;
	remote_IP = (char *)malloc(16 * sizeof(char));
  atomic_store_explicit(&epoch_id, 0, memory_order_relaxed);
  // This (sadly) seems to be the only way to initialize the locks
  // in struct_bit_vector, i.e. the atomic_flags
  memset(&send_bit_vector, 0, sizeof(struct bit_vector));
  memset(conf_bit_vec, 0, MACHINE_NUM * sizeof(struct multiple_owner_bit));
  for (i = 0; i < MACHINE_NUM; i++) {
    conf_bit_vec[i].bit = UP_STABLE;
    send_bit_vector.bit_vec[i].bit = UP_STABLE;
	}
  //send_bit_vector.state_lock = ATOMIC_FLAG_INIT; // this does not compile
  send_bit_vector.state = UP_STABLE;

  print_for_debug = false;
	next_rmw_entry_available = 0;

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
  memset(&latency_count, 0, sizeof(struct latency_counters));
	latency_count.hot_writes  = (uint32_t*) malloc(sizeof(uint32_t) * (LATENCY_BUCKETS + 1)); // the last latency bucket is to capture possible outliers (> than LATENCY_MAX)
  memset(latency_count.hot_writes, 0, sizeof(uint32_t) * (LATENCY_BUCKETS + 1));
	latency_count.hot_reads   = (uint32_t*) malloc(sizeof(uint32_t) * (LATENCY_BUCKETS + 1)); // the last latency bucket is to capture possible outliers (> than LATENCY_MAX)
  memset(latency_count.hot_reads, 0, sizeof(uint32_t) * (LATENCY_BUCKETS + 1));
	latency_count.releases  = (uint32_t*) malloc(sizeof(uint32_t) * (LATENCY_BUCKETS + 1)); // the last latency bucket is to capture possible outliers (> than LATENCY_MAX)
  memset(latency_count.releases, 0, sizeof(uint32_t) * (LATENCY_BUCKETS + 1));
	latency_count.acquires = (uint32_t*) malloc(sizeof(uint32_t) * (LATENCY_BUCKETS + 1)); // the last latency bucket is to capture possible outliers (> than LATENCY_MAX)
  memset(latency_count.acquires, 0, sizeof(uint32_t) * (LATENCY_BUCKETS + 1));
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

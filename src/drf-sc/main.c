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
atomic_uint_fast64_t committed_glob_sess_rmw_id[GLOBAL_SESSION_NUM];
FILE* rmw_verify_fp[WORKERS_PER_MACHINE];



int main(int argc, char *argv[])
{
  print_parameters_in_the_start();
  static_assert_compile_parameters();
	int i, c;
	num_threads = -1;
	is_roce = -1; machine_id = -1;
	remote_IP = (char *) malloc(16 * sizeof(char));
  dev_name = (char *) malloc(16 * sizeof(char));
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
  memset(committed_glob_sess_rmw_id, 0, GLOBAL_SESSION_NUM * sizeof(uint64_t));

	struct thread_params *param_arr;
	pthread_t *thread_arr;
  handle_program_inputs(argc, argv);

	/* Launch  threads */
	assert(machine_id < MACHINE_NUM && machine_id >=0);
  assert(!(is_roce == 1 && ENABLE_MULTICAST));
	num_threads =  WORKERS_PER_MACHINE;

	param_arr = malloc(num_threads * sizeof(struct thread_params));
	thread_arr = malloc((WORKERS_PER_MACHINE + 1) * sizeof(pthread_t));
	memset((struct thread_stats*) t_stats, 0, WORKERS_PER_MACHINE * sizeof(struct thread_stats));
	qps_are_set_up = 0;
	cache_init(0, WORKERS_PER_MACHINE);

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
    // PAXOS VERIFIER
    if (VERIFY_PAXOS || PRINT_LOGS) {
      char fp_name[40];
      sprintf(fp_name, "../PaxosVerifier/thread%d.out", GET_GLOBAL_T_ID(machine_id, i));
      rmw_verify_fp[i] = fopen(fp_name, "w+");
    }
    //--
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

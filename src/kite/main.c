#include "kvs.h"
#include "util.h"



//Global Vars
int is_roce, machine_id, num_threads;
struct latency_counters latency_count;
struct thread_stats t_stats[WORKERS_PER_MACHINE];
struct client_stats c_stats[CLIENTS_PER_MACHINE];
struct remote_qp remote_qp[MACHINE_NUM][WORKERS_PER_MACHINE][QP_NUM];

struct bit_vector send_bit_vector;
//struct bit_vector conf_bit_vector;
struct multiple_owner_bit conf_bit_vec[MACHINE_NUM];

atomic_bool qps_are_set_up;
atomic_uint_fast64_t epoch_id;
atomic_bool print_for_debug;
const uint16_t machine_bit_id[SEND_CONF_VEC_SIZE * 8] = {1, 2, 4, 8, 16, 32, 64, 128, 256, 512,
																		 1024, 2048, 4096, 8192, 16384, 32768};
//struct rmw_info rmw;
atomic_uint_fast64_t committed_glob_sess_rmw_id[GLOBAL_SESSION_NUM];
FILE* rmw_verify_fp[WORKERS_PER_MACHINE];
FILE* client_log[CLIENTS_PER_MACHINE];

struct wrk_clt_if interface[WORKERS_PER_MACHINE];
uint64_t last_pulled_req[SESSIONS_PER_MACHINE];
uint64_t last_pushed_req[SESSIONS_PER_MACHINE];
uint64_t time_approx;

//struct epoch_info epoch;


int main(int argc, char *argv[])
{
	printf("MICA OP size %ld/%d added padding %d  \n",
         sizeof(mica_op_t), MICA_OP_SIZE, MICA_OP_PADDING_SIZE);


  printf("Rmw-local_entry %ld \n", sizeof(struct rmw_local_entry));

	uint16_t i = 0;
  print_parameters_in_the_start();
  static_assert_compile_parameters();
	num_threads = -1;
	is_roce = -1; machine_id = -1;
	workers_with_filled_qp_attr = 0;
  init_globals();
	/* Handle Inputs */
  handle_program_inputs(argc, argv);
	assert(machine_id < MACHINE_NUM && machine_id >=0);
	assert(!(is_roce == 1 && ENABLE_MULTICAST));

  /* Launch  threads */
	num_threads = TOTAL_THREADS;
	struct thread_params *param_arr = malloc(TOTAL_THREADS * sizeof(struct thread_params));
	pthread_t *thread_arr = malloc(TOTAL_THREADS * sizeof(pthread_t));
	pthread_attr_t attr;
	cpu_set_t pinned_hw_threads;
	pthread_attr_init(&attr);
	bool occupied_cores[TOTAL_CORES] = { 0 };
  char node_purpose[15];
  sprintf(node_purpose, "Worker");
	for(i = 0; i < TOTAL_THREADS; i++) {
		if (i < WORKERS_PER_MACHINE) {
			// PAXOS VERIFIER
			if (VERIFY_PAXOS || PRINT_LOGS || COMMIT_LOGS) {
				char fp_name[40];
				sprintf(fp_name, "../PaxosVerifier/thread%d.out", GET_GLOBAL_T_ID(machine_id, i));
				rmw_verify_fp[i] = fopen(fp_name, "w+");
			}
      spawn_threads(param_arr, i, "Worker", &pinned_hw_threads,
                    &attr, thread_arr, worker, occupied_cores);
		}
    else {
			assert(ENABLE_CLIENTS);
      if (CLIENT_LOGS) {
        uint16_t cl_id = (uint16_t) (machine_id * CLIENTS_PER_MACHINE +
                                    (i - WORKERS_PER_MACHINE));
        char fp_name[40];

        sprintf(fp_name, "cLogs/client%u.out", cl_id);
        cl_id =  (uint16_t)(i - WORKERS_PER_MACHINE);
        client_log[cl_id] = fopen(fp_name, "w+");
      }
			spawn_threads(param_arr, i, "Client", &pinned_hw_threads,
                    &attr, thread_arr, client, occupied_cores);
		}
	}

	for(i = 0; i < num_threads; i++)
		pthread_join(thread_arr[i], NULL);
	return 0;
}

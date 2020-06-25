#include "kvs.h"
#include "main.h"
#include "util.h"
#include "../../include/general_util/init_func.h"


//Global Vars
int is_roce, machine_id, num_threads;
struct latency_counters latency_count;
t_stats_t t_stats[WORKERS_PER_MACHINE];
struct client_stats c_stats[CLIENTS_PER_MACHINE];
remote_qp_t ***rem_qp; //[MACHINE_NUM][WORKERS_PER_MACHINE][QP_NUM];

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
all_qp_attr_t *all_qp_attr;
atomic_uint_fast32_t workers_with_filled_qp_attr;

//struct epoch_info epoch;


int main(int argc, char *argv[])
{
	uint16_t i = 0;
  print_parameters_in_the_start();
  static_assert_compile_parameters();
  kite_static_assert_compile_parameters();
  init_globals(QP_NUM);
  kite_init_globals();
	/* Handle Inputs */
  handle_program_inputs(argc, argv);

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
      fopen_client_logs(i);
			spawn_threads(param_arr, i, "Client", &pinned_hw_threads,
                    &attr, thread_arr, client, occupied_cores);
		}
	}

	for(i = 0; i < num_threads; i++)
		pthread_join(thread_arr[i], NULL);
	return 0;
}

//
// Created by vasilis on 23/06/2020.
//

#ifndef KITE_GENERIC_FUNC_H
#define KITE_GENERIC_FUNC_H

#include "top.h"
#include "stats.h"
#include <getopt.h>
#include "kvs.h"

static void static_assert_compile_parameters()
{
  assert(MICA_OP_SIZE == sizeof(mica_op_t));
  static_assert(IS_ALIGNED(MICA_VALUE_SIZE, 32), "VALUE_SIZE must be aligned with 32 bytes ");
  static_assert(IS_ALIGNED(2 * MICA_VALUE_SIZE, 64), "2 * VALUE_SIZE must be aligned with 64 bytes");
  static_assert(MICA_VALUE_SIZE >= VALUE_SIZE, "");
  static_assert(SESSIONS_PER_THREAD < K_64, "");
  static_assert(SESSIONS_PER_THREAD > 0, "");
  static_assert(VALUE_SIZE % 8 == 0 || !USE_BIG_OBJECTS,
    "Big objects are enabled but the value size is not a multiple of 8");

  static_assert(!(ENABLE_CLIENTS && !CLIENTS_PER_MACHINE), "");
  static_assert(sizeof(client_op_t) == CLIENT_OP_SIZE, "");
  static_assert(sizeof(client_op_t) % 64 == 0, "");
  static_assert(sizeof(struct wrk_clt_if) % 64 == 0, "");
  static_assert(sizeof(struct wrk_clt_if) == INTERFACE_SIZE, "");
  for (uint16_t i = 0; i < WORKERS_PER_MACHINE; i++) {
    bool is_interface_aligned = (uint64_t) &interface % 64 == 0;
    bool same_cl =  ((uint64_t)&interface[i].clt_pull_ptr[SESSIONS_PER_THREAD - 1] / 64) ==
                    ((uint64_t)&interface[i].wrkr_pull_ptr[0] / 64);
    //long ptr_dif = &interface[i].clt_pull_ptr[SESSIONS_PER_THREAD - 1] -
    //                   &interface[i].wrkr_pull_ptr[0];
    //printf("%d %lu %lu\n", same_cl, ((uint64_t)&interface[i].clt_pull_ptr[SESSIONS_PER_THREAD - 1]/ 64), ((uint64_t)&interface[i].wrkr_pull_ptr[0]/64));
    assert(!same_cl);
    assert(is_interface_aligned);
  }
}

static void handle_program_inputs(int argc, char *argv[])
{
  num_threads = -1;
  is_roce = -1; machine_id = -1;
  int c;
  char *tmp_ip;
  static struct option opts[] = {
    { .name = "machine-id",			.has_arg = 1, .val = 'm' },
    { .name = "is-roce",			.has_arg = 1, .val = 'r' },
    { .name = "all-ips",       .has_arg = 1, .val ='a'},
    { .name = "device_name",			.has_arg = 1, .val = 'd'},
    { 0 }
  };

  /* Parse and check arguments */
  while(true) {
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
      case 'a':
        tmp_ip = optarg;
        break;
      case 'd':
        dev_name = optarg;
        break;
      default:
        printf("Invalid argument %d\n", c);
        assert(false);
    }
  }
  if (machine_id == -1) assert(false);
  assert(machine_id < MACHINE_NUM);
  assert(!(is_roce == 1 && ENABLE_MULTICAST));

  char* chars_array = strtok(tmp_ip, ",");
  remote_ips = (char **) (malloc(REM_MACH_NUM * sizeof(char *)));
  int rm_id = 0;
  for ( int m_id = 0; m_id < MACHINE_NUM; m_id++) {
    assert(chars_array != NULL); // not enough IPs were passed
    printf("ip %s \n", chars_array);
    if (m_id ==  machine_id) {
      local_ip = (char *) (malloc(16));
      memcpy(local_ip, chars_array, strlen(chars_array) + 1);
      printf("local_ip = %s  \n", local_ip);
    }
    else {
      remote_ips[rm_id] = (char *) (malloc(16));
      memcpy(remote_ips[rm_id], chars_array, strlen(chars_array) + 1);
      printf("remote_ip[%d] = %s  \n", rm_id, remote_ips[rm_id]);
      rm_id++;
    }

    //else remote_ip_vector.push_back(chars_array);
    chars_array = strtok(NULL, ",");
  }
}

static void init_globals(int qp_num)
{
  time_approx = 0;
  workers_with_filled_qp_attr = 0;
  dev_name = (char *) malloc(16 * sizeof(char));
  all_qp_attr = calloc(1, sizeof(all_qp_attr_t));
  all_qp_attr->size = MACHINE_NUM * WORKERS_PER_MACHINE * qp_num * sizeof(qp_attr_t);
  all_qp_attr->buf = malloc(all_qp_attr->size);

  all_qp_attr->wrkr_qp = (qp_attr_t***) malloc(MACHINE_NUM * sizeof(qp_attr_t **));
  qp_attr_t *qp_buf = (qp_attr_t *) all_qp_attr->buf;
  rem_qp = (remote_qp_t***) malloc(MACHINE_NUM * sizeof(remote_qp_t **));
  for (int m_i = 0; m_i < MACHINE_NUM; m_i++) {
    rem_qp[m_i] = (remote_qp_t**) malloc(WORKERS_PER_MACHINE * sizeof(remote_qp_t *));
    all_qp_attr->wrkr_qp[m_i] = (qp_attr_t**) malloc(WORKERS_PER_MACHINE * sizeof(qp_attr_t *));
    for (int w_i = 0; w_i < WORKERS_PER_MACHINE; w_i++) {
      rem_qp[m_i][w_i] = (remote_qp_t*) malloc(qp_num * sizeof(remote_qp_t));
      all_qp_attr->wrkr_qp[m_i][w_i] = &qp_buf[(m_i * WORKERS_PER_MACHINE * qp_num) + (w_i * qp_num)];
    }
  }
  print_for_debug = false;
  memset((struct thread_stats*) t_stats, 0, WORKERS_PER_MACHINE * sizeof(struct thread_stats));
  memset((struct client_stats*) c_stats, 0, CLIENTS_PER_MACHINE * sizeof(struct client_stats));
  qps_are_set_up = false;
  custom_mica_init(0);

  uint16_t per_machine_s_i = 0;
  for (uint16_t w_i = 0; w_i < WORKERS_PER_MACHINE; w_i++) {
    for (uint16_t s_i = 0; s_i < SESSIONS_PER_THREAD; s_i++) {
      interface[w_i].clt_pull_ptr[s_i] = 0;
      interface[w_i].clt_push_ptr[s_i] = 0;
      interface[w_i].wrkr_pull_ptr[s_i] = 0;
      last_pulled_req[per_machine_s_i] = 0;
      last_pushed_req[per_machine_s_i] = 0;
      per_machine_s_i++;
      for (uint16_t r_i = 0; r_i < PER_SESSION_REQ_NUM; r_i++)
        interface[w_i].req_array[s_i][r_i].state = INVALID_REQ;
    }
  }

  /* Latency Measurements initializations */
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
}

// pin threads starting from core 0
static int pin_thread(int t_id) {
  int core;
  core = PHYSICAL_CORE_DISTANCE * t_id;
  if(core >= LOGICAL_CORES_PER_SOCKET) { //if you run out of cores in numa node 0
    if (WORKER_HYPERTHREADING) { //use hyperthreading rather than go to the other socket
      core = LOGICAL_CORES_PER_SOCKET + PHYSICAL_CORE_DISTANCE * (t_id - PHYSICAL_CORES_PER_SOCKET);
      if (core >= TOTAL_CORES_) { // now go to the other socket
        core = PHYSICAL_CORE_DISTANCE * (t_id - LOGICAL_CORES_PER_SOCKET) + 1 ;
        if (core >= LOGICAL_CORES_PER_SOCKET) { // again do hyperthreading on the second socket
          core = LOGICAL_CORES_PER_SOCKET + 1 +
                 PHYSICAL_CORE_DISTANCE * (t_id - (LOGICAL_CORES_PER_SOCKET + PHYSICAL_CORES_PER_SOCKET));
        }
      }
    }
    else { //spawn clients to numa node 1
      core = PHYSICAL_CORE_DISTANCE * (t_id - PHYSICAL_CORES_PER_SOCKET) + 1;
      if (core >= LOGICAL_CORES_PER_SOCKET) { // start hyperthreading
        core = LOGICAL_CORES_PER_SOCKET + (PHYSICAL_CORE_DISTANCE * (t_id - LOGICAL_CORES_PER_SOCKET));
        if (core >= TOTAL_CORES_) {
          core = LOGICAL_CORES_PER_SOCKET + 1 +
                 PHYSICAL_CORE_DISTANCE * (t_id - (LOGICAL_CORES_PER_SOCKET + PHYSICAL_CORES_PER_SOCKET));
        }
      }
    }

  }
  assert(core >= 0 && core < TOTAL_CORES);
  return core;
}

// pin a thread avoid collisions with pin_thread()
static int pin_threads_avoiding_collisions(int c_id) {
  int c_core;
  if (!WORKER_HYPERTHREADING || WORKERS_PER_MACHINE < PHYSICAL_CORES_PER_SOCKET) {
    if (c_id < WORKERS_PER_MACHINE) c_core = PHYSICAL_CORE_DISTANCE * c_id + 2;
    else c_core = (WORKERS_PER_MACHINE * 2) + (c_id * 2);

    //if (DISABLE_CACHE == 1) c_core = 4 * i + 2; // when bypassing the kvs
    //if (DISABLE_HYPERTHREADING == 1) c_core = (FOLLOWERS_PER_MACHINE * 4) + (c_id * 4);
    if (c_core > TOTAL_CORES_) { //spawn clients to numa node 1 if you run out of cores in 0
      c_core -= TOTAL_CORES_;
    }
  }
  else { //we are keeping workers on the same socket
    c_core = (WORKERS_PER_MACHINE - PHYSICAL_CORES_PER_SOCKET) * 4 + 2 + (4 * c_id);
    if (c_core > TOTAL_CORES_) c_core = c_core - (TOTAL_CORES_ + 2);
    if (c_core > TOTAL_CORES_) c_core = c_core - (TOTAL_CORES_ - 1);
  }
  assert(c_core >= 0 && c_core < TOTAL_CORES);
  return c_core;
}

static void spawn_threads(struct thread_params *param_arr, uint16_t t_id, char* node_purpose,
                   cpu_set_t *pinned_hw_threads, pthread_attr_t *attr, pthread_t *thread_arr,
                   void *(*__start_routine) (void *), bool *occupied_cores)
{
  param_arr[t_id].id = t_id < WORKERS_PER_MACHINE ? t_id : t_id - WORKERS_PER_MACHINE;
  int core = pin_thread(t_id); // + 8 + t_id * 20;
  my_printf(yellow, "Creating %s thread %d at core %d \n", node_purpose, param_arr[t_id].id, core);
  CPU_ZERO(pinned_hw_threads);
  CPU_SET(core, pinned_hw_threads);
  pthread_attr_setaffinity_np(attr, sizeof(cpu_set_t), pinned_hw_threads);
  pthread_create(&thread_arr[t_id], attr, __start_routine, &param_arr[t_id]);
  occupied_cores[core] = 1;
}

static void fopen_client_logs(uint16_t i)
{
  if (CLIENT_LOGS) {
    uint16_t cl_id = (uint16_t) (machine_id * CLIENTS_PER_MACHINE +
                                 (i - WORKERS_PER_MACHINE));
    char fp_name[40];

    sprintf(fp_name, "cLogs/client%u.out", cl_id);
    cl_id =  (uint16_t)(i - WORKERS_PER_MACHINE);
    client_log[cl_id] = fopen(fp_name, "w+");
  }
}



#endif //KITE_GENERIC_FUNC_H

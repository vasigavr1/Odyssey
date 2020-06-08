#include "util.h"
#include "../../include/kite_inline_util/generic_util.h"
#include <getopt.h>
//#include "util.h"

//struct hrd_qp_attr all_qp_attr[WORKERS_PER_MACHINE][QP_NUM];
all_qp_attr_t *all_qp_attr;
atomic_uint_fast32_t workers_with_filled_qp_attr;

void static_assert_compile_parameters()
{
  static_assert(RMW_ACK_BASE_TS_STALE > RMW_ACK, "assumption used to check if replies are acks");
  assert(MICA_OP_SIZE == sizeof(mica_op_t));
  static_assert(IS_ALIGNED(MICA_VALUE_SIZE, 32), "VALUE_SIZE must be aligned with 32 bytes ");
  static_assert(IS_ALIGNED(2 * MICA_VALUE_SIZE, 64), "2 * VALUE_SIZE must be aligned with 64 bytes");
  static_assert(MICA_VALUE_SIZE >= VALUE_SIZE, "");
  static_assert(PAXOS_TS > ALL_ABOARD_TS, "Paxos TS must be bigger than ALL Aboard TS");
  static_assert(!(COMMIT_LOGS && (PRINT_LOGS || VERIFY_PAXOS)), " ");
  static_assert(sizeof(struct key) == TRUE_KEY_SIZE, " ");
  static_assert(sizeof(struct network_ts_tuple) == TS_TUPLE_SIZE, "");
//  static_assert(sizeof(struct cache_key) ==  KEY_SIZE, "");
//  static_assert(sizeof(cache_meta) == 8, "");

  static_assert(SESSIONS_PER_THREAD < K_64, "");
  static_assert(SESSIONS_PER_THREAD > 0, "");
  static_assert(MACHINE_NUM < 16, "the bit_vec vector is 16 bits-- can be extended");
  static_assert(VALUE_SIZE % 8 == 0 || !USE_BIG_OBJECTS, "Big objects are enabled but the value size is not a multiple of 8");
  static_assert(VALUE_SIZE >= 2, "first round of release can overload the first 2 bytes of value");
  //static_assert(VALUE_SIZE > RMW_BYTE_OFFSET, "");
  static_assert(VALUE_SIZE == (RMW_VALUE_SIZE), "RMW requires the value to be at least this many bytes");
  static_assert(MACHINE_NUM <= 255, ""); // kvs meta has 1 B for machine id

  // WRITES
  static_assert(W_SEND_SIZE >= W_MES_SIZE &&
                W_SEND_SIZE >= ACC_MES_SIZE &&
                W_SEND_SIZE >= COM_MES_SIZE &&
                W_SEND_SIZE <= MAX_WRITE_SIZE, "");
  static_assert(W_SEND_SIZE <= MTU, "");
  // READS
  static_assert(R_SEND_SIZE >= R_MES_SIZE &&
                R_SEND_SIZE >= PROP_MES_SIZE &&
                R_SEND_SIZE <= MAX_READ_SIZE, "");
  static_assert(R_SEND_SIZE <= MTU, "");
  // R_REPS
//  static_assert(R_REP_SEND_SIZE >= PROP_REP_MES_SIZE &&
//                R_REP_SEND_SIZE >= ACC_REP_MES_SIZE &&
//                R_REP_SEND_SIZE >= READ_REP_MES_SIZE &&
//                R_REP_SEND_SIZE >= RMW_ACQ_REP_MES_SIZE, "");
  static_assert(R_REP_SEND_SIZE <= MTU, "");

  // COALESCING
  static_assert(MAX_WRITE_COALESCE < 256, "");
  static_assert(MAX_READ_COALESCE < 256, "");
  static_assert(MAX_REPS_IN_REP < 256, "");
  static_assert(PROP_COALESCE > 0, "");
  static_assert(R_COALESCE > 0, "");
  static_assert(W_COALESCE > 0, "");
  static_assert(ACC_COALESCE > 0, "");
  static_assert(COM_COALESCE > 0, "");

  // NETWORK STRUCTURES
  static_assert(sizeof(struct read) == R_SIZE, "");
  static_assert(sizeof(struct r_message) == R_MES_SIZE, "");
  static_assert(sizeof(struct write) == W_SIZE, "");
  static_assert(sizeof(struct w_message) == W_MES_SIZE, "");
  static_assert(sizeof(struct propose) == PROP_SIZE, "");
  static_assert(PROP_REP_ACCEPTED_SIZE == PROP_REP_LOG_TOO_LOW_SIZE + 1, "");
  static_assert(sizeof(struct rmw_rep_last_committed) == PROP_REP_ACCEPTED_SIZE, "");
  static_assert(sizeof(struct rmw_rep_message) == PROP_REP_MES_SIZE, "");
  static_assert(sizeof(struct accept) == ACCEPT_SIZE, "");
  static_assert(sizeof(struct rmw_acq_rep) == RMW_ACQ_REP_SIZE, "");
  static_assert(sizeof(struct commit) == COMMIT_SIZE, "");
  // UD- REQS
  static_assert(sizeof(struct r_rep_message_ud_req) == R_REP_RECV_SIZE, "");
  static_assert(sizeof(struct ack_message_ud_req) == ACK_RECV_SIZE, "");
  static_assert(sizeof(struct w_message_ud_req) == W_RECV_SIZE, "");
  static_assert(sizeof(struct r_message_ud_req) == R_RECV_SIZE, "");

  // we want to have more write slots than credits such that we always know that if a machine fails
  // the pressure will appear in the credits and not the write slots
  static_assert(PENDING_WRITES > (W_CREDITS * MAX_MES_IN_WRITE), " ");
  // RMWs
  static_assert(!ENABLE_RMWS || LOCAL_PROP_NUM >= SESSIONS_PER_THREAD, "");
  static_assert(GLOBAL_SESSION_NUM < K_64, "global session ids are stored in uint16_t");

  // ACCEPT REPLIES MAP TO PROPOSE REPLIES
  static_assert(ACC_REP_SIZE == PROP_REP_LOG_TOO_LOW_SIZE, "");
  static_assert(ACC_REP_SMALL_SIZE == PROP_REP_SMALL_SIZE, "");
  static_assert(ACC_REP_ONLY_TS_SIZE == PROP_REP_ONLY_TS_SIZE, "");
  //static_assert(ACC_REP_ACCEPTED_SIZE == PROP_REP_ACCEPTED_SIZE, "");



  static_assert(!(VERIFY_PAXOS && PRINT_LOGS), "only one of those can be set");
#if VERIFY_PAXOS == 1
  static_assert(EXIT_ON_PRINT == 1, "");
#endif
  //static_assert(sizeof(trace_op_t) == 18 + VALUE_SIZE  + 8 + 4, "");
  static_assert(TRACE_ONLY_CAS + TRACE_ONLY_FA + TRACE_MIXED_RMWS == 1, "");


//  printf("Client op  %u  %u \n", sizeof(client_op_t), PADDING_BYTES_CLIENT_OP);
//  printf("Interface \n \n %u  \n \n", sizeof(struct wrk_clt_if));
  static_assert(!(ENABLE_CLIENTS && !CLIENTS_PER_MACHINE), "");
  static_assert(!(ENABLE_CLIENTS && !ACCEPT_IS_RELEASE && CLIENT_MODE > CLIENT_UI),
                "If we are using the lock-free data structures rmws must act as releases");

  static_assert(!(ENABLE_CLIENTS && CLIENT_MODE > CLIENT_UI && ENABLE_ALL_ABOARD), "All-aboard does not work with the RC semantics");
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

void print_parameters_in_the_start()
{
  my_printf(green, "READ REPLY: r_rep message %lu/%d, r_rep message ud req %llu/%d,"
                 "read info %llu\n",
               sizeof(struct r_rep_message), R_REP_SEND_SIZE,
               sizeof(struct r_rep_message_ud_req), R_REP_RECV_SIZE,
               sizeof (r_info_t));
  my_printf(green, "W_COALESCE %d, R_COALESCE %d, ACC_COALESCE %u, "
                 "PROPOSE COALESCE %d, COM_COALESCE %d, MAX_WRITE_COALESCE %d,"
                 "MAX_READ_COALESCE %d \n",
               W_COALESCE, R_COALESCE, ACC_COALESCE, PROP_COALESCE, COM_COALESCE,
               MAX_WRITE_COALESCE, MAX_READ_COALESCE);


  my_printf(cyan, "ACK: ack message %lu/%d, ack message ud req %llu/%d\n",
              sizeof(struct ack_message), ACK_SIZE,
              sizeof(struct ack_message_ud_req), ACK_RECV_SIZE);
  my_printf(yellow, "READ: read %lu/%d, read message %lu/%d, read message ud req %lu/%d\n",
                sizeof(struct read), R_SIZE,
                sizeof(struct r_message), R_SEND_SIZE,
                sizeof(struct r_message_ud_req), R_RECV_SIZE);
  my_printf(cyan, "Write: write %lu/%d, write message %lu/%d, write message ud req %llu/%d\n",
              sizeof(struct write), W_SIZE,
              sizeof(struct w_message), W_SEND_SIZE,
              sizeof(struct w_message_ud_req), W_RECV_SIZE);

  my_printf(green, "W INLINING %d, PENDING WRITES %d \n",
               W_ENABLE_INLINING, PENDING_WRITES);
  my_printf(green, "R INLINING %d, PENDING_READS %d \n",
               R_ENABLE_INLINING, PENDING_READS);
  my_printf(green, "R_REP INLINING %d \n",
               R_REP_ENABLE_INLINING);
  my_printf(cyan, "W CREDITS %d, W BUF SLOTS %d, W BUF SIZE %d\n",
              W_CREDITS, W_BUF_SLOTS, W_BUF_SIZE);

  my_printf(yellow, "Using Quorom %d , Remote Quorum Machines %d \n", USE_QUORUM, REMOTE_QUORUM);
  my_printf(green, "SEND W DEPTH %d, MESSAGES_IN_BCAST_BATCH %d, W_BCAST_SS_BATCH %d \n",
               SEND_W_Q_DEPTH, MESSAGES_IN_BCAST_BATCH, W_BCAST_SS_BATCH);
}

void init_globals()
{
  uint32_t i = 0;
  time_approx = 0;
//  remote_IP = (char *) malloc(16 * sizeof(char));
  dev_name = (char *) malloc(16 * sizeof(char));
  all_qp_attr = calloc(1, sizeof(all_qp_attr_t));
  epoch_id = MEASURE_SLOW_PATH ? 1 : 0;
  // This (sadly) seems to be the only way to initialize the locks
  // in struct_bit_vector, i.e. the atomic_flags
  memset(&send_bit_vector, 0, sizeof(struct bit_vector));
  memset(conf_bit_vec, 0, MACHINE_NUM * sizeof(struct multiple_owner_bit));
  // EPOCH
  //epoch.epoch_id = 0;
  //epoch.all_machines = true;
  for (i = 0; i < MACHINE_NUM; i++) {
    //epoch.per_machine_bit[i] = true;
    conf_bit_vec[i].bit = UP_STABLE;
    send_bit_vector.bit_vec[i].bit = UP_STABLE;
  }
  //send_bit_vector.state_lock = ATOMIC_FLAG_INIT; // this does not compile
  send_bit_vector.state = UP_STABLE;
  print_for_debug = false;
  memset(committed_glob_sess_rmw_id, 0, GLOBAL_SESSION_NUM * sizeof(uint64_t));
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

void handle_program_inputs(int argc, char *argv[])
{
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
  char* chars_array = strtok(tmp_ip, ",");
  remote_ips = malloc(REM_MACH_NUM * sizeof(char*));
  int rm_id = 0;
  for ( int m_id = 0; m_id < MACHINE_NUM; m_id++) {
    assert(chars_array != NULL); // not enough IPs were passed
    printf("ip %s \n", chars_array);
    if (m_id ==  machine_id) {
      local_ip = malloc(16);
      memcpy(local_ip, chars_array, strlen(chars_array) + 1);
      printf("local_ip = %s  \n", local_ip);
    }
    else {
      remote_ips[rm_id] = malloc(16);
      memcpy(remote_ips[rm_id], chars_array, strlen(chars_array) + 1);
      printf("remote_ip[%d] = %s  \n", rm_id, remote_ips[rm_id]);
      rm_id++;
    }

    //else remote_ip_vector.push_back(chars_array);
    chars_array = strtok(NULL, ",");
  }

}

void spawn_threads(struct thread_params *param_arr, uint16_t t_id, char* node_purpose,
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


//When manufacturing the trace
uint8_t compute_opcode(struct opcode_info *opc_info, uint *seed)
{
  uint8_t  opcode = 0;
  uint8_t cas_opcode = USE_WEAK_CAS ? COMPARE_AND_SWAP_WEAK : COMPARE_AND_SWAP_STRONG;
  bool is_rmw = false, is_update = false, is_sc = false; bool is_rmw_acquire = false;
  if (ENABLE_RMWS) {
    if (ALL_RMWS_SINGLE_KEY) is_rmw = true;
    else
      is_rmw = rand_r(seed) % 1000 < RMW_RATIO;
  }
  if (!is_rmw) {
    is_update = rand() % 1000 < WRITE_RATIO; //rand_r(seed) % 1000 < WRITE_RATIO;
    is_sc = rand() % 1000 < SC_RATIO; //rand_r(seed) % 1000 < SC_RATIO;
  }

  if (is_rmw) {
    //if (!ALL_RMWS_SINGLE_KEY && !RMW_ONE_KEY_PER_THREAD)
    //printf("Worker %u, command %u is an RMW \n", t_id, i);
    is_rmw_acquire = (rand_r(seed) % 1000 < RMW_ACQUIRE_RATIO) &&
                     ENABLE_RMW_ACQUIRES;
    if (!is_rmw_acquire) {
      opc_info->rmws++;
      if (TRACE_ONLY_CAS) opcode = cas_opcode;
      else if (TRACE_ONLY_FA) opcode = FETCH_AND_ADD;
      else if (TRACE_MIXED_RMWS)
        opcode = (uint8_t) ((rand_r(seed) % 1000 < TRACE_CAS_RATIO) ? cas_opcode : FETCH_AND_ADD);
    if (opcode == cas_opcode) opc_info->cas++;
    else opc_info->fa++;
    }
    else {
      opcode = (uint8_t) OP_ACQUIRE;
      opc_info->rmw_acquires++;
    }
  }
  else if (is_update) {
    if (is_sc && ENABLE_RELEASES) {
      opcode = OP_RELEASE;
      opc_info->sc_writes++;
    }
    else {
      opcode = KVS_OP_PUT;
      opc_info->writes++;
    }
  }
  else  {
    if (is_sc && ENABLE_ACQUIRES) {
      opcode = OP_ACQUIRE;
      opc_info->sc_reads++;
    }
    else {
      opcode = KVS_OP_GET;
      opc_info->reads++;
    }
  }

  opc_info->is_rmw = is_rmw;
  opc_info->is_update = is_update;
  opc_info->is_sc = is_sc;
  opc_info->is_rmw_acquire = is_rmw_acquire;
  return opcode;
}


// Parse a trace, use this for skewed workloads as uniform trace can be manufactured easily
trace_t* parse_trace(char* path, int t_id){
    trace_t *trace;
    FILE * fp;
    ssize_t read;
    size_t len = 0;
    char* ptr, *word, *saveptr, *line = NULL;
    uint32_t i = 0, hottest_key_counter = 0, cmd_count = 0, word_count = 0;
    struct opcode_info *opc_info = calloc(1, sizeof(struct opcode_info));

    fp = fopen(path, "r");
    if (fp == NULL){
        printf ("ERROR: Cannot open file: %s\n", path);
        exit(EXIT_FAILURE);
    }

    while ((getline(&line, &len, fp)) != -1) cmd_count++;
    fclose(fp);
    if (line) free(line);
    len = 0;
    line = NULL;

    fp = fopen(path, "r");
    if (fp == NULL){
        printf ("ERROR: Cannot open file: %s\n", path);
        exit(EXIT_FAILURE);
    }
    // printf("File %s has %d lines \n", path, cmd_count);
    trace = (trace_t *)malloc((cmd_count + 1) * sizeof(trace_t));
    struct timespec time;
    clock_gettime(CLOCK_MONOTONIC, &time);
    uint seed = (uint)(time.tv_nsec + ((machine_id * WORKERS_PER_MACHINE) + t_id) + (uint64_t)trace);
    srand (seed);
    int debug_cnt = 0;
    //parse file line by line and insert trace to cmd.
    for (i = 0; i < cmd_count; i++) {
        if ((read = getline(&line, &len, fp)) == -1)
          my_printf(red, "ERROR: Problem while reading the trace\n");
        word_count = 0;
        word = strtok_r (line, " ", &saveptr);
        trace[i].opcode = 0;

        //Before reading the request deside if it's gone be r_rep or write
       //bool is_rmw = false, is_update = false, is_sc = false;
      trace[i].opcode = compute_opcode(opc_info, &seed);

      while (word != NULL) {
        if (word[strlen(word) - 1] == '\n')
          word[strlen(word) - 1] = 0;

        if (word_count == 0){
          uint32_t key_id = (uint32_t) strtoul(word, &ptr, 10);
          if (USE_A_SINGLE_KEY)
            key_id =  0;
          if (key_id == 0)
            hottest_key_counter++;
          uint128 key_hash = CityHash128((char *) &(key_id), 4);
          debug_cnt++;
          memcpy(trace[i].key_hash, &(key_hash.second), 8);
        }
        word_count++;
        word = strtok_r(NULL, " ", &saveptr);
        if (word == NULL && word_count < 1) {
          printf("Thread %d Error: Reached word %d in line %d : %s \n", t_id, word_count, i, line);
          assert(false);
        }
      }

    }
    if (t_id  == 0){
        my_printf(cyan, "Skewed TRACE: Exponent %d, Hottest key accessed: %.2f%%  \n", SKEW_EXPONENT_A,
                    (100 * hottest_key_counter / (double) cmd_count));
        printf("Writes: %.2f%%, SC Writes: %.2f%%, Reads: %.2f%% SC Reads: %.2f%% RMWs: %.2f%%\n"
                 "Trace w_size %d \n",
               (double) (opc_info->writes * 100) / cmd_count,
               (double) (opc_info->sc_writes * 100) / cmd_count,
               (double) (opc_info->reads * 100) / cmd_count,
               (double) (opc_info->sc_reads * 100) / cmd_count,
               (double) (opc_info->rmws * 100) / cmd_count, cmd_count);
    }
    trace[cmd_count].opcode = NOP;
    // printf("Thread %d Trace w_size: %d, debug counter %d hot keys %d, cold keys %d \n",l_id, cmd_count, debug_cnt,
    //         t_stats[l_id].hot_keys_per_trace, t_stats[l_id].cold_keys_per_trace );
    assert(cmd_count == debug_cnt);
    fclose(fp);
    if (line)
        free(line);
    return trace;
}


// Manufactures a trace with a uniform distrbution without a backing file
trace_t* manufacture_trace(int t_id)
{
  trace_t *trace = (trace_t *) calloc ((TRACE_SIZE + 1), sizeof(trace_t));
  struct timespec time;
  //struct random_data *buf;
  clock_gettime(CLOCK_MONOTONIC, &time);
  uint seed = (uint)(time.tv_nsec + ((machine_id * WORKERS_PER_MACHINE) + t_id) + (uint64_t)trace);
  srand (seed);
  struct opcode_info *opc_info = calloc(1, sizeof(struct opcode_info));
  uint32_t i;
  //parse file line by line and insert trace to cmd.
  for (i = 0; i < TRACE_SIZE; i++) {
    trace[i].opcode = 0;

    //Before reading the request decide if it's gone be r_rep or write
    trace[i].opcode = compute_opcode(opc_info, &seed);

    //--- KEY ID----------
    uint32 key_id;
    if(USE_A_SINGLE_KEY == 1) key_id =  0;
    uint128 key_hash;// = CityHash128((char *) &(key_id), 4);
    if (opc_info->is_rmw) {
      if (ALL_RMWS_SINGLE_KEY || ENABLE_ALL_CONFLICT_RMW)
        key_id = 0;
      else if (RMW_ONE_KEY_PER_THREAD)
        key_id = (uint32_t) t_id;
      else if (ENABLE_NO_CONFLICT_RMW)
        key_id = (uint32_t) ((machine_id * WORKERS_PER_MACHINE) + t_id);
      else key_id = (uint32_t) (rand_r(&seed) % KVS_NUM_KEYS);

      //printf("Wrkr %u key %u \n", t_id, key_id);
      key_hash = CityHash128((char *) &(key_id), 4);
    }
    else
    {
      key_id = (uint32) rand() % KVS_NUM_KEYS;
      key_hash = CityHash128((char *) &(key_id), 4);
    }
    memcpy(trace[i].key_hash, &(key_hash.second), 8);
  }

  if (t_id == 0) {
    my_printf(cyan, "UNIFORM TRACE \n");
    printf("Writes: %.2f%%, SC Writes: %.2f%%, Reads: %.2f%% SC Reads: %.2f%% RMWs: %.2f%%, "
             "CAS: %.2f%%, F&A: %.2f%%, RMW-Acquires: %.2f%%\n Trace w_size %u/%d, Write ratio %d \n",
           (double) (opc_info->writes * 100) / TRACE_SIZE,
           (double) (opc_info->sc_writes * 100) / TRACE_SIZE,
           (double) (opc_info->reads * 100) / TRACE_SIZE,
           (double) (opc_info->sc_reads * 100) / TRACE_SIZE,
           (double) (opc_info->rmws * 100) / TRACE_SIZE,
           (double) (opc_info->cas * 100) / TRACE_SIZE,
           (double) (opc_info->fa * 100) / TRACE_SIZE,
           (double) (opc_info->rmw_acquires * 100) / TRACE_SIZE,
           opc_info->writes + opc_info->sc_writes + opc_info->reads + opc_info->sc_reads + opc_info->rmws +
           opc_info->rmw_acquires,
           TRACE_SIZE, WRITE_RATIO);
  }
  trace[TRACE_SIZE].opcode = NOP;
  return trace;
  // printf("CLient %d Trace w_size: %d, debug counter %d hot keys %d, cold keys %d \n",l_id, cmd_count, debug_cnt,
  //         t_stats[l_id].hot_keys_per_trace, t_stats[l_id].cold_keys_per_trace );
}

// Initiialize the trace
trace_t* trace_init(uint16_t t_id) {
  trace_t *trace;
    //create the trace path path
    if (FEED_FROM_TRACE == 1) {
      char path[2048];
      char cwd[1024];
      char *was_successful = getcwd(cwd, sizeof(cwd));
      if (!was_successful) {
        printf("ERROR: getcwd failed!\n");
        exit(EXIT_FAILURE);
      }
      snprintf(path, sizeof(path), "%s%s%04d%s%d%s", cwd,
               "/../../../traces/current-splited-traces/t_",
               GET_GLOBAL_T_ID(machine_id, t_id), "_a_0.", SKEW_EXPONENT_A, ".txt");

      //initialize the command array from the trace file
      trace = parse_trace(path, t_id);
    }
    else {
      trace = manufacture_trace(t_id);
    }
  assert(trace != NULL);
  return trace;
}

void dump_stats_2_file(struct stats* st){
    uint8_t typeNo = 0;
    assert(typeNo >=0 && typeNo <=3);
    int i = 0;
    char filename[128];
    FILE *fp;
    double total_MIOPS;
    char* path = "../../results/scattered-results";

    sprintf(filename, "%s/%s-%s_s_%d_v_%d_m_%d_w_%d_r_%d-%d.csv", path,
            EMULATE_ABD == 1 ? "DRF-" : "REAL-",
            "ABD",
            SESSIONS_PER_THREAD,
            USE_BIG_OBJECTS == 1 ? ((EXTRA_CACHE_LINES * 64) + BASE_VALUE_SIZE): BASE_VALUE_SIZE,
            MACHINE_NUM, WORKERS_PER_MACHINE,
            WRITE_RATIO,
            machine_id);
    printf("%s\n", filename);
    fp = fopen(filename, "w"); // "w" means that we are going to write on this file
    fprintf(fp, "machine_id: %d\n", machine_id);

    fprintf(fp, "comment: thread ID, total MIOPS,"
            "reads sent, read-replies sent, acks sent, "
            "received read-replies, received reads, received acks\n");
    for(i = 0; i < WORKERS_PER_MACHINE; ++i){
        total_MIOPS = st->cache_hits_per_thread[i];
        fprintf(fp, "client: %d, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f\n",
                i, total_MIOPS, st->cache_hits_per_thread[i], st->reads_sent[i],
                st->r_reps_sent[i], st->acks_sent[i],
                st->received_r_reps[i],st->received_reads[i],
                st->received_acks[i]);
    }

    fclose(fp);
}

int spawn_stats_thread() {
    pthread_t *thread_arr = malloc(sizeof(pthread_t));
    pthread_attr_t attr;
    cpu_set_t cpus_stats;
    int core = -1;
    pthread_attr_init(&attr);
    CPU_ZERO(&cpus_stats);
    if(num_threads > 17) {
        core = 39;
        CPU_SET(core, &cpus_stats);
    }
    else {
        core = 2 * (num_threads) + 2;
        CPU_SET(core, &cpus_stats);
    }
    my_printf(yellow, "Creating stats thread at core %d\n", core);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus_stats);
    return pthread_create(&thread_arr[0], &attr, print_stats, NULL);
}

// pin threads starting from core 0
int pin_thread(int t_id) {
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
int pin_threads_avoiding_collisions(int c_id) {
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



/* ---------------------------------------------------------------------------
------------------------------DRF--------------------------------------
---------------------------------------------------------------------------*/
// set the different queue depths for the queue pairs
void set_up_queue_depths(int** recv_q_depths, int** send_q_depths)
{
  /* -------LEADER-------------
  * 1st Dgram send Reads -- receive Reads
  * 2nd Dgram send Read Replies  -- receive Read Replies
  * 3rd Dgram send Writes  -- receive Writes
  * 4th Dgram send Acks -- receive Acks
  * */

  *send_q_depths = malloc(QP_NUM * sizeof(int));
  *recv_q_depths = malloc(QP_NUM * sizeof(int));
  //RECV
  (*recv_q_depths)[R_QP_ID] = ENABLE_MULTICAST == 1? 1 : RECV_R_Q_DEPTH;
  (*recv_q_depths)[R_REP_QP_ID] = ENABLE_MULTICAST == 1? 1 : RECV_R_REP_Q_DEPTH;
  (*recv_q_depths)[W_QP_ID] = RECV_W_Q_DEPTH;
  (*recv_q_depths)[ACK_QP_ID] = RECV_ACK_Q_DEPTH;
  //SEND
  (*send_q_depths)[R_QP_ID] = SEND_R_Q_DEPTH;
  (*send_q_depths)[R_REP_QP_ID] = SEND_R_REP_Q_DEPTH;
  (*send_q_depths)[W_QP_ID] = SEND_W_Q_DEPTH;
  (*send_q_depths)[ACK_QP_ID] = SEND_ACK_Q_DEPTH;

}

/* ---------------------------------------------------------------------------
------------------------------DRF WORKER --------------------------------------
---------------------------------------------------------------------------*/
// Initialize the rmw struct
void set_up_rmw_struct()
{
//  memset(&rmw, 0, sizeof(struct rmw_info));
//  if (ENABLE_DEBUG_RMW_KV_PTR) {
//    for (int i = 0; i < RMW_ENTRIES_NUM; i++)
//      rmw.entry[i].dbg = (struct dbg_glob_entry *) calloc(1, sizeof(struct dbg_glob_entry));
//  }
}

// Initialize the pending ops struct
p_ops_t* set_up_pending_ops(uint32_t pending_writes, uint32_t pending_reads, uint16_t t_id)
{
  uint32_t i, j;
   p_ops_t *p_ops = (p_ops_t *) calloc(1, sizeof(p_ops_t));
  set_up_q_info(&p_ops->q_info);


  //p_ops->w_state = (uint8_t *) malloc(pending_writes * sizeof(uint8_t *));
  p_ops->r_state = (uint8_t *) malloc(pending_reads * sizeof(uint8_t *));
  //p_ops->w_session_id = (uint32_t *) calloc(pending_writes, sizeof(uint32_t));
  p_ops->r_session_id = (uint32_t *) calloc(pending_reads, sizeof(uint32_t));
  p_ops->w_index_to_req_array = (uint32_t *) calloc(pending_writes, sizeof(uint32_t));
  p_ops->r_index_to_req_array = (uint32_t *) calloc(pending_reads, sizeof(uint32_t));
  //p_ops->session_has_pending_op = (bool *) calloc(SESSIONS_PER_THREAD, sizeof(bool));
  //p_ops->acks_seen = (uint8_t *) calloc(pending_writes, sizeof(uint8_t));
  p_ops->read_info = (r_info_t *) calloc(pending_reads, sizeof(r_info_t));
  p_ops->p_ooe_writes =
    (struct pending_out_of_epoch_writes *) calloc(1, sizeof(struct pending_out_of_epoch_writes));


  // R_REP_FIFO
  p_ops->r_rep_fifo = (struct r_rep_fifo *) calloc(1, sizeof(struct r_rep_fifo));
  p_ops->r_rep_fifo->r_rep_message =
    (struct r_rep_message_template *) calloc((size_t)R_REP_FIFO_SIZE, (size_t)ALIGNED_R_REP_SEND_SIDE);
  p_ops->r_rep_fifo->rem_m_id = (uint8_t *) malloc(R_REP_FIFO_SIZE * sizeof(uint8_t));
  p_ops->r_rep_fifo->pull_ptr = 1;
  for (i= 0; i < R_REP_FIFO_SIZE; i++) p_ops->r_rep_fifo->rem_m_id[i] = MACHINE_NUM;
  p_ops->r_rep_fifo->message_sizes = (uint16_t *) calloc((size_t) R_REP_FIFO_SIZE, sizeof(uint16_t));

  // W_FIFO
  p_ops->w_fifo = (struct write_fifo *) calloc(1, sizeof(struct write_fifo));
  p_ops->w_fifo->w_message =
    (struct w_message_template *) calloc((size_t)W_FIFO_SIZE, (size_t) ALIGNED_W_SEND_SIDE);

  // R_FIFO
  p_ops->r_fifo = (struct read_fifo *) calloc(1, sizeof(struct read_fifo));
  p_ops->r_fifo->r_message =
    (struct r_message_template *) calloc(R_FIFO_SIZE, (size_t) ALIGNED_R_SEND_SIDE);


  // PREP STRUCT
  p_ops->prop_info = (struct prop_info *) aligned_alloc(64, sizeof(struct prop_info));
  memset(p_ops->prop_info, 0, sizeof(struct prop_info));
  assert(IS_ALIGNED(p_ops->prop_info, 64));
  for (i = 0; i < LOCAL_PROP_NUM; i++) {
    loc_entry_t *loc_entry = &p_ops->prop_info->entry[i];
    loc_entry->sess_id = (uint16_t) i;
    loc_entry->glob_sess_id = get_glob_sess_id((uint8_t)machine_id, t_id, (uint16_t) i);
    loc_entry->l_id = (uint64_t) loc_entry->sess_id;
    loc_entry->rmw_id.id = (uint64_t) loc_entry->glob_sess_id;
    loc_entry->help_rmw = (struct rmw_help_entry *) calloc(1, sizeof(struct rmw_help_entry));
    loc_entry->help_loc_entry = (loc_entry_t *) calloc(1, sizeof(loc_entry_t));
    loc_entry->help_loc_entry->sess_id = (uint16_t) i;
    loc_entry->help_loc_entry->helping_flag = IS_HELPER;
    loc_entry->help_loc_entry->glob_sess_id = loc_entry->glob_sess_id;
    loc_entry->state = INVALID_RMW;
  }
  p_ops->sess_info = (sess_info_t *) calloc(SESSIONS_PER_THREAD, sizeof(sess_info_t));
  p_ops->w_meta = (per_write_meta_t *) calloc(pending_writes, sizeof(per_write_meta_t));



   uint32_t max_incoming_w_r = (uint32_t) MAX(MAX_INCOMING_R, MAX_INCOMING_W);
  p_ops->ptrs_to_mes_headers =
    (struct r_message **) malloc(max_incoming_w_r * sizeof(struct r_message *));
  p_ops->coalesce_r_rep =
    (bool *) malloc(max_incoming_w_r* sizeof(bool));


  // PTRS to W_OPS
  //p_ops->ptrs_to_w_ops = (struct write **) malloc(MAX_INCOMING_W * sizeof(struct write *));
  // PTRS to R_OPS
  p_ops->ptrs_to_mes_ops = (void **) malloc(max_incoming_w_r * sizeof(struct read *));
  // PTRS to local ops to find the write after sending the first round of a release
  p_ops->ptrs_to_local_w = (struct write **) malloc(pending_writes * sizeof(struct write *));
  p_ops->overwritten_values = (uint8_t *) calloc(pending_writes, SEND_CONF_VEC_SIZE);


  for (i = 0; i < SESSIONS_PER_THREAD; i++) {
    p_ops->sess_info[i].ready_to_release = true;
  }
  for (i = 0; i < W_FIFO_SIZE; i++) {
    struct w_message *w_mes = (struct w_message *) &p_ops->w_fifo->w_message[i];
    w_mes->m_id = (uint8_t) machine_id;
//    for (j = 0; j < MAX_W_COALESCE; j++){
//      p_ops->w_fifo->w_message[i].write[j].m_id = (uint8_t) machine_id;
//      p_ops->w_fifo->w_message[i].write[j].val_len = VALUE_SIZE >> SHIFT_BITS;
//    }
  }
  p_ops->w_fifo->info[0].message_size = W_MES_HEADER;

  for (i = 0; i < R_FIFO_SIZE; i++) {
    struct r_message *r_mes = (struct r_message *) &p_ops->r_fifo->r_message[i];
    r_mes->m_id= (uint8_t) machine_id;
  }
  p_ops->r_fifo->info[0].message_size = R_MES_HEADER;

  for (i = 0; i < R_REP_FIFO_SIZE; i++) {
    struct rmw_rep_message *rmw_mes = (struct rmw_rep_message *) &p_ops->r_rep_fifo->r_rep_message[i];
    assert(((void *)rmw_mes - (void *)p_ops->r_rep_fifo->r_rep_message) % ALIGNED_R_REP_SEND_SIDE == 0);
    rmw_mes->m_id = (uint8_t) machine_id;
  }
  for (i = 0; i < pending_reads; i++)
    p_ops->r_state[i] = INVALID;
  for (i = 0; i < pending_writes; i++) {
    p_ops->w_meta[i].w_state = INVALID;
    p_ops->ptrs_to_local_w[i] = NULL;
  }
 return p_ops;
}

// Initialize the quorum info that contains the system configuration
void set_up_q_info(struct quorum_info **q_info)
{
  (*q_info) = (struct quorum_info *) calloc(1, sizeof(struct quorum_info));
  (*q_info)->active_num = REM_MACH_NUM;
  (*q_info)->first_active_rm_id = 0;
  (*q_info)->last_active_rm_id = REM_MACH_NUM - 1;
  for (uint8_t i = 0; i < REM_MACH_NUM; i++) {
    uint8_t m_id = i < machine_id ? i : (uint8_t) (i + 1);
    (*q_info)->active_ids[i] = m_id;
    (*q_info)->send_vector[i] = true;
  }

}

// Set up the memory registrations required
void set_up_mr(struct ibv_mr **mr, void *buf, uint8_t enable_inlining, uint32_t buffer_size,
                    struct hrd_ctrl_blk *cb)
{
  if (!enable_inlining)
    *mr = register_buffer(cb->pd, buf, buffer_size);
}


// Set up all Broadcast WRs
void set_up_bcast_WRs(struct ibv_send_wr *w_send_wr, struct ibv_sge *w_send_sgl,
                      struct ibv_send_wr *r_send_wr, struct ibv_sge *r_send_sgl,
                      uint16_t remote_thread,  struct hrd_ctrl_blk *cb,
                      struct ibv_mr *w_mr, struct ibv_mr *r_mr)
{
  uint16_t i, j;
  for (j = 0; j < MAX_BCAST_BATCH; j++) { // Number of Broadcasts
    if (!W_ENABLE_INLINING) w_send_sgl[j].lkey = w_mr->lkey;
    if (!R_ENABLE_INLINING) r_send_sgl[j].lkey = r_mr->lkey;
    // BROADCASTS
    for (i = 0; i < MESSAGES_IN_BCAST; i++) {
      uint16_t rm_id;
      if (i < machine_id) rm_id = (uint16_t) i;
      else rm_id = (uint16_t) ((i + 1) % MACHINE_NUM);
      uint16_t index = (uint16_t) ((j * MESSAGES_IN_BCAST) + i);
      assert (index < MESSAGES_IN_BCAST_BATCH);
      w_send_wr[index].wr.ud.ah = remote_qp[rm_id][remote_thread][W_QP_ID].ah;
      w_send_wr[index].wr.ud.remote_qpn = (uint32) remote_qp[rm_id][remote_thread][W_QP_ID].qpn;
      w_send_wr[index].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
      r_send_wr[index].wr.ud.ah = remote_qp[rm_id][remote_thread][R_QP_ID].ah;
      r_send_wr[index].wr.ud.remote_qpn = (uint32) remote_qp[rm_id][remote_thread][R_QP_ID].qpn;
      r_send_wr[index].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
      w_send_wr[index].opcode = IBV_WR_SEND;
      w_send_wr[index].num_sge = 1;
      w_send_wr[index].sg_list = &w_send_sgl[j];
      r_send_wr[index].opcode = IBV_WR_SEND;
      r_send_wr[index].num_sge = 1;
      r_send_wr[index].sg_list = &r_send_sgl[j];
      if (W_ENABLE_INLINING == 1) w_send_wr[index].send_flags = IBV_SEND_INLINE;
      else w_send_wr[index].send_flags = 0;
      if (R_ENABLE_INLINING == 1) r_send_wr[index].send_flags = IBV_SEND_INLINE;
      else r_send_wr[index].send_flags = 0;
      w_send_wr[index].next = (i == MESSAGES_IN_BCAST - 1) ? NULL : &w_send_wr[index + 1];
      r_send_wr[index].next = (i == MESSAGES_IN_BCAST - 1) ? NULL : &r_send_wr[index + 1];
    }
  }
}

// Set up the r_rep replies and acks send and recv wrs
void set_up_ack_n_r_rep_WRs(struct ibv_send_wr *ack_send_wr, struct ibv_sge *ack_send_sgl,
                            struct ibv_send_wr *r_rep_send_wr, struct ibv_sge *r_rep_send_sgl,
                            struct hrd_ctrl_blk *cb, struct ibv_mr *r_rep_mr,
                            struct ack_message *acks, uint16_t remote_thread) {
  uint16_t i;
  // ACKS
  for (i = 0; i < MAX_ACK_WRS; ++i) {
    ack_send_wr[i].wr.ud.ah = remote_qp[i][remote_thread][ACK_QP_ID].ah;
    ack_send_wr[i].wr.ud.remote_qpn = (uint32) remote_qp[i][remote_thread][ACK_QP_ID].qpn;
    ack_send_sgl[i].addr = (uint64_t) (uintptr_t) &acks[i];
    ack_send_sgl[i].length = ACK_SIZE;
    ack_send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
    ack_send_wr[i].opcode = IBV_WR_SEND;
    ack_send_wr[i].send_flags = IBV_SEND_INLINE;
    ack_send_wr[i].num_sge = 1;
    ack_send_wr[i].sg_list = &ack_send_sgl[i];
    ack_send_wr[i].next = NULL;
  }
  // READ REPLIES
  for (i = 0; i < MAX_R_REP_WRS; ++i) {
    if (R_REP_ENABLE_INLINING) r_rep_send_wr[i].send_flags = IBV_SEND_INLINE;
    else {
      r_rep_send_sgl[i].lkey = r_rep_mr->lkey;
      r_rep_send_wr[i].send_flags = 0;
    }
    r_rep_send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
    r_rep_send_wr[i].opcode = IBV_WR_SEND;
    r_rep_send_wr[i].num_sge = 1;
    r_rep_send_wr[i].sg_list = &r_rep_send_sgl[i];
  }
}


// Set up the receive info
void init_recv_info(struct hrd_ctrl_blk *cb, struct recv_info **recv,
                    uint32_t push_ptr, uint32_t buf_slots,
                    uint32_t slot_size, uint32_t posted_recvs,
                    struct ibv_qp *recv_qp, int max_recv_wrs, void* buf)
{
  (*recv) = malloc(sizeof(struct recv_info));
  (*recv)->push_ptr = push_ptr;
  (*recv)->buf_slots = buf_slots;
  (*recv)->slot_size = slot_size;
  (*recv)->posted_recvs = posted_recvs;
  (*recv)->recv_qp = recv_qp;
  (*recv)->buf = buf;
  (*recv)->recv_wr = (struct ibv_recv_wr *)malloc(max_recv_wrs * sizeof(struct ibv_recv_wr));
  (*recv)->recv_sgl = (struct ibv_sge *)malloc(max_recv_wrs * sizeof(struct ibv_sge));

  for (int i = 0; i < max_recv_wrs; i++) {
    (*recv)->recv_sgl[i].length = slot_size;
    (*recv)->recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
    (*recv)->recv_wr[i].sg_list = &((*recv)->recv_sgl[i]);
    (*recv)->recv_wr[i].num_sge = 1;
  }

}


// Prepost Receives on the Leader Side
// Post receives for the coherence traffic in the init phase
void pre_post_recvs(uint32_t* push_ptr, struct ibv_qp *recv_qp, uint32_t lkey, void* buf,
                    uint32_t max_reqs, uint32_t number_of_recvs, uint16_t QP_ID, uint32_t message_size)
{
  uint32_t i;//, j;
  for(i = 0; i < number_of_recvs; i++) {
        hrd_post_dgram_recv(recv_qp,	(buf + ((*push_ptr) * message_size)),
                            message_size, lkey);
      MOD_ADD(*push_ptr, max_reqs);
  }
}

// Set up the credits
void set_up_credits(uint16_t credits[][MACHINE_NUM])
{
  int i = 0;
  for (i = 0; i < MACHINE_NUM; i++) {
    credits[R_VC][i] = R_CREDITS;
    credits[W_VC][i] = W_CREDITS;
  }

}

// If reading CAS rmws out of the trace, CASes that compare against 0 succeed the rest fail
void randomize_op_values(trace_op_t *ops, uint16_t t_id)
{
  if (!ENABLE_CLIENTS) {
    for (uint16_t i = 0; i < MAX_OP_BATCH; i++) {
      if (TRACE_ONLY_FA)
        ops[i].value[0] = 1;
      else if (rand() % 1000 < RMW_CAS_CANCEL_RATIO)
        memset(ops[i].value, 1, VALUE_SIZE);
      else memset(ops[i].value, 0, VALUE_SIZE);
    }
  }
}

/* ---------------------------------------------------------------------------
------------------------------MULTICAST --------------------------------------
---------------------------------------------------------------------------*/

// Initialize the mcast_essentials structure that is necessary
void init_multicast(struct mcast_info **mcast_data, struct mcast_essentials **mcast,
                    int t_id, struct hrd_ctrl_blk *cb, int protocol)
{

  int *recv_q_depth = malloc(MCAST_QP_NUM * sizeof(int));
  //recv_q_depth[0] = protocol == FOLLOWER ? FLR_RECV_PREP_Q_DEPTH : 1;
  //recv_q_depth[1] = protocol == FOLLOWER ? FLR_RECV_COM_Q_DEPTH : 1;
  *mcast_data = malloc(sizeof(struct mcast_info));
  (*mcast_data)->t_id = t_id;
  setup_multicast(*mcast_data, recv_q_depth);
//   char char_buf[40];
//   inet_ntop(AF_INET6, (*mcast_data)->mcast_ud_param.ah_attr.grh.dgid.raw, char_buf, 40);
//   printf("client: joined dgid: %s mlid 0x%x sl %d\n", char_buf,	(*mcast_data)->mcast_ud_param.ah_attr.dlid, (*mcast_data)->mcast_ud_param.ah_attr.sl);
  *mcast = malloc(sizeof(struct mcast_essentials));

  for (uint16_t i = 0; i < MCAST_QP_NUM; i++){
      (*mcast)->recv_cq[i] = (*mcast_data)->cm_qp[i].cq;
      (*mcast)->recv_qp[i] = (*mcast_data)->cm_qp[i].cma_id->qp;
      (*mcast)->send_ah[i] = ibv_create_ah(cb->pd, &((*mcast_data)->mcast_ud_param[i].ah_attr));
      (*mcast)->qpn[i]  =  (*mcast_data)->mcast_ud_param[i].qp_num;
      (*mcast)->qkey[i]  =  (*mcast_data)->mcast_ud_param[i].qkey;

   }
  (*mcast)->recv_mr = ibv_reg_mr((*mcast_data)->cm_qp[0].pd, (void *)cb->dgram_buf,
                                 (size_t)TOTAL_BUF_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                                                       IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);


  free(*mcast_data);
  //if (protocol == FOLLOWER) assert((*mcast)->recv_mr != NULL);
}


// wrapper around getaddrinfo socket function
int get_addr(char *dst, struct sockaddr *addr)
{
    struct addrinfo *res;
    int ret;
    ret = getaddrinfo(dst, NULL, NULL, &res);
    if (ret) {
        printf("getaddrinfo failed - invalid hostname or IP address %s\n", dst);
        return ret;
    }
    memcpy(addr, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);
    return ret;
}

//Handle the addresses
void resolve_addresses(struct mcast_info *mcast_data)
{
    int ret, i, t_id = mcast_data->t_id;
    char mcast_addr[40];
    // Source addresses (i.e. local IPs)
    mcast_data->src_addr = (struct sockaddr*)&mcast_data->src_in;
    ret = get_addr(local_ip, ((struct sockaddr *)&mcast_data->src_in)); // to bind
    if (ret) printf("Client: failed to get src address \n");
    for (i = 0; i < MCAST_QPS; i++) {
        ret = rdma_bind_addr(mcast_data->cm_qp[i].cma_id, mcast_data->src_addr);
        if (ret) perror("Client: address bind failed");
    }
    // Destination addresses(i.e. multicast addresses)
    for (i = 0; i < MCAST_GROUPS_NUM; i ++) {
        mcast_data->dst_addr[i] = (struct sockaddr*)&mcast_data->dst_in[i];
        int m_cast_group_id = t_id * MACHINE_NUM + i;
        sprintf(mcast_addr, "224.0.%d.%d", m_cast_group_id / 256, m_cast_group_id % 256);
//        printf("mcast addr %d: %s\n", i, mcast_addr);
        ret = get_addr((char*) &mcast_addr, ((struct sockaddr *)&mcast_data->dst_in[i]));
        if (ret) printf("Client: failed to get dst address \n");
    }
}

// Set up the Send and Receive Qps for the multicast
void set_up_qp(struct cm_qps* qps, int *max_recv_q_depth)
{
    int ret, i, recv_q_depth;
    // qps[0].pd = ibv_alloc_pd(qps[0].cma_id->verbs); //new
    for (i = 0; i < MCAST_QP_NUM; i++) {
        qps[i].pd = ibv_alloc_pd(qps[i].cma_id->verbs);
        if (i > 0) qps[i].pd = qps[0].pd;
        recv_q_depth = max_recv_q_depth[i];
        qps[i].cq = ibv_create_cq(qps[i].cma_id->verbs, recv_q_depth, &qps[i], NULL, 0);
        struct ibv_qp_init_attr init_qp_attr;
        memset(&init_qp_attr, 0, sizeof init_qp_attr);
        init_qp_attr.cap.max_send_wr = 1;
        init_qp_attr.cap.max_recv_wr = (uint32) recv_q_depth;
        init_qp_attr.cap.max_send_sge = 1;
        init_qp_attr.cap.max_recv_sge = 1;
        init_qp_attr.qp_context = &qps[i];
        init_qp_attr.sq_sig_all = 0;
        init_qp_attr.qp_type = IBV_QPT_UD;
        init_qp_attr.send_cq = qps[i].cq;
        init_qp_attr.recv_cq = qps[i].cq;
        ret = rdma_create_qp(qps[i].cma_id, qps[i].pd, &init_qp_attr);
        if (ret) printf("unable to create QP \n");
    }
}

// Initial function to call to setup multicast, this calls the rest of the relevant functions
void setup_multicast(struct mcast_info *mcast_data, int *recv_q_depth)
{
    int ret, i, clt_id = mcast_data->t_id;
    static enum rdma_port_space port_space = RDMA_PS_UDP;
    // Create the channel
    mcast_data->channel = rdma_create_event_channel();
    if (!mcast_data->channel) {
        printf("Client %d :failed to create event channel\n", mcast_data->t_id);
        exit(1);
    }
    // Set up the cma_ids
    for (i = 0; i < MCAST_QPS; i++ ) {
        ret = rdma_create_id(mcast_data->channel, &mcast_data->cm_qp[i].cma_id, &mcast_data->cm_qp[i], port_space);
        if (ret) printf("Client %d :failed to create cma_id\n", mcast_data->t_id);
    }
    // deal with the addresses
    resolve_addresses(mcast_data);
    // set up the 2 qps
    set_up_qp(mcast_data->cm_qp, recv_q_depth);

    struct rdma_cm_event* event;
    for (i = 0; i < MCAST_GROUPS_NUM; i ++) {
        int qp_i = i;
        ret = rdma_resolve_addr(mcast_data->cm_qp[i].cma_id, mcast_data->src_addr, mcast_data->dst_addr[i], 20000);
        if (ret) printf("Client %d: failed to resolve address: %d, qp_i %d \n", clt_id, i, qp_i);
        if (ret) perror("Reason");
        while (rdma_get_cm_event(mcast_data->channel, &event) == 0) {
            switch (event->event) {
                case RDMA_CM_EVENT_ADDR_RESOLVED:
//                     printf("Client %d: RDMA ADDRESS RESOLVED address: %d \n", m_id, i);
                    ret = rdma_join_multicast(mcast_data->cm_qp[qp_i].cma_id, mcast_data->dst_addr[i], mcast_data);
                    if (ret) printf("unable to join multicast \n");
                    break;
                case RDMA_CM_EVENT_MULTICAST_JOIN:
                    mcast_data->mcast_ud_param[i] = event->param.ud;
//                     printf("RDMA JOIN MUlTICAST EVENT %d \n", i);
                    break;
                case RDMA_CM_EVENT_MULTICAST_ERROR:
                default:
                    break;
            }
            rdma_ack_cm_event(event);
            if (event->event == RDMA_CM_EVENT_MULTICAST_JOIN) break;
        }
        //if (i != RECV_MCAST_QP) {
            // destroying the QPs works fine but hurts performance...
            //  rdma_destroy_qp(mcast_data->cm_qp[i].cma_id);
            //  rdma_destroy_id(mcast_data->cm_qp[i].cma_id);
        //}
    }
    // rdma_destroy_event_channel(mcast_data->channel);
    // if (mcast_data->mcast_ud_param == NULL) mcast_data->mcast_ud_param = event->param.ud;
}


// call to test the multicast
void multicast_testing(struct mcast_essentials *mcast, int clt_gid, struct hrd_ctrl_blk *cb)
{

    struct ibv_wc mcast_wc;
    printf ("Client: Multicast Qkey %u and qpn %u \n", mcast->qkey[COM_MCAST_QP], mcast->qpn[COM_MCAST_QP]);


    struct ibv_sge mcast_sg;
    struct ibv_send_wr mcast_wr;
    struct ibv_send_wr *mcast_bad_wr;

    memset(&mcast_sg, 0, sizeof(mcast_sg));
    mcast_sg.addr	  = (uintptr_t)cb->dgram_buf;
    mcast_sg.length = 10;
    //mcast_sg.lkey	  = cb->dgram_buf_mr->lkey;

    memset(&mcast_wr, 0, sizeof(mcast_wr));
    mcast_wr.wr_id      = 0;
    mcast_wr.sg_list    = &mcast_sg;
    mcast_wr.num_sge    = 1;
    mcast_wr.opcode     = IBV_WR_SEND_WITH_IMM;
    mcast_wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
    mcast_wr.imm_data   = (uint32) clt_gid + 120 + (machine_id * 10);
    mcast_wr.next       = NULL;

    mcast_wr.wr.ud.ah          = mcast->send_ah[COM_MCAST_QP];
    mcast_wr.wr.ud.remote_qpn  = mcast->qpn[COM_MCAST_QP];
    mcast_wr.wr.ud.remote_qkey = mcast->qkey[COM_MCAST_QP];

    if (ibv_post_send(cb->dgram_qp[W_QP_ID], &mcast_wr, &mcast_bad_wr)) {
        fprintf(stderr, "Error, ibv_post_send() failed\n");
        assert(false);
    }

    printf("THe mcast was sent, I am waiting for confirmation imm data %d\n", mcast_wr.imm_data);
    hrd_poll_cq(cb->dgram_send_cq[W_QP_ID], 1, &mcast_wc);
    printf("The mcast was sent \n");
    hrd_poll_cq(mcast->recv_cq[COM_MCAST_QP], 1, &mcast_wc);
    printf("Client %d imm data recved %d \n", clt_gid, mcast_wc.imm_data);
    hrd_poll_cq(mcast->recv_cq[COM_MCAST_QP], 1, &mcast_wc);
    printf("Client %d imm data recved %d \n", clt_gid, mcast_wc.imm_data);
    hrd_poll_cq(mcast->recv_cq[COM_MCAST_QP], 1, &mcast_wc);
    printf("Client %d imm data recved %d \n", clt_gid, mcast_wc.imm_data);

    exit(0);
}

//--------------------------------------------------
//--------------PUBLISHING QPS---------------------
//--------------------------------------------------


// Worker calls this function to connect with all workers
void get_qps_from_all_other_machines(struct hrd_ctrl_blk *cb)
{
  int g_i, qp_i, w_i, m_i;
  int ib_port_index = 0;
  // -- CONNECT WITH EVERYONE
  for(g_i = 0; g_i < WORKER_NUM; g_i++) {
    if (g_i / WORKERS_PER_MACHINE == machine_id) continue; // skip the local machine
    w_i = g_i % WORKERS_PER_MACHINE;
    m_i = g_i / WORKERS_PER_MACHINE;
    for (qp_i = 0; qp_i < QP_NUM; qp_i++) {
      /* Compute the control block and physical port index for client @i */
      int local_port_i = ib_port_index;// + cb_i;

      struct qp_attr *wrkr_qp = &all_qp_attr->wrkr_qp[m_i][w_i][qp_i];
      struct ibv_ah_attr ah_attr = {
        //-----INFINIBAND----------
        .is_global = 0,
        .dlid = (uint16_t) wrkr_qp->lid,
        .sl = (uint8_t) wrkr_qp->sl,
        .src_path_bits = 0,
        /* port_num (> 1): device-local port for responses to this worker */
        .port_num = (uint8_t) (local_port_i + 1),
      };
      // ---ROCE----------
      if (is_roce == 1) {
        ah_attr.is_global = 1;
        ah_attr.dlid = 0;
        ah_attr.grh.dgid.global.interface_id =  wrkr_qp->gid_global_interface_id;
        ah_attr.grh.dgid.global.subnet_prefix = wrkr_qp->gid_global_subnet_prefix;
        ah_attr.grh.sgid_index = 0;
        ah_attr.grh.hop_limit = 1;
      }
      remote_qp[m_i][w_i][qp_i].ah = ibv_create_ah(cb->pd, &ah_attr);
      remote_qp[m_i][w_i][qp_i].qpn = wrkr_qp->qpn;
      // printf("%d %d %d success\n", m_i, w_i, qp_i );
      assert(remote_qp[m_i][w_i][qp_i].ah != NULL);

    }
  }
}


#define BASE_SOCKET_PORT 8080

// Machines with id higher than 0 connect with machine-id 0.
// First they sent it their qps-attrs and then receive everyone's
void set_up_qp_attr_client()
{
  int sock = 0, valread;
  struct sockaddr_in serv_addr;
  if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
  {
    printf("\n Socket creation error \n");
    assert(false);
  }

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons((uint16_t)(BASE_SOCKET_PORT + machine_id - 1));

  // Convert IPv4 and IPv6 addresses from text to binary form
  if(inet_pton(AF_INET, remote_ips[0], &serv_addr.sin_addr) <= 0)
  {
    printf("\nInvalid address/ Address not supported \n");
    assert(false);
  }

  while (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0);
  struct qp_attr *qp_attr_to_send = &all_qp_attr->wrkr_qp[machine_id][0][0];
  size_t mes_size = WORKERS_PER_MACHINE * QP_NUM * sizeof(struct qp_attr);
  send(sock, qp_attr_to_send, mes_size, 0);
  struct qp_attr tmp[WORKERS_PER_MACHINE][QP_NUM];
  memcpy(tmp, qp_attr_to_send, mes_size);
  // printf("Attributes sent\n");
  valread = (int) recv(sock, all_qp_attr, sizeof(all_qp_attr_t), MSG_WAITALL);
  assert(valread == sizeof(all_qp_attr_t));
  int cmp = memcmp(qp_attr_to_send, qp_attr_to_send, mes_size);
  assert(cmp == 0);
  // printf("Received all attributes, size %ld \n", sizeof(all_qp_attr_t));
}

// Machine 0 acts as a "server"; it receives all qp attributes,
// and broadcasts them to everyone
void set_up_qp_attr_server()
{
  int server_fd[REM_MACH_NUM], new_socket[REM_MACH_NUM], valread;
  struct sockaddr_in address;
  int opt = 1;
  int addrlen = sizeof(address);

  for (int rm_i = 0; rm_i < REM_MACH_NUM; rm_i++) {
    // Creating socket file descriptor
    if ((server_fd[rm_i] = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
      printf("socket failed \n");
      assert(false);
    }

    // Forcefully attaching socket to the port 8080
    if (setsockopt(server_fd[rm_i], SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT,
                   &opt, sizeof(opt))) {
      printf("setsockopt \n");
      assert(false);
    }
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons((uint16_t)(BASE_SOCKET_PORT + rm_i));

    // Forcefully attaching socket to the port 8080
    if (bind(server_fd[rm_i], (struct sockaddr *) &address,
             sizeof(address)) < 0) {
      printf("bind failed \n");
      assert(false);
    }
    // printf("Succesful bind \n");
    if (listen(server_fd[rm_i], 3) < 0) {
      printf("listen");
      assert(false);
    }
    // printf("Succesful listen \n");
    if ((new_socket[rm_i] = accept(server_fd[rm_i], (struct sockaddr *) &address,
                             (socklen_t *) &addrlen)) < 0) {
      printf("accept");
      assert(false);
    }
    // printf("Successful accept \n");
    size_t mes_size = WORKERS_PER_MACHINE * QP_NUM * sizeof(struct qp_attr);
    valread = (int) recv(new_socket[rm_i], &all_qp_attr->wrkr_qp[rm_i + 1][0][0], mes_size, MSG_WAITALL);
    assert(valread == mes_size);
    //printf("Server received qp_attributes from machine %u size %ld \n",
    //       rm_i + 1, mes_size);
  }
  for (int rm_i = 0; rm_i < REM_MACH_NUM; rm_i++) {
    send(new_socket[rm_i], all_qp_attr, sizeof(all_qp_attr_t), 0);
  }
}


// Used by all kinds of threads to publish their QPs
void fill_qps(int t_id, struct hrd_ctrl_blk *cb)
{
  uint32_t qp_i;
  for (qp_i = 0; qp_i < QP_NUM; qp_i++) {
    struct qp_attr *qp_attr = &all_qp_attr->wrkr_qp[machine_id][t_id][qp_i];
    qp_attr->lid = hrd_get_local_lid(cb->dgram_qp[qp_i]->context, cb->dev_port_id);
    qp_attr->qpn = cb->dgram_qp[qp_i]->qp_num;
    qp_attr->sl = DEFAULT_SL;
    //   ---ROCE----------
    if (is_roce == 1) {
      union ibv_gid ret_gid;
      ibv_query_gid(cb->ctx, IB_PHYS_PORT, 0, &ret_gid);
      qp_attr->gid_global_interface_id = ret_gid.global.interface_id;
      qp_attr->gid_global_subnet_prefix = ret_gid.global.subnet_prefix;
    }
  }
  // Signal to other threads that you have filled your qp attributes
  atomic_fetch_add_explicit(&workers_with_filled_qp_attr, 1, memory_order_seq_cst);
}

// Followers and leaders both use this to establish connections
void setup_connections_and_spawn_stats_thread(uint32_t global_id,
                                              struct hrd_ctrl_blk *cb)
{
  int t_id = global_id % WORKERS_PER_MACHINE;
  fill_qps(t_id, cb);

  if (t_id == 0) {
    while(workers_with_filled_qp_attr != WORKERS_PER_MACHINE);
    if (machine_id == 0) set_up_qp_attr_server();
    else set_up_qp_attr_client();
    get_qps_from_all_other_machines(cb);
    assert(!qps_are_set_up);
    // Spawn a thread that prints the stats
    if (CLIENT_MODE != CLIENT_UI) {
      if (spawn_stats_thread() != 0)
        my_printf(red, "Stats thread was not successfully spawned \n");
    }
    atomic_store_explicit(&qps_are_set_up, true, memory_order_release);
  }
  else {
    while (!atomic_load_explicit(&qps_are_set_up, memory_order_acquire));  usleep(200000);
  }
  assert(qps_are_set_up);
//    printf("Thread %d has all the needed ahs\n", global_id );
}
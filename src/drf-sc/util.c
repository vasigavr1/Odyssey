#include "util.h"



// Worker calls this function to connect with all workers
void get_qps_from_all_other_machines(uint32_t g_id, struct hrd_ctrl_blk *cb)
{
    int i, qp_i;
    int ib_port_index = 0;
    struct ibv_ah *wrkr_ah[WORKER_NUM][QP_NUM];
    struct hrd_qp_attr *wrkr_qp[WORKER_NUM][QP_NUM];

    // -- CONNECT WITH EVERYONE
    for(i = 0; i < WORKER_NUM; i++) {
      if (i / WORKERS_PER_MACHINE == machine_id) continue; // skip the local machine
        for (qp_i = 0; qp_i < QP_NUM; qp_i++) {
            /* Compute the control block and physical port index for client @i */
            // int cb_i = i % num_server_ports;
            int local_port_i = ib_port_index;// + cb_i;

            char worker_name[QP_NAME_SIZE];
            sprintf(worker_name, "worker-dgram-%d-%d", i, qp_i);

            /* Get the UD queue pair for the ith machine */
            wrkr_qp[i][qp_i] = NULL;
            //printf("Leader %d is Looking for follower %s \n", l_id, worker_name);
            while(wrkr_qp[i][qp_i] == NULL) {
                wrkr_qp[i][qp_i] = hrd_get_published_qp(worker_name);
                if(wrkr_qp[i][qp_i] == NULL)
                    usleep(200000);
            }
             // green_printf("main:Worker %d found qp %d of  wrkr %d. Worker LID: %d\n",
               //     g_id, qp_i, i, wrkr_qp[i][qp_i]->lid);
            struct ibv_ah_attr ah_attr = {
                //-----INFINIBAND----------
                .is_global = 0,
                .dlid = (uint16_t) wrkr_qp[i][qp_i]->lid,
                .sl = (uint8_t) wrkr_qp[i][qp_i]->sl,
                .src_path_bits = 0,
                /* port_num (> 1): device-local port for responses to this worker */
                .port_num = (uint8_t) (local_port_i + 1),
            };

            // ---ROCE----------
            if (is_roce == 1) {
                ah_attr.is_global = 1;
                ah_attr.dlid = 0;
                ah_attr.grh.dgid.global.interface_id =  wrkr_qp[i][qp_i]->gid_global_interface_id;
                ah_attr.grh.dgid.global.subnet_prefix = wrkr_qp[i][qp_i]->gid_global_subnet_prefix;
                ah_attr.grh.sgid_index = 0;
                ah_attr.grh.hop_limit = 1;
            }
            //
            wrkr_ah[i][qp_i]= ibv_create_ah(cb->pd, &ah_attr);
            assert(wrkr_ah[i][qp_i] != NULL);
            remote_qp[i / WORKERS_PER_MACHINE][i % WORKERS_PER_MACHINE][qp_i].ah =
                wrkr_ah[i][qp_i];
            remote_qp[i / WORKERS_PER_MACHINE][i % WORKERS_PER_MACHINE][qp_i].qpn =
                wrkr_qp[i][qp_i]->qpn;
        }
    }
}

// Parse a trace, use this for skewed workloads as uniform trace can be manufactured easilly
int parse_trace(char* path, struct trace_command **cmds, int g_id){
    FILE * fp;
    ssize_t read;
    size_t len = 0;
    char* ptr;
    char* word;
    char *saveptr;
    char* line = NULL;
    int i = 0;
    int cmd_count = 0;
    int word_count = 0;
    int letter_count = 0;
    int writes = 0;
    uint32_t hottest_key_counter = 0;

    fp = fopen(path, "r");
    if (fp == NULL){
        printf ("ERROR: Cannot open file: %s\n", path);
        exit(EXIT_FAILURE);
    }

    while ((read = getline(&line, &len, fp)) != -1)
        cmd_count++;

    fclose(fp);
    if (line)
        free(line);

    len = 0;
    line = NULL;

    fp = fopen(path, "r");
    if (fp == NULL){
        printf ("ERROR: Cannot open file: %s\n", path);
        exit(EXIT_FAILURE);
    }
    // printf("File %s has %d lines \n", path, cmd_count);
    (*cmds) = (struct trace_command *)malloc((cmd_count + 1) * sizeof(struct trace_command));
    struct timespec time;
    clock_gettime(CLOCK_MONOTONIC, &time);
    uint64_t seed = time.tv_nsec + ((machine_id * WORKERS_PER_MACHINE) + g_id) + (uint64_t)(*cmds);
    srand ((uint)seed);
    int debug_cnt = 0;
    //parse file line by line and insert trace to cmd.
    for (i = 0; i < cmd_count; i++) {
        if ((read = getline(&line, &len, fp)) == -1)
            die("ERROR: Problem while reading the trace\n");
        word_count = 0;
        word = strtok_r (line, " ", &saveptr);
        (*cmds)[i].opcode = 0;

        //Before reading the request deside if it's gone be r_rep or write
        uint8_t is_update = (rand() % 1000 < WRITE_RATIO) ? (uint8_t) 1 : (uint8_t) 0;
        if (is_update) {
            (*cmds)[i].opcode = (uint8_t) 1; // WRITE_OP
            writes++;
        }
        else  (*cmds)[i].opcode = (uint8_t) 0; // READ_OP

        while (word != NULL) {
            letter_count = 0;
            if (word[strlen(word) - 1] == '\n')
                word[strlen(word) - 1] = 0;

            if(word_count == 1) {
                // DO NOTHING;

            } else if(word_count == 2) {
              // DO NOTHING;
            } else if(word_count == 3){
                (*cmds)[i].key_id = (uint32_t) strtoul(word, &ptr, 10);
                if(USE_A_SINGLE_KEY == 1)
                    (*cmds)[i].key_id =  0;
                if ((*cmds)[i].key_id < CACHE_NUM_KEYS) { // hot
                    if ((*cmds)[i].opcode == 1) // hot write
                        (*cmds)[i].opcode = (uint8_t) HOT_WRITE;
                    else (*cmds)[i].opcode = (uint8_t) HOT_READ; // hot r_rep
                }
                (*cmds)[i].key_hash = CityHash128((char *) &((*cmds)[i].key_id), 4);
                debug_cnt++;
            }else if(word_count == 0) {
              // DO NOTHING;
            }

            word_count++;
            word = strtok_r(NULL, " ", &saveptr);
            if (word == NULL && word_count < 4) {
                printf("Client %d Error: Reached word %d in line %d : %s \n",g_id, word_count, i, line);
                assert(false);
            }
        }

    }
    if (g_id  == 0) printf("Write Ratio: %.2f%% \n", (double) (writes * 100) / cmd_count);
    (*cmds)[cmd_count].opcode = NOP;
    // printf("Thread %d Trace w_size: %d, debug counter %d hot keys %d, cold keys %d \n",l_id, cmd_count, debug_cnt,
    //         t_stats[l_id].hot_keys_per_trace, t_stats[l_id].cold_keys_per_trace );
    assert(cmd_count == debug_cnt);
    fclose(fp);
    if (line)
        free(line);
    return cmd_count;
}


// Manufactures a trace with a uniform distrbution without a backing file
void manufacture_trace(struct trace_command_uni **cmds, int t_id)
{
  (*cmds) = (struct trace_command_uni *)malloc((TRACE_SIZE + 1) * sizeof(struct trace_command_uni));
  struct timespec time;
  clock_gettime(CLOCK_MONOTONIC, &time);
  uint64_t seed = time.tv_nsec + ((machine_id * WORKERS_PER_MACHINE) + t_id) + (uint64_t)(*cmds);
  srand ((uint)seed);
  uint32_t i, writes = 0, reads = 0, sc_reads = 0, sc_writes = 0, rmws = 0,
          keys_that_get_rmwed[MACHINE_NUM * WORKERS_PER_MACHINE];
  if (ENABLE_RMWS) {
    for (i = 0; i < MACHINE_NUM * WORKERS_PER_MACHINE; i++)
      keys_that_get_rmwed[i] = i;
  }
  //parse file line by line and insert trace to cmd.
  for (i = 0; i < TRACE_SIZE; i++) {
    (*cmds)[i].opcode = 0;

    //Before reading the request decide if it's gone be r_rep or write
    uint8_t is_update = (rand() % 1000 < WRITE_RATIO) ? (uint8_t) 1 : (uint8_t) 0;
    uint8_t is_sc = (rand() % 1000 < SC_RATIO) ? (uint8_t) 1 : (uint8_t) 0;
    bool is_rmw = false;
    if (ENABLE_RMWS) {
      if (ALL_RMWS_SINGLE_KEY)  {
        is_rmw = true;
      }
      if (ENABLE_NO_CONFLICT_RMW) {
        is_rmw = (t_id == 0) && (i == (machine_id * WORKERS_PER_MACHINE + t_id));
      }
      else if (ENABLE_SINGLE_KEY_RMW) {
        is_rmw = i == (REM_MACH_NUM * WORKERS_PER_MACHINE);
      }
    }
    if (is_rmw) {
      if (!ALL_RMWS_SINGLE_KEY) printf("Worker %u, command %u is an RMW \n", t_id, i);
      rmws++;
      (*cmds)[i].opcode = PROPOSE_OP;
    }
    else if (is_update) {
      if (is_sc && ENABLE_RELEASES) {
        (*cmds)[i].opcode = OP_RELEASE;
        sc_writes++;
      }
      else {
        (*cmds)[i].opcode = CACHE_OP_PUT;
        writes++;
      }

    }
    else  {
      if (is_sc && ENABLE_ACQUIRES) {
        (*cmds)[i].opcode = OP_ACQUIRE;
        sc_reads++;
      }
      else {
        (*cmds)[i].opcode = CACHE_OP_GET;
        reads++;
      }
    }


    //--- KEY ID----------
    uint32 key_id;
    if(USE_A_SINGLE_KEY == 1) key_id =  0;
    uint128 key_hash = CityHash128((char *) &(key_id), 4);
    if (is_rmw) {
      if (ALL_RMWS_SINGLE_KEY)
        key_id = 0;
      else  key_id = (uint32_t) i;
      key_hash = CityHash128((char *) &(key_id), 4);
    }
    else {
      bool found = false;
      do {
        key_id = (uint32) rand() % CACHE_NUM_KEYS;
        found = false;
        for (uint32_t j = 0; j < MACHINE_NUM * WORKERS_PER_MACHINE; j++)
          if (key_id == keys_that_get_rmwed[j]) found = true;
      } while (found);
      key_hash = CityHash128((char *) &(key_id), 4);
    }
    memcpy((*cmds)[i].key_hash, &(key_hash.second), 8);

  }

  if (t_id  == 0) printf("Writes: %.2f%%, SC Writes: %.2f%%, Reads: %.2f%% SC Reads: %.2f%% RMWs: %.2f%%\n"
                           "Trace w_size %d \n",
                         (double) (writes * 100) / TRACE_SIZE,
                         (double) (sc_writes * 100) / TRACE_SIZE,
                         (double) (reads * 100) / TRACE_SIZE,
                         (double) (sc_reads * 100) / TRACE_SIZE,
                         (double) (rmws * 100) / TRACE_SIZE,TRACE_SIZE);
  (*cmds)[TRACE_SIZE].opcode = NOP;
  // printf("CLient %d Trace w_size: %d, debug counter %d hot keys %d, cold keys %d \n",l_id, cmd_count, debug_cnt,
  //         t_stats[l_id].hot_keys_per_trace, t_stats[l_id].cold_keys_per_trace );
}

// Initiialize the trace
void trace_init(void **cmds, int g_id) {
    //create the trace path path
    if (FEED_FROM_TRACE == 1) {
        char local_client_id[3];
        char machine_num[4];
        //get / creat path for the trace
        if (WORKERS_PER_MACHINE <= 23) sprintf(local_client_id, "%d", (g_id % WORKERS_PER_MACHINE));
        else  sprintf(local_client_id, "%d", (g_id % 23));// the traces are for 8 clients
        // sprintf(local_client_id, "%d", (l_id % 4));
        sprintf(machine_num, "%d", machine_id);
        char path[2048];
        char cwd[1024];
        char *was_successful = getcwd(cwd, sizeof(cwd));

        if (!was_successful) {
            printf("ERROR: getcwd failed!\n");
            exit(EXIT_FAILURE);
        }

        snprintf(path, sizeof(path), "%s%s%s%s%s%s%d%s", cwd,
                 "/../../traces/current-splited-traces/s_",
                 machine_num, "_c_", local_client_id, "_a_", SKEW_EXPONENT_A, ".txt");
        //initialize the command array from the trace file
        // printf("Thread: %d attempts to r_rep the trace: %s\n", l_id, path);
        parse_trace(path, (struct trace_command **)cmds, g_id % WORKERS_PER_MACHINE);
        //printf("Trace r_rep by client: %d\n", l_id);
    }else {
      manufacture_trace((struct trace_command_uni **)cmds, g_id);
    }

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
            ENABLE_LIN == 1 ? "LIN" : "SC",
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
    yellow_printf("Creating stats thread at core %d\n", core);
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

        //if (DISABLE_CACHE == 1) c_core = 4 * i + 2; // when bypassing the cache
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


// Used by all kinds of threads to publish their QPs
void publish_qps(uint32_t qp_num, uint32_t global_id, const char* qp_name, struct hrd_ctrl_blk *cb)
{
  uint32_t qp_i;
  //cyan_printf("Wrkr attempting to %d publish its dgrams \n", global_id);
  for (qp_i = 0; qp_i < qp_num; qp_i++) {
    char dgram_qp_name[QP_NAME_SIZE];
    sprintf(dgram_qp_name, "%s-%d-%d", qp_name, global_id, qp_i);
    hrd_publish_dgram_qp(cb, qp_i, dgram_qp_name, DEFAULT_SL);
    // yellow_printf("Wrkr %d published dgram %s \n", global_id, dgram_qp_name);
  }
  //yellow_printf("Wrkr %d published its dgrams \n", global_id);
}

// Followers and leaders both use this to establish connections
void setup_connections_and_spawn_stats_thread(uint32_t global_id, struct hrd_ctrl_blk *cb)
{

    int t_id = global_id % WORKERS_PER_MACHINE;
    publish_qps(QP_NUM, global_id, "worker-dgram", cb);

    if (t_id == 0) {
      get_qps_from_all_other_machines(global_id, cb);
      assert(qps_are_set_up == 0);
      // Spawn a thread that prints the stats
      if (spawn_stats_thread() != 0)
          red_printf("Stats thread was not successfully spawned \n");
      atomic_store_explicit(&qps_are_set_up, 1, memory_order_release);
    }
    else {
        while (atomic_load_explicit(&qps_are_set_up, memory_order_acquire)== 0);  usleep(200000);
    }
    assert(qps_are_set_up == 1);
//    printf("Thread %d has all the needed ahs\n", global_id );
}

/* ---------------------------------------------------------------------------
------------------------------ABD--------------------------------------
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
------------------------------ABD WORKER --------------------------------------
---------------------------------------------------------------------------*/
// Initialize the rmw struct
void set_up_rmw_struct()
{
  memset(&rmw, 0, sizeof(struct rmw_info));
//  for (uint16_t i = 0; i < RMW_ENTRIES_NUM; i++)
//    rmw.empty_fifo[i] = i;
//  rmw.ef_size = RMW_ENTRIES_NUM;
}

// Initialize the pending ops struct
void set_up_pending_ops(struct pending_ops **p_ops, uint32_t pending_writes, uint32_t pending_reads)
{
  uint32_t i, j;
  (*p_ops) = (struct pending_ops *) malloc(sizeof(struct pending_ops));
  memset((*p_ops), 0, sizeof(struct pending_ops));
  //(*p_writes)->write_ops = (struct write_op*) malloc(w_size * sizeof(struct write_op));

  (*p_ops)->w_state = (uint8_t *) malloc(pending_writes * sizeof(uint8_t *));
  (*p_ops)->r_state = (uint8_t *) malloc(pending_reads * sizeof(uint8_t *));
  (*p_ops)->w_session_id = (uint32_t *) malloc(pending_writes * sizeof(uint32_t));
  (*p_ops)->r_session_id = (uint32_t *) malloc(pending_reads * sizeof(uint32_t));
  memset((*p_ops)->w_session_id, 0, pending_writes * sizeof(uint32_t));
  memset((*p_ops)->r_session_id, 0, pending_reads * sizeof(uint32_t));
  (*p_ops)->session_has_pending_op = (bool *) malloc(SESSIONS_PER_THREAD * sizeof(bool));
  memset((*p_ops)->session_has_pending_op, 0, SESSIONS_PER_THREAD * sizeof(bool));
  (*p_ops)->acks_seen = (uint8_t *) malloc(pending_writes * sizeof(uint8_t));
  memset((*p_ops)->acks_seen, 0, pending_writes * sizeof(uint8_t));
  (*p_ops)->read_info = (struct read_info *) malloc(pending_reads * sizeof(struct read_info));
  memset((*p_ops)->read_info, 0, pending_reads * sizeof(struct read_info));

  (*p_ops)->p_ooe_writes =
    (struct pending_out_of_epoch_writes *) malloc(sizeof(struct pending_out_of_epoch_writes));
  memset((*p_ops)->p_ooe_writes, 0, sizeof(struct pending_out_of_epoch_writes));

  // R_REP_FIFO
  (*p_ops)->r_rep_fifo = (struct r_rep_fifo *) malloc(sizeof(struct r_rep_fifo));
  memset((*p_ops)->r_rep_fifo, 0, sizeof(struct r_rep_fifo));
  (*p_ops)->r_rep_fifo->r_rep_message = (struct r_rep_message *) malloc(R_REP_FIFO_SIZE * sizeof(struct r_rep_message));
  memset((*p_ops)->r_rep_fifo->r_rep_message, 0, R_REP_FIFO_SIZE * sizeof(struct r_rep_message));
  (*p_ops)->r_rep_fifo->rem_m_id = (uint8_t *) malloc(R_REP_FIFO_SIZE * sizeof(uint8_t));
  (*p_ops)->r_rep_fifo->pull_ptr = 1;
  //memset((*p_ops)->r_rep_fifo->rem_m_id, 0, R_REP_FIFO_SIZE * sizeof(uint8_t));
  for (i= 0; i < R_REP_FIFO_SIZE; i++) (*p_ops)->r_rep_fifo->rem_m_id[i] = MACHINE_NUM;
  (*p_ops)->r_rep_fifo->message_sizes = (uint16_t *) malloc(R_REP_FIFO_SIZE * sizeof(uint16_t));
  memset((*p_ops)->r_rep_fifo->message_sizes, 0, R_REP_FIFO_SIZE * sizeof(uint16_t));
  // W_FIFO
  (*p_ops)->w_fifo = (struct write_fifo *) malloc(sizeof(struct write_fifo));
  memset((*p_ops)->w_fifo, 0, sizeof(struct write_fifo));
  (*p_ops)->w_fifo->w_message =
    (struct w_message *) malloc(W_FIFO_SIZE * sizeof(struct w_message));
  memset((*p_ops)->w_fifo->w_message, 0, W_FIFO_SIZE * sizeof(struct w_message));
  // R_FIFO
  (*p_ops)->r_fifo = (struct read_fifo *) malloc(sizeof(struct read_fifo));
  memset((*p_ops)->r_fifo, 0, sizeof(struct read_fifo));
  (*p_ops)->r_fifo->r_message =
    (struct r_message *) malloc(R_FIFO_SIZE * sizeof(struct r_message));
  memset((*p_ops)->r_fifo->r_message, 0, R_FIFO_SIZE * sizeof(struct r_message));
  // PREP STRUCT
  (*p_ops)->prop_info = (struct prop_info *) malloc(sizeof(struct prop_info));
  memset((*p_ops)->prop_info, 0, sizeof(struct prop_info));
  (*p_ops)->prop_info->l_id = 1;
  for (i = 0; i < LOCAL_PROP_NUM; i++) {
    (*p_ops)->prop_info->entry[i].help_rmw = (struct rmw_help_entry *) malloc(sizeof(struct rmw_help_entry));
    memset((*p_ops)->prop_info->entry[i].help_rmw, 0, sizeof(struct rmw_help_entry));

    (*p_ops)->prop_info->entry[i].help_loc_entry = (struct rmw_local_entry *) malloc(sizeof(struct rmw_local_entry));
    memset((*p_ops)->prop_info->entry[i].help_loc_entry, 0, sizeof(struct rmw_local_entry));
  }

  //(*p_ops)->prep_info->prep_fifo = (struct prep_fifo *) malloc(sizeof(struct prep_fifo));
  //memset((*p_ops)->prep_info->prep_fifo, 0, sizeof(struct prep_fifo));

  //(*p_ops)->prep_info->prep_fifo->r_message =
  //  (struct r_message *) malloc(LOCAL_PREP_NUM * sizeof(struct r_message));
 // memset((*p_ops)->prep_info->prep_fifo->r_message, 0, LOCAL_PREP_NUM * sizeof(struct r_message));


//  (*p_ops)->r_payloads = (struct read_payload *) malloc(pending_reads * sizeof(struct read_payload));
  (*p_ops)->ptrs_to_r_headers = (struct r_message **) malloc(MAX_INCOMING_R * sizeof(struct r_message *));
  // PTRS to W_OPS
  (*p_ops)->ptrs_to_w_ops = (struct write **) malloc(MAX_INCOMING_W * sizeof(struct write *));
  // PTRS to R_OPS
  (*p_ops)->ptrs_to_r_ops = (struct read **) malloc(MAX_INCOMING_R * sizeof(struct read *));
  // PTRS to local ops to find the write after sending the first round of a release
  (*p_ops)->ptrs_to_local_w = (struct write **) malloc(pending_writes * sizeof(struct write *));
  (*p_ops)->overwritten_values = (uint8_t *) malloc(pending_writes * SEND_CONF_VEC_SIZE);
  memset((*p_ops)->overwritten_values, 0, pending_writes * SEND_CONF_VEC_SIZE);

  for (i = 0; i < W_FIFO_SIZE; i++) {
    (*p_ops)->w_fifo->w_message[i].m_id = (uint8_t) machine_id;
    for (j = 0; j < MAX_W_COALESCE; j++){
      //(*p_ops)->w_fifo->w_message[i].write[j].m_id = (uint8_t) machine_id;
      (*p_ops)->w_fifo->w_message[i].write[j].val_len = VALUE_SIZE >> SHIFT_BITS;
      (*p_ops)->w_fifo->w_message[i].write[j].opcode = CACHE_OP_PUT;
    }
  }
  for (i = 0; i < R_FIFO_SIZE; i++) {
    (*p_ops)->r_fifo->r_message[i].m_id = (uint8_t) machine_id;
    for (j = 0; j < MAX_R_COALESCE; j++){
      (*p_ops)->r_fifo->r_message[i].read[j].opcode = CACHE_OP_GET;
    }
  }

  for (i = 0; i < R_REP_FIFO_SIZE; i++) {
    (*p_ops)->r_rep_fifo->r_rep_message[i].m_id = (uint8_t) machine_id;
    (*p_ops)->r_rep_fifo->r_rep_message[i].opcode = (uint8_t) READ_REPLY;
  }

  for (i = 0; i < pending_reads; i++)
    (*p_ops)->r_state[i] = INVALID;
  for (i = 0; i < pending_writes; i++) {
    (*p_ops)->w_state[i] = INVALID;
    (*p_ops)->ptrs_to_local_w[i] = NULL;
  }

}

// Initialize the quorum info that contains the system configuration
void set_up_q_info(struct quorum_info **q_info)
{
  (*q_info) = (struct quorum_info *) malloc(sizeof(struct quorum_info));
  memset((*q_info), 0, sizeof(struct quorum_info));
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
                      struct ibv_recv_wr *w_recv_wr, struct ibv_sge *w_recv_sgl,
                      struct ibv_recv_wr *r_recv_wr, struct ibv_sge *r_recv_sgl,
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
  // W RECVs
  for (i = 0; i < MAX_RECV_W_WRS; i++) {
    w_recv_sgl[i].length = (uint32_t)W_RECV_SIZE;
    w_recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
    w_recv_wr[i].sg_list = &w_recv_sgl[i];
    w_recv_wr[i].num_sge = 1;
  }
  // R RECVs
  for (i = 0; i < MAX_RECV_R_WRS; i++) {
    r_recv_sgl[i].length = (uint32_t)R_RECV_SIZE;
    r_recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
    r_recv_wr[i].sg_list = &r_recv_sgl[i];
    r_recv_wr[i].num_sge = 1;
  }

}

// Set up the r_rep replies and acks send and recv wrs
void set_up_ack_n_r_rep_WRs(struct ibv_send_wr *ack_send_wr, struct ibv_sge *ack_send_sgl,
                            struct ibv_send_wr *r_rep_send_wr, struct ibv_sge *r_rep_send_sgl,
                            struct ibv_recv_wr *ack_recv_wr, struct ibv_sge *ack_recv_sgl,
                            struct ibv_recv_wr *r_rep_recv_wr, struct ibv_sge *r_rep_recv_sgl,
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

  // ACK Receives
  for (i = 0; i < MAX_RECV_ACK_WRS; i++) {
    ack_recv_sgl[i].length = ACK_RECV_SIZE;
    ack_recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
    ack_recv_wr[i].sg_list = &ack_recv_sgl[i];
    ack_recv_wr[i].num_sge = 1;
  }
  for (i = 0; i < MAX_RECV_R_REP_WRS; i++) {
    r_rep_recv_sgl[i].length = (uint32_t)R_REP_RECV_SIZE;
    r_rep_recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
    r_rep_recv_wr[i].sg_list = &r_rep_recv_sgl[i];
    r_rep_recv_wr[i].num_sge = 1;
  }
}



// construct a prep_message-- max_size must be in bytes
void init_fifo(struct fifo **fifo, uint32_t max_size, uint32_t fifos_num)
{
  (*fifo) = (struct fifo *)malloc(fifos_num * sizeof(struct fifo));
  memset((*fifo), 0, fifos_num *  sizeof(struct fifo));
  for (int i = 0; i < fifos_num; ++i) {
    (*fifo)[i].fifo = malloc(max_size);
    memset((*fifo)[i].fifo, 0, max_size);
  }
}

// Set up the receive info
void init_recv_info(struct recv_info **recv, uint32_t push_ptr, uint32_t buf_slots,
                    uint32_t slot_size, uint32_t posted_recvs, struct ibv_recv_wr *recv_wr,
                    struct ibv_qp * recv_qp, struct ibv_sge* recv_sgl, void* buf)
{
  (*recv) = malloc(sizeof(struct recv_info));
  (*recv)->push_ptr = push_ptr;
  (*recv)->buf_slots = buf_slots;
  (*recv)->slot_size = slot_size;
  (*recv)->posted_recvs = posted_recvs;
  (*recv)->recv_wr = recv_wr;
  (*recv)->recv_qp = recv_qp;
  (*recv)->recv_sgl = recv_sgl;
  (*recv)->buf = buf;
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
    ret = get_addr(local_IP, ((struct sockaddr *)&mcast_data->src_in)); // to bind
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

//
// Created by vasilis on 24/06/2020.
//

#ifndef KITE_TRACE_UTIL_H
#define KITE_TRACE_UTIL_H

#include "top.h"
#include "city.h"



typedef struct opcode_info {
  bool is_rmw;
  bool is_update;
  bool is_sc;
  bool is_rmw_acquire;
  uint32_t writes;
  uint32_t reads;
  uint32_t sc_reads;
  uint32_t sc_writes;
  uint32_t rmws;
  uint32_t rmw_acquires;
  uint32_t cas; // number of compare and swaps
  uint32_t fa; // number of Fetch and adds
} opcode_info_t;

//When manufacturing the trace
static uint8_t compute_opcode(struct opcode_info *opc_info, uint *seed)
{
  uint8_t  opcode = 0;
  uint8_t cas_opcode = USE_WEAK_CAS ? COMPARE_AND_SWAP_WEAK : COMPARE_AND_SWAP_STRONG;
  bool is_rmw = false, is_update = false, is_sc = false;
  if (ENABLE_RMWS ) {
    if (ALL_RMWS_SINGLE_KEY) is_rmw = true;
    else
      is_rmw = rand_r(seed) % 1000 < RMW_RATIO;
  }
  if (!is_rmw) {
    is_update = rand() % 1000 < WRITE_RATIO; //rand_r(seed) % 1000 < WRITE_RATIO;
    is_sc = rand() % 1000 < SC_RATIO; //rand_r(seed) % 1000 < SC_RATIO;
  }

  if (is_rmw) {
    opc_info->rmws++;
    if (TRACE_ONLY_CAS) opcode = cas_opcode;
    else if (TRACE_ONLY_FA) opcode = FETCH_AND_ADD;
    else if (TRACE_MIXED_RMWS)
      opcode = (uint8_t) ((rand_r(seed) % 1000 < TRACE_CAS_RATIO) ? cas_opcode : FETCH_AND_ADD);
    if (opcode == cas_opcode) opc_info->cas++;
    else opc_info->fa++;
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
  return opcode;
}


// Parse a trace, use this for skewed workloads as uniform trace can be manufactured easily
static  trace_t* parse_trace(char* path, int t_id){
  trace_t *trace;
  FILE * fp;
  ssize_t read;
  size_t len = 0;
  char* ptr, *word, *saveptr, *line = NULL;
  uint32_t i = 0, hottest_key_counter = 0, cmd_count = 0, word_count = 0;
  opcode_info_t *opc_info = (opcode_info_t*) calloc(1, sizeof(opcode_info_t));

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
    //bool is_read = false, is_update = false, is_sc = false;
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
static trace_t* manufacture_trace(int t_id)
{
  trace_t *trace = (trace_t *) calloc ((TRACE_SIZE + 1), sizeof(trace_t));
  struct timespec time;
  //struct random_data *recv_buf;
  clock_gettime(CLOCK_MONOTONIC, &time);
  uint seed = (uint)(time.tv_nsec + ((machine_id * WORKERS_PER_MACHINE) + t_id) + (uint64_t)trace);
  srand (seed);
  opcode_info_t *opc_info = (opcode_info_t *) calloc(1, sizeof(opcode_info_t));
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
    else {
      key_id = (uint32) rand() % KVS_NUM_KEYS;
      key_hash = CityHash128((char *) &(key_id), 4);
    }
    memcpy(trace[i].key_hash, &(key_hash.second), 8);
    trace[i].key_id = key_id;
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
static trace_t* trace_init(uint16_t t_id) {
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



#endif //KITE_TRACE_UTIL_H

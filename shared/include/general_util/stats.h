//
// Created by vasilis on 22/05/20.
//

#ifndef KITE_STATS_H
#define KITE_STATS_H


//LATENCY Measurements
#include <stdint.h>
#include <time.h>
#include "generic_opcodes.h"


#define MAX_LATENCY 400 //in us
#define LATENCY_BUCKETS 200 //latency accuracy

// Store statistics from the workers, for the stats thread to use
typedef struct thread_stats t_stats_t;


typedef struct client_stats {
  uint64_t microbench_pushes;
  uint64_t microbench_pops;
  //uint64_t ms_enqueues;
  // uint64_t ms_dequeues;
} c_stats_t;


typedef struct latency_flags {
  req_type measured_req_flag;
  uint32_t measured_sess_id;
  struct key* key_to_measure;
  struct timespec start;
} latency_info_t;


struct latency_counters{
  uint32_t* acquires;
  uint32_t* releases;
  uint32_t* hot_reads;
  uint32_t* hot_writes;
  long long total_measurements;
  uint32_t max_acq_lat;
  uint32_t max_rel_lat;
  uint32_t max_read_lat;
  uint32_t max_write_lat;
};


struct local_latency {
  int measured_local_region;
  uint8_t local_latency_start_polling;
  char* flag_to_poll;
};






#endif //KITE_STATS_H

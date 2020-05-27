//
// Created by vasilis on 22/05/20.
//

#ifndef KITE_STATS_H
#define KITE_STATS_H


//LATENCY Measurements
#define MAX_LATENCY 400 //in us
#define LATENCY_BUCKETS 200 //latency accuracy

// Store statistics from the workers, for the stats thread to use
struct thread_stats { // 2 kvs lines
  long long cache_hits_per_thread;

  uint64_t reads_per_thread;
  uint64_t writes_per_thread;
  uint64_t acquires_per_thread;
  uint64_t releases_per_thread;



  long long reads_sent;
  long long acks_sent;
  long long r_reps_sent;
  uint64_t writes_sent;
  uint64_t writes_asked_by_clients;


  long long reads_sent_mes_num;
  long long acks_sent_mes_num;
  long long r_reps_sent_mes_num;
  long long writes_sent_mes_num;


  long long received_reads;
  long long received_acks;
  long long received_r_reps;
  long long received_writes;

  long long received_r_reps_mes_num;
  long long received_acks_mes_num;
  long long received_reads_mes_num;
  long long received_writes_mes_num;


  uint64_t per_worker_acks_sent[MACHINE_NUM];
  uint64_t per_worker_acks_mes_sent[MACHINE_NUM];
  uint64_t per_worker_writes_received[MACHINE_NUM];
  uint64_t per_worker_acks_received[MACHINE_NUM];
  uint64_t per_worker_acks_mes_received[MACHINE_NUM];

  uint64_t per_worker_reads_received[MACHINE_NUM];
  uint64_t per_worker_r_reps_received[MACHINE_NUM];


  uint64_t read_to_write;
  uint64_t failed_rem_writes;
  uint64_t total_writes;
  uint64_t quorum_reads;
  uint64_t rectified_keys;
  uint64_t q_reads_with_low_epoch;

  uint64_t proposes_sent; // number of broadcast
  uint64_t accepts_sent; // number of broadcast
  uint64_t commits_sent;
  uint64_t rmws_completed;
  uint64_t cancelled_rmws;
  uint64_t all_aboard_rmws; // completed ones



  uint64_t stalled_ack;
  uint64_t stalled_r_rep;

  //long long unused[3]; // padding to avoid false sharing
};

struct client_stats {
  uint64_t microbench_pushes;
  uint64_t microbench_pops;
  //uint64_t ms_enqueues;
  // uint64_t ms_dequeues;
};


struct latency_flags {
  req_type measured_req_flag;
  uint32_t measured_sess_id;
  struct key* key_to_measure;
  struct timespec start;
};


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


extern struct latency_counters latency_count;

// TREIBER STRUCTS
struct top {
  uint32_t fourth_key_id;
  uint32_t third_key_id;
  uint32_t sec_key_id;
  uint32_t key_id;
  uint32_t pop_counter;
  uint32_t push_counter;
};
#define NODE_SIZE (VALUE_SIZE - 17)
#define NODE_SIGNATURE 144
struct node {
  uint8_t value[NODE_SIZE];
  bool pushed;
  uint16_t stack_id;
  uint16_t owner;
  uint32_t push_counter;
  uint32_t key_id;
  uint32_t next_key_id;
};


#endif //KITE_STATS_H
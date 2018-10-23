#include "util.h"
#include "inline_util.h"

void print_latency_stats(void);

void *print_stats(void* no_arg) {
  int j;
  uint16_t i, print_count = 0;
  long long all_clients_cache_hits = 0;
  double total_throughput = 0;

  uint sleep_time = SHOW_STATS_LATENCY_STYLE ? 15 : 20;
  struct thread_stats *curr_c_stats, *prev_c_stats;
  curr_c_stats = (struct thread_stats *) malloc(num_threads * sizeof(struct thread_stats));
  prev_c_stats = (struct thread_stats *) malloc(num_threads * sizeof(struct thread_stats));
  struct stats all_stats;
  sleep(4);
  memcpy(prev_c_stats, (void *) t_stats, num_threads * (sizeof(struct thread_stats)));
  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);
  while (true) {
    sleep(sleep_time);
    clock_gettime(CLOCK_REALTIME, &end);
    double seconds = (end.tv_sec - start.tv_sec) + (double) (end.tv_nsec - start.tv_nsec) / 1000000001;
    start = end;
    memcpy(curr_c_stats, (void *) t_stats, num_threads * (sizeof(struct thread_stats)));
//        memcpy(curr_w_stats, (void*) f_stats, FOLLOWERS_PER_MACHINE * (sizeof(struct follower_stats)));
    all_clients_cache_hits = 0;
    print_count++;
    if (EXIT_ON_PRINT == 1 && print_count == PRINT_NUM) {
      if (MEASURE_LATENCY && machine_id == LATENCY_MACHINE) print_latency_stats();
      printf("---------------------------------------\n");
      printf("------------RUN TERMINATED-------------\n");
      printf("---------------------------------------\n");
      exit(0);
    }
    seconds *= MILLION;//1000; // compute only MIOPS
    for (i = 0; i < num_threads; i++) {

      all_clients_cache_hits += curr_c_stats[i].cache_hits_per_thread - prev_c_stats[i].cache_hits_per_thread;
      all_stats.cache_hits_per_thread[i] =
        (curr_c_stats[i].cache_hits_per_thread - prev_c_stats[i].cache_hits_per_thread) / seconds;


//      all_stats.stalled_gid[i] = (curr_c_stats[i].stalled_gid - prev_c_stats[i].stalled_gid) / seconds;
      all_stats.stalled_ack[i] = (curr_c_stats[i].stalled_ack - prev_c_stats[i].stalled_ack) / seconds;
      all_stats.stalled_r_rep[i] =
        (curr_c_stats[i].stalled_r_rep - prev_c_stats[i].stalled_r_rep) / seconds;
      if (WRITE_RATIO < 1000 && ENABLE_LIN)
        all_stats.reads_sent[i] = (curr_c_stats[i].reads_sent - prev_c_stats[i].reads_sent) * (1000 - WRITE_RATIO) / (1000 * seconds);
      else
        all_stats.reads_sent[i] = (curr_c_stats[i].reads_sent - prev_c_stats[i].reads_sent) / (seconds);

      all_stats.rmws_completed[i] = (curr_c_stats[i].rmws_completed - prev_c_stats[i].rmws_completed) / (seconds);
      all_stats.proposes_sent[i] = (curr_c_stats[i].proposes_sent - prev_c_stats[i].proposes_sent) / (seconds);
      all_stats.accepts_sent[i] = (curr_c_stats[i].accepts_sent - prev_c_stats[i].accepts_sent) / (seconds);
      all_stats.commits_sent[i] = (curr_c_stats[i].commits_sent - prev_c_stats[i].commits_sent) / (seconds);

      all_stats.quorum_reads_per_thread[i] = (curr_c_stats[i].quorum_reads - prev_c_stats[i].quorum_reads) / (seconds);

      all_stats.writes_sent[i] = (curr_c_stats[i].writes_sent - prev_c_stats[i].writes_sent) / (seconds);
      all_stats.r_reps_sent[i] = (curr_c_stats[i].r_reps_sent - prev_c_stats[i].r_reps_sent) / seconds;
      all_stats.acks_sent[i] = (curr_c_stats[i].acks_sent - prev_c_stats[i].acks_sent) / seconds;
      all_stats.received_writes[i] = (curr_c_stats[i].received_writes - prev_c_stats[i].received_writes) / seconds;
      all_stats.received_reads[i] = (curr_c_stats[i].received_reads - prev_c_stats[i].received_reads) / seconds;
      all_stats.received_r_reps[i] = (curr_c_stats[i].received_r_reps - prev_c_stats[i].received_r_reps) / seconds;
      all_stats.received_acks[i] = (curr_c_stats[i].received_acks - prev_c_stats[i].received_acks) / seconds;

        all_stats.ack_batch_size[i] = (curr_c_stats[i].acks_sent - prev_c_stats[i].acks_sent) /
                                      (double) (curr_c_stats[i].acks_sent_mes_num -
                                                prev_c_stats[i].acks_sent_mes_num);
        all_stats.write_batch_size[i] = (curr_c_stats[i].writes_sent - prev_c_stats[i].writes_sent) /
                                      (double) (curr_c_stats[i].writes_sent_mes_num -
                                                prev_c_stats[i].writes_sent_mes_num);
      all_stats.r_batch_size[i] = (curr_c_stats[i].reads_sent - prev_c_stats[i].reads_sent) /
                                      (double) (curr_c_stats[i].reads_sent_mes_num -
                                                prev_c_stats[i].reads_sent_mes_num);
      all_stats.r_rep_batch_size[i] = (curr_c_stats[i].r_reps_sent - prev_c_stats[i].r_reps_sent) /
                                      (double) (curr_c_stats[i].r_reps_sent_mes_num -
                                                prev_c_stats[i].r_reps_sent_mes_num);
      all_stats.failed_rem_write[i] = (curr_c_stats[i].failed_rem_writes - prev_c_stats[i].failed_rem_writes) /
                                  (double) (curr_c_stats[i].received_writes -
                                            prev_c_stats[i].received_writes);

      uint64_t curr_true_read_sent = curr_c_stats[i].reads_sent - curr_c_stats[i].releases_per_thread; // TODO this does not account for out-of-epoch writes
      uint64_t prev_true_read_sent = prev_c_stats[i].reads_sent - prev_c_stats[i].releases_per_thread;
      all_stats.reads_that_become_writes[i] = (curr_c_stats[i].read_to_write - prev_c_stats[i].read_to_write) /
                                              (double)((curr_c_stats[i].reads_sent - prev_c_stats[i].reads_sent));
    }

    memcpy(prev_c_stats, curr_c_stats, num_threads * (sizeof(struct thread_stats)));
    total_throughput = (all_clients_cache_hits) / seconds;
  if (SHOW_STATS_LATENCY_STYLE)
    green_printf("%u %.2f \n", print_count, total_throughput);
  else {
    printf("---------------PRINT %d time elapsed %.2f---------------\n", print_count, seconds / MILLION);
    green_printf("SYSTEM MIOPS: %.2f \n", total_throughput);

    for (i = 0; i < num_threads; i++) {
      cyan_printf("T%d: ", i);
      yellow_printf("%.2f MIOPS,  R/S %.2f/s, W/S %.2f/s, QR/S %.2f/s, "
                    "RMWS: %.2f/s, P/S %.2f/s, A/S %.2f/s, C/S %.2f/s ",
                    all_stats.cache_hits_per_thread[i],
                    all_stats.reads_sent[i],
                    all_stats.writes_sent[i],
                    all_stats.quorum_reads_per_thread[i],
                    all_stats.rmws_completed[i],
                    all_stats.proposes_sent[i],
                    all_stats.accepts_sent[i],
                    all_stats.commits_sent[i]);
      yellow_printf(", BATCHES: Acks %.2f, Ws %.2f, Rs %.2f, R_REPs %.2f",
                    all_stats.ack_batch_size[i],
                    all_stats.write_batch_size[i],
                    all_stats.r_batch_size[i],
                    all_stats.r_rep_batch_size[i]);
      yellow_printf(", RtoW %.2f%%, FW %.2f%%",
                    100 * all_stats.reads_that_become_writes[i],
                    100 * all_stats.failed_rem_write[i]);
      printf("\n");
    }
    printf("\n");
    printf("---------------------------------------\n");
    green_printf("SYSTEM MIOPS: %.2f \n", total_throughput);
  }
    if (ENABLE_CACHE_STATS == 1)
      print_cache_stats(start, machine_id);
    // // Write to a file all_clients_throughput, per_worker_remote_throughput[], per_worker_local_throughput[]
    if (DUMP_STATS_2_FILE == 1)
      dump_stats_2_file(&all_stats);

    if (((int)total_throughput) == 0 && ENABLE_INFO_DUMP_ON_STALL) print_for_debug = true;
  }

}

//assuming microsecond latency -- NOT USED IN ABD
void print_latency_stats(void){
    uint8_t protocol = 0;
    FILE *latency_stats_fd;
    int i = 0;
    char filename[128];
    char* path = "../../results/latency";
    const char * workload[] = {
            "WRITES", //
            "READS", //
            "MIXED", //
    };
  sprintf(filename, "%s/latency_%s_w_%d%s_%s.csv", path,
            EMULATE_ABD == 1 ? "SC-ABD" : "DRF-SC",
            WRITE_RATIO / 10, "%",
            workload[MEASURE_READ_LATENCY]);

    latency_stats_fd = fopen(filename, "w");
    fprintf(latency_stats_fd, "#---------------- ACQUIRES --------------\n");
    for(i = 0; i < LATENCY_BUCKETS; ++i)
        fprintf(latency_stats_fd, "reads: %d, %d\n",i * (MAX_LATENCY / LATENCY_BUCKETS), latency_count.acquires[i]);
    fprintf(latency_stats_fd, "reads: -1, %d\n", latency_count.acquires[LATENCY_BUCKETS]); //print outliers
    fprintf(latency_stats_fd, "reads-hl: %d\n", latency_count.max_acq_lat); //print max

    fprintf(latency_stats_fd, "#---------------- RELEASES ---------------\n");
    for(i = 0; i < LATENCY_BUCKETS; ++i)
        fprintf(latency_stats_fd, "writes: %d, %d\n",i * (MAX_LATENCY / LATENCY_BUCKETS), latency_count.releases[i]);
    fprintf(latency_stats_fd, "writes: -1, %d\n",latency_count.releases[LATENCY_BUCKETS]); //print outliers
    fprintf(latency_stats_fd, "writes-hl: %d\n", latency_count.max_rel_lat); //print max

//    fprintf(latency_stats_fd, "#---------------- Hot Reads ----------------\n");
//    for(i = 0; i < LATENCY_BUCKETS; ++i)
//        fprintf(latency_stats_fd, "hr: %d, %d\n",i * (MAX_LATENCY / LATENCY_BUCKETS), latency_count.hot_reads[i]);
//    fprintf(latency_stats_fd, "hr: -1, %d\n",latency_count.hot_reads[LATENCY_BUCKETS]); //print outliers
//
//    fprintf(latency_stats_fd, "#---------------- Hot Writes ---------------\n");
//    for(i = 0; i < LATENCY_BUCKETS; ++i)
//        fprintf(latency_stats_fd, "hw: %d, %d\n",i * (MAX_LATENCY / LATENCY_BUCKETS), latency_count.hot_writes[i]);
//    fprintf(latency_stats_fd, "hw: -1, %d\n",latency_count.hot_writes[LATENCY_BUCKETS]); //print outliers

    fclose(latency_stats_fd);

    printf("Latency stats saved at %s\n", filename);
}

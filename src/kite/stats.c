#include "util.h"
#include "../../include/kite_inline_util/kite_inline_util.h"

void print_latency_stats(void);

void *print_stats(void* no_arg) {
  int j;
  uint16_t i, print_count = 0;
  uint64_t  all_wrkr_completed_reqs = 0, all_wrkr_completed_zk_writes, all_wrkr_sync_percentage;
  double total_throughput = 0;

  uint sleep_time = SHOW_STATS_LATENCY_STYLE ? 8 : 16;
  struct thread_stats curr_w_stats[WORKERS_PER_MACHINE], prev_w_stats[WORKERS_PER_MACHINE];
  struct client_stats curr_c_stats[CLIENTS_PER_MACHINE], prev_c_stats[CLIENTS_PER_MACHINE];
  struct stats all_stats;
  sleep(4);
  memcpy(prev_w_stats, (void *) t_stats, WORKERS_PER_MACHINE * (sizeof(struct thread_stats)));
  memcpy(prev_c_stats, (void *) c_stats, CLIENTS_PER_MACHINE * (sizeof(struct client_stats)));
  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);
  while (true) {
    if (ENABLE_MS_MEASUREMENTS)
      usleep(100000);
    else sleep(sleep_time);

    clock_gettime(CLOCK_REALTIME, &end);
    double seconds = (end.tv_sec - start.tv_sec) + (double) (end.tv_nsec - start.tv_nsec) / 1000000001;
    start = end;
    memcpy(curr_w_stats, (void *) t_stats, WORKERS_PER_MACHINE * (sizeof(struct thread_stats)));
    memcpy(curr_c_stats, (void *) c_stats, CLIENTS_PER_MACHINE * (sizeof(struct client_stats)));
    all_wrkr_completed_reqs = 0;
    all_wrkr_completed_zk_writes = 0, all_wrkr_sync_percentage = 0;
    print_count++;
    if (EXIT_ON_PRINT  && print_count == PRINT_NUM) {
      if (MEASURE_LATENCY && machine_id == LATENCY_MACHINE) print_latency_stats();
      printf("---------------------------------------\n");
      printf("------------RUN TERMINATED-------------\n");
      printf("---------------------------------------\n");
      exit(0);
    }
    seconds *= MILLION;//1000; // compute only MIOPS
    uint64_t total_cancelled_rmws =  0, total_rmws = 0, total_all_aboard_rmws = 0;
    for (i = 0; i < WORKERS_PER_MACHINE; i++) {

      all_wrkr_completed_reqs += curr_w_stats[i].cache_hits_per_thread - prev_w_stats[i].cache_hits_per_thread;
      all_wrkr_completed_zk_writes += (curr_w_stats[i].rmws_completed + curr_w_stats[i].releases_per_thread +
        curr_w_stats[i].writes_per_thread - (prev_w_stats[i].rmws_completed + prev_w_stats[i].releases_per_thread +
                                             prev_w_stats[i].writes_per_thread));

      all_wrkr_sync_percentage += (curr_w_stats[i].rmws_completed + curr_w_stats[i].releases_per_thread +
                                    curr_w_stats[i].acquires_per_thread  -
                                      (prev_w_stats[i].rmws_completed + prev_w_stats[i].releases_per_thread +
                                                                            prev_w_stats[i].acquires_per_thread));


      total_cancelled_rmws += curr_w_stats[i].cancelled_rmws - prev_w_stats[i].cancelled_rmws;
      total_rmws += curr_w_stats[i].rmws_completed - prev_w_stats[i].rmws_completed;
      total_all_aboard_rmws += curr_w_stats[i].all_aboard_rmws - prev_w_stats[i].all_aboard_rmws;

      all_stats.cache_hits_per_thread[i] =
        (curr_w_stats[i].cache_hits_per_thread - prev_w_stats[i].cache_hits_per_thread) / seconds;


      all_stats.stalled_ack[i] = (curr_w_stats[i].stalled_ack - prev_w_stats[i].stalled_ack) / seconds;
      all_stats.stalled_r_rep[i] =
        (curr_w_stats[i].stalled_r_rep - prev_w_stats[i].stalled_r_rep) / seconds;
      all_stats.reads_sent[i] = (curr_w_stats[i].reads_sent - prev_w_stats[i].reads_sent) / (seconds);
      all_stats.rmws_completed[i] = (curr_w_stats[i].rmws_completed - prev_w_stats[i].rmws_completed) / (seconds);
      all_stats.all_aboard_rmws[i] = (curr_w_stats[i].all_aboard_rmws - prev_w_stats[i].all_aboard_rmws) / (seconds);
      all_stats.proposes_sent[i] = (curr_w_stats[i].proposes_sent - prev_w_stats[i].proposes_sent) / (seconds);
      all_stats.accepts_sent[i] = (curr_w_stats[i].accepts_sent - prev_w_stats[i].accepts_sent) / (seconds);
      all_stats.commits_sent[i] = (curr_w_stats[i].commits_sent - prev_w_stats[i].commits_sent) / (seconds);

      all_stats.quorum_reads_per_thread[i] = (curr_w_stats[i].quorum_reads - prev_w_stats[i].quorum_reads) / (seconds);

      all_stats.writes_sent[i] = (curr_w_stats[i].writes_sent - prev_w_stats[i].writes_sent) / (seconds);
      all_stats.r_reps_sent[i] = (curr_w_stats[i].r_reps_sent - prev_w_stats[i].r_reps_sent) / seconds;
      all_stats.acks_sent[i] = (curr_w_stats[i].acks_sent - prev_w_stats[i].acks_sent) / seconds;
      all_stats.received_writes[i] = (curr_w_stats[i].received_writes - prev_w_stats[i].received_writes) / seconds;
      all_stats.received_reads[i] = (curr_w_stats[i].received_reads - prev_w_stats[i].received_reads) / seconds;
      all_stats.received_r_reps[i] = (curr_w_stats[i].received_r_reps - prev_w_stats[i].received_r_reps) / seconds;
      all_stats.received_acks[i] = (curr_w_stats[i].received_acks - prev_w_stats[i].received_acks) / seconds;

      all_stats.ack_batch_size[i] = (curr_w_stats[i].acks_sent - prev_w_stats[i].acks_sent) /
                                    (double) (curr_w_stats[i].acks_sent_mes_num -
                                              prev_w_stats[i].acks_sent_mes_num);
      all_stats.write_batch_size[i] = (curr_w_stats[i].writes_sent - prev_w_stats[i].writes_sent) /
                                    (double) (curr_w_stats[i].writes_sent_mes_num -
                                              prev_w_stats[i].writes_sent_mes_num);
      all_stats.r_batch_size[i] = (curr_w_stats[i].reads_sent - prev_w_stats[i].reads_sent) /
                                      (double) (curr_w_stats[i].reads_sent_mes_num -
                                                prev_w_stats[i].reads_sent_mes_num);
      all_stats.r_rep_batch_size[i] = (curr_w_stats[i].r_reps_sent - prev_w_stats[i].r_reps_sent) /
                                      (double) (curr_w_stats[i].r_reps_sent_mes_num -
                                                prev_w_stats[i].r_reps_sent_mes_num);
      all_stats.failed_rem_write[i] = (curr_w_stats[i].failed_rem_writes - prev_w_stats[i].failed_rem_writes) /
                                  (double) (curr_w_stats[i].received_writes -
                                            prev_w_stats[i].received_writes);

      uint64_t curr_true_read_sent = curr_w_stats[i].reads_sent - curr_w_stats[i].releases_per_thread; // TODO this does not account for out-of-epoch writes
      uint64_t prev_true_read_sent = prev_w_stats[i].reads_sent - prev_w_stats[i].releases_per_thread;
      all_stats.reads_that_become_writes[i] = (curr_w_stats[i].read_to_write - prev_w_stats[i].read_to_write) /
                                              (double)((curr_w_stats[i].reads_sent - prev_w_stats[i].reads_sent));

    }

    uint64_t all_client_microbench_pushes = 0, all_client_microbench_pops = 0;
    for (i = 0; i < CLIENTS_PER_MACHINE; i++) {
      all_client_microbench_pushes += curr_c_stats[i].microbench_pushes - prev_c_stats[i].microbench_pushes;
      all_client_microbench_pops += curr_c_stats[i].microbench_pops - prev_c_stats[i].microbench_pops;
    }

    memcpy(prev_w_stats, curr_w_stats, WORKERS_PER_MACHINE * (sizeof(struct thread_stats)));
    memcpy(prev_c_stats, curr_c_stats, CLIENTS_PER_MACHINE * (sizeof(struct client_stats)));
    total_throughput = (all_wrkr_completed_reqs) / seconds;
    double zk_write_ratio = all_wrkr_completed_zk_writes / (double) all_wrkr_completed_reqs;
    double sync_per = all_wrkr_sync_percentage / (double) all_wrkr_completed_reqs;
    double total_treiber_pushes = (all_client_microbench_pushes) / seconds;
    double total_treiber_pops = (all_client_microbench_pops) / seconds;
    double per_s_all_aboard_rmws = (total_all_aboard_rmws) / seconds;

  if (SHOW_STATS_LATENCY_STYLE)
    my_printf(green, "%u %.2f, canc: %.2f, all-aboard: %.2f t_push: %.2f, t_pop: %.2f zk_wr: %.2f, sync_per %.2f\n",
                 print_count, total_throughput,
                 (total_cancelled_rmws / (double) total_rmws),
                 per_s_all_aboard_rmws,
                 total_treiber_pushes, total_treiber_pops,
                 zk_write_ratio, sync_per);
  else {
    printf("---------------PRINT %d time elapsed %.2f---------------\n", print_count, seconds / MILLION);
    my_printf(green, "SYSTEM MIOPS: %.2f \n", total_throughput);

    for (i = 0; i < WORKERS_PER_MACHINE; i++) {
      my_printf(cyan, "T%d: ", i);
      my_printf(yellow, "%.2f MIOPS,  R/S %.2f/s, W/S %.2f/s, QR/S %.2f/s, "
                    "RMWS: %.2f/s, P/S %.2f/s, A/S %.2f/s, C/S %.2f/s ",
                    all_stats.cache_hits_per_thread[i],
                    all_stats.reads_sent[i],
                    all_stats.writes_sent[i],
                    all_stats.quorum_reads_per_thread[i],
                    all_stats.rmws_completed[i],
                    all_stats.proposes_sent[i],
                    all_stats.accepts_sent[i],
                    all_stats.commits_sent[i]);
      my_printf(yellow, ", BATCHES: Acks %.2f, Ws %.2f, Rs %.2f, R_REPs %.2f",
                    all_stats.ack_batch_size[i],
                    all_stats.write_batch_size[i],
                    all_stats.r_batch_size[i],
                    all_stats.r_rep_batch_size[i]);
      my_printf(yellow, ", RtoW %.2f%%, FW %.2f%%",
                    100 * all_stats.reads_that_become_writes[i],
                    100 * all_stats.failed_rem_write[i]);
      printf("\n");
    }
    printf("\n");
    printf("---------------------------------------\n");
    my_printf(green, "SYSTEM MIOPS: %.2f \n", total_throughput);
  }
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
            EMULATE_ABD == 1 ? "ABD" : "DRF-SC",
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

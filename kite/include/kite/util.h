#ifndef KITE_UTILS_H
#define KITE_UTILS_H

#include "kvs.h"
#include "hrd.h"
#include "main.h"





extern uint64_t seed;
void kite_static_assert_compile_parameters();
void print_parameters_in_the_start();
void kite_init_globals();


/* ---------------------------------------------------------------------------
------------------------------STATS --------------------------------------
---------------------------------------------------------------------------*/
struct stats {
  double r_batch_size[WORKERS_PER_MACHINE];
  double r_rep_batch_size[WORKERS_PER_MACHINE];
  double ack_batch_size[WORKERS_PER_MACHINE];
  double write_batch_size[WORKERS_PER_MACHINE];
  double stalled_ack[WORKERS_PER_MACHINE];
  double stalled_r_rep[WORKERS_PER_MACHINE];
	double failed_rem_write[WORKERS_PER_MACHINE];
  double quorum_reads_per_thread[WORKERS_PER_MACHINE];

	double cache_hits_per_thread[WORKERS_PER_MACHINE];

	double writes_sent[WORKERS_PER_MACHINE];
	double reads_sent[WORKERS_PER_MACHINE];
	double acks_sent[WORKERS_PER_MACHINE];
	double proposes_sent[WORKERS_PER_MACHINE];
	double rmws_completed[WORKERS_PER_MACHINE];
	double accepts_sent[WORKERS_PER_MACHINE];
	double commits_sent[WORKERS_PER_MACHINE];

	double r_reps_sent[WORKERS_PER_MACHINE];
	double received_writes[WORKERS_PER_MACHINE];
	double received_reads[WORKERS_PER_MACHINE];
	double received_acks[WORKERS_PER_MACHINE];
	double received_r_reps[WORKERS_PER_MACHINE];
  double cancelled_rmws[WORKERS_PER_MACHINE];
	double all_aboard_rmws[WORKERS_PER_MACHINE];
	double reads_that_become_writes[WORKERS_PER_MACHINE];
  //double zookeeper_writes[WORKERS_PER_MACHINE];
};
void dump_stats_2_file(struct stats* st);
void print_latency_stats(void);


/* ---------------------------------------------------------------------------
-----------------------------------------------------------------------------
---------------------------------------------------------------------------*/

//Set up the depths of all QPs
void set_up_queue_depths(int**, int**);
// Initialize the struct that holds all pending ops
p_ops_t* set_up_pending_ops(uint32_t pending_writes,
														uint32_t pending_reads,
														struct ibv_send_wr *w_send_wr,
														struct ibv_send_wr *r_send_wr,
                            uint16_t credits[][MACHINE_NUM],
														uint16_t t_id);
// Set up the memory registrations in case inlining is disabled
// Set up the memory registrations required
void set_up_mr(struct ibv_mr **mr, void *buf, uint8_t enable_inlining, uint32_t buffer_size,
               struct hrd_ctrl_blk *cb);
// Set up all Broadcast WRs
void set_up_bcast_WRs(struct ibv_send_wr*, struct ibv_sge*,
                      struct ibv_send_wr*, struct ibv_sge*, uint16_t,
                      struct hrd_ctrl_blk*, struct ibv_mr*, struct ibv_mr*);
// Set up the r_rep replies and acks send and recv wrs
void set_up_ack_n_r_rep_WRs(struct ibv_send_wr*, struct ibv_sge*,
                            struct ibv_send_wr*, struct ibv_sge*,
                            struct hrd_ctrl_blk*, struct ibv_mr*,
                            ack_mes_t*, uint16_t);




// Post receives for the coherence traffic in the init phase
void pre_post_recvs(uint32_t*, struct ibv_qp *, uint32_t lkey, void*,
                    uint32_t, uint32_t, uint16_t, uint32_t);

// Set up the credits
void set_up_credits(uint16_t credits[][MACHINE_NUM]);

void randomize_op_values(trace_op_t *ops, uint16_t t_id);


/* ---------------------------------------------------------------------------
------------------------------UTILITY --------------------------------------
---------------------------------------------------------------------------*/
void print_latency_stats(void);



#endif /* KITE_UTILS_H */

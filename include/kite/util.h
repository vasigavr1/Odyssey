#ifndef KITE_UTILS_H
#define KITE_UTILS_H

#include "kvs.h"
#include "hrd.h"
#include "main.h"





extern uint64_t seed;
void static_assert_compile_parameters();
void print_parameters_in_the_start();
void kite_init_globals();
void handle_program_inputs(int argc, char *argv[]);
void spawn_threads(struct thread_params *param_arr, uint16_t t_id, char* node_purpose,
                   cpu_set_t *pinned_hw_threads, pthread_attr_t *attr, pthread_t *thread_arr,
                   void *(*__start_routine) (void *), bool *occupied_cores);

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
int spawn_stats_thread();
void print_latency_stats(void);








//int get_addr(char*, struct sockaddr*);
//void setup_multicast(struct mcast_info*, int*);
//void resolve_addresses(struct mcast_info*);
//void set_up_qp(struct cm_qps*, int*);
//void multicast_testing(struct mcast_essentials*, int , struct hrd_ctrl_blk*);

/* ---------------------------------------------------------------------------
------------------------------INITIALIZATION --------------------------------------
---------------------------------------------------------------------------*/

struct opcode_info {
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
};

// Worker calls this function to connect with all workers
//void get_qps_from_all_other_machines(uint32_t g_id, struct hrd_ctrl_blk *cb);
// Used by all kinds of threads to publish their QPs
//void publish_qps(uint32_t qp_num, uint32_t global_id, const char* qp_name, struct hrd_ctrl_blk *cb);



trace_t* trace_init(uint16_t t_id);
//void init_multicast(struct mcast_info**, struct mcast_essentials**, int, struct hrd_ctrl_blk*, int);
// Connect with Workers and Clients
//void setup_connections_and_spawn_stats_thread(uint32_t, struct hrd_ctrl_blk *);
/* ---------------------------------------------------------------------------
-----------------------------------------------------------------------------
---------------------------------------------------------------------------*/

//Set up the depths of all QPs
void set_up_queue_depths(int**, int**);
// Initialize the rmw struct
void set_up_rmw_struct();
// Initialize the struct that holds all pending ops
p_ops_t* set_up_pending_ops(uint32_t pending_writes, uint32_t pending_reads, uint16_t t_id);
// Initialize the quorum info that contains the system configuration
void set_up_q_info(struct quorum_info **q_info);
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
                            struct ack_message*, uint16_t);


// Set up the receive info
void init_recv_info(struct hrd_ctrl_blk *cb, struct recv_info **recv,
                    uint32_t push_ptr, uint32_t buf_slots,
                    uint32_t slot_size, uint32_t, struct ibv_qp * recv_qp, int, void* buf);


// Post receives for the coherence traffic in the init phase
void pre_post_recvs(uint32_t*, struct ibv_qp *, uint32_t lkey, void*,
                    uint32_t, uint32_t, uint16_t, uint32_t);

// Set up the credits
void set_up_credits(uint16_t credits[][MACHINE_NUM]);

void randomize_op_values(trace_op_t *ops, uint16_t t_id);


/* ---------------------------------------------------------------------------
------------------------------UTILITY --------------------------------------
---------------------------------------------------------------------------*/

// pin threads starting from core 0
int pin_thread(int t_id);
// pin a thread avoid collisions with pin_thread()
int pin_threads_avoiding_collisions(int c_id);

void print_latency_stats(void);



#endif /* KITE_UTILS_H */

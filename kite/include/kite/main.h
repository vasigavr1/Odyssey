#ifndef KITE_MAIN_H
#define KITE_MAIN_H

#include <stdint.h>
#include <pthread.h>
#include <stdint-gcc.h>
#include "city.h"
#include "config.h"
#include "messages.h"
#include "buffer_sizes.h"
#include "stats.h"

// Threads
void *worker(void *arg);



// ABD EMULATION
//#define MAX_OP_BATCH (EMULATE_ABD == 1 ? (SESSIONS_PER_THREAD + 1) : (MAX_OP_BATCH_))
//#define SC_RATIO (EMULATE_ABD == 1 ? 1000 : (SC_RATIO_))
//#define ENABLE_RELEASES (EMULATE_ABD == 1 ? 1 : (ENABLE_RELEASES_))
//#define ENABLE_ACQUIRES (EMULATE_ABD == 1 ? 1 : (ENABLE_ACQUIRES_))
//#define ENABLE_RMWS (EMULATE_ABD == 1 ? 0 : (ENABLE_RMWS_))




// RMWs
#define LOCAL_PROP_NUM_ (SESSIONS_PER_THREAD)
#define LOCAL_PROP_NUM (ENABLE_RMWS == 1 ? LOCAL_PROP_NUM_ : 0)

// this allows for reads to insert reads
#define PENDING_READS MAX((MAX_OP_BATCH + 1), ((2 * SESSIONS_PER_THREAD) + 1))
#define PENDING_WRITES_ MAX((MAX_OP_BATCH + 1), ((2 * SESSIONS_PER_THREAD) + 1))
#define PENDING_WRITES MAX((PENDING_WRITES_) , ((W_CREDITS * MAX_MES_IN_WRITE) + 1))
#define W_FIFO_SIZE (PENDING_WRITES + LOCAL_PROP_NUM) // Accepts use the write fifo

// The w_fifo needs to have a safety slot that cannot be touched
// such that the fifo push ptr can never coincide with its pull ptr
// zeroing its coalesce_num, as such we take care to allow
// one fewer pending write than slots in the w_ifo
#define MAX_ALLOWED_W_SIZE (PENDING_WRITES - 1)
#define R_FIFO_SIZE (PENDING_READS + LOCAL_PROP_NUM) // Proposes use the read fifo
#define MAX_ALLOWED_R_SIZE (PENDING_READS - 1)





typedef struct kv_resp {
  uint8_t type;
//  uint8_t kv_ptr_state;
//  uint32_t log_no; // the log_number of an RMW
//  mica_op_t *kv_ptr;
//  struct ts_tuple kv_ptr_ts;
//  struct rmw_id kv_ptr_rmw_id;
} kv_resp_t;

typedef  struct r_mes_info {
  uint16_t reads_num; // all non propose messages count as reads
  uint16_t message_size;
  uint16_t max_rep_message_size;
  uint32_t backward_ptr;
} r_mes_info_t;


#define UNUSED_BYTES_IN_REL_BIT_VEC (13 - SEND_CONF_VEC_SIZE)
struct rel_bit_vec{
  uint8_t bit_vector[SEND_CONF_VEC_SIZE];
  uint8_t unused[UNUSED_BYTES_IN_REL_BIT_VEC];
  uint8_t opcode;
}__attribute__((__packed__));

typedef struct w_mes_info {
  uint8_t writes_num; // all non-accept messages: releases, writes, or commits
  uint16_t message_size;
  uint16_t max_rep_message_size;
  uint16_t per_message_sess_id[MAX_MES_IN_WRITE];
  //used when creating the failure bit vector
  // and when checking to see if the session is ready to release
  // can be used for both accepts and releases
  bool per_message_release_flag[MAX_MES_IN_WRITE];
  uint32_t backward_ptr;
  bool is_release;
  uint16_t first_release_byte_ptr;
  //this can be used as an L_id offset, as there is a
  // guarantee that there are no accepts behind it
  uint8_t first_release_w_i;

  // message contains releases, writes, or commits, and thus has a valid l_id
  bool valid_header_l_id;
  bool sent;
} w_mes_info_t;

//
struct read_fifo {
  struct r_message_template *r_message;
  uint32_t push_ptr;
  uint32_t bcast_pull_ptr;
  uint32_t bcast_size; // number of reads not messages!
  r_mes_info_t info[R_FIFO_SIZE];
};

//
typedef struct write_fifo {
  struct w_message_template *w_message;
  uint32_t push_ptr;
  uint32_t bcast_pull_ptr;
  uint32_t bcast_size; // number of writes not messages!
  w_mes_info_t info[W_FIFO_SIZE];
} write_fifo_t;

//
struct r_rep_fifo {
  struct r_rep_message_template *r_rep_message;
  uint8_t *rem_m_id;
  uint16_t *message_sizes;
  uint32_t push_ptr;
  uint32_t pull_ptr;
  uint32_t total_size; // number of r_reps not messages!
  uint32_t mes_size; // number of messages
};


//
typedef struct read_info{
  uint8_t rep_num; // replies num
  uint8_t times_seen_ts;
  bool seen_larger_ts; // used also for log numbers for rmw_acquires
	uint8_t opcode;
  struct ts_tuple ts_to_read;
  struct key key;
	// the value read locally, a greater value received or
	// in case of a 2-round write, the value to be written
  uint8_t value[VALUE_SIZE]; //
  uint8_t *value_to_read;
  bool fp_detected; //detected false positive
  uint64_t epoch_id;
  bool is_read;
  bool complete_flag; // denotes whether completion must be signaled to the client
  uint32_t r_ptr; // reverse ptr to the p_ops
  uint32_t log_no;
  uint32_t val_len;
  struct rmw_id rmw_id;

  // when a data out-of-epoch write is inserted in a write message,
  // there is a chance we may need to change its version, so we need to
  // remember where it is stored in the w_fifo -- NOT NEEDED
  //  uint32_t w_mes_ptr;
  //  uint8_t inside_w_ptr;
} r_info_t ;

struct dbg_glob_entry {
  struct ts_tuple last_committed_ts;
  uint32_t last_committed_log_no;
  struct rmw_id last_committed_rmw_id;
  struct ts_tuple proposed_ts;
  uint32_t proposed_log_no;
  struct rmw_id proposed_rmw_id;
  uint8_t last_committed_flag;
  uint64_t prop_acc_num;
};

struct rmw_help_entry{
  struct ts_tuple ts;
  uint8_t opcode;
  uint8_t value[RMW_VALUE_SIZE];
  struct rmw_id rmw_id;
  uint32_t log_no;
  // RMW that has not grabbed a global entry uses this to
  // implement back-of by polling on the global entry
  uint8_t state;
};


typedef struct rmw_rep_info {
  uint8_t tot_replies;
  uint8_t acks;
  uint8_t rmw_id_commited;
  uint8_t log_too_small;
  uint8_t already_accepted;
//  uint8_t ts_stale;
  uint8_t seen_higher_prop_acc; // Seen a higher prop or accept
  uint8_t log_too_high;
  uint8_t nacks;
  bool no_need_to_bcast; // raised when an alrea-committed reply does not trigger commit bcasts, because it refers to a later log
  bool ready_to_inspect;
  bool inspected;
  // used to know whether to help after a prop-- if you have seen a higher acc,
  // then you should not try to help a lower accept, and thus dont try at all
  uint32_t seen_higher_prop_version;

}rmw_rep_info_t;


// Entry that keep pending thread-local RMWs, the entries are accessed with session id
typedef struct rmw_local_entry {
  struct ts_tuple new_ts;
  struct key key;
  uint8_t opcode;
  uint8_t state;
  uint8_t helping_flag;
  bool fp_detected;
  bool killable; // can the RMW (if CAS) be killed early
  bool must_release;
  bool rmw_is_successful; // was the RMW (if CAS) successful
  bool all_aboard;
	bool avoid_val_in_com;
  bool base_ts_found;
  uint8_t value_to_write[VALUE_SIZE];
  uint8_t value_to_read[VALUE_SIZE];
  struct ts_tuple base_ts;
  uint8_t *compare_val; //for CAS- add value for FAA
  uint32_t rmw_val_len;
  struct rmw_id rmw_id; // this is implicitly the l_id
  struct rmw_rep_info rmw_reps;
  uint64_t epoch_id;
  uint16_t sess_id;
  uint32_t glob_sess_id;
  uint32_t index_to_req_array;
  uint32_t back_off_cntr;
  uint16_t log_too_high_cntr;
  uint32_t all_aboard_time_out;
  uint32_t log_no;
  uint32_t accepted_log_no; // this is the log no that has been accepted locally and thus when committed is guaranteed to be the correct logno
  uint64_t l_id; // the unique l_id of the entry, it typically coincides with the rmw_id except from helping cases
  mica_op_t *kv_ptr;
  struct rmw_help_entry *help_rmw;
  struct rmw_local_entry* help_loc_entry;
  uint32_t stalled_reason;
} loc_entry_t;


// Local state of pending RMWs - one entry per session
// Accessed with session id!
struct prop_info {
  loc_entry_t entry[LOCAL_PROP_NUM];
  // uint64_t l_id; // highest l_id as of yet -- Starts from 1
};

typedef struct sess_info {
  bool stalled;
  bool ready_to_release;
  uint8_t missing_num;
  uint8_t missing_ids[REM_MACH_NUM];
  uint64_t epoch_id;
  // live writes: writes that have not been acked-
  // could be ooe-writes in their read phase
  uint32_t live_writes;

  uint32_t writes_not_yet_inserted; // for debug only

} sess_info_t;

typedef struct per_write_meta {
  uint8_t w_state;
  uint8_t acks_seen;
  uint8_t acks_expected;
  uint8_t expected_ids[REM_MACH_NUM];
  bool seen_expected[REM_MACH_NUM];

  uint32_t sess_id;
} per_write_meta_t;

struct pending_out_of_epoch_writes {
  uint32_t size; //number of pending ooe writes
  uint32_t push_ptr;
  uint32_t pull_ptr;
  uint32_t r_info_ptrs[PENDING_READS]; // ptrs to the read_info struct of p_ops
};

typedef struct pending_ops {
	write_fifo_t *w_fifo;
  struct read_fifo *r_fifo;
  struct r_rep_fifo *r_rep_fifo;
  //write_t **ptrs_to_w_ops; // used for remote writes
  void **ptrs_to_mes_ops; // used for remote reads

  write_t **ptrs_to_local_w; // used for the first phase of release
  uint8_t *overwritten_values;
  struct r_message **ptrs_to_mes_headers;
  bool *coalesce_r_rep;
  r_info_t *read_info;

  struct prop_info *prop_info;
  //
  struct pending_out_of_epoch_writes *p_ooe_writes;
  sess_info_t *sess_info;
  uint64_t local_w_id;
  uint64_t local_r_id;
  uint32_t *r_session_id;
  uint32_t *w_index_to_req_array;
  uint32_t *r_index_to_req_array;

  uint8_t *r_state;
  uint32_t w_push_ptr;
  uint32_t r_push_ptr;
  uint32_t w_pull_ptr;
  uint32_t r_pull_ptr;
  uint32_t w_size; // number of writes in the pending writes (from trace, from reads etc)
  uint32_t r_size;
  // virtual read size: because acquires can result in one more read,
  // knowing the size of the read fifo is not enough to know if
  // you can add an element. Virtual read size captures this by
  // getting incremented by 2, every time an acquire is inserted
	uint32_t virt_r_size;
  uint32_t virt_w_size;  //
  per_write_meta_t *w_meta;
  uint32_t full_w_q_fifo;
  bool all_sessions_stalled;
  quorum_info_t *q_info;
} p_ops_t;

// A helper to debug sessions by remembering which write holds a given session
struct session_dbg {
	uint32_t dbg_cnt[SESSIONS_PER_THREAD];
	//uint8_t is_release[SESSIONS_PER_THREAD];
	//uint32_t request_id[SESSIONS_PER_THREAD];
};

// Registering data structure
extern atomic_uint_fast64_t committed_glob_sess_rmw_id[GLOBAL_SESSION_NUM];



typedef struct commit_info {
  bool overwrite_kv;
  bool no_value;
  uint8_t flag;
  uint32_t log_no;
  struct ts_tuple base_ts;
  rmw_id_t rmw_id;
  uint8_t *value;
  const char* message;
} commit_info_t;

typedef struct thread_stats {
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
} t_stats_t;



typedef struct trace_op {
  uint16_t session_id;
  bool attempt_all_aboard;
  struct ts_tuple ts;
  struct key key;
  uint8_t opcode;// if the opcode is 0, it has never been RMWed, if it's 1 it has
  uint8_t val_len; // this represents the maximum value len
  uint8_t value[VALUE_SIZE]; // if it's an RMW the first 4 bytes point to the entry
  uint8_t *value_to_write;
  uint8_t *value_to_read; //compare value for CAS/  addition argument for F&A
  uint32_t index_to_req_array;
  uint32_t real_val_len; // this is the value length the client is interested in
} trace_op_t;



// A bit of a bit vector: can be a send bit vector
// or a configuration bit vector and can be owned
// by a release or an acquire respectively
struct a_bit_of_vec {
  atomic_flag lock;
  uint8_t bit; // UP_STABLE, DOWN_STABLE, DOWN_TRANSIENT_OWNED
  uint16_t owner_t_id;
  uint64_t owner_local_wr_id; // id of a release/acquire that owns the bit
};

struct bit_vector {
	// state_lock and state are used only for send_bits (i.e. by releases),
	// because otherwise every release would have to check every bit
	// acquires on the other hand need only check one bit
	atomic_flag state_lock;
	uint8_t state; // denotes if any bits are raised, to accelerate the common case
	struct a_bit_of_vec bit_vec[MACHINE_NUM];
};

// This bit vector shows failures that were identified locally
// Releases must send out such a failure and clear the corresponding
// bit after the failure has been quoromized
extern struct bit_vector send_bit_vector;

//
struct multiple_owner_bit {
	atomic_flag lock;
	uint8_t bit;
	// this is not the actual sess_i of the owner, but a proxy of it
	// it counts how many sessions own a bit from a given remote thread
	uint32_t sess_num[WORKERS_PER_MACHINE];
	//A bit can be owned only by the machine it belongs to
	uint64_t owners[WORKERS_PER_MACHINE][SESSIONS_PER_THREAD];
};


// This bit vector shows failures that were identified locally or remotely
// Remote acquires will read those failures and flip the bits after the have
// increased their epoch id
extern struct multiple_owner_bit conf_bit_vec[MACHINE_NUM];


extern t_stats_t t_stats[WORKERS_PER_MACHINE];
struct mica_op;
extern atomic_uint_fast64_t epoch_id;
extern const uint16_t machine_bit_id[16];


extern FILE* rmw_verify_fp[WORKERS_PER_MACHINE];

#endif

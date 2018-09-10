#ifndef ABD_MAIN_H
#define ABD_MAIN_H

#include <stdint.h>
#include <pthread.h>
#include <stdatomic.h>
#include "city.h"
#include "hrd.h"
//-------------------------------------------
/* ----------SYSTEM------------------------ */
//-------------------------------------------
#define TOTAL_CORES 40
#define TOTAL_CORES_ (TOTAL_CORES - 1)
#define SOCKET_NUM 2
#define PHYSICAL_CORES_PER_SOCKET 10
#define LOGICAL_CORES_PER_SOCKET 20
#define PHYSICAL_CORE_DISTANCE 2 // distance between two physical cores of the same socket
#define WORKER_HYPERTHREADING 0 // shcedule two threads on the same core
#define MAX_SERVER_PORTS 1 // better not change that

// CORE CONFIGURATION
#define WORKERS_PER_MACHINE 2
#define MACHINE_NUM 2
#define WRITE_RATIO 500 //Warning write ratio is given out of a 1000, e.g 10 means 10/1000 i.e. 1%
#define SESSIONS_PER_THREAD 10
#define MEASURE_LATENCY 0
#define LATENCY_MACHINE 0
#define LATENCY_THREAD 15
#define MEASURE_READ_LATENCY 2 // 2 means mixed
#define ENABLE_LIN 0
#define R_CREDITS 2
#define MAX_R_COALESCE 40
#define W_CREDITS 2
#define MAX_W_COALESCE 15
#define ENABLE_ASSERTIONS 1
#define USE_QUORUM 1
#define CREDIT_TIMEOUT M_128
#define REL_CREDIT_TIMEOUT M_16
#define ENABLE_ADAPTIVE_INLINING 0 // This did not help
#define MIN_SS_BATCH 127// The minimum SS batch
#define ENABLE_STAT_COUNTING 1
#define MAXIMUM_INLINE_SIZE 188
#define MAX_OP_BATCH_ 200
#define SC_RATIO_ 20// this is out of 1000, e.g. 10 means 1%
#define ENABLE_RELEASES_ 1
#define ENABLE_ACQUIRES_ 1
#define ENABLE_RMWS_ 1
#define EMULATE_ABD 0 // Do not enforce releases to gather all credits or start a new message



#define REM_MACH_NUM (MACHINE_NUM - 1) // Number of remote machines

#define WORKER_NUM (WORKERS_PER_MACHINE * MACHINE_NUM)

#define CACHE_SOCKET 0// (WORKERS_PER_MACHINE < 30 ? 0 : 1 )// socket where the cache is bind
#define SESSION_BYTES 3 // session ids must fit in 3 bytes i.e.



#define ENABLE_CACHE_STATS 0
#define EXIT_ON_PRINT 1
#define PRINT_NUM 4
#define DUMP_STATS_2_FILE 0


/*-------------------------------------------------
-----------------DEBUGGING-------------------------
--------------------------------------------------*/


#define USE_A_SINGLE_KEY 0

#define DEFAULT_SL 0 //default service level



/*-------------------------------------------------
	-----------------TRACE-----------------
--------------------------------------------------*/

#define SKEW_EXPONENT_A 99 // representation divided by 100 (i.e. 99 means a = 0.99)
#define DISABLE_CACHE 0
#define LOAD_BALANCE 1 // Use a uniform access pattern


/*-------------------------------------------------
	-----------------MULTICAST-------------------------
--------------------------------------------------*/
// Multicast defines are not used in the ABD, but we keep them for possible extension
#define ENABLE_MULTICAST 0
#define MULTICAST_TESTING_ 0
#define MULTICAST_TESTING (ENABLE_MULTICAST == 1 ? MULTICAST_TESTING_ : 0)
#define MCAST_QPS MACHINE_NUM


#define MCAST_QP_NUM 2
#define PREP_MCAST_QP 0
#define COM_MCAST_QP 1 //
#define MCAST_GROUPS_NUM 2

// ------COMMON-------------------
#define MAX_BCAST_BATCH (1) //how many broadcasts can fit in a batch
#define MESSAGES_IN_BCAST (REM_MACH_NUM)
#define MESSAGES_IN_BCAST_BATCH (MAX_BCAST_BATCH * MESSAGES_IN_BCAST) //must be smaller than the q_depth

/* --------------------------------------------------------------------------------
 * -----------------------------ABD------------------------------------------------
 * --------------------------------------------------------------------------------
 * --------------------------------------------------------------------------------*/




// ABD EMULATION
#define MAX_OP_BATCH (EMULATE_ABD == 1 ? (SESSIONS_PER_THREAD + 1) : (MAX_OP_BATCH_))
#define SC_RATIO (EMULATE_ABD == 1 ? 1000 : (SC_RATIO_))
#define ENABLE_RELEASES (EMULATE_ABD == 1 ? 1 : (ENABLE_RELEASES_))
#define ENABLE_ACQUIRES (EMULATE_ABD == 1 ? 1 : (ENABLE_ACQUIRES_))
#define ENABLE_RMWS (EMULATE_ABD == 1 ? 0 : (ENABLE_RMWS_))

#define ENABLE_NO_CONFLICT_RMW 1
#define SHOW_STATS_LATENCY_STYLE 1



#define QP_NUM 4
#define R_QP_ID 0
#define R_REP_QP_ID 1
#define W_QP_ID 2
#define ACK_QP_ID 3
#define FC_QP_ID 4 // NOT USED!



#define QUORUM_NUM ((MACHINE_NUM / 2) + 1)
#define REMOTE_QUORUM (USE_QUORUM == 1 ? (QUORUM_NUM - 1): REM_MACH_NUM)
#define EPOCH_BYTES 2
#define TS_TUPLE_SIZE (5) // version and m_id consist the Timestamp tuple
// in the first round of a release the first bytes of the value get overwritten
// before ovewritting them they get stored in astruct with size SEND_CONF_VEC_SIZE
#define SEND_CONF_VEC_SIZE 2 //(CEILING(MACHINE_NUM, 8))


// READS
#define R_SIZE (TRUE_KEY_SIZE + TS_TUPLE_SIZE + 1)// key+ version + m_id + opcode
#define R_MES_HEADER (10) // local id + coalesce num + m_id
#define R_MES_SIZE (R_MES_HEADER + (R_SIZE * MAX_R_COALESCE))
#define R_RECV_SIZE (GRH_SIZE + R_MES_SIZE)
#define MAX_RECV_R_WRS (R_CREDITS * REM_MACH_NUM)
#define MAX_INCOMING_R (MAX_RECV_R_WRS * MAX_R_COALESCE)
#define MAX_R_WRS (MESSAGES_IN_BCAST_BATCH)
#define R_SEND_SIZE (R_MES_SIZE)
#define R_ENABLE_INLINING ((R_SEND_SIZE > MAXIMUM_INLINE_SIZE) ?  0 : 1)

// READ REPLIES
#define MAX_R_REP_COALESCE MAX_R_COALESCE
#define R_REP_MES_HEADER (8 + 3) //l_id, coalesce_num, m_id, opcode // and credits
#define R_REP_SIZE (TS_TUPLE_SIZE + VALUE_SIZE + 1)
#define R_REP_ONLY_TS_SIZE (TS_TUPLE_SIZE + 1)
#define R_REP_SMALL_SIZE (1)
#define R_REP_SEND_SIZE (R_REP_MES_HEADER + (MAX_R_REP_COALESCE * R_REP_SIZE))
#define R_REP_RECV_SIZE (GRH_SIZE + R_REP_SEND_SIZE)
#define MAX_RECV_R_REP_WRS (REM_MACH_NUM * R_CREDITS)
#define MAX_R_REP_WRS (R_CREDITS * REM_MACH_NUM * (CEILING(MAX_R_COALESCE, MAX_R_REP_COALESCE)))

#define R_REP_ENABLE_INLINING ((R_REP_SEND_SIZE > MAXIMUM_INLINE_SIZE) ?  0 : 1)
#define R_REP_FIFO_SIZE (MAX_INCOMING_R)
//#define READ_INFO_SIZE (3 + TS_TUPLE_SIZE + TRUE_KEY_SIZE + VALUE_SIZE) // not correct

// Writes
#define MAX_RECV_W_WRS (W_CREDITS * REM_MACH_NUM)
#define MAX_W_WRS (MESSAGES_IN_BCAST_BATCH)
#define MAX_INCOMING_W (MAX_RECV_W_WRS * MAX_W_COALESCE)

#define WRITE_HEADER (TRUE_KEY_SIZE + TS_TUPLE_SIZE + 2) // opcode + val_len
#define W_SIZE (VALUE_SIZE + WRITE_HEADER)
#define W_MES_HEADER (8 + 1 +1) // local id + m_id+ w_num
#define W_MES_SIZE (W_MES_HEADER + (W_SIZE * MAX_W_COALESCE))
#define W_RECV_SIZE (GRH_SIZE + W_MES_SIZE)
#define W_ENABLE_INLINING ((W_MES_SIZE > MAXIMUM_INLINE_SIZE) ?  0 : 1)

// Acks
#define MAX_RECV_ACK_WRS (REM_MACH_NUM * W_CREDITS)
#define MAX_ACK_WRS (MACHINE_NUM)
#define ACK_SIZE 14
#define ACK_RECV_SIZE (GRH_SIZE + (ACK_SIZE))

// Prepares
#define LOCAL_PREP_NUM (SESSIONS_PER_THREAD)

// RMWs
#define BYTES_OVERRIDEN_IN_KVS_VALUE 4
#define RMW_VALUE_SIZE 8 // rmw cannot be more than 8 bytes
#define RMW_ENTRIES_PER_MACHINE (253 / MACHINE_NUM)
#define RMW_ENTRIES_NUM (RMW_ENTRIES_PER_MACHINE * MACHINE_NUM)

#define KEY_HAS_NEVER_BEEN_RMWED 0
#define KEY_HAS_BEEN_RMWED 1

#define RMW_WAIT_COUNTER M_256

// RMW entry states && prepare entries
#define INVALID_RMW 0
#define PROPOSED 1 // has seen a propose || has been proposed
#define ACCEPTED 2 // has acked an acccept || has fired accepts
#define SAME_KEY_RMW 3  // there is already an entry for the key

#define VC_NUM 2
#define R_VC 0
#define W_VC 1



// BUFFER SIZES
#define R_BUF_SLOTS (REM_MACH_NUM * R_CREDITS)
#define R_BUF_SIZE (R_RECV_SIZE * R_BUF_SLOTS)

#define R_REP_BUF_SLOTS (REM_MACH_NUM * R_CREDITS)
#define R_REP_BUF_SIZE (R_REP_RECV_SIZE * R_REP_BUF_SLOTS)

#define W_BUF_SLOTS (REM_MACH_NUM * W_CREDITS)
#define W_BUF_SIZE (W_RECV_SIZE * W_BUF_SLOTS)

#define ACK_BUF_SLOTS (REM_MACH_NUM * W_CREDITS)
#define ACK_BUF_SIZE (ACK_RECV_SIZE * ACK_BUF_SLOTS)

#define TOTAL_BUF_SIZE (R_BUF_SIZE + R_REP_BUF_SIZE + W_BUF_SIZE + ACK_BUF_SIZE)
#define TOTAL_BUF_SLOTS (R_BUF_SLOTS + R_REP_BUF_SLOTS + W_BUF_SLOTS + ACK_BUF_SLOTS)

#define PENDING_READS (MAX_OP_BATCH + 1)
#define EXTRA_WRITE_SLOTS 50 // to accommodate reads that become writes
#define PENDING_WRITES (MAX_OP_BATCH + 1)
#define W_FIFO_SIZE (PENDING_WRITES)
#define R_FIFO_SIZE (PENDING_READS)

#define W_BCAST_SS_BATCH MAX((MIN_SS_BATCH / (REM_MACH_NUM)), (MESSAGES_IN_BCAST_BATCH + 1))
#define R_BCAST_SS_BATCH MAX((MIN_SS_BATCH / (REM_MACH_NUM)), (MESSAGES_IN_BCAST_BATCH + 2))
#define R_REP_SS_BATCH MAX(MIN_SS_BATCH, (MAX_R_REP_WRS + 1))
#define ACK_SS_BATCH MAX(MIN_SS_BATCH, (MAX_ACK_WRS + 2))


//  Receive
#define RECV_ACK_Q_DEPTH (MAX_RECV_ACK_WRS + 3)
#define RECV_W_Q_DEPTH  (MAX_RECV_W_WRS + 3) //
#define RECV_R_Q_DEPTH (MAX_RECV_R_WRS + 3) //
#define RECV_R_REP_Q_DEPTH (MAX_RECV_R_REP_WRS + 3)

// Send
#define SEND_ACK_Q_DEPTH ((2 * ACK_SS_BATCH) + 3)
#define SEND_W_Q_DEPTH ((2 * W_BCAST_SS_BATCH * REM_MACH_NUM) + 3) // Do not question or doubt the +3!!
#define SEND_R_Q_DEPTH ((2 * R_BCAST_SS_BATCH * REM_MACH_NUM) + 3) //
#define SEND_R_REP_Q_DEPTH ((2 * R_REP_SS_BATCH) + 3)


// DEBUG
#define DEBUG_WRITES 0
#define DEBUG_ACKS 0
#define DEBUG_READS 0
#define DEBUG_TS 0
#define CHECK_DBG_COUNTERS 0
#define VERBOSE_DBG_COUNTER 0
#define DEBUG_SS_BATCH 0
#define R_TO_W_DEBUG 0
#define DEBUG_QUORUM 1
#define DEBUG_RMW 1
#define PUT_A_MACHINE_TO_SLEEP 0
#define MACHINE_THAT_SLEEPS 1
#define ENABLE_INFO_DUMP_ON_STALL 0

#define POLL_CQ_R 0
#define POLL_CQ_W 1
#define POLL_CQ_R_REP 2
#define POLL_CQ_ACK 3

//LATENCY Measurment
#define MAX_LATENCY 400 //in us
#define LATENCY_BUCKETS 200 //latency accuracy

/* SHM key for the 1st request region created by master. ++ for other RRs.*/
#define MASTER_SHM_KEY 24


//Defines for parsing the trace
#define _200_K 200000
#define MAX_TRACE_SIZE _200_K
#define FEED_FROM_TRACE 0
#define TRACE_SIZE K_128
#define NOP 0
#define HOT_WRITE 1
#define HOT_READ 2
#define REMOTE_WRITE 3
#define REMOTE_READ 4
#define LOCAL_WRITE 5
#define LOCAL_READ 6



#define IS_READ(X)  ((X) == HOT_READ || (X) == LOCAL_READ || (X) == REMOTE_READ  ? 1 : 0)
//#define IS_WRITE(X)  ((X) == HOT_WRITE || (X) == LOCAL_WRITE || (X) == REMOTE_WRITE  ? 1 : 0)



struct trace_command {
	uint8_t  opcode;
	uint8_t  home_machine_id;
//	uint8_t  home_worker_id;
	uint32_t key_id;
	uint128 key_hash;
};

struct trace_command_uni {
  uint8_t  opcode;
  uint8_t key_hash[8];
};

/* ah pointer and qpn are accessed together in the critical path
   so we are putting them in the same cache line */
struct remote_qp {
	struct ibv_ah *ah;
	int qpn;
	// no padding needed- false sharing is not an issue, only fragmentation
};

/*
 *  SENT means the message has been sent
 *  READY means all acks have been gathered // OR a commit has been received
 * */

#define INVALID 0
#define VALID 1
#define SENT 2
#define READY 3
#define SENT_PUT 4
#define SENT_RELEASE 5 // Release or second round of acquire!!
#define SENT_BIT_VECTOR 6
#define READY_PUT 7
#define READY_RELEASE 8 // Release or second round of acquire!!
#define READY_BIT_VECTOR 9

// Possible write sources
#define FROM_TRACE 0
#define FROM_READ 1
#define FROM_WRITE 2 //the second round of a release
#define FOR_ACCEPT 3
#define LIN_WRITE 5
// Possible flag values when inserting a read reply
#define READ 0
#define LIN_PUT 1
#define RMW_SMALLER_TS 2
#define RMW_ACK_PROPOSE 3 // Send an 1-byte reply
#define RMW_ALREADY_ACCEPTED 4 // Send byte plus value
#define RMW_NACK_PROPOSE 5 // Send a TS, because you have already acked a higher Propose




//enum op_state {INVALID_, VALID_, SENT_, READY_, SEND_COMMITTS};
enum ts_compare{SMALLER, EQUAL, GREATER, ERROR};

struct quorum_info {
	uint8_t missing_num;
	uint8_t missing_ids[REM_MACH_NUM];
	uint8_t active_num;
	uint8_t active_ids[REM_MACH_NUM];
  bool send_vector[REM_MACH_NUM];
	// These are not a machine_ids, they ranges= from 0 to REM_MACH_NUM -1
	// to facilitate usage with the ib_send_wrs
	uint8_t first_active_rm_id;
	uint8_t last_active_rm_id;
};
 struct rmw_id {
   uint16_t g_id;
   uint64_t id;
 };

struct ts_tuple {
  uint8_t m_id;
  uint8_t version[4];
};
// The format of an ack message
struct ack_message {
	uint8_t local_id[8]; // the first local id that is being acked
  uint8_t m_id;
  uint8_t opcode;
  uint16_t credits;
  uint16_t ack_num;
};


struct ack_message_ud_req {
	uint8_t grh[GRH_SIZE];
  struct ack_message ack;

 };


struct write {
  //uint8_t unused[2];
  //uint8_t w_num; // the first write holds the coalesce number for the entire message
  uint8_t m_id;
  uint8_t version[4];
  uint8_t key[TRUE_KEY_SIZE];	/* 8B */
  uint8_t opcode;
  uint8_t val_len;
  uint8_t value[VALUE_SIZE];
};

struct w_message {
  uint8_t l_id[8];
  uint8_t m_id;
  uint8_t w_num;
  struct write write[MAX_W_COALESCE];
};


struct w_message_ud_req {
  uint8_t unused[GRH_SIZE];
  struct w_message w_mes;
};

//
struct read {
  struct ts_tuple ts;
  uint8_t key[TRUE_KEY_SIZE];	/* 8B */
  uint8_t opcode;
};

//
struct r_message {
  uint8_t l_id[8];
  uint8_t coalesce_num;
  uint8_t m_id;
  struct read read[MAX_R_COALESCE];
};

//
struct r_message_ud_req {
  uint8_t unused[GRH_SIZE];
  struct r_message r_mes;
};

//
struct read_fifo {
  struct r_message *r_message;
  uint32_t push_ptr;
  //uint32_t pull_ptr;
  uint32_t bcast_pull_ptr;
  uint32_t bcast_size; // number of reads not messages!
  //uint32_t size;
  uint32_t backward_ptrs[R_FIFO_SIZE];
};

//
struct write_fifo {
  struct w_message *w_message;
  uint32_t push_ptr;
  uint32_t pull_ptr;
  uint32_t bcast_pull_ptr;
  uint32_t bcast_size; // number of prepares not messages!
  uint32_t size;
  uint32_t backward_ptrs[W_FIFO_SIZE];
};

//
struct prep_fifo {
  struct r_message *r_message;
  uint32_t push_ptr;
  uint32_t bcast_pull_ptr;
  uint32_t bcast_size; // number of preps not messages!
  uint32_t size;
  uint32_t backward_ptrs[LOCAL_PREP_NUM];
};


// Sent when the timestamps are equal or smaller
struct r_rep_small {
  uint8_t opcode;
};


// Sent when you have a bigger ts_tuple
struct r_rep_big {
  uint8_t opcode;
  //uint8_t ts[TS_TUPLE_SIZE];
  struct ts_tuple ts;
  uint8_t value[VALUE_SIZE];

};

//
struct r_rep_message {
  uint8_t l_id[8];
  uint8_t coalesce_num;
  uint8_t m_id;
//  uint8_t credits;
  uint8_t opcode;
  struct r_rep_big r_rep[MAX_R_REP_COALESCE];
};


struct r_rep_message_ud_req {
  uint8_t unused[GRH_SIZE];
  struct r_rep_message r_rep_mes;
};


struct r_rep_fifo {
  struct r_rep_message *r_rep_message;
  uint8_t *rem_m_id;
  uint16_t *message_sizes;
  uint32_t push_ptr;
  //uint32_t inner_push_ptr; // this points to bytes, rather than "read replies"
  uint32_t pull_ptr;
  //uint32_t bcast_pull_ptr;
  uint32_t total_size; // number of r_reps not messages!
  uint32_t mes_size; // number of messages

};


//
struct read_info {
  uint8_t rep_num;
  uint8_t times_seen_ts;
  bool seen_larger_ts;
	uint8_t opcode;
  struct ts_tuple ts_to_read;
  uint8_t key[TRUE_KEY_SIZE];
  uint8_t value[VALUE_SIZE];
  bool fp_detected; //detected false positive
  uint16_t epoch_id;
};

// the first time a key gets RMWed, it grabs an RMW entry
// that lasts for life, the entry is protected by the KVS lock
struct rmw_entry {
  uint8_t opcode; // what kind of RMW
  struct key key;
  uint8_t state;
  struct rmw_id rmw_id;
  struct ts_tuple old_ts;
  struct ts_tuple new_ts;
  uint8_t value[VALUE_SIZE];
  //atomic_flag lock;
};

// Entry that keep pending thread-local RMWs, the entries are accessed with session id
struct prop_entry {
  struct ts_tuple old_ts;
  struct key key;
  uint8_t opcode;
  uint8_t value[RMW_VALUE_SIZE];
  uint8_t state;
  struct rmw_id rmw_id;
  uint32_t debug_cntr;
  uint64_t l_id;
  uint32_t ptr_to_rmw;
  uint16_t epoch_id;
  uint8_t acks;
};

// Local state of pending RMWs - one entry per session
struct prop_info {
  struct prop_entry entry[LOCAL_PREP_NUM];
  uint64_t l_id; // highest l_id as of yet

};

struct pending_ops {
  struct write_fifo *w_fifo;
  struct read_fifo *r_fifo;
  struct write **ptrs_to_w_ops; // used for remote writes
  struct read **ptrs_to_r_ops; // used for remote reads

  struct write **ptrs_to_local_w; // used for the first phase of release
  uint8_t *overwritten_values;
  struct r_message **ptrs_to_r_headers;
//  struct read_payload *r_payloads;
  struct read_info *read_info;
  struct r_rep_fifo *r_rep_fifo;
  struct prop_info *prop_info;
  uint64_t local_w_id;
  uint64_t local_r_id;
  uint32_t *r_session_id;
  uint32_t *w_session_id;

  uint8_t *w_state;
  uint8_t *r_state;
  uint32_t w_push_ptr;
  uint32_t r_push_ptr;
  uint32_t w_pull_ptr;
  uint32_t r_pull_ptr;
  uint32_t w_size;
  uint32_t r_size;
  uint32_t prop_size; // TODO add this if needed
  uint8_t *acks_seen;
  bool *session_has_pending_op;
  bool all_sessions_stalled;
};

// Global struct that holds the RMW information
// Cannot be a FIFO because the incoming commit messages must be processed, such that
// acks to the commits mean that the RMW has happened
struct rmw_info {
  struct rmw_entry entry[RMW_ENTRIES_NUM];
//  uint16_t empty_fifo[RMW_ENTRIES_NUM];
//  uint16_t ef_push_ptr; // empty fifo push ptr
//  uint16_t ef_pull_ptr;
//  uint16_t ef_size; // how many empty slots are there

//  atomic_flag ef_lock;
//  atomic_uint_fast16_t local_rmw_num;
//  atomic_flag local_rmw_lock;

//  uint32_t size;
//  uint8_t lock;
//  uint64_t version; // allow for lock free reads of the struct
};

//typedef _Atomic struct rmw_info atomic_rmw_info;
extern struct rmw_info rmw;

struct recv_info {
	uint32_t push_ptr;
	uint32_t buf_slots;
	uint32_t slot_size;
	uint32_t posted_recvs;
	struct ibv_recv_wr *recv_wr;
	struct ibv_qp * recv_qp;
	struct ibv_sge* recv_sgl;
	void* buf;

};

struct fifo {
  void *fifo;
  uint32_t push_ptr;
  uint32_t pull_ptr;
  uint32_t size;

};


struct thread_stats { // 2 cache lines
	long long cache_hits_per_thread;
  uint64_t reads_per_thread;
  uint64_t writes_per_thread;
  uint64_t acquires_per_thread;
  uint64_t releases_per_thread;


	long long reads_sent;
	long long acks_sent;
	long long r_reps_sent;
  long long writes_sent;


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


  uint64_t stalled_ack;
  uint64_t stalled_r_rep;

	//long long unused[3]; // padding to avoid false sharing
};

#define STABLE_STATE 0
#define TRANSIENT_STATE 1
#define UP_STABLE 0
#define UP_TRANSIENT 1
#define DOWN_STABLE 2
#define DOWN_TRANSIENT 3


extern struct remote_qp remote_qp[MACHINE_NUM][WORKERS_PER_MACHINE][QP_NUM];
extern atomic_char qps_are_set_up;
extern struct thread_stats t_stats[WORKERS_PER_MACHINE];
struct mica_op;
extern atomic_uint_fast16_t epoch_id;
extern atomic_uint_fast8_t config_vector[MACHINE_NUM];
extern atomic_uint_fast8_t config_vect_state[MACHINE_NUM];

// The send vector does not contain all failed nodes,
// but only those locally detected
extern atomic_uint_fast8_t send_config_bit_vector[MACHINE_NUM];
extern atomic_uint_fast8_t send_config_bit_vec_state;
extern const uint16_t machine_bit_id[16];

extern atomic_bool print_for_debug;
extern atomic_uint_fast32_t next_rmw_entry_available;



struct thread_params {
	int id;
};

typedef enum {
  NO_REQ,
  RELEASE,
  ACQUIRE,
  HOT_WRITE_REQ_BEFORE_SAVING_KEY,
  HOT_WRITE_REQ,
  HOT_READ_REQ,
  LOCAL_REQ,
  REMOTE_REQ
} req_type;


struct latency_flags {
  req_type measured_req_flag;
  uint32_t measured_sess_id;
  struct cache_key* key_to_measure;
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

void *follower(void *arg);
void *leader(void *arg);
void *worker(void *arg);
void *print_stats(void*);

#endif

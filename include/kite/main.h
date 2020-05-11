#ifndef ABD_MAIN_H
#define ABD_MAIN_H

#include <stdint.h>
#include <pthread.h>
#include <stdint-gcc.h>
#include "city.h"
#include "common_func.h"




/*-------------------------------------------------
	-----------------TRACE-----------------
--------------------------------------------------*/
#define SKEW_EXPONENT_A 90 // representation divided by 100 (i.e. 99 means a = 0.99)

/*-------------------------------------------------
	-----------------CLIENT---------------------------
--------------------------------------------------*/
#define CLIENT_USE_TRACE 0
#define CLIENT_UI 1
#define BLOCKING_TEST_CASE 2
#define ASYNC_TEST_CASE 3
#define TREIBER_BLOCKING 4
#define TREIBER_ASYNC 5 // Treiber Stack
#define MSQ_ASYNC 6 // Michael & Scott Queue
#define HML_ASYNC 7 // Harris & Michael List
#define PRODUCER_CONSUMER 16

#define CLIENT_MODE HML_ASYNC

#define TREIBER_WRITES_NUM 1
#define TREIBER_NO_CONFLICTS 0

#define MS_WRITES_NUM 4
#define MS_NO_CONFLICT 0
#define CLIENT_LOGS 0

#define HM_NO_CONFLICT 1
#define HM_WRITES_NUM 4

#define PC_WRITES_NUM 5
#define PC_IDEAL 0

#define PER_SESSION_REQ_NUM (MS_WRITES_NUM + 4) //(HM_WRITES_NUM + 15) //((2 * PC_WRITES_NUM) + 5)
#define CLIENT_DEBUG 0

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

/*-------------------------------------------------
	-----------------Paxos-------------------------
--------------------------------------------------*/
#define ALL_ABOARD_TS 2
#define PAXOS_TS 3
//ABD

// ABD EMULATION
#define MAX_OP_BATCH (EMULATE_ABD == 1 ? (SESSIONS_PER_THREAD + 1) : (MAX_OP_BATCH_))
#define SC_RATIO (EMULATE_ABD == 1 ? 1000 : (SC_RATIO_))
#define ENABLE_RELEASES (EMULATE_ABD == 1 ? 1 : (ENABLE_RELEASES_))
#define ENABLE_ACQUIRES (EMULATE_ABD == 1 ? 1 : (ENABLE_ACQUIRES_))
#define ENABLE_RMWS (EMULATE_ABD == 1 ? 0 : (ENABLE_RMWS_))
#define ENABLE_RMW_ACQUIRES (ENABLE_RMWS == 1 ? (ENABLE_RMW_ACQUIRES_) : 0)

// RMW TRACE
#define ENABLE_NO_CONFLICT_RMW 0 // each thread rmws a different key
#define ENABLE_ALL_CONFLICT_RMW 0 // all threads do rmws to one key (0)
#define ENABLE_SINGLE_KEY_RMW 0
#define ALL_RMWS_SINGLE_KEY 0 //  all threads do only rmws to one key (0)
#define RMW_ONE_KEY_PER_THREAD 0 // thread t_id rmws key t_id
//#define RMW_ONE_KEY_PER_SESSION 1 // session id rmws key t_id
#define SHOW_STATS_LATENCY_STYLE 1
#define NUM_OF_RMW_KEYS 50000
#define TRACE_ONLY_CAS 0
#define TRACE_ONLY_FA 1
#define TRACE_MIXED_RMWS 0
#define TRACE_CAS_RATIO 500 // out of a 1000
#define ENABLE_CAS_CANCELLING 1
#define RMW_CAS_CANCEL_RATIO 400 // out of 1000
#define USE_WEAK_CAS 1
#define MAX_TR_NODE_KEY ((GLOBAL_SESSION_NUM * TREIBER_WRITES_NUM) + NUM_OF_RMW_KEYS)
// QUEUE PAIRS
#define QP_NUM 4
#define R_QP_ID 0
#define R_REP_QP_ID 1
#define W_QP_ID 2
#define ACK_QP_ID 3

// QUORUM
#define QUORUM_NUM ((MACHINE_NUM / 2) + 1)
#define REMOTE_QUORUM (USE_QUORUM == 1 ? (QUORUM_NUM - 1 ): REM_MACH_NUM)

// IMPORTANT SIZES
#define EPOCH_BYTES 2
#define TS_TUPLE_SIZE (5) // version and m_id consist the Timestamp tuple
#define LOG_NO_SIZE 4
#define RMW_ID_SIZE 10
#define RMW_VALUE_SIZE 24 //
#define SESSION_BYTES 2 // session ids must fit in 2 bytes i.e.
// in the first round of a release the first bytes of the value get overwritten
// before ovewritting them they get stored in astruct with size SEND_CONF_VEC_SIZE
#define SEND_CONF_VEC_SIZE 2 //(CEILING(MACHINE_NUM, 8))



// ------COMMON-------------------
#define MAX_BCAST_BATCH (1) //how many broadcasts can fit in a batch
#define MESSAGES_IN_BCAST (REM_MACH_NUM)
#define MESSAGES_IN_BCAST_BATCH (MAX_BCAST_BATCH * MESSAGES_IN_BCAST) //must be smaller than the q_depth
// post some extra receives to avoid spurious out_of_buffer errors
#define RECV_WR_SAFETY_MARGIN 2

// WRITES: Releases, writes, accepts and commits -- all sizes in BYTES
#define W_MES_HEADER (11) // local id + m_id+ w_num + opcode
#define EFFECTIVE_MAX_W_SIZE (MAX_WRITE_SIZE - W_MES_HEADER) // all messages have the same header

// Writes-Releases
#define WRITE_HEADER (TRUE_KEY_SIZE + TS_TUPLE_SIZE + 2) // opcode + val_len
#define W_SIZE (VALUE_SIZE + WRITE_HEADER)
#define W_COALESCE (EFFECTIVE_MAX_W_SIZE / W_SIZE)
#define W_MES_SIZE (W_MES_HEADER + (W_SIZE * W_COALESCE))

// ACCEPTS -- ACCEPT coalescing is derived from max write size. ACC reps are derived from accept coalescing
#define ACCEPT_HEADER 47 //original l_id 8 key 8 rmw-id 10, last-committed rmw_id 10, ts 5 log_no 4 opcode 1, val_len 1
#define ACCEPT_SIZE (ACCEPT_HEADER + RMW_VALUE_SIZE)
#define ACC_COALESCE (EFFECTIVE_MAX_W_SIZE / ACCEPT_SIZE)
#define ACC_MES_SIZE (W_MES_HEADER + (ACCEPT_SIZE * ACC_COALESCE))

// COMMITS
#define COMMIT_HEADER 29 //
#define COMMIT_SIZE (COMMIT_HEADER + RMW_VALUE_SIZE)
#define COM_COALESCE (EFFECTIVE_MAX_W_SIZE / COMMIT_SIZE)
#define COM_MES_SIZE (W_MES_HEADER + (COMMIT_SIZE * COM_COALESCE))

// COMBINED FROM Writes, Releases, Accepts, Commits
#define MAX_WRITE_COALESCE MAX_OF_3(W_COALESCE, COM_COALESCE, ACC_COALESCE)
#define MAX_W_MES_SIZE MAX_OF_3(W_MES_SIZE, COM_MES_SIZE, ACC_MES_SIZE)
#define MAX_MES_IN_WRITE (MAX_WRITE_COALESCE)

#define W_SEND_SIZE MAX_W_MES_SIZE
#define W_SEND_SIDE_PADDING FIND_PADDING(W_SEND_SIZE)
#define ALIGNED_W_SEND_SIDE (W_SEND_SIZE + W_SEND_SIDE_PADDING)

#define W_RECV_SIZE (GRH_SIZE + ALIGNED_W_SEND_SIDE)
#define W_ENABLE_INLINING ((MAX_W_MES_SIZE > MAXIMUM_INLINE_SIZE) ?  0 : 1)
#define MAX_RECV_W_WRS ((W_CREDITS * REM_MACH_NUM) + RECV_WR_SAFETY_MARGIN)
#define MAX_W_WRS (MESSAGES_IN_BCAST_BATCH)
#define MAX_INCOMING_W (MAX_RECV_W_WRS * MAX_WRITE_COALESCE)


// READS : Read,Acquires, RMW-Acquires, Proposes
#define R_MES_HEADER (10) // local id + coalesce num + m_id
#define EFFECTIVE_MAX_R_SIZE (MAX_READ_SIZE - R_MES_HEADER)

// reads/acquires/rmw-acquire
#define R_SIZE (TRUE_KEY_SIZE + TS_TUPLE_SIZE + 1)// key+ version + m_id + opcode
#define R_COALESCE (EFFECTIVE_MAX_R_SIZE / R_SIZE)
#define R_MES_SIZE (R_MES_HEADER + (R_SIZE * R_COALESCE))
// proposes
#define PROP_SIZE 36  // l_id 8, RMW_id- 10, ts 5, key 8, log_number 4, opcode 1
#define PROP_COALESCE (EFFECTIVE_MAX_R_SIZE / PROP_SIZE)
#define PROP_MES_SIZE (R_MES_HEADER + (PROP_SIZE * PROP_COALESCE))

// Combining reads + proposes
#define R_SEND_SIZE MAX(R_MES_SIZE, PROP_MES_SIZE)
#define MAX_READ_COALESCE MAX(R_COALESCE, PROP_COALESCE)

#define R_SEND_SIDE_PADDING FIND_PADDING(R_SEND_SIZE)
#define ALIGNED_R_SEND_SIDE (R_SEND_SIZE + R_SEND_SIDE_PADDING)

#define MAX_RECV_R_WRS ((R_CREDITS * REM_MACH_NUM) + RECV_WR_SAFETY_MARGIN)
#define MAX_INCOMING_R (MAX_RECV_R_WRS * MAX_READ_COALESCE)
#define MAX_R_WRS (MESSAGES_IN_BCAST_BATCH)
#define R_ENABLE_INLINING ((R_SEND_SIZE > MAXIMUM_INLINE_SIZE) ?  0 : 1)
#define R_RECV_SIZE (GRH_SIZE + ALIGNED_R_SEND_SIDE)


// READ REPLIES -- Replies to reads/acquires/proposes accepts
#define R_REP_MES_HEADER (11) //l_id 8 , coalesce_num 1, m_id 1, opcode 1 // and credits
// Reads/acquires
#define R_REP_SIZE (TS_TUPLE_SIZE + VALUE_SIZE + 1)
#define R_REP_ONLY_TS_SIZE (TS_TUPLE_SIZE + 1)
#define R_REP_SMALL_SIZE (1)
#define READ_REP_MES_SIZE (R_REP_MES_HEADER + (R_COALESCE * R_REP_SIZE)) // Message size of replies to reads/acquires
// RMW_ACQUIRE
#define RMW_ACQ_REP_SIZE (TS_TUPLE_SIZE + RMW_VALUE_SIZE + RMW_ID_SIZE + LOG_NO_SIZE + 1)
#define RMW_ACQ_REP_MES_SIZE (R_REP_MES_HEADER + (R_COALESCE * RMW_ACQ_REP_SIZE)) //Message size of replies to rmw-acquires
// PROPOSE REPLIES
#define PROP_REP_SIZE (28 + RMW_VALUE_SIZE)  //l_id- 8, RMW_id- 10, ts 5, log_no - 4,  RMW value, opcode 1
#define PROP_REP_SMALL_SIZE 9 // lid and opcode
#define PROP_REP_ONLY_TS_SIZE (9 + TS_TUPLE_SIZE)
#define PROP_REP_ACCEPTED_SIZE (PROP_REP_ONLY_TS_SIZE + RMW_ID_SIZE + RMW_VALUE_SIZE)
#define PROP_REP_MES_SIZE (R_REP_MES_HEADER + (PROP_COALESCE * PROP_REP_SIZE)) //Message size of replies to proposes
// ACCEPT REPLIES
#define ACC_REP_SIZE (28 + RMW_VALUE_SIZE)  //l_id- 8, RMW_id- 10, ts 5, log_no - 4,  RMW value, opcode 1
#define ACC_REP_SMALL_SIZE 9 // lid and opcode
#define ACC_REP_ONLY_TS_SIZE (9 + TS_TUPLE_SIZE)
#define ACC_REP_ACCEPTED_SIZE (ACC_REP_ONLY_TS_SIZE + RMW_ID_SIZE + RMW_VALUE_SIZE)
#define ACC_REP_MES_SIZE (R_REP_MES_HEADER + (ACC_COALESCE * ACC_REP_SIZE)) //Message size of replies to accepts


// COMBINE Reads, Acquires, RMW-acquires, Accepts , Propose
#define MAX_R_REP_MES_SIZE MAX_OF_4(READ_REP_MES_SIZE, RMW_ACQ_REP_MES_SIZE, PROP_REP_MES_SIZE, ACC_REP_MES_SIZE)
#define R_REP_SEND_SIZE MIN(MAX_R_REP_MES_SIZE, MTU)

#define MAX_R_REP_COALESCE MAX_OF_3(R_COALESCE, PROP_COALESCE, ACC_COALESCE)
#define MAX_REPS_IN_REP MAX_R_REP_COALESCE

#define R_REP_SEND_SIDE_PADDING FIND_PADDING(R_REP_SEND_SIZE)
#define ALIGNED_R_REP_SEND_SIDE (R_REP_SEND_SIZE + R_REP_SEND_SIDE_PADDING)
#define R_REP_RECV_SIZE (GRH_SIZE + ALIGNED_R_REP_SEND_SIDE)

#define R_REP_SLOTS_FOR_ACCEPTS (W_CREDITS * REM_MACH_NUM * SESSIONS_PER_THREAD) // the maximum number of accept-related read replies
#define MAX_RECV_R_REP_WRS ((REM_MACH_NUM * R_CREDITS) + R_REP_SLOTS_FOR_ACCEPTS)
#define R_REP_WRS_WITHOUT_ACCEPTS (R_CREDITS * REM_MACH_NUM)
#define MAX_R_REP_WRS (R_REP_WRS_WITHOUT_ACCEPTS + R_REP_SLOTS_FOR_ACCEPTS)

#define R_REP_ENABLE_INLINING ((R_REP_SEND_SIZE > MAXIMUM_INLINE_SIZE) ?  0 : 1)
#define R_REP_FIFO_SIZE (MAX_INCOMING_R + R_REP_SLOTS_FOR_ACCEPTS)

// Acks
#define MAX_RECV_ACK_WRS (REM_MACH_NUM * W_CREDITS)
#define MAX_ACK_WRS (MACHINE_NUM)
#define ACK_SIZE (14)
#define ACK_RECV_SIZE (GRH_SIZE + (ACK_SIZE))

// RMWs
#define LOCAL_PROP_NUM_ (SESSIONS_PER_THREAD)
#define LOCAL_PROP_NUM (ENABLE_RMWS == 1 ? LOCAL_PROP_NUM_ : 0)



// Broadcast Commits from helps in 2 occassions:
// 1. You are helping someone
// 2. You have received an already committed message
#define MUST_BCAST_COMMITS_FROM_HELP 6 // broadcast commits using the help_loc_entry as the source
#define COMMITTED 7 // Local entry only: bcasts broadcasted, but session not yet freed
#define TS_STALE_ON_REMOTE_KVS 8


#define VC_NUM 2
#define R_VC 0
#define W_VC 1

// BUFFER SIZES
#define R_BUF_SLOTS (REM_MACH_NUM * R_CREDITS)
#define R_BUF_SIZE (R_RECV_SIZE * R_BUF_SLOTS)

#define R_REP_BUF_SLOTS ((REM_MACH_NUM * R_CREDITS) + R_REP_SLOTS_FOR_ACCEPTS)
#define R_REP_BUF_SIZE (R_REP_RECV_SIZE * R_REP_BUF_SLOTS)

#define W_BUF_SLOTS (REM_MACH_NUM * W_CREDITS)
#define W_BUF_SIZE (W_RECV_SIZE * W_BUF_SLOTS)

#define ACK_BUF_SLOTS (REM_MACH_NUM * W_CREDITS)
#define ACK_BUF_SIZE (ACK_RECV_SIZE * ACK_BUF_SLOTS)

#define TOTAL_BUF_SIZE (R_BUF_SIZE + R_REP_BUF_SIZE + W_BUF_SIZE + ACK_BUF_SIZE)
#define TOTAL_BUF_SLOTS (R_BUF_SLOTS + R_REP_BUF_SLOTS + W_BUF_SLOTS + ACK_BUF_SLOTS)
// that allows for reads to insert reads
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


/*-------------------------------------------------
-----------------DEBUGGING-------------------------
--------------------------------------------------*/
#define USE_A_SINGLE_KEY 0
#define DEFAULT_SL 0 //default service level
//It may be that ENABLE_ASSERTIONS  must be up for these to work
#define DEBUG_WRITES 0
#define DEBUG_ACKS 0
#define DEBUG_READS 0
#define DEBUG_READ_REPS 0
#define DEBUG_TS 0
#define CHECK_DBG_COUNTERS 0
#define VERBOSE_DBG_COUNTER 0
#define DEBUG_SS_BATCH 0
#define R_TO_W_DEBUG 0
#define DEBUG_QUORUM 0
#define DEBUG_BIT_VECS 0
#define DEBUG_RMW 0
#define DEBUG_RECEIVES 0
#define DEBUG_SESSIONS 0
#define DEBUG_SESS_COUNTER M_16
#define DEBUG_LOG 0
#define ENABLE_INFO_DUMP_ON_STALL 0


#define POLL_CQ_R 0
#define POLL_CQ_W 1
#define POLL_CQ_R_REP 2
#define POLL_CQ_ACK 3

//LATENCY Measurements
#define MAX_LATENCY 400 //in us
#define LATENCY_BUCKETS 200 //latency accuracy

/* SHM key for the 1st request region created by master. ++ for other RRs.*/
#define MASTER_SHM_KEY 24


//Defines for parsing the trace
#define _200_K 200000
#define MAX_TRACE_SIZE _200_K
#define TRACE_SIZE K_128
#define NOP 0

struct trace_command {
  uint8_t opcode;
  uint8_t key_hash[8];
};

/* ah pointer and qpn are accessed together in the critical path
   so we are putting them in the same kvs line */
struct remote_qp {
	struct ibv_ah *ah;
	int qpn;
	// no padding needed- false sharing is not an issue, only fragmentation
};


struct cache_resp {
  uint8_t type;
  uint8_t kv_ptr_state;
  uint32_t log_no; // the log_number of an RMW
  mica_op_t *kv_pair_ptr;
  struct ts_tuple kv_ptr_ts;
  struct rmw_id kv_ptr_rmw_id;
};

// The format of an ack message
struct ack_message {
	uint64_t local_id ; // the first local id that is being acked
  uint8_t m_id;
  uint8_t opcode;
  uint16_t credits;
  uint16_t ack_num;
} __attribute__((__packed__));


struct ack_message_ud_req {
	uint8_t grh[GRH_SIZE];
  struct ack_message ack;
 };


struct write {
  uint8_t m_id;
  uint32_t version;
  struct key key;
  uint8_t opcode;
  uint8_t val_len;
  uint8_t value[VALUE_SIZE];
} __attribute__((__packed__));


struct accept {
	struct network_ts_tuple ts;
  struct key key ;
	uint8_t opcode;
  uint8_t val_len;
	uint8_t value[RMW_VALUE_SIZE];
  uint64_t t_rmw_id ; // upper bits are overloaded to store the bit vector when detecting a failure
  uint16_t glob_sess_id ; // this is useful when helping
  uint32_t log_no ;
  struct net_rmw_id last_registered_rmw_id; // the upper bits are overloaded to indicate that the accept is trying to flip a bit
  uint64_t l_id;
} __attribute__((__packed__));



struct commit {
  struct network_ts_tuple ts;
  struct key key;
  uint8_t opcode;
  uint8_t val_len;
  uint8_t value[RMW_VALUE_SIZE];
  uint64_t t_rmw_id; //rmw lid to be committed
  uint16_t glob_sess_id;
  uint32_t log_no;
} __attribute__((__packed__));

//
struct read {
  struct network_ts_tuple ts;
  struct key key;
  uint8_t opcode;
} __attribute__((__packed__));

//
struct propose {
  struct network_ts_tuple ts;
  struct key key;
  uint8_t opcode;
  uint64_t t_rmw_id;
  uint16_t glob_sess_id;
  uint32_t log_no;
  uint64_t l_id; // the l_id of the rmw local_entry
} __attribute__((__packed__));



/*------- HEADERS---------------------- */

struct w_message {
	uint8_t m_id;
	uint8_t coalesce_num;
	uint8_t opcode;
	uint64_t l_id ;
	struct write write[W_COALESCE];
} __attribute__((__packed__));

//
struct r_message {
  uint8_t coalesce_num;
  uint8_t m_id;
  uint64_t l_id ;
  struct read read[R_COALESCE];
} __attribute__((__packed__));

struct w_message_ud_req {
  uint8_t unused[GRH_SIZE];
  uint8_t w_mes[ALIGNED_W_SEND_SIDE];
};

//
struct r_message_ud_req {
  uint8_t unused[GRH_SIZE];
  uint8_t r_mes[ALIGNED_R_SEND_SIDE];
};




struct r_mes_info {
  uint16_t reads_num; // all non propose messages count as reads
  uint16_t message_size;
  uint16_t max_rep_message_size;
  uint32_t backward_ptr;
};


#define UNUSED_BYTES_IN_REL_BIT_VEC (13 - SEND_CONF_VEC_SIZE)
struct rel_bit_vec{
  uint8_t bit_vector[SEND_CONF_VEC_SIZE];
  uint8_t unused[UNUSED_BYTES_IN_REL_BIT_VEC];
  uint8_t opcode;
}__attribute__((__packed__));

struct w_mes_info {
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
};



// Sent when the timestamps are equal or smaller
struct r_rep_small {
  uint8_t opcode;
};

// Sent when you have a bigger ts_tuple
struct r_rep_big {
  uint8_t opcode;
  struct network_ts_tuple ts;
  uint8_t value[VALUE_SIZE];
}__attribute__((__packed__));

struct rmw_acq_rep {
  uint8_t opcode;
  struct network_ts_tuple ts;
  uint8_t value[RMW_VALUE_SIZE];
  uint32_t log_no; // last committed only
  uint64_t rmw_id; // last committed
  uint16_t glob_sess_id; // last committed
} __attribute__((__packed__));

//
struct r_rep_message {
  uint8_t coalesce_num;
  uint8_t m_id;
  uint8_t opcode;
  uint64_t l_id;
  struct r_rep_big r_rep[MAX_R_REP_COALESCE];
} __attribute__((__packed__));


struct r_rep_message_ud_req {
  uint8_t unused[GRH_SIZE];
  uint8_t r_rep_mes[ALIGNED_R_REP_SEND_SIDE];
};

// Reply for both accepts and proposes
// Reply with the last committed RMW if the
// proposal/accept had a low log number or has already been committed
struct rmw_rep_last_committed {
  uint8_t opcode;
  uint64_t l_id; // the l_id of the rmw local_entry
  struct network_ts_tuple ts;
  uint8_t value[RMW_VALUE_SIZE];
  uint64_t rmw_id; //accepted  OR last committed
  uint16_t glob_sess_id; //accepted  OR last committed
  uint32_t log_no; // last committed only
} __attribute__((__packed__));

//
struct rmw_rep_message {
  uint8_t coalesce_num;
  uint8_t m_id;
  uint8_t opcode;
  uint64_t l_id ;
  struct rmw_rep_last_committed rmw_rep[PROP_COALESCE];
}__attribute__((__packed__));


struct r_message_template {
  uint8_t unused[ALIGNED_R_SEND_SIDE];
};

//
struct read_fifo {
  struct r_message_template *r_message;
  uint32_t push_ptr;
  uint32_t bcast_pull_ptr;
  uint32_t bcast_size; // number of reads not messages!
  struct r_mes_info info[R_FIFO_SIZE];
  //uint32_t backward_ptrs[R_FIFO_SIZE];
};


struct w_message_template {
  uint8_t unused[ALIGNED_W_SEND_SIDE];
};

//
struct write_fifo {
  struct w_message_template *w_message;
  uint32_t push_ptr;
  uint32_t bcast_pull_ptr;
  uint32_t bcast_size; // number of writes not messages!
  struct w_mes_info info[W_FIFO_SIZE];
  //uint32_t size;
  //uint32_t backward_ptrs[W_FIFO_SIZE]; // pointers to the slots in p_ops--one pointer per message
};


struct r_rep_message_template {
  uint8_t unused[ALIGNED_R_REP_SEND_SIDE];
};


struct r_rep_fifo {
  struct r_rep_message_template *r_rep_message;
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
  bool is_rmw;
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
};

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


#define ENABLE_DEBUG_GLOBAL_ENTRY 0

// the first time a key gets RMWed, it grabs an RMW entry
// that lasts for life, the entry is protected by the KVS lock
struct rmw_entbry {
  uint8_t opcode; // what kind of RMW
  struct key key;
  uint8_t state;
  struct rmw_id rmw_id;
  struct rmw_id last_committed_rmw_id;
  struct rmw_id last_registered_rmw_id;
  struct rmw_id accepted_rmw_id; // not really needed, but good for debug
  struct dbg_glob_entry *dbg;
  //struct ts_tuple old_ts;
  struct ts_tuple prop_ts;
  struct ts_tuple accepted_ts; // really needed
  uint8_t last_accepted_value[RMW_VALUE_SIZE]; // last accepted
  uint32_t log_no; // keep track of the biggest log_no that has not been committed
  uint32_t last_committed_log_no;
  uint32_t last_registered_log_no;
  uint32_t accepted_log_no; // not really needed, but good for debug
};

// possible flags explaining how the last committed RMW was committed
#define LOCAL_RMW 0
#define LOCAL_RMW_FROM_HELP 1
#define REMOTE_RMW 2
#define REMOTE_RMW_FROM_REP 3


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


struct rmw_rep_info {
  uint8_t tot_replies;
  uint8_t acks;
  uint8_t rmw_id_commited;
  uint8_t log_too_small;
  uint8_t already_accepted;
  uint8_t ts_stale;
  uint8_t seen_higher_prop_acc; // Seen a higher prop or accept
  uint8_t log_too_high;
  uint8_t nacks;
  // used to know whether to help after a prop-- if you have seen a higher acc,
  // then you should not try to help a lower accept, and thus dont try at all
  bool seen_higher_acc;
  struct ts_tuple kvs_higher_ts;
  uint32_t seen_higher_prop_version;

};


// Entry that keep pending thread-local RMWs, the entries are accessed with session id
struct rmw_local_entry {
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
  uint8_t value_to_write[RMW_VALUE_SIZE];
  uint8_t value_to_read[RMW_VALUE_SIZE];
  uint8_t *compare_val; //for CAS- add value for FAA
  uint32_t rmw_val_len;
  struct rmw_id rmw_id; // this is implicitly the l_id
  struct rmw_id last_registered_rmw_id;
  struct rmw_rep_info rmw_reps;
  uint64_t epoch_id;
  uint16_t sess_id;
  uint32_t index_to_req_array;
  uint32_t back_off_cntr;
  uint32_t all_aboard_time_out;
  // uint32_t index_to_rmw; // this is an index into the global rmw structure
  uint32_t log_no;
  uint32_t accepted_log_no; // this is the log no that has been accepted locally and thus when committed is guaranteed to be the correct logno
  uint64_t l_id; // the unique l_id of the entry, it typically coincides with the rmw_id except from helping cases
  mica_op_t *ptr_to_kv_pair;
  struct rmw_help_entry *help_rmw;
  struct rmw_local_entry* help_loc_entry;
};


// Local state of pending RMWs - one entry per session
// Accessed with session id!
struct prop_info {
  struct rmw_local_entry entry[LOCAL_PROP_NUM];
  uint64_t l_id; // highest l_id as of yet -- Starts from 1
};

struct sess_info {
  bool stalled;
  bool ready_to_release;
  uint8_t missing_num;
  uint8_t missing_ids[REM_MACH_NUM];
  uint64_t epoch_id;
  // live writes: writes that have not been acked-
  // could be ooe-writes in their read phase
  uint32_t live_writes;

  uint32_t writes_not_yet_inserted; // for debug only

};

struct per_write_meta {
  uint8_t w_state;
  uint8_t acks_seen;
  uint8_t acks_expected;
  uint8_t expected_ids[REM_MACH_NUM];
  bool seen_expected[REM_MACH_NUM];

  uint32_t sess_id;
};

struct pending_out_of_epoch_writes {
  uint32_t size; //number of pending ooe writes
  uint32_t push_ptr;
  uint32_t pull_ptr;
  uint32_t r_info_ptrs[PENDING_READS]; // ptrs to the read_info struct of p_ops
};

struct pending_ops {
  struct write_fifo *w_fifo;
  struct read_fifo *r_fifo;
  struct r_rep_fifo *r_rep_fifo;
  //struct write **ptrs_to_w_ops; // used for remote writes
  void **ptrs_to_mes_ops; // used for remote reads

  struct write **ptrs_to_local_w; // used for the first phase of release
  uint8_t *overwritten_values;
  struct r_message **ptrs_to_mes_headers;
  bool *coalesce_r_rep;
//  struct read_payload *r_payloads;
  struct read_info *read_info;

  struct prop_info *prop_info;
  //
  struct pending_out_of_epoch_writes *p_ooe_writes;
  struct sess_info *sess_info;
  uint64_t local_w_id;
  uint64_t local_r_id;
  uint32_t *r_session_id;
  //uint32_t *w_session_id;
  uint32_t *w_index_to_req_array;
  uint32_t *r_index_to_req_array;

  //uint8_t *w_state;
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
  //uint32_t prop_size; // TODO add this if needed
  //uint8_t *acks_seen;
  struct per_write_meta *w_meta;
  uint32_t full_w_q_fifo;
  //bool *session_has_pending_op;
  bool all_sessions_stalled;
  struct quorum_info *q_info;
};

// A helper to debug sessions by remembering which write holds a given session
struct session_dbg {
	uint32_t dbg_cnt[SESSIONS_PER_THREAD];
	//uint8_t is_release[SESSIONS_PER_THREAD];
	//uint32_t request_id[SESSIONS_PER_THREAD];
};





// Global struct that holds the RMW information
// Cannot be a FIFO because the incoming commit messages must be processed, such that
// acks to the commits mean that the RMW has happened
//struct rmw_info {
//  struct rmw_entry entry[RMW_ENTRIES_NUM];
//};

//typedef _Atomic struct rmw_info atomic_rmw_info;
//extern struct rmw_info rmw;

extern atomic_uint_fast64_t committed_glob_sess_rmw_id[GLOBAL_SESSION_NUM];

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

struct send_info {
  struct ibv_send_wr *send_wr;
  struct ibv_sge *r_send_sgl;
  struct ibv_sge *r_recv_sgl;
  struct ibv_wc *r_recv_wc;
  struct ibv_recv_wr *r_recv_wr;
};


struct fifo {
  void *fifo;
  uint32_t push_ptr;
  uint32_t pull_ptr;
  uint32_t size;

};

//#define TRACE_OP_SIZE (18 + VALUE_SIZE  + 8 + 4)
struct trace_op {
  uint16_t session_id;
  bool attempt_all_aboard;
  struct network_ts_tuple ts;
  struct key key;	/* This must be the 1st field and 16B aligned */
  uint8_t opcode;// if the opcode is 0, it has never been RMWed, if it's 1 it has
  uint8_t val_len; // this represents the maximum value len
  uint8_t value[VALUE_SIZE]; // if it's an RMW the first 4 bytes point to the entry
  uint8_t *value_to_write;
  uint8_t *value_to_read; //compare value for CAS/  addition argument for F&A
  uint32_t index_to_req_array;
  uint32_t real_val_len; // this is the value length the client is interested in
}__attribute__((__packed__));

#define INVALID_REQ 0 // entry not being used
#define ACTIVE_REQ 1 // client has issued a reqs
#define IN_PROGRESS_REQ 2 // worker has picked up the req
#define COMPLETED_REQ 3 // worker has completed the req

#define RAW_CLIENT_OP_SIZE (8 + TRUE_KEY_SIZE + VALUE_SIZE + 8 + 8)
#define PADDING_BYTES_CLIENT_OP (FIND_PADDING(RAW_CLIENT_OP_SIZE))
#define CLIENT_OP_SIZE (PADDING_BYTES_CLIENT_OP + RAW_CLIENT_OP_SIZE)
struct client_op {
  atomic_uint_fast8_t state;
  uint8_t opcode;
  uint32_t val_len;
  bool* rmw_is_successful;
  struct key key;
  uint8_t *value_to_read;//[VALUE_SIZE]; // expected val for CAS
  uint8_t value_to_write[VALUE_SIZE]; // desired Val for CAS
  uint8_t padding[PADDING_BYTES_CLIENT_OP];
};

#define IF_CLT_PTRS_SIZE (4 * SESSIONS_PER_THREAD) //  4* because client needs 2 ptrs (pull/push) that are 2 bytes each
#define IF_WRKR_PTRS_SIZE (2 * SESSIONS_PER_THREAD) // 2* because client needs 1 ptr (pull) that is 2 bytes
#define PADDING_IF_CLT_PTRS (FIND_PADDING(IF_CLT_PTRS_SIZE))
#define PADDING_IF_WRKR_PTRS (FIND_PADDING(IF_WRKR_PTRS_SIZE))
#define IF_PTRS_SIZE (IF_CLT_PTRS_SIZE + IF_WRKR_PTRS_SIZE + PADDING_IF_CLT_PTRS + PADDING_IF_WRKR_PTRS))
#define INTERFACE_SIZE ((SESSIONS_PER_THREAD * PER_SESSION_REQ_NUM * CLIENT_OP_SIZE) + (IF_PTRS_SIZE)

// wrkr-client interface
struct wrk_clt_if {
  struct client_op req_array[SESSIONS_PER_THREAD][PER_SESSION_REQ_NUM];
  uint16_t clt_push_ptr[SESSIONS_PER_THREAD];
  uint16_t clt_pull_ptr[SESSIONS_PER_THREAD];
  uint8_t clt_ptr_padding[PADDING_IF_CLT_PTRS];
  uint16_t wrkr_pull_ptr[SESSIONS_PER_THREAD];
  uint8_t wrkr_ptr_padding[PADDING_IF_WRKR_PTRS];
}__attribute__ ((aligned (64)));

extern struct wrk_clt_if interface[WORKERS_PER_MACHINE];

extern uint64_t last_pulled_req[SESSIONS_PER_MACHINE];
extern uint64_t last_pushed_req[SESSIONS_PER_MACHINE];

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
#define UP_STABLE 0
#define DOWN_STABLE 1
#define DOWN_TRANSIENT_OWNED 2
#define UNUSED_STATE 3 // used to denote that the field will not be used

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
extern struct remote_qp remote_qp[MACHINE_NUM][WORKERS_PER_MACHINE][QP_NUM];
extern atomic_bool qps_are_set_up;
extern struct thread_stats t_stats[WORKERS_PER_MACHINE];
extern struct client_stats c_stats[CLIENTS_PER_MACHINE];
struct mica_op;
extern atomic_uint_fast64_t epoch_id;
extern const uint16_t machine_bit_id[16];

extern atomic_bool print_for_debug;
extern FILE* rmw_verify_fp[WORKERS_PER_MACHINE];
extern FILE* client_log[CLIENTS_PER_MACHINE];
extern uint64_t time_approx;



struct thread_params {
	int id;
};

typedef enum {
  NO_REQ,
  RELEASE,
  ACQUIRE,
  WRITE_REQ,
  READ_REQ,
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


void *client(void *arg);
void *worker(void *arg);
void *print_stats(void*);




#endif

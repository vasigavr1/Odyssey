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
#define WORKERS_PER_MACHINE 1
#define MACHINE_NUM 3
#define WRITE_RATIO 500 //Warning write ratio is given out of a 1000, e.g 10 means 10/1000 i.e. 1%
#define SESSIONS_PER_THREAD 22
#define MEASURE_LATENCY 0
#define LATENCY_MACHINE 0
#define LATENCY_THREAD 15
#define MEASURE_READ_LATENCY 2 // 2 means mixed
#define ENABLE_LIN 0
#define R_CREDITS 1
#define MAX_R_COALESCE 40
#define W_CREDITS 2
#define MAX_W_COALESCE 10
#define ENABLE_ASSERTIONS 1
#define USE_QUORUM 1
#define CREDIT_TIMEOUT  M_16 // B_4_EXACT //
#define REL_CREDIT_TIMEOUT M_16
#define ENABLE_ADAPTIVE_INLINING 0 // This did not help
#define MIN_SS_BATCH 127// The minimum SS batch
#define ENABLE_STAT_COUNTING 1
#define MAXIMUM_INLINE_SIZE 188
#define MAX_OP_BATCH_ 50
#define SC_RATIO_ 250// this is out of 1000, e.g. 10 means 1%
#define ENABLE_RELEASES_ 1
#define ENABLE_ACQUIRES_ 1
#define ENABLE_RMWS_ 1
#define EMULATE_ABD 0// Do not enforce releases to gather all credits or start a new message



#define REM_MACH_NUM (MACHINE_NUM - 1) // Number of remote machines
#define SESSIONS_PER_MACHINE (WORKERS_PER_MACHINE * SESSIONS_PER_THREAD)
#define GLOBAL_SESSION_NUM (MACHINE_NUM * SESSIONS_PER_MACHINE)

#define WORKER_NUM (WORKERS_PER_MACHINE * MACHINE_NUM)

#define CACHE_SOCKET 0// (WORKERS_PER_MACHINE < 30 ? 0 : 1 )// socket where the cache is bind
#define SESSION_BYTES 3 // session ids must fit in 3 bytes i.e.



#define ENABLE_CACHE_STATS 0
#define EXIT_ON_PRINT 0
#define PRINT_NUM 4
#define DUMP_STATS_2_FILE 0






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
#define SHOW_STATS_LATENCY_STYLE 0



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

// post some extra receives to avoid spurious out_of_buffer errors
#define RECV_WR_SAFETY_MARGIN 2

// READS
#define R_SIZE (TRUE_KEY_SIZE + TS_TUPLE_SIZE + 1)// key+ version + m_id + opcode
#define R_MES_HEADER (10) // local id + coalesce num + m_id
#define R_MES_SIZE (R_MES_HEADER + (R_SIZE * MAX_R_COALESCE))
#define R_RECV_SIZE (GRH_SIZE + R_MES_SIZE)

#define MAX_RECV_R_WRS ((R_CREDITS * REM_MACH_NUM) + RECV_WR_SAFETY_MARGIN)
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
#define R_REP_SLOTS_FOR_ACCEPTS (2 * REM_MACH_NUM * SESSIONS_PER_THREAD) // the maximum number of accept-related read replies
#define MAX_RECV_R_REP_WRS ((REM_MACH_NUM * R_CREDITS) + R_REP_SLOTS_FOR_ACCEPTS)
#define R_REP_WRS_WITHOUT_ACCEPTS (R_CREDITS * REM_MACH_NUM * (CEILING(MAX_R_COALESCE, MAX_R_REP_COALESCE)))
#define MAX_R_REP_WRS (R_REP_WRS_WITHOUT_ACCEPTS + R_REP_SLOTS_FOR_ACCEPTS)

#define R_REP_ENABLE_INLINING ((R_REP_SEND_SIZE > MAXIMUM_INLINE_SIZE) ?  0 : 1)
#define R_REP_FIFO_SIZE (MAX_INCOMING_R + R_REP_SLOTS_FOR_ACCEPTS)
//#define READ_INFO_SIZE (3 + TS_TUPLE_SIZE + TRUE_KEY_SIZE + VALUE_SIZE) // not correct

// Writes
#define MAX_RECV_W_WRS ((W_CREDITS * REM_MACH_NUM) + RECV_WR_SAFETY_MARGIN)
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
#define ACK_SIZE 14 // bytes like everytinh else
#define ACK_RECV_SIZE (GRH_SIZE + (ACK_SIZE))



// RMWs
#define BYTES_OVERRIDEN_IN_KVS_VALUE 4
#define RMW_VALUE_SIZE (VALUE_SIZE - BYTES_OVERRIDEN_IN_KVS_VALUE)
#define RMW_ENTRIES_PER_MACHINE (253 / MACHINE_NUM)
#define RMW_ENTRIES_NUM (RMW_ENTRIES_PER_MACHINE * MACHINE_NUM)
#define KEY_HAS_NEVER_BEEN_RMWED 0
#define KEY_HAS_BEEN_RMWED 1
#define LOG_NO_SIZE 4
#define RMW_ID_SIZE 10



// Proposes
#define LOCAL_PROP_NUM (SESSIONS_PER_THREAD)
#define MAX_PROP_COALESCE 1

#define PROP_MES_HEADER 2 // coalesce_num , m_id
#define PROP_SIZE 28  // RMW_id- 10, ts 5, key 8, log_number 4, opcode 1
#define PROP_MESSAGE_SIZE (PROP_MES_HEADER + (MAX_PROP_COALESCE * PROP_SIZE))

// Propose replies
#define MAX_PROP_REP_COALESCE (MAX_PROP_COALESCE)
#define PROP_REP_MES_HEADER 3 // coalesce_num , m_id, opcode
#define PROP_REP_SIZE (28 + RMW_VALUE_SIZE)  //l_id- 8, RMW_id- 10, ts 5, log_no - 4,  RMW value, opcode 1
#define PROP_REP_MESSAGE_SIZE (PROP_REP_MES_HEADER + (MAX_PROP_REP_COALESCE * PROP_REP_SIZE))
#define PROP_REP_SMALL_SIZE 9 // lid and opcode
#define PROP_REP_ONLY_TS_SIZE (9 + TS_TUPLE_SIZE)
#define PROP_REP_ACCEPTED_SIZE (PROP_REP_ONLY_TS_SIZE + RMW_ID_SIZE + RMW_VALUE_SIZE)


// ACCEPTS
#define MAX_ACC_COALESCE 1
#define ACCEPT_MES_HEADER 2
#define ACCEPT_SIZE (29 + RMW_VALUE_SIZE) // key 8 rmw-id 10, ts 5 log_no 4 opcode 1, val_len 1, rmw value
#define ACCEPT_MESSAGE_SIZE (ACCEPT_MES_HEADER + (MAX_ACC_COALESCE * ACCEPT_SIZE))

// ACCEPT REPLIES
#define MAX_ACC_REP_COALESCE (MAX_ACC_COALESCE)
#define ACC_REP_MES_HEADER 3 // coalesce_num , m_id, opcode
#define ACC_REP_SIZE (28 + RMW_VALUE_SIZE)  //l_id- 8, RMW_id- 10, ts 5, log_no - 4,  RMW value, opcode 1
#define ACC_REP_MESSAGE_SIZE (ACC_REP_MES_HEADER + (MAX_ACC_REP_COALESCE * ACC_REP_SIZE))
#define ACC_REP_SMALL_SIZE 9 // lid and opcode
#define ACC_REP_ONLY_TS_SIZE (9 + TS_TUPLE_SIZE)
#define ACC_REP_ACCEPTED_SIZE (ACC_REP_ONLY_TS_SIZE + RMW_ID_SIZE + RMW_VALUE_SIZE)


// COMMITS
#define MAX_COM_COALESCE 1
#define COMMIT_MES_HEADER 10
#define COMMIT_SIZE (29 + RMW_VALUE_SIZE)
#define COMMIT_MESSAGE_SIZE (COMMIT_MES_HEADER + (MAX_COM_COALESCE * COMMIT_SIZE))


#define RMW_WAIT_COUNTER M_256

// RMW entry states for local and global entries
#define INVALID_RMW 0
#define PROPOSED 1 // has seen a propose || has been proposed
#define ACCEPTED 2 // has acked an acccept || has fired accepts
#define SAME_KEY_RMW 3  // there is already an entry for the key
#define MUST_BCAST_COMMITS 4 // locally committed-> must broadcast commits
#define COMMITTED 5 //

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
#define EXTRA_WRITE_SLOTS 50 // to accommodate reads that become writes
#define PENDING_WRITES MAX((MAX_OP_BATCH + 1), ((2 * SESSIONS_PER_THREAD) + 1))
#define W_FIFO_SIZE (PENDING_WRITES + LOCAL_PROP_NUM) // Accepts use the write fifo

// The w_fifo needs to have a safety slot that cannot be touched
// such that the fifo push ptr can never coincide with its pull ptr
// zeroing its w_num, as such we take care to allow
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
#define DEBUG_TS 0
#define CHECK_DBG_COUNTERS 0
#define VERBOSE_DBG_COUNTER 0
#define DEBUG_SS_BATCH 0
#define R_TO_W_DEBUG 0
#define DEBUG_QUORUM 0
#define DEBUG_BIT_VECS 0
#define DEBUG_RMW 1
#define DEBUG_RECEIVES 0
#define DEBUG_SESSIONS 0
#define PUT_A_MACHINE_TO_SLEEP 1
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
#define SENT_PUT 4 // typical writes
#define SENT_RELEASE 5 // Release or second round of acquire!!
#define SENT_BIT_VECTOR 6
#define SENT_COMMIT 7 // For commits
#define READY_PUT 8
#define READY_RELEASE 9 // Release or second round of acquire!!
#define READY_BIT_VECTOR 10

// Possible write sources
#define FROM_TRACE 0
#define FROM_READ 1
//#define RELEASE_SECOND 2 // after the read ts for a release
#define RELEASE_THIRD 3 // for the third round of a release
#define FOR_ACCEPT 4
#define FROM_ACQUIRE 5
#define FROM_COMMIT 6

// Possible flag values when inserting a read reply
#define READ 0
#define READ_TS 1
#define NO_OP_ACQ_FLIP_BIT 2 // Send an 1-byte reply to read messages from acquries that are only emant to flip a bit
#define RMW_TS_SMALLER_THAN_KVS 3 //accepts and proposes
#define RMW_ACK_PROPOSE 4 // only proposes: Send an 1-byte reply
#define RMW_ALREADY_ACCEPTED 5 // only proposes: Send value, ts rmw-id
#define RMW_SEEN_HIGHER_PROP_TS 6 //accepts and proposes: Send a TS, because you have already acked a higher Propose
#define RMW_ALREADY_COMMITTED 7 //accepts and proposes
#define RMW_LOG_TOO_SMALL 8 // accepts and proposes
#define RMW_ACK_ACCEPT 9 // only accepts
#define RMW_ACCEPTED_WITH_HIGHER_TS 10 // only accepts
//#define RMW_LOG_TOO_HIGH 9 // this means the propose will be acked

//flags when receiving a commit


// Possible flags when accepting locally
#define ACCEPT_ACK 1
#define NACK_ACCEPT_SEEN_HIGHER_TS 2
#define NACK_ACCEPT_LOG_OUT_OF_DATE 3
#define BROADCAST_COMMITS 4
#define DO_NOT_BROAD_CAST_COMMITS 5



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

// unique RMW id-- each machine must remember how many
// RMW each thread has committed, to avoid committing an RMW twice
 struct rmw_id {
   uint16_t glob_sess_id; // global session id
   uint64_t id; // the local rmw id of the source

 };

// format of a Timestamp tuple (Lamport clock)
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
  uint8_t m_id;
  uint8_t version[4];
  uint8_t key[TRUE_KEY_SIZE];	/* 8B */
  uint8_t opcode;
  uint8_t val_len;
  uint8_t value[VALUE_SIZE];
};

struct w_message {
  uint8_t m_id;
  uint8_t w_num;
  uint8_t l_id[8];
  struct write write[MAX_W_COALESCE];
};


struct accept {
	uint8_t t_rmw_id[8];
	struct ts_tuple ts;
	uint8_t key[TRUE_KEY_SIZE];
	uint8_t opcode;
  uint8_t val_len;
	uint8_t value[RMW_VALUE_SIZE];
  uint8_t glob_sess_id[2]; // this is useful when helping
  uint8_t log_no[4];
};

struct accept_message {
  uint8_t m_id;
  uint8_t acc_num;
  struct accept acc[MAX_ACC_COALESCE];
};


struct w_message_ud_req {
  uint8_t unused[GRH_SIZE];
  struct w_message w_mes;
};

struct commit {
  struct ts_tuple ts;
  uint8_t key[TRUE_KEY_SIZE];
  uint8_t opcode;
  uint8_t val_len;
  uint8_t value[RMW_VALUE_SIZE];
  uint8_t t_rmw_id[8]; //rmw lid to be committed
  uint8_t glob_sess_id[2];
  uint8_t log_no[4];
};

struct commit_message {
  uint8_t m_id;
  uint8_t com_num;
  uint8_t l_id[8]; // local id of the write -- to facilitate the ack
  struct commit com[MAX_ACC_COALESCE];
};

//
struct read {
  struct ts_tuple ts;
  uint8_t key[TRUE_KEY_SIZE];
  uint8_t opcode;
};

//
struct r_message {
  uint8_t coalesce_num;
  uint8_t m_id;
  uint8_t l_id[8];
  struct read read[MAX_R_COALESCE];
};



//
struct propose {
  uint8_t t_rmw_id[8];
  struct ts_tuple ts;
  uint8_t key[TRUE_KEY_SIZE];
  uint8_t opcode;
  uint8_t glob_sess_id[2];
  uint8_t log_no[4];
};

struct prop_message {
  uint8_t coalesce_num;
  uint8_t m_id;
  struct propose prop[MAX_PROP_COALESCE];
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
  //uint32_t pull_ptr;
  uint32_t bcast_pull_ptr;
  uint32_t bcast_size; // number of writes not messages!
  uint32_t size;
  uint32_t backward_ptrs[W_FIFO_SIZE]; // pointers to the slots in p_ops--one pointer per message
};

//


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
  uint8_t coalesce_num;
  uint8_t m_id;
//  uint8_t credits;
  uint8_t opcode;
  uint8_t l_id[8];
  struct r_rep_big r_rep[MAX_R_REP_COALESCE];
};


struct r_rep_message_ud_req {
  uint8_t unused[GRH_SIZE];
  struct r_rep_message r_rep_mes;
};

// Reply for both accepts and proposes
// Reply with the last committed RMW if the
// proposal/accept had a low log number or has already been committed
struct rmw_rep_last_committed {
  uint8_t l_id[8]; // the l_id of the propose
  uint8_t opcode;
  struct ts_tuple ts;
  uint8_t value[RMW_VALUE_SIZE];
  uint8_t rmw_id[8]; //accepted  OR last committed
  uint8_t glob_sess_id[2]; //accepted  OR last committed
  uint8_t log_no[4]; // last committed only
};

//
struct rmw_rep_message {
  uint8_t coalesce_num;
  uint8_t m_id;
  uint8_t opcode;
  struct rmw_rep_last_committed rmw_rep[MAX_PROP_REP_COALESCE];
};

//// The largest possible accept reply: Reply with the last committed RMW if the
//// accept had a low log number or has already been committed
//struct acc_rep_last_committed {
//  uint8_t l_id[8]; // the l_id of the original accept
//  uint8_t opcode;
//  struct ts_tuple ts;
//  uint8_t value[RMW_VALUE_SIZE];
//  uint8_t rmw_id[8]; //accepted  OR last committed
//  uint8_t glob_sess_id[2]; //accepted  OR last committed
//  uint8_t log_no[4]; // last committed only
//};
//
////
//struct acc_rep_message {
//  uint8_t coalesce_num;
//  uint8_t m_id;
//  uint8_t opcode;
//  struct acc_rep_last_committed acc_rep[MAX_ACC_REP_COALESCE];
//};



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
  uint8_t rep_num; // replies num
  uint8_t times_seen_ts;
  bool seen_larger_ts;
	uint8_t opcode;
  struct ts_tuple ts_to_read;
  uint8_t key[TRUE_KEY_SIZE];
	// the value read locally, a greater value received or
	// in case of a 2-round write, the value to be written
  uint8_t value[VALUE_SIZE];
  bool fp_detected; //detected false positive
  uint16_t epoch_id;
  // when a data out-of-epoch write is inserted in a write message,
  // there is a chance we may need to change its version, so we need to
  // remember where it is stored in the w_fifo -- NOT NEEDED
//  uint32_t w_mes_ptr;
//  uint8_t inside_w_ptr;
};

// the first time a key gets RMWed, it grabs an RMW entry
// that lasts for life, the entry is protected by the KVS lock
struct rmw_entry {
  uint8_t opcode; // what kind of RMW
  struct key key;
  uint8_t state;
  struct rmw_id rmw_id;
  struct rmw_id last_committed_rmw_id;
  //struct ts_tuple old_ts;
  struct ts_tuple new_ts;
  uint8_t value[RMW_VALUE_SIZE];
  uint32_t log_no; // keep track of the biggest log_no that has not been committed
  uint32_t last_committed_log_no;
  //atomic_flag lock;
};

struct rmw_help_entry{
  struct ts_tuple ts;
  uint8_t opcode;
  uint8_t value[RMW_VALUE_SIZE];
  struct rmw_id rmw_id;
  uint32_t log_no;
};

// Entry that keep pending thread-local RMWs, the entries are accessed with session id
struct rmw_local_entry {
  struct ts_tuple new_ts;
  struct key key;
  uint8_t opcode;
  uint8_t state;
  uint8_t value_to_write[RMW_VALUE_SIZE];
  uint8_t value_to_read[RMW_VALUE_SIZE];
  struct rmw_id rmw_id; // this is implicitly the l_id
  uint8_t prop_acks;
  uint8_t accept_acks;
  uint8_t prop_replies;
  uint8_t accept_replies;
  uint16_t epoch_id;
  uint16_t sess_id;
  uint32_t debug_cntr;
  uint32_t index_to_rmw; // this is an index into the global rmw structure
  uint32_t log_no;
  cache_meta *ptr_to_kv_pair;
  struct rmw_help_entry *help_rmw;
};

// Local state of pending RMWs - one entry per session
// Accessed with session id!
struct prop_info {
  struct rmw_local_entry entry[LOCAL_PROP_NUM];
  uint64_t l_id; // highest l_id as of yet -- Starts from 1
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
  struct write **ptrs_to_w_ops; // used for remote writes
  struct read **ptrs_to_r_ops; // used for remote reads

  struct write **ptrs_to_local_w; // used for the first phase of release
  uint8_t *overwritten_values;
  struct r_message **ptrs_to_r_headers;
//  struct read_payload *r_payloads;
  struct read_info *read_info;
  struct r_rep_fifo *r_rep_fifo;
  struct prop_info *prop_info;
  //
  struct pending_out_of_epoch_writes *p_ooe_writes;
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
  // virtual read size: because acquires can result in one more read,
  // knowing the size of the read fifo is not enough to know if
  // you can add an element. Virtual read size captures this by
  // getting incremented by 2, every time an acquire is inserted
	uint32_t virt_r_size;
  uint32_t prop_size; // TODO add this if needed
  uint8_t *acks_seen;
  bool *session_has_pending_op;
  bool all_sessions_stalled;
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
struct rmw_info {
  struct rmw_entry entry[RMW_ENTRIES_NUM];
};

//typedef _Atomic struct rmw_info atomic_rmw_info;
extern struct rmw_info rmw;

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
extern atomic_char qps_are_set_up;
extern struct thread_stats t_stats[WORKERS_PER_MACHINE];
struct mica_op;
extern atomic_uint_fast16_t epoch_id;
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

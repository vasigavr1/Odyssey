#ifndef ZOOKEEPER_MAIN_H
#define ZOOKEEPER_MAIN_H

#include "../general_util/top.h"


#include "city.h"
#include "hrd.h"
#include "zk_config.h"




#define ENABLE_MULTIPLE_SESSIONS 1



#define DISABLE_GID_ORDERING 0
#define DISABLE_UPDATING_KVS 0

#define ENABLE_CACHE_STATS 0

#define DUMP_STATS_2_FILE 0


/*-------------------------------------------------
-----------------DEBUGGING-------------------------
--------------------------------------------------*/



#define REMOTE_LATENCY_MARK 100 // mark a remote request for measurement by attaching this to the imm_data of the wr
#define USE_A_SINGLE_KEY 0
#define DISABLE_HYPERTHREADING 0 // do not shcedule two threads on the same core
#define DEFAULT_SL 0 //default service level



/*-------------------------------------------------
	-----------------TRACE-----------------
--------------------------------------------------*/

#define BALANCE_HOT_WRITES 0// Use a uniform access pattern among hot writes
#define SKEW_EXPONENT_A 90 // representation divided by 100 (i.e. 99 means a = 0.99)
#define EMULATING_CREW 1 // emulate crew, to facilitate running the CREW baseline
#define DISABLE_CACHE 0 // Run Baseline
#define LOAD_BALANCE 1 // Use a uniform access pattern
#define FOLLOWER_DOES_ONLY_READS 0



// ------COMMON-------------------
#define MAX_BCAST_BATCH (ENABLE_MULTICAST == 1 ? 4 : 4) //how many broadcasts can fit in a batch
#define MESSAGES_IN_BCAST (ENABLE_MULTICAST == 1 ? 1 : (FOLLOWER_MACHINE_NUM))
#define MESSAGES_IN_BCAST_BATCH MAX_BCAST_BATCH * MESSAGES_IN_BCAST //must be smaller than the q_depth


/* --------------------------------------------------------------------------------
 * -----------------------------ZOOKEEPER---------------------------------------
 * --------------------------------------------------------------------------------
 * --------------------------------------------------------------------------------*/
#define FOLLOWER 1
#define LEADER 2

#define MIN_SS_BATCH 127// The minimum SS batch


//--------FOLLOWER Flow Control


//--------LEADER Flow Control
#define LDR_VC_NUM 2
#define PREP_VC 0
#define COMM_VC 1

#define LDR_CREDIT_DIVIDER (1)
#define LDR_CREDITS_IN_MESSAGE (W_CREDITS / LDR_CREDIT_DIVIDER)
#define FLR_CREDIT_DIVIDER (2)
#define FLR_CREDITS_IN_MESSAGE (COMMIT_CREDITS / FLR_CREDIT_DIVIDER)

// if this is smaller than MAX_BCAST_BATCH + 2 it will deadlock because the signaling messaged is polled before actually posted
#define COM_BCAST_SS_BATCH MAX((MIN_SS_BATCH / (FOLLOWER_MACHINE_NUM)), (MAX_BCAST_BATCH + 2))
#define PREP_BCAST_SS_BATCH MAX((MIN_SS_BATCH / (FOLLOWER_MACHINE_NUM)), (MAX_BCAST_BATCH + 2))



// -------ACKS-------------
#define USE_QUORUM 1
#define QUORUM_NUM ((MACHINE_NUM / 2) + 1)
#define LDR_QUORUM_OF_ACKS (USE_QUORUM == 1 ? (QUORUM_NUM - 1): FOLLOWER_MACHINE_NUM) //()

#define MAX_LIDS_IN_AN_ACK K_64_
#define ACK_SIZE 12
#define COM_ACK_HEADER_SIZE 4 // follower id, opcode, coalesce_num
#define FLR_ACK_SEND_SIZE (12) // a local global id and its metadata
#define LDR_ACK_RECV_SIZE (GRH_SIZE + (FLR_ACK_SEND_SIZE))


// -- COMMITS-----

#define COM_SIZE 8 // gid(8)
#define COM_MES_HEADER_SIZE 4 // opcode + coalesce num
//#define MAX_COM_COALESCE 2
#define LDR_COM_SEND_SIZE (COM_SIZE + COM_MES_HEADER_SIZE)
#define FLR_COM_RECV_SIZE (GRH_SIZE + LDR_COM_SEND_SIZE)
#define COM_ENABLE_INLINING ((LDR_COM_SEND_SIZE < MAXIMUM_INLINE_SIZE) ? 1: 0)
#define COMMIT_FIFO_SIZE ((COM_ENABLE_INLINING == 1) ? (COMMIT_CREDITS) : (COM_BCAST_SS_BATCH))

//---WRITES---

#define WRITE_HEADER (KEY_SIZE + 2 + 8) // opcode + val_len
#define W_SIZE (VALUE_SIZE + WRITE_HEADER)
#define FLR_W_SEND_SIZE (MAX_W_COALESCE * W_SIZE)
#define LDR_W_RECV_SIZE (GRH_SIZE + FLR_W_SEND_SIZE)
#define FLR_W_ENABLE_INLINING ((FLR_W_SEND_SIZE > MAXIMUM_INLINE_SIZE) ?  0 : 1)

//--PREPARES

#define PREP_MES_HEADER 6 // opcode(1), coalesce_num(1) l_id (4)
#define PREP_SIZE (KEY_SIZE + 2 + VALUE_SIZE + 8) // Size of a write
#define LDR_PREP_SEND_SIZE (PREP_MES_HEADER + (MAX_PREP_COALESCE * PREP_SIZE))
#define FLR_PREP_RECV_SIZE (GRH_SIZE + LDR_PREP_SEND_SIZE)

#define LEADER_PREPARE_ENABLE_INLINING ((LDR_PREP_SEND_SIZE > MAXIMUM_INLINE_SIZE) ?  0 : 1)


//---------LEADER-----------------------
// PREP_ACK_QP_ID 0: send Prepares -- receive ACKs
#define LDR_MAX_PREP_WRS (MESSAGES_IN_BCAST_BATCH)
#define LDR_MAX_RECV_ACK_WRS (3 * FOLLOWER_MACHINE_NUM * PREPARE_CREDITS)
// COMMIT_W_QP_ID 1: send Commits  -- receive Writes
#define LDR_MAX_COM_WRS (MESSAGES_IN_BCAST_BATCH)
#define LDR_MAX_RECV_W_WRS (FOLLOWER_MACHINE_NUM * W_CREDITS)
// Credits WRs
#define LDR_MAX_CREDIT_WRS ((W_CREDITS / LDR_CREDITS_IN_MESSAGE ) * FOLLOWER_MACHINE_NUM)
#define LDR_MAX_CREDIT_RECV ((COMMIT_CREDITS / FLR_CREDITS_IN_MESSAGE ) * FOLLOWER_MACHINE_NUM)


//--------FOLLOWER--------------
// // PREP_ACK_QP_ID 0: receive Prepares -- send ACKs
#define FLR_MAX_ACK_WRS (1)
#define FLR_MAX_RECV_PREP_WRS (3 * PREPARE_CREDITS) // if not enough prep messges get lost
// COMMIT_W_QP_ID 1: send Writes  -- receive Commits
#define FLR_MAX_W_WRS (W_CREDITS)
#define FLR_MAX_RECV_COM_WRS (COMMIT_CREDITS)
// Credits WRs
#define FLR_MAX_CREDIT_WRS 1 //(COMMIT_CREDITS / FLR_CREDITS_IN_MESSAGE )
#define FLR_MAX_CREDIT_RECV (W_CREDITS / LDR_CREDITS_IN_MESSAGE)
#define ACK_SEND_SS_BATCH MAX(MIN_SS_BATCH, (FLR_MAX_ACK_WRS + 2))

//-- LEADER

#define LEADER_ACK_BUF_SLOTS (2 * FOLLOWER_MACHINE_NUM * PREPARE_CREDITS)
#define LEADER_ACK_BUF_SIZE (LDR_ACK_RECV_SIZE * LEADER_ACK_BUF_SLOTS)
#define LEADER_W_BUF_SLOTS (2 * FOLLOWER_MACHINE_NUM * W_CREDITS)
#define LEADER_W_BUF_SIZE (LDR_W_RECV_SIZE * LEADER_W_BUF_SLOTS)

#define LEADER_BUF_SIZE (LEADER_W_BUF_SIZE + LEADER_ACK_BUF_SIZE)
#define LEADER_BUF_SLOTS (LEADER_W_BUF_SLOTS + LEADER_ACK_BUF_SLOTS)

#define LEADER_REMOTE_W_SLOTS (FOLLOWER_MACHINE_NUM * W_CREDITS * MAX_W_COALESCE)
#define LEADER_PENDING_WRITES (SESSIONS_PER_THREAD + LEADER_REMOTE_W_SLOTS + 1)
#define PREP_FIFO_SIZE (LEADER_PENDING_WRITES)


//--FOLLOWER
#define FLR_PREP_BUF_SLOTS (3 * PREPARE_CREDITS)
#define FLR_PREP_BUF_SIZE (FLR_PREP_RECV_SIZE * FLR_PREP_BUF_SLOTS)
#define FLR_COM_BUF_SLOTS (COMMIT_CREDITS)
#define FLR_COM_BUF_SIZE (FLR_COM_RECV_SIZE * FLR_COM_BUF_SLOTS)
#define FLR_BUF_SIZE (FLR_PREP_BUF_SIZE + FLR_COM_BUF_SIZE)
#define FLR_BUF_SLOTS (FLR_PREP_BUF_SLOTS + FLR_COM_BUF_SLOTS)
#define W_FIFO_SIZE (SESSIONS_PER_THREAD + 1)
#define MAX_PREP_BUF_SLOTS_TO_BE_POLLED (2 * PREPARE_CREDITS)
#define FLR_PENDING_WRITES (2 * PREPARE_CREDITS * MAX_PREP_COALESCE) // 2/3 of the buffer
#define FLR_DISALLOW_OUT_OF_ORDER_PREPARES 1

#define MAX_LIDS_IN_A_COMMIT MIN(FLR_PENDING_WRITES, LEADER_PENDING_WRITES)



#define ZK_TRACE_BATCH SESSIONS_PER_THREAD
#define ZK_UPDATE_BATCH  MAX (FLR_PENDING_WRITES, LEADER_PENDING_WRITES)
/*-------------------------------------------------
-----------------QUEUE DEPTHS-------------------------
--------------------------------------------------*/

#define COM_CREDIT_SS_BATCH MAX(MIN_SS_BATCH, (FLR_MAX_CREDIT_WRS + 1))
#define WRITE_SS_BATCH MAX(MIN_SS_BATCH, (FLR_MAX_W_WRS + 1))


#define FOLLOWER_QP_NUM 3 /* The number of QPs for the follower */
#define LEADER_QP_NUM 3 /* The number of QPs for the leader */
#define QP_NUM 3
#define PREP_ACK_QP_ID 0
#define COMMIT_W_QP_ID 1
#define FC_QP_ID 2

/*
 * -------LEADER-------------
 * 1st Dgram send Prepares -- receive ACKs
 * 2nd Dgram send Commits  -- receive Writes
 * 3rd Dgram  -- receive Credits
 *
 * ------FOLLOWER-----------
 * 1st Dgram receive prepares -- send Acks
 * 2nd Dgram receive Commits  -- send Writes
 * 3rd Dgram  send Credits
 * */

// LDR - Receive
#define LDR_RECV_ACK_Q_DEPTH (LDR_MAX_RECV_ACK_WRS + 3)
#define LDR_RECV_W_Q_DEPTH  (LDR_MAX_RECV_W_WRS + 3) //
#define LDR_RECV_CR_Q_DEPTH (LDR_MAX_CREDIT_RECV + 3) //()
// LDR - Send
#define LDR_SEND_PREP_Q_DEPTH ((PREP_BCAST_SS_BATCH * FOLLOWER_MACHINE_NUM) + 10 ) //
#define LDR_SEND_COM_Q_DEPTH ((COM_BCAST_SS_BATCH * FOLLOWER_MACHINE_NUM) + 10 ) //
#define LDR_SEND_CR_Q_DEPTH 1 //()

// FLR - Receive
#define FLR_RECV_PREP_Q_DEPTH (FLR_MAX_RECV_PREP_WRS + 3) //
#define FLR_RECV_COM_Q_DEPTH (FLR_MAX_RECV_COM_WRS + 3) //
#define FLR_RECV_CR_Q_DEPTH 1 //()
// FLR - Send
#define FLR_SEND_ACK_Q_DEPTH (ACK_SEND_SS_BATCH + 3) //
#define FLR_SEND_W_Q_DEPTH (WRITE_SS_BATCH + 3) //
#define FLR_SEND_CR_Q_DEPTH (COM_CREDIT_SS_BATCH + 3) //


// DEBUG

#define FLR_CHECK_DBG_COUNTERS 1




/* SHM key for the 1st request region created by master. ++ for other RRs.*/
#define MASTER_SHM_KEY 24


//Defines for parsing the trace
#define _200_K 200000
#define MAX_TRACE_SIZE _200_K
#define TRACE_SIZE K_128 // used only when manufacturing a trace
#define NOP 0



/*
 *  SENT means we sent the prepare message // OR an ack has been sent
 *  READY means all acks have been gathered // OR a commit has been received
 *  SEND_COMMITS menas it has been propagated to the
 *  cache and commits should be sent out
 * */
enum write_state {INVALID, VALID, SENT, READY, SEND_COMMITTS};


// The format of an ack message
struct ack_message {
	uint8_t local_id[8]; // the first local id that is being acked
  uint8_t follower_id;
  uint8_t opcode;
  uint16_t ack_num;

};


struct ack_message_ud_req {
	uint8_t grh[GRH_SIZE];
  struct ack_message ack;

 };


// The format of a commit message
struct com_message {
  uint8_t l_id[8];
  uint16_t com_num;
	uint16_t opcode;

};

// commit message plus the grh
struct com_message_ud_req {
	uint8_t grh[GRH_SIZE];
  struct com_message com;

};

struct prepare {
	uint8_t flr_id;
  uint8_t unused;
  uint16_t sess_id;
	uint8_t g_id[4]; //send the bottom half of the gid
	uint8_t key[8];
	uint8_t opcode; //override opcode
	uint8_t val_len;
	uint8_t value[VALUE_SIZE];
} __attribute__((__packed__));

// prepare message
struct prep_message {
	uint8_t opcode;
	uint8_t coalesce_num;
	uint8_t l_id[4]; // send the bottom half of the lid
	struct prepare prepare[MAX_PREP_COALESCE];
};

struct prep_message_ud_req {
	uint8_t grh[GRH_SIZE];
	struct prep_message prepare;
};


struct write {
  uint8_t w_num; // the first write holds the coalesce number for the entire message
  uint8_t flr_id;
  uint8_t unused[2];
  uint32_t sess_id;
  uint8_t key[KEY_SIZE];	/* 8B */
  uint8_t opcode;
  uint8_t val_len;
  uint8_t value[VALUE_SIZE];
} __attribute__((__packed__));

typedef  struct w_message {
  struct write write[MAX_W_COALESCE];
} zk_w_mes_t;


struct w_message_ud_req {
  uint8_t unused[GRH_SIZE];
  struct w_message w_mes;
};



// The entires in the commit prep_message are distinct batches of commits
typedef struct commit_fifo {
  struct com_message *commits;
  uint16_t push_ptr;
  uint16_t pull_ptr;
  uint32_t size; // number of commits rather than  messages
} com_fifo_t;

struct fifo {
	void *fifo;
	uint32_t push_ptr;
	uint32_t pull_ptr;
	uint32_t size;

};


struct prep_fifo {
	struct prep_message *prep_message;
	uint32_t push_ptr;
	uint32_t pull_ptr;
	uint32_t bcast_pull_ptr;
	uint32_t bcast_size; // number of prepares not messages!
	uint32_t size;
	uint32_t backward_ptrs[PREP_FIFO_SIZE];
};


// A data structute that keeps track of the outstanding writes
typedef struct pending_writes {
	uint64_t *g_id;
	struct prep_fifo *prep_fifo;
  struct fifo *w_fifo;
	struct prepare **ptrs_to_ops;
	uint64_t local_w_id;
	uint32_t *session_id;
	enum write_state *w_state;
	uint32_t push_ptr;
	uint32_t pull_ptr;
	uint32_t prep_pull_ptr; // Where to pull prepares from
	uint32_t size;
	uint32_t unordered_ptr;
	uint8_t *flr_id;
	uint8_t *acks_seen;
//  uint8_t *ack_bit_vectors;
	bool *is_local;
	bool *session_has_pending_write;
	bool all_sessions_stalled;
} p_writes_t;


// struct for the follower to keep track of the acks it has sent
typedef struct pending_acks {
	uint32_t slots_ahead;
	uint32_t acks_to_send;

} p_acks_t;


typedef struct zk_trace_op {
	uint16_t session_id;
	struct key key;
	uint8_t opcode;// if the opcode is 0, it has never been RMWed, if it's 1 it has
	uint8_t val_len; // this represents the maximum value len
	uint8_t value[VALUE_SIZE]; // if it's an RMW the first 4 bytes point to the entry
	uint8_t *value_to_write;
	uint8_t *value_to_read; //compare value for CAS/  addition argument for F&A
	uint32_t index_to_req_array;
	uint32_t real_val_len; // this is the value length the client is interested in
} zk_trace_op_t;

typedef struct zk_resp {
	uint8_t type;
} zk_resp_t;

typedef struct thread_stats { // 2 cache lines
	long long cache_hits_per_thread;
	long long remotes_per_client;
	long long locals_per_client;

	long long preps_sent;
	long long acks_sent;
	long long coms_sent;
  long long writes_sent;

  long long preps_sent_mes_num;
  long long acks_sent_mes_num;
  long long coms_sent_mes_num;
  long long writes_sent_mes_num;


  long long received_coms;
	long long received_acks;
	long long received_preps;
  long long received_writes;

  long long received_coms_mes_num;
  long long received_acks_mes_num;
  long long received_preps_mes_num;
  long long received_writes_mes_num;


	uint64_t batches_per_thread; // Leader only
  uint64_t total_writes; // Leader only

	uint64_t stalled_gid;
  uint64_t stalled_ack_prep;
  uint64_t stalled_com_credit;
	//long long unused[3]; // padding to avoid false sharing
} thread_stats_t;

//extern remote_qp_t remote_follower_qp[FOLLOWER_MACHINE_NUM][FOLLOWERS_PER_MACHINE][FOLLOWER_QP_NUM];
//extern remote_qp_t remote_leader_qp[LEADERS_PER_MACHINE][LEADER_QP_NUM];
extern thread_stats_t t_stats[WORKERS_PER_MACHINE];
struct mica_op;
extern atomic_uint_fast64_t global_w_id, committed_global_w_id;
extern bool is_leader;


void *follower(void *arg);
void *leader(void *arg);
void print_latency_stats(void);



#endif

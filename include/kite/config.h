//
// Created by vasilis on 27/04/20.
//

#ifndef KITE_CONFIG_H
#define KITE_CONFIG_H


#ifndef _GNU_SOURCE
# define _GNU_SOURCE
#endif





// Generic header files
#include "../general_util/top.h"
#include "opcodes.h"



// CORE CONFIGURATION
#define WRITE_RATIO 500 //Warning write ratio is given out of a 1000, e.g 10 means 10/1000 i.e. 1%
#define MEASURE_LATENCY 0
#define LATENCY_MACHINE 0
#define LATENCY_THREAD 15
#define MEASURE_READ_LATENCY 2 // 2 means mixed
#define R_CREDITS 3 //
#define W_CREDITS 8
#define MAX_READ_SIZE 300 //300 in terms of bytes for Reads/Acquires/RMW-Acquires/Proposes
#define MAX_WRITE_SIZE 800 // only writes 400 -- only rmws 1200 in terms of bytes for Writes/Releases/Accepts/Commits
#define MIN_SS_BATCH 127// The minimum SS batch
#define ENABLE_STAT_COUNTING 1
#define MAX_OP_BATCH_ 51
#define SC_RATIO_ 500// this is out of 1000, e.g. 10 means 1%
#define ENABLE_RELEASES_ 1
#define ENABLE_ACQUIRES_ 1
#define RMW_RATIO 1000// this is out of 1000, e.g. 10 means 1%
#define ENABLE_RMWS_ 1
#define FEED_FROM_TRACE 0 // used to enable skew++
#define ENABLE_MS_MEASUREMENTS 0 // finer granularity measurements


#define MEASURE_SLOW_PATH 0




// Important Knobs

#define ENABLE_LOCK_FREE_READING 1
#define ENABLE_COMMITS_WITH_NO_VAL 1
#define ENABLE_CAS_CANCELLING 1
#define ENABLE_ADAPTIVE_INLINING 0 // This did not help
#define PUT_A_MACHINE_TO_SLEEP 0
#define MACHINE_THAT_SLEEPS 1
#define ENABLE_ALL_ABOARD 1
#define EMULATE_ABD 0
#define TURN_OFF_KITE_ 1
#define TURN_OFF_KITE (EMULATE_ABD ? 1 : TURN_OFF_KITE_)
#define ACCEPT_IS_RELEASE 1

// TIMEOUTS
#define WRITE_FIFO_TIMEOUT M_1
#define RMW_BACK_OFF_TIMEOUT 1500 //K_32 //K_32// M_1
#define CREDIT_TIMEOUT  M_16 // B_4_EXACT //
#define ALL_ABOARD_TIMEOUT_CNT K_16
#define LOG_TOO_HIGH_TIME_OUT 10




// Where to BIND the KVS
#define KVS_SOCKET 0// (WORKERS_PER_MACHINE < 30 ? 0 : 1 )// socket where the cache is bind

// PRINTS -- STATS
#define EXIT_ON_PRINT 0
#define PRINT_NUM 4
#define VERIFY_PAXOS 0
#define PRINT_LOGS 0
#define COMMIT_LOGS 0
#define DUMP_STATS_2_FILE 0


// DEBUG
#define DEBUG_SEQLOCKS 0






//////////////////////////////////////////////////////
/////////////~~~~STRUCTS~~~~~~/////////////////////////
//////////////////////////////////////////////////////




//enum op_state {INVALID_, VALID_, SENT_, READY_, SEND_COMMITTS};
typedef enum compare_t{SMALLER, EQUAL, GREATER, ERROR} compare_t;

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
typedef struct rmw_id {
  //uint32_t glob_sess_id; // global session id
  uint64_t id; // the local rmw id of the source
} rmw_id_t;


// flags that help to compare TS
#define REGULAR_TS 0
#define NETW_TS 1
#define META_TS 2

// format of a Timestamp tuple (Lamport clock)
struct network_ts_tuple {
  uint8_t m_id;
  uint32_t version;
} __attribute__((__packed__));

struct ts_tuple {
  uint8_t m_id;
  uint32_t version;
};


typedef atomic_uint_fast64_t seqlock_t;

#define MICA_VALUE_SIZE (VALUE_SIZE + (FIND_PADDING_CUST_ALIGN(VALUE_SIZE, 32)))
#define MICA_OP_SIZE_  (100 + (2 * (MICA_VALUE_SIZE)))
#define MICA_OP_PADDING_SIZE  (FIND_PADDING(MICA_OP_SIZE_))

#define MICA_OP_SIZE  (MICA_OP_SIZE_ + MICA_OP_PADDING_SIZE)
struct mica_op {
  // Cache-line -1
  uint8_t value[MICA_VALUE_SIZE];
  uint8_t last_accepted_value[MICA_VALUE_SIZE];


  // Cache-line -2
  struct key key;
  seqlock_t seqlock;

  uint8_t opcode; // what kind of RMW
  uint8_t state;
  uint8_t unused[2];

  // BYTES: 20 - 32
  uint32_t log_no; // keep track of the biggest log_no that has not been committed
  uint32_t accepted_log_no; // not really needed, but good for debug
  uint32_t last_committed_log_no;

  // BYTES: 32 - 64 -- each takes 8
  struct ts_tuple ts; // base base_ts
  struct ts_tuple prop_ts;
  struct ts_tuple accepted_ts;
  struct ts_tuple base_acc_ts;


  // Cache-line 3 -- each rmw_id takes up 8 bytes
  struct rmw_id rmw_id;
  //struct rmw_id last_registered_rmw_id; // i was using it to put in accepts, when accepts carried last-registered-rmw-id
  struct rmw_id last_committed_rmw_id;
  struct rmw_id accepted_rmw_id; // not really needed, but useful for debugging
  uint64_t epoch_id;
  uint32_t key_id; // strictly for debug

  uint8_t padding[MICA_OP_PADDING_SIZE];
};






#endif //KITE_CONFIG_H

//
// Created by vasilis on 23/06/2020.
//

#ifndef KITE_TOP_H
#define KITE_TOP_H

#include "sizes.h"
#include "generic_macros.h"
#include "generic_opcodes.h"
#include "stats.h"


#include <pthread.h>
#include <sched.h>
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <numaif.h>
#include <malloc.h>
#include <infiniband/verbs.h>
#include <stdatomic.h>


// Multicast
#include <rdma/rdma_cma.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <byteswap.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdbool.h>

enum {error_sys, kite_sys, zookeeper_sys};

#ifdef KITE
#define COMPILED_SYSTEM kite_sys
#endif


#ifdef ZOOKEEPER
#define COMPILED_SYSTEM zookeeper_sys
#endif

#ifndef KITE
#ifndef ZOOKEEPER
#define COMPILED_SYSTEM kite_sys
#endif
#endif



// Stats thread
void *print_stats(void*);
void *client(void *);

//Forward declaring
typedef struct key mica_key_t;

#define USE_BIG_OBJECTS 0
#define EXTRA_CACHE_LINES 0
#define BASE_VALUE_SIZE 32
#define SHIFT_BITS (USE_BIG_OBJECTS == 1 ? 3 : 0) // number of bits to shift left or right to calculate the value length
#define VALUE_SIZE_ (USE_BIG_OBJECTS ? ((EXTRA_CACHE_LINES * 64) + BASE_VALUE_SIZE) : BASE_VALUE_SIZE) //(169 + 64)// 46 + 64 + 64//32 //(46 + 64)
#define VALUE_SIZE (VALUE_SIZE_ + (FIND_PADDING_CUST_ALIGN(VALUE_SIZE_, 8)))
#define KVS_NUM_KEYS (1 * MILLION)


//-------------------------------------------
/* ----------SYSTEM------------------------ */
//-------------------------------------------
#define TOTAL_CORES 40
#define TOTAL_CORES_ (TOTAL_CORES - 1)
#define SOCKET_NUM 2
#define PHYSICAL_CORES_PER_SOCKET 10
#define LOGICAL_CORES_PER_SOCKET 20
#define PHYSICAL_CORE_DISTANCE 2 // distance between two physical cores of the same socket
#define WORKER_HYPERTHREADING 0 // schedule two threads on the same core
#define MAX_SERVER_PORTS 1 //
// Where to BIND the KVS
#define KVS_SOCKET 0// (WORKERS_PER_MACHINE < 30 ? 0 : 1 )// socket where the cache is bind

// CORE CONFIGURATION
#define WORKERS_PER_MACHINE 10
#define MACHINE_NUM 5
#define SESSIONS_PER_THREAD 22
#define ENABLE_CLIENTS 1
#define CLIENTS_PER_MACHINE_ 5
#define CLIENTS_PER_MACHINE (ENABLE_CLIENTS ? CLIENTS_PER_MACHINE_ : 0)
#define MAX_OP_BATCH SESSIONS_PER_THREAD
#define ENABLE_LOCK_FREE_READING 1

#define PUT_A_MACHINE_TO_SLEEP 0
#define MACHINE_THAT_SLEEPS 1

#define MEASURE_LATENCY 0
#define LATENCY_MACHINE 0
#define LATENCY_THREAD 15
#define MEASURE_READ_LATENCY 2 // 2 means mixed
#define ENABLE_STAT_COUNTING 1

#define CREDIT_TIMEOUT  M_16 // B_4_EXACT //

// PRINTS -- STATS
#define EXIT_ON_PRINT 0
#define PRINT_NUM 4
#define ENABLE_MS_MEASUREMENTS 0 // finer granularity measurements
#define SHOW_STATS_LATENCY_STYLE 1


// QUORUM
#define QUORUM_NUM ((MACHINE_NUM / 2) + 1)
#define REMOTE_QUORUM ((QUORUM_NUM) - 1)

//-------------------------------------------
/* ----------TRACE------------------------ */
//-------------------------------------------
#define WRITE_RATIO 1000 //Warning write ratio is given out of a 1000, e.g 10 means 10/1000 i.e. 1%
#define SC_RATIO 500// this is out of 1000, e.g. 10 means 1%
#define ENABLE_RELEASES (1 && COMPILED_SYSTEM == kite_sys)
#define ENABLE_ACQUIRES (1 && COMPILED_SYSTEM == kite_sys)
#define RMW_RATIO 1000// this is out of 1000, e.g. 10 means 1%
#define ENABLE_RMWS (1 && COMPILED_SYSTEM == kite_sys)
#define FEED_FROM_TRACE 0 // used to enable skew++
// RMW TRACE
#define ENABLE_NO_CONFLICT_RMW 0 // each thread rmws a different key
#define ENABLE_ALL_CONFLICT_RMW 0 // all threads do rmws to one key (0)
#define ENABLE_SINGLE_KEY_RMW 0
#define ALL_RMWS_SINGLE_KEY 0 //  all threads do only rmws to one key (0)
#define RMW_ONE_KEY_PER_THREAD 0 // thread t_id rmws key t_id
//#define RMW_ONE_KEY_PER_SESSION 1 // session id rmws key t_id
#define TRACE_ONLY_CAS 0
#define TRACE_ONLY_FA 1
#define TRACE_MIXED_RMWS 0
#define TRACE_CAS_RATIO 500 // out of a 1000
#define RMW_CAS_CANCEL_RATIO 400 // out of 1000
#define USE_WEAK_CAS 1
#define USE_A_SINGLE_KEY 0
#define TRACE_SIZE K_128
#define NOP 0

// HELPING CONSTANTS DERIVED FROM CORE CONFIGURATION
#define TOTAL_THREADS (WORKERS_PER_MACHINE + CLIENTS_PER_MACHINE)
#define REM_MACH_NUM (MACHINE_NUM - 1) // Number of remote machines
#define SESSIONS_PER_MACHINE (WORKERS_PER_MACHINE * SESSIONS_PER_THREAD)
#define SESSIONS_PER_CLIENT_ (SESSIONS_PER_MACHINE / CLIENTS_PER_MACHINE_)
#define SESSIONS_PER_CLIENT MAX(1, SESSIONS_PER_CLIENT_)
#define WORKERS_PER_CLIENT (ENABLE_CLIENTS ? (WORKERS_PER_MACHINE / CLIENTS_PER_MACHINE ) : 0)
#define GLOBAL_SESSION_NUM (MACHINE_NUM * SESSIONS_PER_MACHINE)
#define WORKER_NUM (WORKERS_PER_MACHINE * MACHINE_NUM)


#define ENABLE_ASSERTIONS 1
#define ENABLE_ADAPTIVE_INLINING 0 // This did not help
/*-------------------------------------------------
-----------------DEBUGGING-------------------------
--------------------------------------------------*/
//It may be that ENABLE_ASSERTIONS  must be up for these to work
#define DEBUG_PREPARES 0 // zookeeper only
#define DEBUG_COMMITS 0
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
#define ENABLE_DEBUG_RMW_KV_PTR 0
#define DEBUG_SEQLOCKS 0


/* Request sizes */
#define KEY_SIZE 8 //
#define GRH_SIZE 40 // Global Routing Header
#define MTU 4096

#define MAXIMUM_INLINE_SIZE 188
#define DEFAULT_SL 0 //default service level



/*-------------------------------------------------
	-----------------CLIENT---------------------------
--------------------------------------------------*/
enum {
  CLIENT_USE_TRACE,
  CLIENT_UI,
  BLOCKING_TEST_CASE,
  ASYNC_TEST_CASE,
  TREIBER_BLOCKING,
  TREIBER_DEBUG,
  TREIBER_ASYNC, // Treiber Stack
  MSQ_ASYNC, // Michael & Scott Queue
  HML_ASYNC, // Harris & Michael List
  PRODUCER_CONSUMER
};

#define CLIENT_MODE CLIENT_USE_TRACE

#define TREIBER_WRITES_NUM 1
#define TREIBER_NO_CONFLICTS 0
#define ENABLE_TR_ASSERTIONS_ 1
#define ENABLE_TR_ASSERTIONS (ENABLE_CLIENTS && CLIENT_MODE == TREIBER_ASYNC ? ENABLE_TR_ASSERTIONS_ : 0)

#define MS_WRITES_NUM 1
#define MS_NO_CONFLICT 0
#define ENABLE_MS_ASSERTIONS_ 0
#define ENABLE_MS_ASSERTIONS (ENABLE_CLIENTS && CLIENT_MODE == MSQ_ASYNC ? ENABLE_MS_ASSERTIONS_ : 0)
#define CLIENT_LOGS 0

#define HM_NO_CONFLICT 0
#define HM_WRITES_NUM 4

#define PC_WRITES_NUM 5
#define PC_IDEAL 0

#define PER_SESSION_REQ_NUM (MS_WRITES_NUM + 4) //(HM_WRITES_NUM + 15) //(TREIBER_WRITES_NUM + 3) //   (HM_WRITES_NUM + 15) //   ((2 * PC_WRITES_NUM) + 5)
#define CLIENT_DEBUG 0






/*-------------------------------------------------
	-----------------TRACE-----------------
--------------------------------------------------*/
#define SKEW_EXPONENT_A 90 // representation divided by 100 (i.e. 99 means a = 0.99)

/* SHM key for the 1st request region created by master. ++ for other RRs.*/
#define MASTER_SHM_KEY 24

/*-------------------------------------------------
	-----------------MULTICAST-------------------------
--------------------------------------------------*/
// Multicast defines are not used, but are kept them for possible extension
#define ENABLE_MULTICAST_ 1
#define ENABLE_MULTICAST (COMPILED_SYSTEM  ==  kite_sys ? 0 : ENABLE_MULTICAST_)
#define MULTICAST_TESTING_ 0
#define MULTICAST_TESTING (ENABLE_MULTICAST == 1 ? MULTICAST_TESTING_ : 0)
#define MCAST_QPS MACHINE_NUM

#define MCAST_QP_NUM 2
#define PREP_MCAST_QP 0
#define COM_MCAST_QP 1 //
#define MCAST_GROUPS_NUM 2

// This helps us set up the necessary rdma_cm_ids for the multicast groups
struct cm_qps
{
  int receive_q_depth;
  struct rdma_cm_id* cma_id;
  bool accepted;
  bool established;
  struct ibv_pd* pd;
  struct ibv_cq* cq;
  struct ibv_mr* mr;
  void *mem;
};

typedef struct  {
  struct rdma_event_channel *channel;
  struct sockaddr_storage dst_in[REM_MACH_NUM];
  struct sockaddr *dst_addr[REM_MACH_NUM];
  struct sockaddr_storage src_in;
  struct sockaddr *src_addr;
  struct cm_qps cm_qp[REM_MACH_NUM];
  //Send-only stuff
  struct rdma_ud_param mcast_ud_param[MACHINE_NUM];
} connect_cm_info_t;

// This helps us set up the multicasts
typedef struct mcast_info
{
  int	t_id;
  struct rdma_event_channel *channel;
  struct sockaddr_storage dst_in[MCAST_GROUPS_NUM];
  struct sockaddr *dst_addr[MCAST_GROUPS_NUM];
  struct sockaddr_storage src_in;
  struct sockaddr *src_addr;
  struct cm_qps cm_qp[MCAST_QPS];
  //Send-only stuff
  struct rdma_ud_param mcast_ud_param[MCAST_GROUPS_NUM];

} mcast_info_t;

// this contains all data we need to perform our mcasts
typedef struct mcast_essentials {
  struct ibv_cq *recv_cq[MCAST_QP_NUM];
  struct ibv_qp *recv_qp[MCAST_QP_NUM];
  struct ibv_mr *recv_mr;
  struct ibv_ah *send_ah[MCAST_QP_NUM];
  uint32_t qpn[MCAST_QP_NUM];
  uint32_t qkey[MCAST_QP_NUM];
} mcast_essentials_t;


/*-------------------------------------------------
	-----------------BROADCAST-------------------------
--------------------------------------------------*/
#define MAX_BCAST_BATCH (ENABLE_MULTICAST == 1 ? 4 : 4) //how many broadcasts can fit in a batch
#define MESSAGES_IN_BCAST (ENABLE_MULTICAST == 1 ? 1 : (REM_MACH_NUM))
#define MESSAGES_IN_BCAST_BATCH MAX_BCAST_BATCH * MESSAGES_IN_BCAST //must be smaller than the q_depth

//////////////////////////////////////////////////////
/////////////~~~~GLOBALS~~~~~~/////////////////////////
//////////////////////////////////////////////////////

/* info about a QP that msut be shared in intit phase */
typedef struct qp_attr {
  // ROCE
  uint64_t gid_global_interface_id;	// Needed for RoCE only
  uint64_t gid_global_subnet_prefix; 	// Needed for RoCE only
  //
  int lid;
  int qpn;
  uint8_t sl;
} qp_attr_t;

typedef struct {
  size_t size;
  void *buf;
  qp_attr_t ***wrkr_qp; //wrkr_qp[MACHINE_NUM][WORKERS_PER_MACHINE][4]; //
} all_qp_attr_t;

/* ah pointer and qpn are accessed together in the critical path
   so we are putting them in the same kvs line */
typedef struct remote_qp {
  struct ibv_ah *ah;
  int qpn;
  // no padding needed- false sharing is not an issue, only fragmentation
} remote_qp_t;

extern remote_qp_t ***rem_qp; //[MACHINE_NUM][WORKERS_PER_MACHINE][QP_NUM];
extern int is_roce, machine_id, num_threads;
extern char **remote_ips, *local_ip, *dev_name;
extern all_qp_attr_t *all_qp_attr;
extern atomic_uint_fast32_t workers_with_filled_qp_attr;
extern atomic_bool print_for_debug;
extern atomic_bool qps_are_set_up;
extern FILE* client_log[CLIENTS_PER_MACHINE];
extern uint64_t time_approx;
extern struct latency_counters latency_count;
extern c_stats_t c_stats[CLIENTS_PER_MACHINE];

typedef struct trace_command {
  uint8_t opcode;
  uint8_t key_hash[8];
  uint32_t key_id;
} trace_t;

typedef struct thread_params {
  int id;
} thread_params_t;

//////////////////////////////////////////////////////
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

typedef struct key {
  unsigned int bkt			:32;
  unsigned int server			:16;
  unsigned int tag			:16;
} mica_key_t;

typedef atomic_uint_fast64_t seqlock_t;
typedef struct mica_op mica_op_t;

typedef struct recv_info {
  uint32_t push_ptr;
  uint32_t buf_slots;
  uint32_t slot_size;
  uint32_t posted_recvs;
  struct ibv_recv_wr *recv_wr;
  struct ibv_qp * recv_qp;
  struct ibv_sge* recv_sgl;
  void* buf;
} recv_info_t;

typedef struct quorum_info {
  uint8_t missing_num;
  uint8_t missing_ids[REM_MACH_NUM];
  uint8_t active_num;
  uint8_t active_ids[REM_MACH_NUM];
  bool send_vector[REM_MACH_NUM];

  // These are not a machine_ids, they ranges= from 0 to REM_MACH_NUM -1
  // to facilitate usage with the ib_send_wrs
  uint8_t first_active_rm_id;
  uint8_t last_active_rm_id;

  // The number of send_wrs we are reconfiguring
  uint8_t num_of_send_wrs;
  struct ibv_send_wr **send_wrs_ptrs;

  //Create the revive condition: which credits must reach what target
  uint8_t num_of_credit_targets;
  uint16_t **credit_ptrs;
  uint16_t *targets;

} quorum_info_t;

//////////////////////////////////////////////////////
/////////////~~~~CLIENT STRUCTS~~~~~~/////////////////////////
//////////////////////////////////////////////////////
#define RAW_CLIENT_OP_SIZE (8 + KEY_SIZE + VALUE_SIZE + 8 + 8)
#define PADDING_BYTES_CLIENT_OP (FIND_PADDING(RAW_CLIENT_OP_SIZE))
#define CLIENT_OP_SIZE (PADDING_BYTES_CLIENT_OP + RAW_CLIENT_OP_SIZE)
typedef struct client_op {
  atomic_uint_fast8_t state;
  uint8_t opcode;
  uint32_t val_len;
  bool* rmw_is_successful;
  mica_key_t key;
  uint8_t *value_to_read;//[VALUE_SIZE]; // expected val for CAS
  uint8_t value_to_write[VALUE_SIZE]; // desired Val for CAS
  uint8_t padding[PADDING_BYTES_CLIENT_OP];
} client_op_t;

#define IF_CLT_PTRS_SIZE (4 * SESSIONS_PER_THREAD) //  4* because client needs 2 ptrs (pull/push) that are 2 bytes each
#define IF_WRKR_PTRS_SIZE (2 * SESSIONS_PER_THREAD) // 2* because client needs 1 ptr (pull) that is 2 bytes
#define PADDING_IF_CLT_PTRS (FIND_PADDING(IF_CLT_PTRS_SIZE))
#define PADDING_IF_WRKR_PTRS (FIND_PADDING(IF_WRKR_PTRS_SIZE))
#define IF_PTRS_SIZE (IF_CLT_PTRS_SIZE + IF_WRKR_PTRS_SIZE + PADDING_IF_CLT_PTRS + PADDING_IF_WRKR_PTRS))
#define INTERFACE_SIZE ((SESSIONS_PER_THREAD * PER_SESSION_REQ_NUM * CLIENT_OP_SIZE) + (IF_PTRS_SIZE)

// wrkr-client interface
struct wrk_clt_if {
  client_op_t req_array[SESSIONS_PER_THREAD][PER_SESSION_REQ_NUM];
  uint16_t clt_push_ptr[SESSIONS_PER_THREAD];
  uint16_t clt_pull_ptr[SESSIONS_PER_THREAD];
  uint8_t clt_ptr_padding[PADDING_IF_CLT_PTRS];
  uint16_t wrkr_pull_ptr[SESSIONS_PER_THREAD];
  uint8_t wrkr_ptr_padding[PADDING_IF_WRKR_PTRS];
}__attribute__ ((aligned (64)));

extern struct wrk_clt_if interface[WORKERS_PER_MACHINE];

extern uint64_t last_pulled_req[SESSIONS_PER_MACHINE];
extern uint64_t last_pushed_req[SESSIONS_PER_MACHINE];




//////////////////////////////////////////////////////
/////////////~~~~FUNCTIONS~~~~~~/////////////////////////
//////////////////////////////////////////////////////

struct fifo {
  void *fifo;
  uint32_t push_ptr;
  uint32_t pull_ptr;
  uint32_t size;

};


typedef enum {yellow, red, green, cyan, magenta, regular} color_t;
static void my_printf(color_t color, const char *format, ...)
{

  size_t LIMIT = 1000;
  va_list args;
  size_t i;

  char buf1[LIMIT], buf2[LIMIT];
  memset(buf1, 0, LIMIT);
  memset(buf2, 0, LIMIT);

  va_start(args, format);

  /* Marshal the stuff to print in a buffer */
  vsnprintf(buf1, LIMIT, format, args);

  /* Probably a bad check for buffer overflow */
  for(i = LIMIT - 1; i >= LIMIT - 50; i --) {
    assert(buf1[i] == 0);
  }

  /* Add markers for the color and the reset to regular
   * colors found in
   * http://web.theurbanpenguin.com/adding-color-to-your-output-from-c/
   * */
  switch(color) {
    case yellow:
      snprintf(buf2, 1000, "\033[33m%s\033[0m", buf1);
      break;
    case red:
      snprintf(buf2, 1000, "\033[31m%s\033[0m", buf1);
      break;
    case green:
      // snprintf(buf2, 1000, "\033[32m%s\033[0m", buf1); // darker green
      snprintf(buf2, 1000, "\033[1m\033[32m%s\033[0m", buf1);
      break;
    case cyan :
      snprintf(buf2, 1000, "\033[1m\033[36m%s\033[0m", buf1);
      //snprintf(buf2, 1000, "\033[36m%s\033[0m", buf1); //darker cyan
      break;
    case magenta:
      snprintf(buf2, 1000, "\033[35m%s\033[0m", buf1);
      break;
    case regular:
      snprintf(buf2, 1000, "\033[0m%s\033[0m", buf1);
      break;
    default:
      printf("Wrong printf color /%d \n", color);
      assert(false);
  }

  /* Probably another bad check for buffer overflow */
  for(i = LIMIT - 1; i >= LIMIT - 50; i --) {
    assert(buf2[i] == 0);
  }

  printf("%s", buf2);

  va_end(args);
}


// first argument here should be the state and then a bunch of allowed flags
static inline void check_state_with_allowed_flags(int num_of_flags, ...)
{
  if (ENABLE_ASSERTIONS) {
    va_list valist;
    va_start(valist, num_of_flags);
    bool found = false;
    int state = va_arg(valist, int);
    const uint16_t max_num_flags = 20;
    assert(num_of_flags < max_num_flags);
    int flags[max_num_flags];
    for (uint8_t i = 0; i < num_of_flags - 1; i++) {
      flags[i] = va_arg(valist, int);
      if (state == flags[i]) found = true;
    }
    if (!found) {
      my_printf(red, "Checking state failed state: %u, Allowed flags: \n", state);
      for (uint8_t i = 0; i < num_of_flags - 1; i++) {
        my_printf(red, "%u ", flags[i]);
      }
      my_printf(red, "\n");
      assert(false);
    }

    va_end(valist);
  }
}

// first argument here should be the state and then a bunch of disallowed flags
static inline void check_state_with_disallowed_flags(int num_of_flags, ...)
{
  if (ENABLE_ASSERTIONS) {
    va_list valist;
    va_start(valist, num_of_flags);
    bool found = false;
    int state = va_arg(valist, int);
    assert(num_of_flags < 10);
    int flags[10];
    for (uint8_t i = 0; i < num_of_flags - 1; i++) {
      flags[i] = va_arg(valist, int);
      if (state == flags[i]) found = true;
    }
    if (found) {
      my_printf(red, "Checking state failed state: %u, Disallowed flags: \n", state);
      for (uint8_t i = 0; i < num_of_flags - 1; i++) {
        my_printf(red, "%u ", flags[i]);
      }
      my_printf(red, "\n");
      assert(false);
    }

    va_end(valist);
  }
}

#endif //KITE_TOP_H

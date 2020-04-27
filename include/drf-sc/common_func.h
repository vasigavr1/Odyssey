//
// Created by vasilis on 27/04/20.
//

#ifndef KITE_COMMON_FUNC_H
#define KITE_COMMON_FUNC_H


#ifndef _GNU_SOURCE
# define _GNU_SOURCE
#endif



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
#include <time.h>
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

// Generic header files
#include "hrd_sizes.h"
#include "opcodes.h"

#define USE_BIG_OBJECTS 0
#define EXTRA_CACHE_LINES 0
#define BASE_VALUE_SIZE 32
#define SHIFT_BITS (USE_BIG_OBJECTS == 1 ? 3 : 0) // number of bits to shift left or right to calculate the value length
#define VALUE_SIZE (USE_BIG_OBJECTS ? ((EXTRA_CACHE_LINES * 64) + BASE_VALUE_SIZE) : BASE_VALUE_SIZE) //(169 + 64)// 46 + 64 + 64//32 //(46 + 64)

/* Request sizes */
#define KEY_SIZE 16
#define TRUE_KEY_SIZE 8 // the key that is actually used by MICA
#define GRH_SIZE 40 // Global Routing Header
#define MTU 4096

#define MAXIMUM_INLINE_SIZE 188



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
#define MAX_SERVER_PORTS 1 // better not change that

// CORE CONFIGURATION
#define WORKERS_PER_MACHINE 20
#define MACHINE_NUM 3
#define WRITE_RATIO 1000 //Warning write ratio is given out of a 1000, e.g 10 means 10/1000 i.e. 1%
#define SESSIONS_PER_THREAD 40
#define MEASURE_LATENCY 0
#define LATENCY_MACHINE 0
#define LATENCY_THREAD 15
#define MEASURE_READ_LATENCY 2 // 2 means mixed
#define R_CREDITS 4 //
#define W_CREDITS 8
#define MAX_READ_SIZE 300 //300 in terms of bytes for Reads/Acquires/RMW-Acquires/Proposes
#define MAX_WRITE_SIZE 800 // only writes 400 -- only rmws 1200 in terms of bytes for Writes/Releases/Accepts/Commits
#define ENABLE_ASSERTIONS 0
#define USE_QUORUM 1
#define CREDIT_TIMEOUT  M_16 // B_4_EXACT //
#define WRITE_FIFO_TIMEOUT M_1
#define RMW_BACK_OFF_TIMEOUT 1500 //K_32 //K_32// M_1
#define ENABLE_ADAPTIVE_INLINING 0 // This did not help
#define MIN_SS_BATCH 127// The minimum SS batch
#define ENABLE_STAT_COUNTING 1
#define MAX_OP_BATCH_ 51
#define SC_RATIO_ 0// this is out of 1000, e.g. 10 means 1%
#define ENABLE_RELEASES_ 1
#define ENABLE_ACQUIRES_ 1
#define RMW_RATIO 1000// this is out of 1000, e.g. 10 means 1%
#define RMW_ACQUIRE_RATIO 0000 // this is the ratio out of all RMWs and is out of 1000
#define ENABLE_RMWS_ 1
#define ENABLE_RMW_ACQUIRES_ 1
#define EMULATE_ABD 0
#define FEED_FROM_TRACE 0 // used to enable skew++
#define ACCEPT_IS_RELEASE 0
#define PUT_A_MACHINE_TO_SLEEP 0
#define MACHINE_THAT_SLEEPS 1
#define ENABLE_MS_MEASUREMENTS 0 // finer granularity measurements
#define ENABLE_CLIENTS 0
#define CLIENTS_PER_MACHINE_ 5
#define CLIENTS_PER_MACHINE (ENABLE_CLIENTS ? CLIENTS_PER_MACHINE_ : 0)
#define MEASURE_SLOW_PATH 0
#define ENABLE_ALL_ABOARD 1
#define ALL_ABOARD_TIMEOUT_CNT K_16
#define ENABLE_LOCK_FREE_READING 1

// HELPING CONSTANTS DERIVED FROM CORE CONFIGURATION
#define TOTAL_THREADS (WORKERS_PER_MACHINE + CLIENTS_PER_MACHINE)
#define REM_MACH_NUM (MACHINE_NUM - 1) // Number of remote machines
#define SESSIONS_PER_MACHINE (WORKERS_PER_MACHINE * SESSIONS_PER_THREAD)
#define SESSIONS_PER_CLIENT_ (SESSIONS_PER_MACHINE / CLIENTS_PER_MACHINE_)
#define SESSIONS_PER_CLIENT MAX(1, SESSIONS_PER_CLIENT_)
#define WORKERS_PER_CLIENT (ENABLE_CLIENTS ? (WORKERS_PER_MACHINE / CLIENTS_PER_MACHINE ) : 0)
#define GLOBAL_SESSION_NUM (MACHINE_NUM * SESSIONS_PER_MACHINE)
#define WORKER_NUM (WORKERS_PER_MACHINE * MACHINE_NUM)


// Where to BIND the KVS
#define KVS_SOCKET 0// (WORKERS_PER_MACHINE < 30 ? 0 : 1 )// socket where the cache is bind

// PRINTS -- STATS
#define ENABLE_CACHE_STATS 0
#define EXIT_ON_PRINT 0
#define PRINT_NUM 4
#define VERIFY_PAXOS 0
#define PRINT_LOGS 0
#define COMMIT_LOGS 0
#define DUMP_STATS_2_FILE 0



//////////////////////////////////////////////////////
/////////////~~~~MACROS~~~~~~/////////////////////////
//////////////////////////////////////////////////////

#define COMPILER_BARRIER() asm volatile ("" ::: "memory")
#define GET_GLOBAL_T_ID(m_id, t_id) ((m_id * WORKERS_PER_MACHINE) + t_id)
#define MY_ASSERT(COND, STR, ARGS...) \
  if (ENABLE_ASSERTIONS) { if (!(COND)) { red_printf((STR), (ARGS)); assert(false); }}
#define FIND_PADDING(size) ((64 - (size % 64)) % 64)
#define MAX_OF_3(x1, y1, x2) (MAX(x1, y1) > (x2) ? (MAX(x1, y1)) : (x2))
#define MAX_OF_4(x1, y1, x2, y2) (MAX(x1, y1) > MAX(x2, y2) ? (MAX(x1, y1)) : (MAX(x2, y2)))

/* Useful when `x = (x + 1) % N` is done in a loop */
#define MOD_ADD(x, N) do { \
	x = x + 1; \
	if(x == N) { \
		x = 0; \
	} \
} while(0)

#define MOD_ADD_WITH_BASE(x, N, B) do { \
	x = x + 1; \
	if(x == B + N) { \
		x = B; \
	} \
} while(0)

/* Compile time assert. !!(@condition) converts @condition into a 0/1 bool. */
#define ct_assert(condition) ((void) sizeof(char[-1 + 2 * !!(condition)]))

/* Ensure that x is between a and b, inclusive */
#define range_assert(x, a, b) (assert(x >= a && x <= b))

#ifndef likely
#  define likely(x)       __builtin_expect((x), 1)
#endif

#ifndef unlikely
#  define unlikely(x)       __builtin_expect((x), 0)
#endif


/* Compare, print, and exit */
#define CPE(val, msg, err_code) \
	if(unlikely(val)) { fprintf(stderr, msg); fprintf(stderr, " Error %d \n", err_code); \
	exit(err_code);}


#define CEILING(x,y) (((x) + (y) - 1) / (y))
#define MAX(x,y) ((x) > (y) ? (x) : (y))
#define MIN(x,y) (x < y ? x : y)


#define forceinline inline __attribute__((always_inline))
#define _unused(x) ((void)(x))	/* Make production build happy */

/* Is pointer x aligned to A-byte alignment? */
#define is_aligned(x, A) (((uint64_t) x) % A == 0)



//////////////////////////////////////////////////////
/////////////~~~~GLOBALS~~~~~~/////////////////////////
//////////////////////////////////////////////////////

extern int is_roce, machine_id, num_threads;
extern char **remote_ips, *local_ip, *dev_name;



//////////////////////////////////////////////////////
/////////////~~~~STRUCTS~~~~~~/////////////////////////
//////////////////////////////////////////////////////
struct key {
  unsigned int bkt			:32;
  unsigned int server			:16;
  unsigned int tag			:16;
};


/* Fixed-w_size 16 byte keys */
typedef struct {
  //unsigned long long __unused	:64;
  unsigned int bkt			:32;
  unsigned int server			:16;
  unsigned int tag			:16;
} mica_key_t;

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

struct net_rmw_id {
  uint16_t glob_sess_id; // global session id
  uint64_t id; // the local rmw id of the source
}__attribute__((__packed__));


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
#define MICA_OP_SIZE_  (128 + (2 * (VALUE_SIZE)))
#define MICA_OP_PADDING_SIZE  (FIND_PADDING(MICA_OP_SIZE_))
#define MICA_OP_SIZE  (MICA_OP_SIZE_ + MICA_OP_PADDING_SIZE)
typedef struct  {
  seqlock_t seqlock;
  struct rmw_id last_committed_rmw_id;
  uint32_t last_committed_log_no;
  uint8_t value[VALUE_SIZE];
  struct key key;
  uint8_t opcode; // what kind of RMW
  uint8_t state;
  uint32_t log_no; // keep track of the biggest log_no that has not been committed
  uint32_t last_registered_log_no;
  uint32_t accepted_log_no; // not really needed, but good for debug
  struct ts_tuple ts;
  struct ts_tuple new_ts;
  struct ts_tuple accepted_ts; // really needed


  struct rmw_id rmw_id;

  struct rmw_id last_registered_rmw_id;
  struct rmw_id accepted_rmw_id; // not really needed, but good for debug
  uint32_t dbg_cntr_1;
  uint32_t dbg_cntr_2;
  uint8_t last_accepted_value[VALUE_SIZE]; // last accepted
  uint8_t padding[MICA_OP_PADDING_SIZE];
  uint16_t epoch_id;
} mica_op_t;



//////////////////////////////////////////////////////
/////////////~~~~FUNCTIONS~~~~~~/////////////////////////
//////////////////////////////////////////////////////
typedef enum {yellow, red, green, cyan, magenta, regular} Color;
static void my_printf(Color color, const char *format, ...)
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
      snprintf(buf2, 1000, "\033[32m%s\033[0m", buf1);
      break;
    case cyan :
      snprintf(buf2, 1000, "\033[36m%s\033[0m", buf1);
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


#endif //KITE_COMMON_FUNC_H
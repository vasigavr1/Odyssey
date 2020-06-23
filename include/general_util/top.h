//
// Created by vasilis on 23/06/2020.
//

#ifndef KITE_TOP_H
#define KITE_TOP_H

#include "sizes.h"
#include "generic_macros.h"


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

// CORE CONFIGURATION
#define WORKERS_PER_MACHINE 10
#define MACHINE_NUM 5
#define SESSIONS_PER_THREAD 22
#define ENABLE_CLIENTS 0
#define CLIENTS_PER_MACHINE_ 5
#define CLIENTS_PER_MACHINE (ENABLE_CLIENTS ? CLIENTS_PER_MACHINE_ : 0)

// HELPING CONSTANTS DERIVED FROM CORE CONFIGURATION
#define TOTAL_THREADS (WORKERS_PER_MACHINE + CLIENTS_PER_MACHINE)
#define REM_MACH_NUM (MACHINE_NUM - 1) // Number of remote machines
#define SESSIONS_PER_MACHINE (WORKERS_PER_MACHINE * SESSIONS_PER_THREAD)
#define SESSIONS_PER_CLIENT_ (SESSIONS_PER_MACHINE / CLIENTS_PER_MACHINE_)
#define SESSIONS_PER_CLIENT MAX(1, SESSIONS_PER_CLIENT_)
#define WORKERS_PER_CLIENT (ENABLE_CLIENTS ? (WORKERS_PER_MACHINE / CLIENTS_PER_MACHINE ) : 0)
#define GLOBAL_SESSION_NUM (MACHINE_NUM * SESSIONS_PER_MACHINE)
#define WORKER_NUM (WORKERS_PER_MACHINE * MACHINE_NUM)


#define ENABLE_ASSERTIONS 0


/* Request sizes */
#define KEY_SIZE 8 //
#define GRH_SIZE 40 // Global Routing Header
#define MTU 4096

#define MAXIMUM_INLINE_SIZE 188



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
#define ENABLE_MULTICAST 0
#define MULTICAST_TESTING_ 0
#define MULTICAST_TESTING (ENABLE_MULTICAST == 1 ? MULTICAST_TESTING_ : 0)
#define MCAST_QPS MACHINE_NUM

#define MCAST_QP_NUM 2
#define PREP_MCAST_QP 0
#define COM_MCAST_QP 1 //
#define MCAST_GROUPS_NUM 2

//////////////////////////////////////////////////////
/////////////~~~~GLOBALS~~~~~~/////////////////////////
//////////////////////////////////////////////////////

extern int is_roce, machine_id, num_threads;
extern char **remote_ips, *local_ip, *dev_name;



//////////////////////////////////////////////////////
/////////////~~~~FUNCTIONS~~~~~~/////////////////////////
//////////////////////////////////////////////////////
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

typedef struct key {
  unsigned int bkt			:32;
  unsigned int server			:16;
  unsigned int tag			:16;
} mica_key_t;

typedef struct mica_op mica_op_t;

#endif //KITE_TOP_H

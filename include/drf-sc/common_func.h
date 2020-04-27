//
// Created by vasilis on 27/04/20.
//

#ifndef KITE_COMMON_FUNC_H
#define KITE_COMMON_FUNC_H


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
#include "hrd_sizes.h"


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
#define VALUE_SIZE (USE_BIG_OBJECTS ? ((EXTRA_CACHE_LINES * 64) + BASE_VALUE_SIZE) : BASE_VALUE_SIZE) //(169 + 64)// 46 + 64 + 64//32 //(46 + 64)

/* Request sizes */
#define KEY_SIZE 16
#define TRUE_KEY_SIZE 8 // the key that is actually used by MICA
#define GRH_SIZE 40 // Global Routing Header
#define MTU 4096

#define MAXIMUM_INLINE_SIZE 188



//////////////////////////////////////////////////////
/////////////~~~~MACROS~~~~~~/////////////////////////
//////////////////////////////////////////////////////

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

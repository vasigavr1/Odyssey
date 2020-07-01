//
// Created by vasilis on 06/05/20.
//

#ifndef KITE_GENERIC_MACROS_H
#define KITE_GENERIC_MACROS_H

//////////////////////////////////////////////////////
/////////////~~~~MACROS~~~~~~/////////////////////////
//////////////////////////////////////////////////////

#define COMPILER_BARRIER() asm volatile ("" ::: "memory")
#define GET_GLOBAL_T_ID(m_id, t_id) ((m_id * WORKERS_PER_MACHINE) + t_id)
#define MY_ASSERT(COND, STR, ARGS...) \
  if (ENABLE_ASSERTIONS) { if (!(COND)) { my_printf(red, (STR), (ARGS)); assert(false); }}
#define FIND_PADDING(size) ((64 - (size % 64)) % 64)
#define FIND_PADDING_CUST_ALIGN(size, align) ((align - (size % align)) % align)
#define MAX_OF_3(x1, y1, x2) (MAX(x1, y1) > (x2) ? (MAX(x1, y1)) : (x2))
#define MAX_OF_4(x1, y1, x2, y2) (MAX(x1, y1) > MAX(x2, y2) ? (MAX(x1, y1)) : (MAX(x2, y2)))

/* Useful when `x = (x + 1) % N` is done in a loop */
#define MOD_INCR(x, N) do { \
	x = x + 1; \
	if(x == N) { \
		x = 0; \
	} \
} while(0)


#define MOD_SUB(x, y, N) do { \
	x = (N + x - y) % N; \
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
#define IS_ALIGNED(x, A) (((uint64_t) x) % A == 0)

#define IS_WRITE(X) (((X) == KVS_OP_PUT || (X) == OP_RELEASE) ? 1 : 0)

#endif //KITE_GENERIC_MACROS_H

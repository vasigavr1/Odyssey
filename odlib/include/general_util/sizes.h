/*
*
* Adapted from https://github.com/efficient/rdma_bench
*/

#define K_1 1024
#define K_2 2048
#define K_4 4096
#define K_8  8142
#define K_16 65536


#define K_32 32768

#define K_64 65536
#define K_64_ 65535

#define K_128 131072
#define K_128_ 131071

#define K_256 262144
#define K_256_ 262143

#define K_512 524288
#define K_512_ 524287

#define M_1 1048576
#define M_1_ 1048575

#define M_2 2097152
#define M_2_ 2097151

#define M_4 4194304
#define M_4_ 4194303

#define M_8 8388608
#define M_8_ 8388607

#define M_16 16777216
#define M_16_ 16777215

#define M_32 33554432
#define M_32_ 33554431

#define M_128 134217728
#define M_128_ 134217727

#define M_256 268435456
#define M_256_ 268435455

#define M_512 536870912
#define M_512_ 536870911

#define M_1024 1073741824
#define M_1024_ 1073741823

#define THOUSAND 1000
#define MILLION (THOUSAND * THOUSAND)
#define BILLION (THOUSAND * MILLION)
#define TRILLION (THOUSAND * BILLION)

#define SIXTY_FOUR_ONES 9223372036854775807
#define SIXTY_THREE_ONES 4611686018427387903

#define B_4_EXACT 4000000000
#define B_4_ 4294967295
#define B_4 4294967296
#define B_512 549755813888
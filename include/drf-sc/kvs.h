#ifndef ABD_CACHE_H
#define ABD_CACHE_H


// Optik Options
#ifndef CORE_NUM
#define DEFAULT
#define CORE_NUM 8
#endif
#ifndef _GNU_SOURCE
# define _GNU_SOURCE
#endif
//#include "../optik/optik_mod.h"
//#include "main.h"

#include "main.h"

#define KVS_DEBUG 0
#define KVS_NUM_BKTS (8 * 1024 * 1024)
#define KVS_NUM_KEYS (1000 * 1000)
#define KVS_LOG_CAP  (1024 * 1024 * 1024)


//#define MICA_OP_METADATA (sizeof(struct mica_key) + sizeof(uint8_t) + sizeof(uint8_t))
//#define MICA_MIN_VALUE (64 - MICA_OP_METADATA)
//#define MICA_MAX_VALUE (USE_BIG_OBJECTS == 1 ? (MICA_MIN_VALUE + (EXTRA_CACHE_LINES * 64)) : MICA_MIN_VALUE)

#define MICA_LOG_BITS 40
#define MICA_INDEX_SHM_KEY 1185
#define MICA_LOG_SHM_KEY 2185



struct mica_slot {
	uint32_t in_use	:1;
	uint32_t tag	:(64 - MICA_LOG_BITS - 1);
	uint64_t offset	:MICA_LOG_BITS;
};

struct mica_bkt {
	struct mica_slot slots[8];
};

typedef struct  {
	struct mica_bkt *ht_index;
	uint8_t *ht_log;

	/* Metadata */
	int instance_id;	/* ID of this MICA instance. Used for shm keys */
	int node_id;

	int num_bkts;	/* Number of buckets requested by user */
	int bkt_mask;	/* Mask down from a mica_key's @bkt to a bucket */

	uint64_t log_cap;	/* Capacity of circular log in bytes */
	uint64_t log_mask;	/* Mask down from a slot's @offset to a log offset */

	/* State */
	uint64_t log_head;

	/* Stats */
	long long num_get_op;	/* Number of GET requests executed */
	long long num_put_op;	/* Number of PUT requests executed */
	long long num_get_fail;	/* Number of GET requests failed */
	long long num_put_fail;	/* Number of GET requests failed */
	long long num_insert_op;	/* Number of PUT requests executed */
	long long num_index_evictions; /* Number of entries evicted from index */
} mica_kv_t;

extern mica_kv_t *KVS;


char* code_to_str(uint8_t code);


typedef volatile struct
{
  uint8_t epoch_id[2];
  uint8_t lock;
  uint8_t m_id;
  uint32_t version;
} cache_meta;

/* Fixed-w_size 16 byte keys */
struct cache_key {
	cache_meta meta; // This should be 8B (unused --> in mica)
	uint32_t bkt;//			:32;
  uint16_t server;
  uint16_t tag;
//	unsigned int server			:16;
//	unsigned int tag			:16;
};


struct cache_op {
	struct cache_key key;	/* This must be the 1st field and 16B aligned */
	uint8_t opcode;// if the opcode is 0, it has never been RMWed, if it's 1 it has
	uint8_t val_len;
	uint8_t value[VALUE_SIZE]; // if it's an RMW the first 4 bytes point to the entry
};




void custom_mica_init(int kvs_id);
void custom_mica_populate_fixed_len(mica_kv_t *, int n, int val_len);



uint128* mica_gen_keys(int n);


void str_to_binary(uint8_t* value, char* str, int size);
void print_cache_stats(struct timespec start, int id);


/* The leader and follower send their local requests to this, reads get served
 * But writes do not get served, writes are only propagated here to see whether their keys exist */
void cache_batch_op_trace(uint16_t op_num, uint16_t t_id, struct trace_op *op,
                          struct cache_resp *resp, struct pending_ops *);
/* The leader sends the writes to be committed with this function*/
void cache_batch_op_updates(uint16_t , uint16_t , struct write**, struct pending_ops*, uint32_t,  uint32_t, bool);
// The worker send here the incoming reads, the reads check the incoming ts if it is  bigger/equal to the local
// the just ack it, otherwise they send the value back
void cache_batch_op_reads(uint32_t op_num, uint16_t t_id, struct pending_ops *p_ops,
                          uint32_t pull_ptr, uint32_t max_op_size, bool zero_ops);

void cache_batch_op_first_read_round(uint16_t op_num, uint16_t t_id, struct read_info **writes,
                                     struct pending_ops *p_ops,
                                     uint32_t pull_ptr, uint32_t max_op_size, bool zero_ops);

// Send an isolated write to the kvs-no batching
void cache_isolated_op(int t_id, struct write *write);

#endif

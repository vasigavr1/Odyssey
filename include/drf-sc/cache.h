#ifndef ABD_CACHE_H
#define ABD_CACHE_H


// Optik Options
#ifndef CORE_NUM
#define DEFAULT
#define CORE_NUM 8
#endif
#include "optik_mod.h"
#include "hrd.h"
#include "main.h"
#include "mica.h"
#define CACHE_DEBUG 0
#define CACHE_NUM_BKTS (1 *1024 * 1024) //64K buckets seems to be enough to store most of 250K keys
#define CACHE_NUM_KEYS (250 * 1000)

#define WRITE_RATIO 50 //Warning write ratio is given out of a 1000, e.g 10 means 10/1000 i.e. 1%
#define CACHE_BATCH_SIZE 500

//Cache States
#define VALID_STATE 1
#define INVALID_STATE 2
#define INVALID_REPLAY_STATE 3
#define WRITE_STATE 4
#define WRITE_REPLAY_STATE 5

//Cache Opcode
// signal that this is the second round of an acquire that flips the config bit
#define OP_ACQUIRE_FLIP_BIT 107
#define OP_LIN_RELEASE 108
#define OP_RELEASE 109
#define OP_ACQUIRE 110

#define CACHE_OP_GET 111
#define CACHE_OP_PUT 112
#define CACHE_OP_UPD 113
#define CACHE_OP_INV 114
#define CACHE_OP_ACK 115
#define CACHE_OP_BRC 116       //Warning although this is cache opcode it's returned by cache to either Broadcast upd or inv
#define ACK_NOT_YET_SENT 117
#define CACHE_OP_LIN_PUT 118
#define UPDATE_EPOCH_OP_GET 119
//Cache Response
#define EMPTY 120
#define CACHE_GET_SUCCESS 121
#define CACHE_PUT_SUCCESS 122
#define CACHE_LOCAL_GET_SUCCESS 123
#define CACHE_INV_SUCCESS 124
#define CACHE_ACK_SUCCESS 125
#define CACHE_LAST_ACK_SUCCESS 126
#define RETRY 127
// READ_REPLIES
#define READ_REPLY 27
#define TS_SMALLER 28
#define TS_EQUAL 29
#define TS_GREATER_LIN_PUT 30
#define TS_GREATER 31
// this offset is added to the read reply opcode
// to denote that the machine doing the acquire was
// previously considered to have failed
#define FALSE_POSITIVE_OFFSET 10


#define KEY_HIT 220


#define CACHE_MISS 130
#define CACHE_GET_STALL 131
#define CACHE_PUT_STALL 132
#define CACHE_UPD_FAIL 133
#define CACHE_INV_FAIL 134
#define CACHE_ACK_FAIL 135
#define CACHE_GET_FAIL 136
#define CACHE_PUT_FAIL 137

#define UNSERVED_CACHE_MISS 140

#define IS_WRITE(X) (((X) == CACHE_OP_PUT || (X) == OP_RELEASE) ? 1 : 0)

char* code_to_str(uint8_t code);

/* Fixed-w_size 16 byte keys */
struct cache_key {
	cache_meta meta; // This should be 8B (unused --> in mica)
	unsigned int bkt			:32;
	unsigned int server			:16;
	unsigned int tag			:16;
};


struct cache_op {
	struct cache_key key;	/* This must be the 1st field and 16B aligned */
	uint8_t opcode;
	uint8_t val_len;
	uint8_t value[MICA_MAX_VALUE];
};

struct key {
  unsigned int bkt			:32;
  unsigned int server			:16;
  unsigned int tag			:16;
};


struct cache_meta_stats { //TODO change this name
	/* Stats */
	long long num_get_success;
	long long num_put_success;
	long long num_upd_success;
	long long num_inv_success;
	long long num_ack_success;
	long long num_get_stall;
	long long num_put_stall;
	long long num_upd_fail;
	long long num_inv_fail;
	long long num_ack_fail;
	long long num_get_miss;
	long long num_put_miss;
	long long num_unserved_get_miss;
	long long num_unserved_put_miss;
};

struct extended_cache_meta_stats {
	long long num_hit;
	long long num_miss;
	long long num_stall;
	long long num_coherence_fail;
	long long num_coherence_success;
	struct cache_meta_stats metadata;
};


struct cache {
	int num_threads;
	struct mica_kv hash_table;
	long long total_ops_issued; ///this is only for get and puts
	struct extended_cache_meta_stats aggregated_meta;
	struct cache_meta_stats* meta;
};

typedef enum {
	NO_REQ,
	HOT_WRITE_REQ_BEFORE_SAVING_KEY,
	HOT_WRITE_REQ,
	HOT_READ_REQ,
	LOCAL_REQ,
	REMOTE_REQ
} req_type;


struct latency_flags {
	req_type measured_req_flag;
	uint16_t last_measured_op_i;
	struct cache_key* key_to_measure;
};

//Add latency to histogram (in microseconds)
static inline void bookkeep_latency(int useconds, req_type rt){
	uint32_t** latency_counter;
	switch (rt){
		case REMOTE_REQ:
			latency_counter = &latency_count.remote_reqs;
			break;
		case LOCAL_REQ:
			latency_counter = &latency_count.local_reqs;
			break;
		case HOT_READ_REQ:
			latency_counter = &latency_count.hot_reads;
			break;
		case HOT_WRITE_REQ:
			latency_counter = &latency_count.hot_writes;
			break;
		default: assert(0);
	}
	latency_count.total_measurements++;
	if (useconds > MAX_LATENCY)
		(*latency_counter)[MAX_LATENCY]++;
	else
		(*latency_counter)[useconds / (MAX_LATENCY / LATENCY_BUCKETS)]++;
}


void str_to_binary(uint8_t* value, char* str, int size);
void print_cache_stats(struct timespec start, int id);


void cache_init(int cache_id, int num_threads);
void cache_populate_fixed_len(struct mica_kv* kv, int n, int val_len);

/* The leader and follower send their local requests to this, reads get served
 * But writes do not get served, writes are only propagated here to see whether their keys exist */
void cache_batch_op_trace(int op_num, int t_id, struct cache_op **op,
                          struct mica_resp *resp, struct pending_ops *);
/* The leader sends the writes to be committed with this function*/
void cache_batch_op_updates(uint32_t , int , struct write**, uint32_t,  uint32_t, bool);
// The worker send here the incoming reads, the reads check the incoming ts if it is  bigger/equal to the local
// the just ack it, otherwise they send the value back
void cache_batch_op_reads(uint32_t op_num, uint16_t t_id, struct pending_ops *p_ops,
                          uint32_t pull_ptr, uint32_t max_op_size, bool zero_ops);

void cache_batch_op_lin_writes_and_unseen_reads(uint32_t op_num, int t_id, struct read_info **writes,
																								uint32_t pull_ptr, uint32_t max_op_size, bool zero_ops);

#endif

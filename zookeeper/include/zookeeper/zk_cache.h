#ifndef ZOOKEEPER_CACHE_H
#define ZOOKEEPER_CACHE_H


// Optik Options
#ifndef CORE_NUM
#define DEFAULT
#define CORE_NUM 8
#endif
#include "optik_mod.h"
#include "hrd.h"
//#include "main.h"
#include "mica.h"
#define CACHE_DEBUG 0
#define CACHE_NUM_BKTS (8 * 1024 * 1024) //64K buckets seems to be enough to store most of 250K keys
#define CACHE_NUM_KEYS (1000 * 1000)

#define MAX_OP_BATCH 1000

//Cache States
#define VALID_STATE 1
#define INVALID_STATE 2
#define INVALID_REPLAY_STATE 3
#define WRITE_STATE 4
#define WRITE_REPLAY_STATE 5

//Cache Opcode
#define KVS_OP_GET 111
#define KVS_OP_PUT 112
#define CACHE_OP_UPD 113
#define CACHE_OP_INV 114
#define KVS_OP_ACK 115
#define CACHE_OP_BRC 116       //Warning although this is cache opcode it's returned by cache to either Broadcast upd or inv

//Cache Response
#define EMPTY 120
#define CACHE_GET_SUCCESS 121
#define CACHE_PUT_SUCCESS 122
#define CACHE_UPD_SUCCESS 123
#define CACHE_INV_SUCCESS 124
#define CACHE_ACK_SUCCESS 125
#define CACHE_LAST_ACK_SUCCESS 126
#define RETRY 127

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

char* code_to_str(uint8_t code);
struct key_home{
	uint8_t machine;
	uint8_t worker;
};

/* Fixed-size 16 byte keys */
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









void str_to_binary(uint8_t* value, char* str, int size);
void print_cache_stats(struct timespec start, int id);


void cache_init(int cache_id, int num_threads);
void cache_populate_fixed_len(struct mica_kv* kv, int n, int val_len);

///* The leader and follower send their local requests to this, reads get served
// * But writes do not get served, writes are only propagated here to see whether their keys exist */
//void zk_KVS_batch_op_trace(int op_num, int thread_id, struct cache_op **op, struct mica_resp *resp);
///* The leader sends the writes to be committed with this function*/
//void zk_KVS_batch_op_updates(uint32_t , int , zk_prepare_t**, struct mica_resp *,uint32_t,  uint32_t, bool);


#endif

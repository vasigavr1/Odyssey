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
#define CACHE_NUM_BKTS (8 * 1024 * 1024) //64K buckets seems to be enough to store most of 250K keys
#define CACHE_NUM_KEYS (1000 * 1000)


//#define CACHE_BATCH_SIZE 500

//Cache States
#define VALID_STATE 1
#define INVALID_STATE 2
#define INVALID_REPLAY_STATE 3
#define WRITE_STATE 4
#define WRITE_REPLAY_STATE 5

//Cache Opcode
#define COMMIT_OP 102
#define ACCEPT_OP 103
#define PROPOSE_OP 104
#define OP_RELEASE_BIT_VECTOR 105// first round of a release that carries a bit vector
#define OP_RELEASE_SECOND_ROUND 106 // second round is the actual release
// The sender sends this opcode to flip a bit it owns after an acquire detected a failure
#define OP_ACQUIRE_FLIP_BIT 107
#define OP_RELEASE 109
#define OP_ACQUIRE 110
// The receiver renames the opcode of an OP_ACQUIRE  to this to recognize
// that the acquire detected a failure and add the offset to the reply opcode
#define OP_ACQUIRE_FP 10
#define CACHE_OP_GET 111
#define CACHE_OP_PUT 112
#define ACQUIRE_RMW_OP 113

#define CACHE_OP_ACK 115
#define ACK_NOT_YET_SENT 117
#define CACHE_OP_GET_TS 118 // first round of release, or out-of-epoch write
#define UPDATE_EPOCH_OP_GET 119



//Cache Response
//#define RETRY_RMW_NO_ENTRIES 0
#define RETRY_RMW_KEY_EXISTS 1
#define RMW_SUCCESS 118

#define EMPTY 120
#define CACHE_GET_TS_SUCCESS 21
#define CACHE_GET_SUCCESS 121
#define CACHE_PUT_SUCCESS 122
#define CACHE_LOCAL_GET_SUCCESS 123
#define CACHE_INV_SUCCESS 124
#define CACHE_ACK_SUCCESS 125
#define CACHE_LAST_ACK_SUCCESS 126
#define RETRY 127
#define CACHE_MISS 130
#define CACHE_GET_STALL 131
#define CACHE_PUT_STALL 132
#define CACHE_UPD_FAIL 133
#define CACHE_INV_FAIL 134
#define CACHE_ACK_FAIL 135
#define CACHE_GET_FAIL 136
#define CACHE_PUT_FAIL 137

// READ_REPLIES
#define INVALID_OPCODE 5 // meaningless opcode to help with debugging
// an r_rep message can be a reply to a read or a prop or an accept
#define ACCEPT_REPLY 25
#define PROP_REPLY 26
#define READ_REPLY 27
#define TS_SMALLER 28
#define TS_EQUAL 29
#define TS_GREATER_TS_ONLY 30 // Response when reading the ts only (1st round of release)
#define TS_GREATER 31
#define RMW_ACK 32 // 1 byte reply
#define SEEN_HIGHER_PROP 33 // send that TS
#define SEEN_LOWER_ACC 34 // send value, rmw-id, TS
#define RMW_TS_STALE 35 // Ts was smaller than the KVS stored TS: send that TS
#define RMW_ID_COMMITTED 36 // send the entire committed rmw
#define LOG_TOO_SMALL 37 // send the entire committed rmw
#define LOG_TOO_HIGH 38 // single byte-nack only proposes
#define SEEN_HIGHER_ACC 39 //both accs and props- send only TS different op than SEEN_HIGHER_PROP only for debug
// NO_OP_PROP_REP: Purely for debug: this is sent to proposes when an accept has been received
// for the same RMW-id and TS, that means the proposer will never see this opcode because
// it has already gathered prop reps quorum and sent accepts
#define NO_OP_PROP_REP 40


// this offset is added to the read reply opcode
// to denote that the machine doing the acquire was
// previously considered to have failed
#define FALSE_POSITIVE_OFFSET 20


#define KEY_HIT 220
#define UNSERVED_CACHE_MISS 140
#define IS_WRITE(X) (((X) == CACHE_OP_PUT || (X) == OP_RELEASE) ? 1 : 0)

char* code_to_str(uint8_t code);

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
	uint8_t value[MICA_MAX_VALUE]; // if it's an RMW the first 4 bytes point to the entry
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

/* The leader and follower send their local requests to this, reads get served
 * But writes do not get served, writes are only propagated here to see whether their keys exist */
void cache_batch_op_trace(uint16_t op_num, uint16_t t_id, struct cache_op *op,
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

// Send an isolated write to the cache-no batching
void cache_isolated_op(int t_id, struct write *write);

#endif

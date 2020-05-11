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


//typedef volatile struct
//{
//  uint8_t epoch_id[2];
//  uint8_t lock;
//  uint8_t m_id;
//  uint32_t version;
//} cache_meta;

/* Fixed-w_size 16 byte keys */
//struct cache_key {
//	cache_meta meta; // This should be 8B (unused --> in mica)
//	uint32_t bkt;//			:32;
//  uint16_t server;
//  uint16_t tag;
////	unsigned int server			:16;
////	unsigned int tag			:16;
//};


//struct cache_op {
//	struct cache_key key;	/* This must be the 1st field and 16B aligned */
//	uint8_t opcode;// if the opcode is 0, it has never been RMWed, if it's 1 it has
//	uint8_t val_len;
//	uint8_t value[VALUE_SIZE]; // if it's an RMW the first 4 bytes point to the entry
//};




void custom_mica_init(int kvs_id);
void custom_mica_populate_fixed_len(mica_kv_t *, int n, int val_len);



uint128* mica_gen_keys(int n);


void str_to_binary(uint8_t* value, char* str, int size);
void print_cache_stats(struct timespec start, int id);


/* ---------------------------------------------------------------------------
//------------------------------ KVS UTILITY GENERIC -----------------------------
//---------------------------------------------------------------------------*/


// Locate the buckets for the requested keys
static inline void KVS_locate_one_bucket(uint16_t op_i, uint *bkt, struct key op_key,
																				 struct mica_bkt **bkt_ptr, uint *tag,
																				 mica_op_t **kv_ptr, uint8_t *key_in_store,
																				 mica_kv_t *KVS)
{
	bkt[op_i] = op_key.bkt & KVS->bkt_mask;
	bkt_ptr[op_i] = &KVS->ht_index[bkt[op_i]];
//  printf("bkt %u \n", bkt[op_i]);
	__builtin_prefetch(bkt_ptr[op_i], 0, 0);
	tag[op_i] = op_key.tag;
	key_in_store[op_i] = 0;
	kv_ptr[op_i] = NULL;
}

// Locate the buckets for the requested keys
static inline void KVS_locate_one_bucket_with_key(uint16_t op_i, uint *bkt, struct key *op_key,
																									struct mica_bkt **bkt_ptr, uint *tag,
																									mica_op_t **kv_ptr,
																									uint8_t *key_in_store, mica_kv_t *KVS)
{
	bkt[op_i] = op_key->bkt & KVS->bkt_mask;
	bkt_ptr[op_i] = &KVS->ht_index[bkt[op_i]];
	__builtin_prefetch(bkt_ptr[op_i], 0, 0);
	tag[op_i] = op_key->tag;
	key_in_store[op_i] = 0;
	kv_ptr[op_i] = NULL;
}

// After locating the buckets locate all kv pairs
static inline void KVS_locate_all_kv_pairs(uint16_t op_num, uint *tag, struct mica_bkt **bkt_ptr,
																					 mica_op_t **kv_ptr, mica_kv_t *KVS)
{
	for(uint16_t op_i = 0; op_i < op_num; op_i++) {
		for (uint8_t j = 0; j < 8; j++) {
			if (bkt_ptr[op_i]->slots[j].in_use == 1 &&
					bkt_ptr[op_i]->slots[j].tag == tag[op_i]) {
				uint64_t log_offset = bkt_ptr[op_i]->slots[j].offset &
															KVS->log_mask;
				/*
                 * We can interpret the log entry as mica_op, even though it
                 * may not contain the full MICA_MAX_VALUE value.
                 */
//        printf("kv_ptr[%u]: offset %lu : %p \n",
//               op_i, log_offset, (void *)&KVS->ht_log[log_offset]);
				kv_ptr[op_i] = (mica_op_t *) &KVS->ht_log[log_offset];

				/* Small values (1--64 bytes) can span 2 kvs lines */
				__builtin_prefetch(kv_ptr[op_i], 0, 0);
				__builtin_prefetch((uint8_t *) kv_ptr[op_i] + 64, 0, 0);

				/* Detect if the head has wrapped around for this index entry */
				if (KVS->log_head - bkt_ptr[op_i]->slots[j].offset >= KVS->log_cap) {
					kv_ptr[op_i] = NULL;  /* If so, we mark it "not found" */
				}
				break;
			}
		}
	}
}

// Locate a kv_pair inside a bucket: used in a loop for all kv-pairs
static inline void KVS_locate_one_kv_pair(int op_i, uint *tag, struct mica_bkt **bkt_ptr,
																					mica_op_t **kv_ptr, mica_kv_t *KVS)
{
	for(uint8_t j = 0; j < 8; j++) {
		if(bkt_ptr[op_i]->slots[j].in_use == 1 &&
			 bkt_ptr[op_i]->slots[j].tag == tag[op_i]) {
			uint64_t log_offset = bkt_ptr[op_i]->slots[j].offset &
														KVS->log_mask;
			/*
               * We can interpret the log entry as mica_op, even though it
               * may not contain the full MICA_MAX_VALUE value.
               */
			kv_ptr[op_i] = (mica_op_t *) &KVS->ht_log[log_offset];

			/* Small values (1--64 bytes) can span 2 kvs lines */
			__builtin_prefetch(kv_ptr[op_i], 0, 0);
			__builtin_prefetch((uint8_t *) kv_ptr[op_i] + 64, 0, 0);

			/* Detect if the head has wrapped around for this index entry */
			if(KVS->log_head - bkt_ptr[op_i]->slots[j].offset >= KVS->log_cap) {
				kv_ptr[op_i] = NULL;	/* If so, we mark it "not found" */
			}

			break;
		}
	}
}


#endif

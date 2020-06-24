//
// Created by vasilis on 23/06/2020.
//

#ifndef ZOOKEEPER_CONFIG_H
#define ZOOKEEPER_CONFIG_H

#include "../general_util/top.h"
#include "zk_opcodes.h"

// CORE CONFIGURATION

#define WRITE_RATIO 480  //Warning write ratio is given out of a 1000, e.g 10 means 10/1000 i.e. 1%
#define ENABLE_STAT_COUNTING 0

#define W_CREDITS 6
#define MAX_W_COALESCE 6
#define PREPARE_CREDITS 6
#define MAX_PREP_COALESCE 9
#define COMMIT_CREDITS 30
#define MEASURE_LATENCY 1
#define LATENCY_MACHINE 1
#define LATENCY_THREAD 1
#define MEASURE_READ_LATENCY 0 // 2 means mixed due to complete lack of imagination
#define EXIT_ON_PRINT 1
#define PRINT_NUM 8
#define FEED_FROM_TRACE 0
#define CACHE_BATCH_SIZE 1000





#define FOLLOWERS_PER_MACHINE (WORKERS_PER_MACHINE)
#define LEADERS_PER_MACHINE (WORKERS_PER_MACHINE)
#define FOLLOWER_MACHINE_NUM (MACHINE_NUM - 1)
#define LEADER_MACHINE 0 // which machine is the leader
#define FOLLOWER_NUM (FOLLOWERS_PER_MACHINE * FOLLOWER_MACHINE_NUM)

#define CACHE_SOCKET (FOLLOWERS_PER_MACHINE < 39 ? 0 : 1 )// socket where the cache is bind


#define MICA_VALUE_SIZE (VALUE_SIZE + (FIND_PADDING_CUST_ALIGN(VALUE_SIZE, 32)))
#define MICA_OP_SIZE_  (20 + ((MICA_VALUE_SIZE)))
#define MICA_OP_PADDING_SIZE  (FIND_PADDING(MICA_OP_SIZE_))

#define MICA_OP_SIZE  (MICA_OP_SIZE_ + MICA_OP_PADDING_SIZE)
struct mica_op {
  // Cache-line -1
  uint8_t value[MICA_VALUE_SIZE];
  // Cache-line -2
  struct key key;
  seqlock_t seqlock;
  uint32_t key_id; // strictly for debug
  uint8_t padding[MICA_OP_PADDING_SIZE];
};


#endif //KITE_CONFIG_H

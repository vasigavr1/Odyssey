cmake_minimum_required(VERSION 3.18)
project(kite)

set(KeyValueCache_VERSION_MAJOR 1)
set(KeyValueCache_VERSION_MINOR 0)

include_directories(include/libhrd include/kite include/mica include/general_util  include/zookeeper
        include/kite_inline_util /usr/include/)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3 -std=gnu1x -Wno-address-of-packed-member \
-Wno-format-truncation -g  -march=native")




set(COMMON_FILES
        src/mica/kvs.c
        #libhrd
        include/libhrd/hrd.h
        src/libhrd/hrd_util.c
        src/libhrd/hrd_conn.c
        #mica
        src/mica/city.c
        include/mica/kvs.h
        include/mica/city.h
        /usr/include/infiniband/verbs.h
        src/client/client.c
        include/general_util/generic_macros.h
        include/general_util/client_if_util.h
        include/general_util/interface.h
        include/general_util/latency_util.h
        include/general_util/stats.h
        include/general_util/top.h
        include/general_util/init_func.h
        include/general_util/generic_inline_util.h
        include/general_util/init_connect.h
        include/general_util/generic_opcodes.h
        include/general_util/trace_util.h
        include/general_util/rdma_gen_util.h
        include/general_util/debug_util.h
        include/general_util/inline_util.h
        include/general_util/sizes.h
        )

set(KITE_SOURCE_FILES
        src/kite/util.c
        src/kite/main.c
        src/kite/worker.c
        src/kite/stats.c
        include/kite_inline_util/kite_inline_util.h
        include/kite/main.h
        include/kite/util.h
        include/kite/config.h
        include/kite/opcodes.h
        include/kite_inline_util/generic_util.h
        include/kite_inline_util/kvs_util.h
        include/kite_inline_util/kite_debug_util.h
        include/kite_inline_util/config_util.h
        include/kite_inline_util/paxos_util.h
        include/kite_inline_util/reserve_stations_util.h
        include/kite_inline_util/communication_utility.h
        include/kite_inline_util/paxos_generic_util.h
        include/kite/messages.h
        include/kite/buffer_sizes.h
        )

set(ZK_SOURCE_FILES
        src/zookeeper/zk_main.c
        src/zookeeper/zk_leader.c
        src/zookeeper/zk_follower.c
        src/zookeeper/zk_stats.c
        src/zookeeper/zk_util.c
        include/zookeeper/zk_cache.h
        include/zookeeper/zk_inline_util.h
        include/zookeeper/zk_main.h
        include/zookeeper/zk_util.h
        include/zookeeper/zk_config.h
        include/zookeeper/zk_kvs_util.h
        include/zookeeper/zk_opcodes.h
        include/zookeeper/zk_debug_util.h
        include/zookeeper/zk_reservation_stations_util.h_.h
        )



add_executable(kite ${KITE_SOURCE_FILES} ${COMMON_FILES})

add_executable(zookeeper ${ZK_SOURCE_FILES} ${COMMON_FILES})

set_target_properties(kite PROPERTIES COMPILE_FLAGS -DKITE)
set_target_properties(zookeeper PROPERTIES COMPILE_FLAGS -DZOOKEEPER)

target_link_libraries(kite pthread ibverbs rt numa rdmacm)
target_link_libraries(zookeeper pthread ibverbs rt numa rdmacm)


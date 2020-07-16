cmake_minimum_required(VERSION 3.18)
project(odyssey)

set(odyssey_VERSION_MAJOR 1)

include_directories(
        shared/include/libhrd
        kite/include/kite
        shared/include/mica
        shared/include/general_util
        shared/include/multicast
        zookeeper/include/zookeeper
        kite/include/kite_inline_util
        /usr/include/)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3 -std=gnu1x -march=native -Wno-address-of-packed-member \
-Wno-format-truncation -g  -D_GNU_SOURCE ")
#set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -O3 -std=gnu2x -flto ")

# -fno-omit-frame-pointer  enable the frame pointer to trace stack
#-fno-inline
#-fsanitize=address -fsanitize=thread
# -fsanitize=address -fno-inline -fno-omit-frame-pointer


set(COMMON_FILES
        shared/src/mica/kvs.c
        #libhrd
        shared/include/libhrd/hrd.h
        shared/src/libhrd/hrd_util.c
        shared/src/libhrd/hrd_conn.c
        #mica
        shared/src/mica/city.c
        shared/include/mica/kvs.h
        shared/include/mica/city.h
        /usr/include/infiniband/verbs.h
        shared/src/client/client.c
        shared/include/general_util/generic_macros.h
        shared/include/general_util/client_if_util.h
        shared/include/general_util/interface.h
        shared/include/general_util/latency_util.h
        shared/include/general_util/stats.h
        shared/include/general_util/top.h
        shared/include/general_util/init_func.h
        shared/include/general_util/generic_inline_util.h
        shared/include/general_util/init_connect.h
        shared/include/general_util/generic_opcodes.h
        shared/include/general_util/trace_util.h
        shared/include/general_util/rdma_gen_util.h
        shared/include/general_util/debug_util.h
        shared/include/general_util/inline_util.h
        shared/include/general_util/sizes.h
        shared/include/general_util/config_util.h
        shared/include/multicast/multicast.h
        shared/src/multicast/multicast.c
        shared/include/general_util/network_context.h
        shared/include/general_util/fifo.h)

set(KITE_SOURCE_FILES
        kite/src/kite/util.c
        kite/src/kite/main.c
        kite/src/kite/worker.c
        kite/src/kite/stats.c
        kite/include/kite_inline_util/kite_inline_util.h
        kite/include/kite/main.h
        kite/include/kite/util.h
        kite/include/kite/config.h
        kite/include/kite/opcodes.h
        kite/include/kite_inline_util/generic_util.h
        kite/include/kite_inline_util/kvs_util.h
        kite/include/kite_inline_util/kite_debug_util.h
        kite/include/kite_inline_util/kite_config_util.h
        kite/include/kite_inline_util/paxos_util.h
        kite/include/kite_inline_util/reserve_stations_util.h
        kite/include/kite_inline_util/communication_utility.h
        kite/include/kite_inline_util/paxos_generic_util.h
        kite/include/kite/messages.h
        kite/include/kite/buffer_sizes.h
        )

set(ZK_SOURCE_FILES
        zookeeper/src/zookeeper/zk_main.c
        zookeeper/src/zookeeper/zk_leader.c
        zookeeper/src/zookeeper/zk_follower.c
        zookeeper/src/zookeeper/zk_stats.c
        zookeeper/src/zookeeper/zk_util.c
        zookeeper/include/zookeeper/zk_inline_util.h
        zookeeper/include/zookeeper/zk_main.h
        zookeeper/include/zookeeper/zk_util.h
        zookeeper/include/zookeeper/zk_config.h
        zookeeper/include/zookeeper/zk_kvs_util.h
        zookeeper/include/zookeeper/zk_opcodes.h
        zookeeper/include/zookeeper/zk_debug_util.h
        zookeeper/include/zookeeper/zk_reservation_stations_util.h
        zookeeper/include/zookeeper/zk_generic_util.h)



add_executable(kite ${KITE_SOURCE_FILES} ${COMMON_FILES})

add_executable(zookeeper ${ZK_SOURCE_FILES} ${COMMON_FILES})

set_target_properties(kite PROPERTIES COMPILE_FLAGS -DKITE )
set_target_properties(zookeeper PROPERTIES COMPILE_FLAGS -DZOOKEEPER)

target_link_libraries(kite pthread ibverbs rt numa rdmacm)
target_link_libraries(zookeeper pthread ibverbs rt numa rdmacm)


cmake_minimum_required(VERSION 3.16)
project(odyssey)

set(odyssey_VERSION_MAJOR 1)

include_directories(
        odlib/incldue
        odlib/include/fifo
        odlib/include/libhrd
        kite/include/kite
        odlib/include/mica
        odlib/include/general_util
        odlib/include/multicast
        odlib/include/network_api
        odlib/include/client_api
        odlib/include/trace
        zookeeper/include/zookeeper
        derecho/include/derecho
        kite/include/kite_inline_util
        /usr/include/
        /usr/include/rdma
        /usr/include/infiniband
        hermes/include/hermes
        cht/include/cht
        odlib/include/protocols
        craq/include/craq
  )

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3 -std=gnu1x -march=native -Wno-address-of-packed-member \
-Wno-format-truncation -D_GNU_SOURCE -g ")
#set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -O3 -std=gnu2x -flto ")
# -g
# -fno-omit-frame-pointer  enable the frame pointer to trace stack
#-fno-inline
#-fsanitize=address -fsanitize=thread
# -fsanitize=address -fno-inline -fno-omit-frame-pointer


set(ODLIB_FILES
        odlib/src/main/main.c
        odlib/src/main/worker.c
        odlib/src/mica/kvs.c
        odlib/src/main/stats.c
        #libhrd
        odlib/include/libhrd/hrd.h
        odlib/src/libhrd/hrd_util.c
        odlib/src/libhrd/hrd_conn.c
        #mica
        odlib/src/mica/city.c
        odlib/include/mica/kvs.h
        odlib/include/mica/city.h
        /usr/include/infiniband/verbs.h
        odlib/src/client/client.c
        odlib/include/general_util/generic_macros.h
        odlib/include/client_api/wrkr_side_calls.h
        odlib/include/client_api/interface.h
        odlib/include/general_util/latency_util.h
        odlib/include/general_util/stats.h
        odlib/include/general_util/top.h
        odlib/include/general_util/init_func.h
        odlib/include/general_util/generic_inline_util.h
        odlib/include/general_util/init_connect.h
        odlib/include/general_util/generic_opcodes.h
        odlib/include/trace/trace_util.h
        odlib/include/general_util/rdma_gen_util.h
        odlib/include/general_util/debug_util.h
        odlib/include/general_util/inline_util.h
        odlib/include/general_util/sizes.h
        odlib/include/general_util/config_util.h
        odlib/include/multicast/multicast.h
        odlib/src/multicast/multicast.c
        odlib/include/network_api/network_context.h
        odlib/include/fifo/fifo.h
        odlib/include/network_api/netw_func.h
        odlib/include/network_api/ctx_templates.h
        odlib/src/network_api/init_qp_meta.c
        odlib/src/network_api/init_ctx.c
        odlib/src/network_api/main_loop_api.c odlib/src/trace/trace.c odlib/src/client_api/interface.c odlib/src/client/wrkr_side_calls.c odlib/include/protocols/top_prot_sel.h odlib/include/protocols/main_prot_sel.h odlib/include/protocols/wrkr_prot_sel.h odlib/include/protocols/kvs_prot_sel.h odlib/include/protocols/stats_prot_sel.h craq/include/craq/cr_kvs_util.h)

set(KITE_SOURCE_FILES
        kite/src/kite/util.c
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
        zookeeper/src/zookeeper/zk_stats.c
        zookeeper/src/zookeeper/zk_util.c
        zookeeper/include/zookeeper/zk_inline_util.h
        zookeeper/include/zookeeper/zk_bqr.h
        zookeeper/include/zookeeper/zk_latency.h
        zookeeper/include/zookeeper/zk_main.h
        zookeeper/include/zookeeper/zk_util.h
        zookeeper/include/zookeeper/zk_config.h
        zookeeper/include/zookeeper/zk_kvs_util.h
        zookeeper/include/zookeeper/zk_opcodes.h
        zookeeper/include/zookeeper/zk_debug_util.h
        zookeeper/include/zookeeper/zk_reservation_stations_util.h
        zookeeper/include/zookeeper/zk_generic_util.h)

set(DR_SOURCE_FILES
        derecho/include/derecho/dr_config.h
        derecho/include/derecho/dr_inline_util.h
        derecho/src/derecho/dr_stats.c
        derecho/include/derecho/dr_messages.h
        derecho/include/derecho/dr_reserve_stations.h
        derecho/include/derecho/dr_debug_util.h
        derecho/include/derecho/dr_kvs_util.h
        derecho/include/derecho/dr_generic_util.h
        derecho/include/derecho/dr_util.h)

set(HERMES_SOURCE_FILES
        hermes/include/hermes/hr_config.h
        hermes/include/hermes/hr_inline_util.h
        hermes/include/hermes/hr_util.h
        hermes/include/hermes/hr_messages.h
        hermes/src/hermes/hr_stats.c
        hermes/include/hermes/hr_kvs_util.h
        hermes/include/hermes/hr_debug_util.h hermes/src/hermes/hr_main_loop.c hermes/src/hermes/hr_kvs.c hermes/src/hermes/hr_util.c)


set(CHT_SOURCE_FILES
        cht/include/cht/cht_util.h
        cht/include/cht/cht_config.h
        cht/include/cht/cht_kvs_util.h
        cht/include/cht/cht_inline_util.h
        cht/include/cht/cht_debug_util.h
        cht/include/cht/cht_messages.h
        cht/src/cht/cht_stats.c
        cht/include/cht/cht_reserve_stations.h )

set(CRAQ_SOURCE_FILES
        craq/include/craq/cr_config.h
        craq/include/craq/cr_util.h
        craq/include/craq/cr_inline_util.h
        craq/include/craq/cr_debug_util.h
        craq/src/craq/cr_stats.c craq/include/craq/cr_messages.h craq/include/craq/cr_generic_util.h)


#set(CMAKE_BUILD_TYPE Release)

add_executable(derecho ${DR_SOURCE_FILES} ${ODLIB_FILES})
add_executable(kite ${KITE_SOURCE_FILES} ${ODLIB_FILES})
add_executable(zookeeper ${ZK_SOURCE_FILES} ${ODLIB_FILES})
add_executable(hermes ${HERMES_SOURCE_FILES} ${ODLIB_FILES})
add_executable(cht ${CHT_SOURCE_FILES} ${ODLIB_FILES})
add_executable(craq ${CRAQ_SOURCE_FILES} ${ODLIB_FILES})


set_target_properties(derecho PROPERTIES COMPILE_FLAGS -DDERECHO)
set_target_properties(kite PROPERTIES COMPILE_FLAGS -DKITE )
set_target_properties(zookeeper PROPERTIES COMPILE_FLAGS -DZOOKEEPER)
set_target_properties(hermes PROPERTIES COMPILE_FLAGS -DHERMES)
set_target_properties(cht PROPERTIES COMPILE_FLAGS -DCHT)
set_target_properties(craq PROPERTIES COMPILE_FLAGS -DCRAQ)


target_link_libraries(derecho pthread ibverbs rt numa rdmacm)
target_link_libraries(kite pthread ibverbs rt numa rdmacm)
target_link_libraries(zookeeper pthread ibverbs rt numa rdmacm)
target_link_libraries(hermes pthread ibverbs rt numa rdmacm)
target_link_libraries(cht pthread ibverbs rt numa rdmacm)
target_link_libraries(craq pthread ibverbs rt numa rdmacm)



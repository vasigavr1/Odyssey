cmake_minimum_required(VERSION 3.16)
project(odyssey)

set(odyssey_VERSION_MAJOR 1)


set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3 -std=gnu1x -march=native -Wno-address-of-packed-member \
-Wno-format-truncation -D_GNU_SOURCE  -g ")
#set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -O3 -std=gnu2x -flto ")
# -g
# -fno-omit-frame-pointer  enable the frame pointer to trace stack
#-fno-inline
#-fsanitize=address -fsanitize=thread
# -fsanitize=address -fno-inline -fno-omit-frame-pointer

set(COMMON_INCLUDE_DIRS
        /usr/include/
        /usr/include/rdma
        /usr/include/infiniband
        odlib/incldue
        odlib/include/fifo
        odlib/include/libhrd
        odlib/include/mica
        odlib/include/general_util
        odlib/include/multicast
        odlib/include/network_api
        odlib/include/client_api
        odlib/include/trace
        odlib/include/protocols)

set (PAXOS_INCLUDE_DIRS
        paxos/include/paxos/
        paxos/include/paxos_inline_util/
        paxos/include/cp_core)

set (KITE_INCLUDE_DIRS
        kite/include/kite
        kite/include/kite_inline_util)


set(ODLIB_FILES
        odlib/src/main/od_main.c
        odlib/src/main/od_worker.c
        odlib/src/mica/od_kvs.c
        odlib/src/main/od_stats.c
        #libhrd
        odlib/include/libhrd/od_hrd.h
        odlib/src/libhrd/od_hrd_util.c
        odlib/src/libhrd/od_hrd_conn.c
        #mica
        odlib/src/mica/od_city.c
        odlib/include/mica/od_kvs.h
        odlib/include/mica/od_city.h
        /usr/include/infiniband/verbs.h
        odlib/src/client/od_client.c
        odlib/include/general_util/od_generic_macros.h
        odlib/include/client_api/od_wrkr_side_calls.h
        odlib/include/client_api/od_interface.h
        odlib/include/general_util/od_latency_util.h
        odlib/include/general_util/od_stats.h
        odlib/include/general_util/od_top.h
        odlib/include/general_util/od_init_func.h
        odlib/include/general_util/od_generic_inline_util.h
        odlib/include/general_util/od_init_connect.h
        odlib/include/general_util/od_generic_opcodes.h
        odlib/include/trace/od_trace_util.h
        odlib/include/general_util/od_rdma_gen_util.h
        odlib/include/general_util/od_debug_util.h
        odlib/include/general_util/od_inline_util.h
        odlib/include/general_util/od_sizes.h
        odlib/include/general_util/od_config_util.h
        odlib/include/multicast/od_multicast.h
        odlib/src/multicast/od_multicast.c
        odlib/include/network_api/od_network_context.h
        odlib/include/fifo/od_fifo.h
        odlib/include/network_api/od_netw_func.h
        odlib/include/network_api/od_templates.h
        odlib/src/network_api/od_init_qp_meta.c
        odlib/src/network_api/od_init_ctx.c
        odlib/src/network_api/od_main_loop_api.c
        odlib/src/trace/od_trace.c
        odlib/src/client_api/od_interface.c
        odlib/src/client/od_wrkr_side_calls.c
        odlib/include/protocols/od_top_prot_sel.h
        odlib/include/protocols/od_main_prot_sel.h
        odlib/include/protocols/od_wrkr_prot_sel.h
        odlib/include/protocols/od_kvs_prot_sel.h
        odlib/include/protocols/od_stats_prot_sel.h)

set(KITE_SOURCE_FILES
        kite/src/kite/kt_util.c
        kite/src/kite/kt_stats.c
        kite/include/kite_inline_util/kt_inline_util.h
        kite/include/kite/kt_util.h
        kite/include/kite/kt_config.h
        kite/include/kite/kt_opcodes.h
        kite/include/kite_inline_util/kt_generic_util.h
        kite/include/kite_inline_util/kt_kvs_util.h
        kite/include/kite_inline_util/kt_debug_util.h
        kite/include/kite_inline_util/kt_config_util.h
        kite/include/kite_inline_util/kt_paxos_util.h
        kite/include/kite_inline_util/kt_reserve_stations_util.h
        kite/include/kite_inline_util/kt_communication_utility.h
        kite/include/kite_inline_util/kt_paxos_generic_util.h
        kite/include/kite/kt_messages.h
        kite/include/kite/kt_buffer_sizes.h
        kite/include/kite/kt_main.h)

set(PAXOS_SOURCE_FILES
        paxos/include/paxos_inline_util/cp_inline_util.h
        paxos/include/paxos/cp_main.h
        paxos/include/paxos/cp_util.h
        paxos/include/paxos/cp_config.h
        paxos/include/paxos/cp_opcodes.h
        paxos/include/paxos_inline_util/cp_generic_util.h
        paxos/include/paxos_inline_util/cp_kvs_util.h
        paxos/include/paxos_inline_util/cp_debug_util.h
        paxos/include/paxos_inline_util/cp_reserve_stations_util.h
        paxos/include/cp_core/cp_core_debug.h
        paxos/include/paxos/cp_messages.h
        paxos/include/paxos/cp_buffer_sizes.h
        paxos/src/paxos/cp_util.c
        paxos/src/paxos/cp_stats.c
        paxos/include/cp_core/cp_core_util.h
        paxos/include/cp_core/cp_core_generic_util.h
        paxos/src/cp_core/cp_handle_rmw_rep.c paxos/src/cp_core/cp_commit_alg.c paxos/src/cp_core/cp_handle_remote_rmw.c paxos/src/cp_core/cp_rmw_fsm.c paxos/src/cp_core/inspect_accepts.c paxos/src/cp_core/inspect_props.c)

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
        hermes/include/hermes/hr_debug_util.h
        hermes/src/hermes/hr_main_loop.c
        hermes/src/hermes/hr_kvs.c
        hermes/src/hermes/hr_util.c)


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
        craq/src/craq/cr_stats.c
        craq/include/craq/cr_messages.h
        craq/include/craq/cr_generic_util.h
        craq/include/craq/cr_kvs_util.h)


#set(CMAKE_BUILD_TYPE Release)

add_executable(derecho ${DR_SOURCE_FILES} ${ODLIB_FILES})
add_executable(kite ${KITE_SOURCE_FILES} ${ODLIB_FILES})
add_executable(zookeeper ${ZK_SOURCE_FILES} ${ODLIB_FILES})
add_executable(hermes ${HERMES_SOURCE_FILES} ${ODLIB_FILES})
add_executable(cht ${CHT_SOURCE_FILES} ${ODLIB_FILES})
add_executable(craq ${CRAQ_SOURCE_FILES} ${ODLIB_FILES})
add_executable(paxos ${PAXOS_SOURCE_FILES} ${ODLIB_FILES})

target_include_directories(derecho PUBLIC
        ${COMMON_INCLUDE_DIRS} derecho/include/derecho/)
target_include_directories(kite PUBLIC
        ${COMMON_INCLUDE_DIRS} ${KITE_INCLUDE_DIRS})
target_include_directories(zookeeper PUBLIC
        ${COMMON_INCLUDE_DIRS} zookeeper/include/zookeeper)
target_include_directories(hermes PUBLIC
        ${COMMON_INCLUDE_DIRS} hermes/include/hermes)
target_include_directories(cht PUBLIC
        ${COMMON_INCLUDE_DIRS} cht/include/cht)
target_include_directories(craq PUBLIC
        ${COMMON_INCLUDE_DIRS} craq/include/craq)
target_include_directories(paxos PUBLIC
        ${COMMON_INCLUDE_DIRS} ${PAXOS_INCLUDE_DIRS})

set_target_properties(derecho PROPERTIES COMPILE_FLAGS -DDERECHO)
set_target_properties(kite PROPERTIES COMPILE_FLAGS -DKITE )
set_target_properties(zookeeper PROPERTIES COMPILE_FLAGS -DZOOKEEPER)
set_target_properties(hermes PROPERTIES COMPILE_FLAGS -DHERMES)
set_target_properties(cht PROPERTIES COMPILE_FLAGS -DCHT)
set_target_properties(craq PROPERTIES COMPILE_FLAGS -DCRAQ)
set_target_properties(paxos PROPERTIES COMPILE_FLAGS -DPAXOS)


target_link_libraries(derecho pthread ibverbs rt numa rdmacm)
target_link_libraries(kite pthread ibverbs rt numa rdmacm)
target_link_libraries(zookeeper pthread ibverbs rt numa rdmacm)
target_link_libraries(hermes pthread ibverbs rt numa rdmacm)
target_link_libraries(cht pthread ibverbs rt numa rdmacm)
target_link_libraries(craq pthread ibverbs rt numa rdmacm)
target_link_libraries(paxos pthread ibverbs rt numa rdmacm)





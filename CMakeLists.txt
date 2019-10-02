cmake_minimum_required(VERSION 2.8)
project(drf-sc)

set(KeyValueCache_VERSION_MAJOR 1)
set(KeyValueCache_VERSION_MINOR 0)

include_directories( include/libhrd include/drf-sc include/optik include/mica /usr/include/)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall ")

set(SOURCE_FILES
        #Cache
        src/drf-sc/util.c
        src/drf-sc/cache.c
        src/drf-sc/main.c
        src/drf-sc/worker.c
        src/drf-sc/stats.c
        #libhrd
        src/libhrd/hrd_util.c
        src/libhrd/hrd_conn.c
        #mica
        src/mica/city.c
        src/mica/mica.c
        src/mica/test.c
        ##### header files ####
        #drf-sc
        include/drf-sc/cache.h
        include/drf-sc/inline_util.h
        include/drf-sc/main.h
        include/drf-sc/util.h
        #libhrd
        include/libhrd/hrd.h
        include/libhrd/hrd_sizes.h
        #mica
        include/mica/city.h
        include/mica/mica.h
        #optik
        include/optik/optik_mod.h
        include/optik/utils.h
        include/optik/random.h
        include/optik/getticks.h
        include/optik/atomic_ops_if.h
        include/optik/ssalloc.h
        include/optik/measurements.h
        /usr/include/infiniband/verbs.h
        #Paxos Verifier
        src/PaxosVerifier/PaxosVerifier.cpp src/drf-sc/client.c)



add_executable(drf-sc ${SOURCE_FILES})

target_link_libraries(drf-sc pthread ibverbs rt memcached numa rdmacm)



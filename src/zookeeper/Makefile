LD      := gcc
LDFLAGS := -O3 -std=gnu2x -flto -lgsl -lgslcblas -libverbs -lrt -lpthread -lmemcached -lnuma -lrdmacm
CC  = gcc
CFLAGS   = -O3 -std=gnu2x -Wno-address-of-packed-member -Wno-format-truncation -g  -march=native -DZOOKEEPER \
           -I../../include/mica -I../../include/libhrd -I../../include/zookeeper -I../../include/optik \
           -I../../include/general_util
APPS    := zookeeper clean-o
PROF    := -g -fno-omit-frame-pointer

all: ${APPS}

zookeeper: ../libhrd/hrd_conn.o ../libhrd/hrd_util.o  ../mica/city.o  ../mica/kvs.o \
           zk_main.o zk_util.o zk_leader.o zk_follower.o zk_stats.o#zk_cache.o
	${LD} -o $@ $^ ${LDFLAGS}



PHONY: clean
clean:
	rm -f *.o ../libhrd/*.o ../mica/*.o ${APPS}
clean-o:
	rm -f *.o ../libhrd/*.o ../mica/*.o

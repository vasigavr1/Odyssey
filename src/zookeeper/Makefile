LD      := gcc-7
LDFLAGS := -O3 -std=gnu11 -flto -lgsl -lgslcblas -libverbs -lrt -lpthread -lmemcached -lnuma -lrdmacm
CC  = gcc-7
CFLAGS   = -O3 -std=gnu11 -g -DZOOKEEPER -I../../include/mica -I../../include/libhrd -I../../include/zookeeper -I../../include/optik \
           -I../../include/general_util
APPS    := zookeeper clean-o
PROF    := -g -fno-omit-frame-pointer

all: ${APPS}

zookeeper: ../libhrd/hrd_conn.o ../libhrd/hrd_util.o  ../mica/city.o zk_main.o #zk_leader.o zk_follower.o zk_cache.o zk_util.o zk_stats.o
	${LD} -o $@ $^ ${LDFLAGS}



PHONY: clean
clean:
	rm -f *.o ../libhrd/*.o ../mica/*.o ${APPS}
clean-o:
	rm -f *.o ../libhrd/*.o ../mica/*.o

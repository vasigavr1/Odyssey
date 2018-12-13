LD      := gcc
LDFLAGS := -O3 -std=gnu11 -flto -lrt -lpthread -lnuma -libverbs -lmemcached  -lrdmacm#-lpapi
CC = gcc
CFLAGS =  -O3 -std=gnu11  -I../../include/mica -I../../include/libhrd -I../../include/drf-sc
APPS    := test

all: ${APPS}

#test: ../libhrd/hrd_conn.o ../libhrd/hrd_util.o mica.o city.o test.o
#	${LD} -o $@ $^ ${LDFLAGS}
#	rm -rd test.o test

PHONY: clean
clean:
	rm -f *.o ../libhrd/*.o ${APPS}

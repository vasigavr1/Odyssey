LD      := gcc
LDFLAGS := -O3 -std=gnu2x -flto  -libverbs -lpthread -lnuma -lrdmacm
CC  = gcc
CFLAGS   = -O3 -std=gnu2x -Wno-address-of-packed-member  -Wno-format-truncation -g -march=native -I../../include/mica -I../../include/libhrd -I../../include/drf-sc
APPS    := drf-sc clean-o
PROF    := -g -fno-omit-frame-pointer

all: ${APPS}

drf-sc: ../libhrd/hrd_conn.o ../libhrd/hrd_util.o  ../mica/city.o main.o client.o worker.o kvs.o util.o stats.o
	${LD} -o $@ $^ ${LDFLAGS}



PHONY: clean
clean:
	rm -f *.o ../libhrd/*.o ../mica/*.o ${APPS}
clean-o:
	rm -f *.o ../libhrd/*.o ../mica/*.o

LD      := gcc
LDFLAGS := -O3 -std=gnu2x -flto -libverbs -lpthread -lnuma -lrdmacm
CC  = gcc
CFLAGS   = -O3 -std=gnu2x -Wno-address-of-packed-member -Wno-format-truncation -g  -march=native -DKITE \
      -I../../include/mica -I../../include/libhrd -I../../include/kite -I../../include/kite_inline_util -I../../include/general_util
APPS    := kite clean-o
PROF    := -g -fno-omit-frame-pointer

all: ${APPS}

kite: ../libhrd/hrd_conn.o ../libhrd/hrd_util.o  ../mica/city.o ../mica/kvs.o main.o client.o worker.o  util.o stats.o
	${LD} -o $@ $^ ${LDFLAGS}



PHONY: clean
clean:
	rm -f *.o ../libhrd/*.o ../mica/*.o ${APPS}
clean-o:
	rm -f *.o ../libhrd/*.o ../mica/*.o

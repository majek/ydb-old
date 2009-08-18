

all:
	rm -f /tmp/berkeleydb /tmp/tokyocabinet ydb.log
	rm -rf /tmp/ydb
	mkdir /tmp/ydb
	
	gcc -Wall -O2 -g bench.c sto_bdb.c sto_tc.c storage.c sto_ydb.c \
		-I../src	\
		-L../src	\
		-o bench 	\
		-lrt -ltokyocabinet -ldb -lydb -Wl,-rpath=../src
	./bench
	
	

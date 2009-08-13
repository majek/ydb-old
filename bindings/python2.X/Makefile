CC=gcc
C_FLAGS=
L_FLAGS=


all:
	$(CC) -pthread -fno-strict-aliasing -DNDEBUG -g -fwrapv -O2 -Wall -fPIC -I/usr/include/python2.5 -I../../src -c ydbmodule.c
	$(CC) -pthread -shared -lydb -L../../src -Wl,-O1 -Wl,-Bsymbolic-functions -Wl,-rpath=../../src ydbmodule.o -o ydb.so

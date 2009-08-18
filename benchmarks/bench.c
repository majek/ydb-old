#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include "backend.h"

#define TIMESPEC_SUBTRACT(a,b) ((long long)((a).tv_sec - (b).tv_sec) * 1000000000LL + (a).tv_nsec - (b).tv_nsec)


typedef MC_STORAGE_API * (*storage_engine_create)(char *dir);
typedef void (*storage_engine_destroy)(MC_STORAGE_API *api);

struct storage_engines{
	char *name;
	char *path;
	storage_engine_create create;
	storage_engine_destroy destroy;
};


struct storage_engines engines[] = {
	{"tc", "/tmp/tokyocabinet", 	&storage_tc_create, &storage_tc_destroy,},
	{"bdb", "/tmp/berkeleydb", 	&storage_bdb_create, &storage_bdb_destroy,},
	{"ydb", "/tmp/ydb", 		&storage_ydb_create, &storage_ydb_destroy,},
	{ 0 },
};


void dumb_replace(MC_STORAGE_API *api) {
	int i;
	for(i=0; i<40000; i++) {
		storage_set(api, "a", 1, "b", 1);
	}
}

void many_items(MC_STORAGE_API *api) {
	int i;
	for(i=0; i<40000; i++) {
		char key[256];
		snprintf(key, sizeof(key), "key_%i", i);
		storage_set(api, "v", 1, key, strlen(key));
	}
}

void more_than_ram(MC_STORAGE_API *api) {
	int i, j;
	char value[1*1024*1024];
	char key[256];
	memset(value, 'x', sizeof(value));
	memset(key, '\0', sizeof(key));
	
	srandom(0x1234);
	
	for(j=0; j<16; j++) {
		for(i=0; i<16000; i++) {
			int k = random() % 16384;
			int l = random() % 16384 + 4096;
			snprintf(key, sizeof(key), "key_%i", k);
			storage_set(api, value, l, key, strlen(key));
		}
	}
}


typedef void (*do_fun_t)(MC_STORAGE_API *api);

struct do_funs_t{
	char *name;
	do_fun_t do_fun;
};

struct do_funs_t do_funs[] = {
	{"dumb_replace", dumb_replace},
	{"many_items", many_items},
	{"more_than_ram1", more_than_ram},
	{"more_than_ram2", more_than_ram},
	{NULL, NULL}
};

int main(int argc, char **argv) {
	int eng_no, fun_no;
	
	struct timespec t0, t1;
	long long td;
	for(fun_no=0; do_funs[fun_no].name; fun_no++) {
		printf("Running benchmark: %s\n", do_funs[fun_no].name);
		for(eng_no=0; engines[eng_no].name; eng_no++) {
			clock_gettime(CLOCK_MONOTONIC, &t0);

			struct storage_engines eng = engines[eng_no];
			MC_STORAGE_API *api = eng.create(eng.path);
			do_funs[fun_no].do_fun(api);
			eng.destroy(api);
			
			clock_gettime(CLOCK_MONOTONIC, &t1);
			td = TIMESPEC_SUBTRACT(t1, t0);
			printf(" %4s: %4llims\n", eng.name, td/1000000);
		}
	}

	return(0);
}

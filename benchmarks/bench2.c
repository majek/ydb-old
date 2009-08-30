#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <unistd.h>
#include <assert.h>


#include "backend.h"

#define TIMESPEC_SUBTRACT(a,b) ((long long)((a).tv_sec - (b).tv_sec) * 1000000000LL + (a).tv_nsec - (b).tv_nsec)

//	{"ydb", "/tmp/ydb", 		&storage_ydb_create, &storage_ydb_destroy,},


unsigned int get_rand() {
	static int fd;
	static unsigned int buf[4096];
	static int buf_pos;
	
	if(fd == 0) {
		fd = open("/dev/urandom", O_RDONLY);
		buf_pos = sizeof(buf)/sizeof(buf[0]);
	}
	
	if(buf_pos == sizeof(buf)/sizeof(buf[0])) {
		buf_pos = 0;
		int r = read(fd, buf, sizeof(buf));
		assert(r == sizeof(buf));
	}
	
	return(buf[buf_pos++]);
}

extern int ydb_log_size;
extern int ydb_overcommit_factor;

int main() {
	ydb_log_size = 128*1024*1024;
	ydb_overcommit_factor = 3;
	struct timespec t0, t1;
	long long td;

	char value[65536];
	memset(value, 'v', sizeof(value));

	MC_STORAGE_API *api = storage_ydb_create("/tmp/ydb");
	int i=0, j, r;
	while(1) {
		i++;
		clock_gettime(CLOCK_MONOTONIC, &t0);
		
		for(j=0; j<4096; j++) {
			char key[256];
			r = get_rand() % (100*1000);
			int vlen = 80 + (get_rand() % 128);
			int klen = snprintf(key, sizeof(key), "key_%i", r);
			storage_set(api, value, vlen, key, klen);
		}
		
		clock_gettime(CLOCK_MONOTONIC, &t1);
		td = TIMESPEC_SUBTRACT(t1, t0);
		printf(" %4i: %4llims  %.3f rec/sec\n", i, td/1000000, (4096.0/(td/1000000000.0)));
	}
	storage_ydb_destroy(api);
	return(0);
}


#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <ydb.h>

char value[2*1024*1024];
char value2[2*1024*1024];

int main(int argc, char **argv) {
	char key[256];
	memset(key, 0, sizeof(key));
	memset(value, 0, sizeof(value));
	memset(value2, 0, sizeof(value2));
	int i;

	if(argc != 2) {
		printf("Usage: %s [ydb_directory]\n", argv[0]);
		exit(-1);
	}
	
	char ydb_dir[256];
	snprintf(ydb_dir, sizeof(ydb_dir),"%s", argv[1]);
	YDB ydb = ydb_open(ydb_dir, 2, 4*1024*1024, YDB_CREAT);
	
	snprintf(key, sizeof(key), "xxxx");
	ydb_add(ydb, key, sizeof(key), value, sizeof(value)/2);

	/* add */
	for(i=0; i < 64; i++) {
		snprintf(key, sizeof(key), "key %i", i);
		ydb_add(ydb, key, strlen(key), "a", 1); /* something small, for gc */
		ydb_add(ydb, key, sizeof(key), value, sizeof(value)/2);
	}
	/* replace */
	for(i=1; i < 64; i++) {
		snprintf(key, sizeof(key), "key %i", i);
		ydb_add(ydb, key, sizeof(key), value, sizeof(value)/4);
	}
	/* delete */
	for(i=2; i < 64; i++) {
		snprintf(key, sizeof(key), "key %i", i);
		ydb_del(ydb, key, sizeof(key));
	}
	/* get */
	for(i=3; i < 64; i++) {
		snprintf(key, sizeof(key), "key %i", i);
		ydb_get(ydb, key, sizeof(key), value2, sizeof(value2));
	}
	ydb_sync(ydb);
	ydb_close(ydb);
	
	/* reopen */
	ydb = ydb_open(ydb_dir, 4, 5*1024*1024, 0);
	ydb_add(ydb, key, sizeof(key), value, sizeof(value)/2);
	ydb_close(ydb);
	return(0);
}

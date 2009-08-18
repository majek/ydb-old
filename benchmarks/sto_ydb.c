#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "backend.h"

#include <ydb.h>

struct ydb_data{
	YDB ydb;
};
typedef struct ydb_data YDB_DATA;


static int get(void *storage_data, char *dst, int dst_sz, char *key, int key_sz) {
	YDB_DATA *ydbd = (YDB_DATA *)storage_data;
	return ydb_get(ydbd->ydb, key, key_sz, dst, dst_sz);
}

static int del(void *storage_data, char *key, int key_sz) {
	YDB_DATA *ydbd = (YDB_DATA *)storage_data;
	return ydb_del(ydbd->ydb, key, key_sz);
}


static int set(void *storage_data, char *value, int value_sz, char *key, int key_sz) {
	YDB_DATA *ydbd = (YDB_DATA *)storage_data;
	return ydb_add(ydbd->ydb, key, key_sz, value, value_sz);
}


static void sync(void *storage_data) {
	YDB_DATA *ydbd = (YDB_DATA *)storage_data;
	ydb_sync(ydbd->ydb);
}


MC_STORAGE_API *storage_ydb_create(char *db_file) {
	YDB_DATA *ydbd = (YDB_DATA *)zmalloc(sizeof(YDB_DATA));

	ydbd->ydb = ydb_open(db_file, 4, 512*1024*1024, YDB_CREAT);
	if(ydbd->ydb == NULL) {
		fprintf(stderr, "ydb_create()=NULL\n");
		abort();
	}
	
	MC_STORAGE_API *api = (MC_STORAGE_API *)zmalloc(sizeof(MC_STORAGE_API));
	api->get = &get;
	api->set = &set;
	api->del = &del;
	api->sync = &sync;
	api->storage_data = ydbd;
	return(api);
}

void storage_ydb_destroy(MC_STORAGE_API *api) {
	YDB_DATA *ydbd = (YDB_DATA *)api->storage_data;
	
	ydb_close(ydbd->ydb);
	
	free(ydbd);
	free(api);
}



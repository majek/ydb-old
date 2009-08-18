
#include <stdlib.h>
#include <string.h>

#include <sys/types.h>
#include <event.h>
#include "backend.h"

#include <tcutil.h>
#include <tchdb.h>
#include <stdbool.h>

struct tc_data{
	char *db_file;
	TCHDB *hdb;
};
typedef struct tc_data TC_DATA;

extern const char *tcversion;

// -1 -> not exists
int tc_get(void *storage_data, char *dst, int size, char *key, int key_sz) {
	TC_DATA *tcd = (TC_DATA *)storage_data;
	int ret = tchdbget3(tcd->hdb, key, key_sz, dst, size);
	return(ret);
}

// -1 -> not exits
// else: removed
int tc_del(void *storage_data, char *key, int key_sz) {
	TC_DATA *tcd = (TC_DATA *)storage_data;
	if( tchdbout(tcd->hdb, key, key_sz) )
		return(0);
	return(-1);
}

// written bytes
// -1 -> not saved
int tc_set(void *storage_data, char *value, int value_sz, char *key, int key_sz) {
	TC_DATA *tcd = (TC_DATA *)storage_data;
	int ret = tchdbput(tcd->hdb, key, key_sz, value, value_sz);
	if(ret)
		return(value_sz);
	return(-1);
}

void tc_sync(void *storage_data) {
	TC_DATA *tcd = (TC_DATA *)storage_data;
	tchdbsync(tcd->hdb);
}


MC_STORAGE_API *storage_tc_create(char *db_file) {	
	TC_DATA *tcd = (TC_DATA *)zmalloc(sizeof(TC_DATA));
	tcd->db_file = strdup(db_file);
	tcd->hdb = tchdbnew();
	if(!tchdbopen(tcd->hdb, db_file, HDBOWRITER | HDBOCREAT)) {
		int ecode = tchdbecode(tcd->hdb);
		fprintf(stderr, "tchdbopen()= %s\n", tchdberrmsg(ecode));
		abort();
	}

	MC_STORAGE_API *api = (MC_STORAGE_API *)zmalloc(sizeof(MC_STORAGE_API));
	api->get = &tc_get;
	api->set = &tc_set;
	api->del = &tc_del;
	api->sync = &tc_sync;
	api->storage_data = tcd;
	return(api);
}

void storage_tc_destroy(MC_STORAGE_API *api) {
	TC_DATA *tcd = (TC_DATA *)api->storage_data;
	
	tchdbsync(tcd->hdb);
	tchdbclose(tcd->hdb);
	tchdbdel(tcd->hdb);
	
	free(tcd->db_file);
	free(tcd);
	free(api);
}


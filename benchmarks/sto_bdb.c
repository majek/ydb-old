
#include <stdlib.h>
#include <string.h>

#include <sys/types.h>
#include <event.h>
#include "backend.h"


#include <stdbool.h>

#include <db.h>


struct bdb_data{
	char *db_file;
	DB *dbp;
};
typedef struct bdb_data BDB_DATA;


// -1 -> not exists
int bdb_get(void *storage_data, char *dst, int size, char *key, int key_sz) {
	BDB_DATA *bdb = (BDB_DATA *)storage_data;
	DBT k, d;
	memset(&k, 0, sizeof(DBT));
	memset(&d, 0, sizeof(DBT));

	k.data = key;
	k.size = key_sz;
	
	d.data = dst;
	d.ulen = size;
	d.flags = DB_DBT_USERMEM;

	int ret = bdb->dbp->get(bdb->dbp, NULL, &k, &d, 0);
	if(ret == DB_NOTFOUND)
		return(-1);
	if(ret) {
		fprintf(stderr, "bdb_get()=%s", db_strerror(ret));
		return(-1);
	}
	return(d.size);
}

// -1 -> not exits
// else: removed
int bdb_del(void *storage_data, char *key, int key_sz) {
	BDB_DATA *bdb = (BDB_DATA *)storage_data;
	DBT k;
	memset(&k, 0, sizeof(DBT));

	k.data = key;
	k.size = key_sz;
	
	int ret = bdb->dbp->del(bdb->dbp, NULL, &k, 0);
	if(ret == DB_NOTFOUND)
		return(-1);
	if(ret) {
		fprintf(stderr, "bdb_del()=%s", db_strerror(ret));
		return(-1);
	}
	return(0);
}

// written bytes
// -1 -> not saved
int bdb_set(void *storage_data, char *value, int value_sz, char *key, int key_sz) {
	BDB_DATA *bdb = (BDB_DATA *)storage_data;
	DBT k, d;
	memset(&k, 0, sizeof(DBT));
	memset(&d, 0, sizeof(DBT));

	k.data = key;
	k.size = key_sz;
	
	d.data = value;
	d.size = value_sz;
	
	int ret = bdb->dbp->put(bdb->dbp, NULL, &k, &d, 0);
	if(ret) {
		fprintf(stderr, "bdb_put()=%s", db_strerror(ret));
		return(-1);
	}
	return(value_sz);
}

void bdb_sync(void *storage_data) {
	BDB_DATA *bdb = (BDB_DATA *)storage_data;
	bdb->dbp->sync(bdb->dbp, 0);
}

MC_STORAGE_API *storage_bdb_create(char *db_file) {
	int ret;
	
	BDB_DATA *bdb = (BDB_DATA *)zmalloc(sizeof(BDB_DATA));
	bdb->db_file = strdup(db_file);

	ret = db_create(&bdb->dbp, NULL, 0);
	if(ret != 0) {
		fprintf(stderr, "db_create()=%s\n", db_strerror(ret));
		abort();
	}
	
	ret = bdb->dbp->open(bdb->dbp, /* DB structure pointer */
		NULL,		/* Transaction pointer */
		db_file,	/* On-disk file that holds the database. */
		NULL,		/* Optional logical database name */
		DB_BTREE,	/* Database access method */
		DB_CREATE,	/* Open flags */
		0);		/* File mode (using defaults) */
	if (ret != 0){
		fprintf(stderr, "bdb_open()=%s\n", db_strerror(ret));
		abort();
	}
	
	MC_STORAGE_API *api = (MC_STORAGE_API *)zmalloc(sizeof(MC_STORAGE_API));
	api->get = &bdb_get;
	api->set = &bdb_set;
	api->del = &bdb_del;
	api->sync = &bdb_sync;
	api->storage_data = bdb;
	return(api);
}

void storage_bdb_destroy(MC_STORAGE_API *api) {
	BDB_DATA *bdb = (BDB_DATA *)api->storage_data;
	
	bdb->dbp->sync(bdb->dbp, 0);
	bdb->dbp->close(bdb->dbp, 0);
	
	free(bdb->db_file);
	free(bdb);
	free(api);
}


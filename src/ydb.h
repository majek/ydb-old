#ifndef YDB_H
#define YDB_H


/* A database structure. */
typedef void *YDB;

#define YDB_CREAT	(0x01)
#define YDB_RDONLY	(0x02)
#define YDB_GCDISABLE	(0x04)

/* Open a database from a specified directory, or create a new one. */
YDB ydb_open(char *directory,
	     int overcommit_factor,
	     unsigned long long min_log_size,
	     int flags);

/* Make sure everything needed is flushed to disk. */
void ydb_sync(YDB ydb);

/* Close the database and free database handler */
void ydb_close(YDB ydb);

/* Add/modify a key */
int ydb_add(YDB ydb, char *key, unsigned short key_sz,
	    char *value, unsigned int value_sz);

/* Delete a key */
int ydb_del(YDB ydb, char *key, unsigned short key_sz);

/* Retrieve a value for selected key */
int ydb_get(YDB ydb, char *key, unsigned short key_sz,
	    char *buf, unsigned int buf_sz);


void ydb_prefetch(YDB ydb, char **keys, unsigned short *key_szs, int items_counter);


#endif

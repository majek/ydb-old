#ifndef YDB_H
#define YDB_H

/***************************************************************************
 *  *
 *  *
 *  *
 */

/* A database structure. */
typedef void *YDB;

/* Open a database from a specified directory, or create a new one. */
YDB ydb_open(char *directory,
	     int overcommit_factor, unsigned long long max_file_size);

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


#endif

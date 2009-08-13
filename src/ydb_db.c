#include "ydb_internal.h"

#include <stdlib.h>
#include <string.h>

// getrlimit
#include <sys/resource.h>
// fsync
#include <unistd.h>

static void db_close(struct db *db, int save_index);


YDB ydb_open(char *top_dir,
	     int overcommit_ratio, unsigned long long max_file_size) {

	// 8MiB minimal limit
	if(max_file_size < (MAX_RECORD_SIZE))
		return(NULL);
	// 2GiB maximum for 32-bit platform, we're mmaping the file on load
	if(sizeof(char*) == 4 && max_file_size > (2<<30))
		return(NULL);
	
	struct db *db = (struct db *)zmalloc(sizeof(struct db));
	db->magic = YDB_STRUCT_MAGIC;
	db->top_dir = strdup(top_dir);
	db->overcommit_ratio = overcommit_ratio;
	db->lock = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
	
	char buf[256];
	snprintf(buf, sizeof(buf), "%s%s%s", top_dir, PATH_DELIMITER, "index.ydb");
	
	int logno = 0;
	u64 record_offset = 0;
	if(tree_open(&db->tree, buf, &logno, &record_offset) < 0) {
		log_error("Failed to load index file from %s", top_dir);
		/* TODO: memleaks here */
		//return(NULL);
	}
	
	int r = loglist_open(&db->loglist, top_dir, max_file_size, max_descriptors());
	if(r < 0){
		/* TODO: memleaks here */
		return(NULL);
	}
	/* rebuild the original tree in memory */
	int record_sz = END_OF_FILE;
	while(1) {
		if(record_sz == END_OF_FILE) {
			/* find an edge */
			struct log *log = slot_get(&db->loglist, logno);
			if(log)
				db_info("Loading metadata from log %i/0x%x, offsets %lli-%lli", logno, logno, record_offset, log->file_size);

		}
		char key[MAX_KEY_SIZE];
		u16 key_sz = MAX_KEY_SIZE-1;
		int flags;
		u64 value_offset;
		u32 value_sz;
	
		record_sz = loglist_get_record(&db->loglist,
					logno, record_offset,
					key, &key_sz,
					&value_offset, &value_sz,
					&flags);
		if(record_sz == END_OF_FILE) {
			logno++;
			record_offset = 0;
			continue;
		}
		if(record_sz == NO_MORE_DATA)
			break;
		
		if(FLAG_DELETE & flags) {
			tree_del(&db->tree, key, key_sz, logno, record_offset);
		}else{
			tree_add(&db->tree, key, key_sz, logno, value_offset, value_sz, record_offset);
		}
		record_offset += record_sz;
	}
	db_info("Loaded!");
	
	/* Compare used logs */
	int start = MIN(rarr_min(db->tree.refcnt), rarr_min(db->loglist.logs));
	int stop  = MAX(rarr_max(db->tree.refcnt), rarr_max(db->loglist.logs));
	for(logno=start; logno<stop; logno++) {
		uint refcnt = refcnt_get(&db->tree, logno);
		struct log *log = slot_get(&db->loglist, logno);
		if(refcnt == 0 && log == NULL)
			continue;
		if(refcnt && log)
			continue;
		if(refcnt) {
			db_error("Log %i(0x%x) used, but file is not loaded!", logno, logno);
			db_error("Sorry to say but you'd lost %i key-values.", refcnt);
			db_error("Closing db");
			/*  we're in inconsistent state */
			db_close(db, 0);
			return(NULL);
		}
		if(log)
			continue;
	}
	return db;
}

int db_unlink_old_logs(struct db *db) {
	int logno;
	int counter;
	int start = MIN(rarr_min(db->tree.refcnt), rarr_min(db->loglist.logs));
	int stop  = MAX(rarr_max(db->tree.refcnt), rarr_max(db->loglist.logs));
	for(logno=start; logno < stop; logno++) {
		uint refcnt = refcnt_get(&db->tree, logno);
		struct log *log = slot_get(&db->loglist, logno);
		if(refcnt == 0 && log == NULL)
			continue;
		if(refcnt && log)
			continue;
		if(refcnt) {
			/* yeah, there are references, but nobody cares */
			continue;
		}
		if(log) {
			if(loglist_is_writer(&db->loglist, logno))
				continue;
			if(logno >= (db->tree.commited_last_record_logno - 1))
				continue;
			db_info("Log '%s' (%i) doesn't contain any used values. Deleting.", log->fname, logno);
			
			loglist_unlink(&db->loglist, logno);
			counter++;
			continue;
		}
	}
	return(counter);
}

#define USED_BYTES(db)	\
	(db->tree.key_bytes + db->tree.value_bytes)

/* TODO: two adds instead of one */
#define DOUBLE_RATIO(db, default)	\
	(USED_BYTES(db) ? ((double)db->loglist.total_bytes / (double)USED_BYTES(db)) : default)
#define INT_RATIO(db, default)	\
	(USED_BYTES(db) ? (db->loglist.total_bytes / USED_BYTES(db)) : default)

void ydb_close(YDB ydb) {
	struct db *db = (struct db *) ydb;
	assert(db->magic == YDB_STRUCT_MAGIC);
	
	db_close(db, 1);
}

static void db_close(struct db *db, int save_index) {
	/* wait till gc thread exits, this can take a while */
	if(db->gc_running)
		gc_join(db);
	/* no need to lock, as the thread doesn't exist */
	
	if(fsync(db->loglist.write_fd) < 0)
		perror("fsync()");

	double ratio = DOUBLE_RATIO(db, 999.0);

	if(save_index)
		tree_save_index(&db->tree);
	log_info("Closing log. %llu/%llu Overcommit ratio:%.2f",
			USED_BYTES(db), db->loglist.total_bytes, ratio);
	
	tree_close(&db->tree);
	loglist_close(&db->loglist);
	free(db->top_dir);
	free(db);
}

void ydb_sync(YDB ydb) {
	struct db *db = (struct db *) ydb;
	assert(db->magic == YDB_STRUCT_MAGIC);
	DB_LOCK(db);
	if(fsync(db->loglist.write_fd) < 0)
		perror("fsync()");
	DB_UNLOCK(db);
}


int ydb_add(YDB ydb, char *key, unsigned short key_sz,
	    char *value, unsigned int value_sz) {
	struct db *db = (struct db *) ydb;
	assert(db->magic == YDB_STRUCT_MAGIC);
	
	if(value_sz > MAX_VALUE_SIZE)
		return(-1);

	/* TODO: error handling on write? */
	DB_LOCK(db);
	db_add(db, key, key_sz, value, value_sz);
	
	/* written two full log files and no gc running*/
	if(db->gc_running == 0 && 
	   (db->loglist.appended_bytes > (db->loglist.max_file_size << 1))) {
		db_info("Saving index");
		tree_save_index(&db->tree);
		db_unlink_old_logs(db);
	}

	u64 used_bytes = db->tree.key_bytes + db->tree.value_bytes;
	if(db->gc_running == 0 && (
	    /* overcommit threshold is reached */
	    used_bytes * db->overcommit_ratio > db->loglist.total_bytes)) {
		gc_spawn(db);
	}
	
	if(db->gc_finished)
		gc_join(db);
	DB_UNLOCK(db);
	return(value_sz);
}

/* locking done outside */
int db_add(struct db *db, char *key, u16 key_sz, char *value, u32 value_sz) {
	/* TODO: error handling on write? */
	struct append_info af;
	af = loglist_append(&db->loglist, key, key_sz, value, value_sz, FLAG_SET);
	
	tree_add(&db->tree, key, key_sz, af.logno,
				af.value_offset, value_sz, af.record_offset);
	return 1;
}



int ydb_del(YDB ydb, char *key, unsigned short key_sz) {
	struct db *db = (struct db *) ydb;
	assert(db->magic == YDB_STRUCT_MAGIC);
	
	DB_LOCK(db);
	struct append_info af;
	af = loglist_append(&db->loglist, key, key_sz, NULL, 0, FLAG_DELETE);
	tree_del(&db->tree, key, key_sz, af.logno, af.record_offset);
	DB_UNLOCK(db);
	return(-1);
}

int ydb_get(YDB ydb, char *key, unsigned short key_sz,
	    char *buf, unsigned int buf_sz) {
	struct db *db = (struct db *) ydb;
	assert(db->magic == YDB_STRUCT_MAGIC);
	
	DB_LOCK(db);
	struct item *item = tree_get(&db->tree, key, key_sz);
	if(item == NULL)
		goto error;
		
	int needed = item->value_sz + MAX_PADDING + sizeof(struct ydb_value_record);
	if(buf_sz < needed) {
		db_error("ydb_get:buffer too small. is %i, should be at least %i", buf_sz, needed);
		goto error;
	}
	
	int r = loglist_get(&db->loglist, item->logno, item->value_offset, item->value_sz, buf, buf_sz);
	if(r < 0)
		goto error;
	
	DB_UNLOCK(db);
	return(r);

error:
	DB_UNLOCK(db);
	return(-1);
}





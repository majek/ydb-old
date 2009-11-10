#define _GNU_SOURCE

#include "ydb_internal.h"

#include <stdlib.h>
#include <string.h>

// getrlimit
#include <sys/resource.h>
// fsync
#include <unistd.h>
// fadvise

#include <fcntl.h>

static void db_close(struct db *db, int save_index);

void print_stats(struct db *db) {
	int kcounter, vcounter;
	double kavg, kdev, vavg, vdev;
	
	stddev_get(db->tree.keys_stddev, &kcounter, &kavg, &kdev);
	stddev_get(db->tree.values_stddev, &vcounter, &vavg, &vdev);
	log_info("Items: %i(%i)  Avg/stddev: Keys=%.3f/%.3f Values=%.3f/%.3f",
			kcounter, vcounter,
			kavg, kdev, vavg, vdev);
}


YDB ydb_open(char *top_dir,
	     int overcommit_ratio,
	     unsigned long long min_log_size,
	     int flags) {

	log_info(" **** ");
	log_info("ydb_open(%s, %i, %llu, 0x%x)", top_dir, overcommit_ratio, min_log_size, flags);

	/* no minimal limit, but warn */
	if(min_log_size < (MAX_RECORD_SIZE))
		log_warn("min_log_size is low, are you sure? %llu < %i", min_log_size, MAX_RECORD_SIZE);
	// 2GiB maximum for 32-bit platform, we're mmaping the file on load
	if(sizeof(char*) == 4 && min_log_size >= (1<<31))
		log_warn("min_log_size is greater than 1<<31 on 32 bits machine. That's brave.");
	
	struct db *db = (struct db *)zmalloc(sizeof(struct db));
	db->magic = YDB_STRUCT_MAGIC;
	db->top_dir = strdup(top_dir);
	db->overcommit_ratio = overcommit_ratio;
	db->lock = (pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER;
	db->flags = flags;
	
	char buf[256];
	snprintf(buf, sizeof(buf), "%s%s%s", top_dir, PATH_DELIMITER, "index.ydb");
	
	int logno = 0;
	u64 record_offset = 0;
	if(tree_open(&db->tree, buf, &logno, &record_offset, flags) < 0) {
		if(!(flags & YDB_CREAT)) {
			log_error("Failed to load index file from %s", top_dir);
			/* TODO: memleaks here */
			return(NULL);
		}
	}
	
	int r = loglist_open(&db->loglist, top_dir, min_log_size, max_descriptors());
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
				log_info("Loading metadata from log %i/0x%x, offsets %lli-%lli", logno, logno, record_offset, log->file_size);

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
			log_error("Log %i(0x%x) used, but file is not readable!", logno, logno);
			log_error("Sorry to say but you'd lost %i keys.", refcnt);
			log_error("Closing db");
			/*  we're in inconsistent state */
			db_close(db, 0);
			return(NULL);
		}
		if(log)
			continue;
	}
	
	log_info("Memory footprint of a single key: %i+key_sz", sizeof(struct item));
	print_stats(db);
	return(db);
}


int db_unlink_old_logs(struct db *db) {
	int logno;
	int counter = 0;
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
			log_info("Log '%s' (%i) doesn't contain any used values. Deleting.", log->fname, logno);
			
			loglist_unlink(&db->loglist, logno);
			counter++;
			continue;
		}
	}
	return(counter);
}


void ydb_close(YDB ydb) {
	struct db *db = (struct db *) ydb;
	assert(db->magic == YDB_STRUCT_MAGIC);
	
	db_close(db, 1);
	log_info(" **** ");
}

static void db_close(struct db *db, int save_index) {
	/* wait till gc thread exits, this can take a while */
	if(db->gc_running) {
		gc_join(db);
		if(save_index){
			log_info("Saving index, on close, after gc.");
			tree_save_index(&db->tree);
			/* Unlink old logs after gc. */
			db_unlink_old_logs(db);
			save_index = 0;
		}
	}
	/* no need to lock, as the thread doesn't exist */
	loglist_fsync(&db->loglist);

	if(save_index)
		tree_save_index(&db->tree);
	log_info("Closing log. Usefull/total bytes: %llu/%llu. Current overcommit ratio/allowed ratio:%.2f/%i",
		USED_BYTES(db), db->loglist.total_bytes, 
		DOUBLE_RATIO(db, 999.0), db->overcommit_ratio);
	
	tree_close(&db->tree);
	loglist_close(&db->loglist);
	free(db->top_dir);
	free(db);
}

void ydb_sync(YDB ydb) {
	struct db *db = (struct db *) ydb;
	assert(db->magic == YDB_STRUCT_MAGIC);
	DB_LOCK(db);
	loglist_fsync(&db->loglist);
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
	if(db_add(db, key, key_sz, value, value_sz) < 0) {
		value_sz = -1;
		goto release;
	}
	
	/* written two full log files and no gc running*/
	if(db->gc_running == 0 && 
	   (db->loglist.write_logno > (db->tree.commited_last_record_logno + 2))) {
		log_info("Saving index, logno_in_last_index:%i curr_logno:%i",
			db->tree.commited_last_record_logno,
			db->loglist.write_logno);
		tree_save_index(&db->tree);
		db_unlink_old_logs(db);
	}

	/* overcommit threshold is reached, clear old stuff than gc. */
	if(db->gc_running == 0 && 
	   (DOUBLE_RATIO(db, 999.0) > (double)db->overcommit_ratio)) {
		if(DOUBLE_RATIO(db, 999.0) > (double)db->overcommit_ratio) {
			struct timespec a;
			clock_gettime(CLOCK_MONOTONIC, &a);
			/* no more often than once 30 seconds */
			if(TIMESPEC_SUBTRACT(a, db->gc_spawned)/1000000 > 30*1000) {
				print_stats(db);
				db->gc_spawned = a;
				db_unlink_old_logs(db);
				log_info("Starting GC %.2f/%i",
						DOUBLE_RATIO(db, 999.0),
						db->overcommit_ratio);
				gc_spawn(db);
			}
		}
	}
	
	if(db->gc_finished) {
		gc_join(db);
		if(db->gc_ok) {
			log_info("Saving index after gc.");
			tree_save_index(&db->tree);
			/* Unlink old logs after gc. */
			db_unlink_old_logs(db);
		}
	}
release:
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
		log_error("ydb_get:buffer too small. is %i, should be at least %i", buf_sz, needed);
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

static int item_cmp(const void *a, const void *b) {
	struct item *ii_a = *((struct item **)a);
	struct item *ii_b = *((struct item **)b);
	if(ii_a->logno == ii_b->logno){
		if(ii_a->value_offset == ii_b->value_offset)
			return(0);
		if(ii_a->value_offset < ii_b->value_offset)
			return(-1);
		return(1);
	}
	if(ii_a->logno < ii_b->logno)
		return(-1);
	return(1);
}


void ydb_prefetch(YDB ydb, char **keys, unsigned short *key_szs, int items_counter) {
	struct db *db = (struct db *) ydb;
	assert(db->magic == YDB_STRUCT_MAGIC);

	struct item **items_start = (struct item **)malloc(sizeof(struct item_index *) * items_counter);
	struct item **items = items_start;
	DB_LOCK(db);
	int i;
	for(i=0; i < items_counter; i++, keys++, key_szs++) {
		char *key = *keys;
		int key_sz = *key_szs;
		struct item *item = tree_get(&db->tree, key, key_sz);
		if(item) {
			*items = item;
			items++;
		}
		
	}
	DB_UNLOCK(db);
	
	int items_sz = items - items_start;
	qsort(items_start, items_sz, sizeof(struct item *), item_cmp);
	
	items = items_start;
	int fd = -1;
	int count = 0;
	int offset = 0;
	for(i=0; i < items_sz; i++, items++) {
		struct item *item = *items;
		
		struct log *log = slot_get(&db->loglist, item->logno);
		
		size_t read_size = ROUND_UP(item->value_sz, PADDING) + sizeof(struct ydb_value_record);
		
		/* merge? */
		if(fd == log->fd  &&  (offset + count + 4096) >= item->value_offset) {
			count = (item->value_offset+read_size) - offset;
		}else{
			if(fd != -1) {
				posix_fadvise(fd, offset, count, POSIX_FADV_WILLNEED);
			}
			if(fd != log->fd) {
				/* clear previous fadvices */
				posix_fadvise(log->fd, 0, 0, POSIX_FADV_NORMAL);
			}
			fd = log->fd;
			count = read_size;
			offset = item->value_offset;
		}
	}
	if(fd != -1) {
		posix_fadvise(fd, offset, count, POSIX_FADV_WILLNEED);
	}
	
	free(items_start);
}

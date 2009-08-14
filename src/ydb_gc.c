#define _GNU_SOURCE // for O_NOATIME
#define _FILE_OFFSET_BITS 64 // we like files > 2GB

#include "ydb_internal.h"

#include <stdlib.h>
#include <string.h>


// open
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
// mmap
#include <sys/mman.h>
// close
#include <unistd.h>


#ifndef O_LARGEFILE
#define O_LARGEFILE 0
#endif
#ifndef O_NOATIME
#define O_NOATIME 0
#endif

//#define NO_THREADS

/*
if(!db->flusher_pid && db->tree.used_bytes * db->overcommit_factor > db->llist.total_bytes)
	flusher_spawn();
*/
void *gc_run_thread(void *vdb);


void gc_spawn(struct db *db) {
	if(db->gc_running)
		return;
	db->gc_running = 1;
	db->gc_finished = 0;

#ifndef NO_THREADS
	if(pthread_create(&db->gc_thread, NULL, gc_run_thread, db) != 0) {
		log_perror("pthread_create()");
		return;
	}
#else
	gc_run_thread(db);
#endif
}

void gc_join(struct db *db) {
	if(!db->gc_running)
		return;
#ifndef NO_THREADS
	if (pthread_join(db->gc_thread, NULL) != 0) {
		log_perror("pthread_join()");
	}
#endif
	db->gc_running = 0;
	db->gc_finished = 0;
}

char value[VALUE_BUFFER_SIZE];

static int gc_item_move(struct db *db, char *key, u16 key_sz, int logno, u64 value_offset) {
#ifndef NO_THREADS
	DB_LOCK(db);
#endif
	
	int ret = 0; /* not added */
	struct item *item = tree_get(&db->tree, key, key_sz);
	if(item == NULL) /* already deleted */
		goto release;
	
	if(item->logno != logno || item->value_offset != value_offset) /* updated */
		goto release;
	/* not detelted, not updated. we need to move it */
	// 1. get the value
	log_info("gc_get");
	int value_sz = loglist_get(&db->loglist, item->logno, item->value_offset, item->value_sz, value, sizeof(value));
	if(value_sz < 0)
		goto release;
	// 2. set the same value
	/* TODO: error handling on write? */
	db_add(db, key, key_sz, value, value_sz);
	ret = 1; /* moved with success*/
	
release:
#ifndef NO_THREADS
	DB_UNLOCK(db);
#endif
	return(ret);
}


/*
As we can't traverse current tree, becouse of the concurrency, we 
load previous index file.
*/
void *gc_run_thread(void *vdb) {
	struct db *db = (struct db *)vdb;
	db_info("Starting GC thread");
	
	int fd = open(db->tree.fname, O_RDONLY|O_LARGEFILE|O_NOATIME);
	if(fd < 0) {
		db_info("GC thread stopped. No index file found %s", db->tree.fname);
		/* Index file can legally not exist, just go on. */
		return((void*)-1);
	}
	u64 file_size;
	if(get_fd_size(fd, &file_size) < 0)
		goto error_close;
	
	char *mmap_ptr = mmap(NULL, file_size, PROT_READ|PROT_WRITE, MAP_PRIVATE, fd, 0);
	if(mmap_ptr == MAP_FAILED) {
		log_perror("mmap()");
		goto error_close;
	}
	
#ifndef NO_THREADS
	DB_LOCK(db);
#endif

	u64 used_bytes = db->tree.key_bytes + db->tree.value_bytes;
	u64 bytes_threshold = (double)used_bytes * ((double)db->overcommit_ratio/2.0);
	int can_have_logs = (bytes_threshold / db->loglist.max_file_size);
	int recent_logno = db->loglist.write_logno - can_have_logs;

	db_info("GC, max_log:%i, recent_log:%i", db->loglist.write_logno, recent_logno);

#ifndef NO_THREADS
	DB_UNLOCK(db);
#endif
	
	int counter = 0;
	int ok_counter = 0;
	
	char *ptr = mmap_ptr + sizeof(struct index_header);
	char *end_ptr = mmap_ptr + file_size;
	while(ptr < end_ptr) {
		struct index_item *ii = (struct index_item *)ptr;
		ptr += sizeof(struct index_item) + ROUND_UP(ii->key_sz, PADDING);
		
		if(ii->magic != INDEX_ITEM_MAGIC)
			goto error_unmap;
		u32 read_checksum = ii->checksum;
		ii->checksum = 0;
		if(read_checksum != adler32(ii, sizeof(struct index_item) + ii->key_sz))
			goto error_unmap;
	
		counter++;
		if(ii->logno >= recent_logno)
			continue;
		ok_counter += gc_item_move(db, ii->key, ii->key_sz, ii->logno, ii->value_offset);
	}

	if(munmap(mmap_ptr, file_size) < 0)
		log_perror("munmap()");
	close(fd);
	db_info("Finished GC thread, success, items/moved %i/%i", counter, ok_counter);
#ifndef NO_THREADS
	DB_LOCK(db);
#endif
	db->gc_finished = 1;
#ifndef NO_THREADS
	DB_UNLOCK(db);
#endif
	return((void*)0);

error_unmap:
	if(munmap(mmap_ptr, file_size) < 0)
		log_perror("munmap()");
error_close:
	close(fd);
	db_info("Finished GC thread, error");
#ifndef NO_THREADS
	DB_LOCK(db);
#endif
	db->gc_finished = 1;
#ifndef NO_THREADS
	DB_UNLOCK(db);
#endif
	return((void*)-1);
}






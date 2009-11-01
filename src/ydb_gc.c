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
	log_info("GC joined.");
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
	int value_sz = loglist_get(&db->loglist, item->logno, item->value_offset, item->value_sz, value, sizeof(value));
	if(value_sz < 0)
		goto release;
	// 2. set the same value
	/* TODO: error handling on write? */
	ret = db_add(db, key, key_sz, value, value_sz);
	
release:
#ifndef NO_THREADS
	DB_UNLOCK(db);
#endif
	return(ret);
}


static int se_cmp(const void *a, const void *b) {
	struct index_item *ii_a = *((struct index_item **)a);
	struct index_item *ii_b = *((struct index_item **)b);
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

/*
As we can't traverse current tree, becouse of the concurrency, we 
load previous index file.
*/
void *gc_run_thread(void *vdb) {
	struct db *db = (struct db *)vdb;
	struct timespec a, b;
	log_info("Starting GC thread");
	
	int fd = open(db->tree.fname, O_RDONLY|O_LARGEFILE|O_NOATIME);
	if(fd < 0) {
		log_info("GC thread stopped. No index file found %s", db->tree.fname);
		/* Index file can legally not exist, just go on. */
		goto error_unlock;
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
	db->gc_ok = 0;

	int ocr = db->overcommit_ratio;
	u64 allowed_bytes = USED_BYTES(db) * ((ocr-1)/2 + 1);
	s64 to_free_bytes = db->loglist.total_bytes - allowed_bytes;
	s64 freed_bytes = 0;
	log_info("GC: allowed_bytes:%lli used_bytes:%lli to_free_bytes:%lli", allowed_bytes, db->loglist.total_bytes, to_free_bytes);


#ifndef NO_THREADS
	DB_UNLOCK(db);
#endif
	
	int counter = 0;
	int ok_counter = 0;
	
	char *ptr = mmap_ptr;
	struct index_header *ih = (struct index_header *)ptr;
	if(ih->magic != INDEX_HEADER_MAGIC)
		goto error_unmap;
	u32 read_checksum = ih->checksum;
	ih->checksum = 0;
	if(read_checksum != adler32(ih, sizeof(struct index_header)))
		goto error_unmap;
	u64 items_counter = ih->key_counter;
	
	struct index_item **se = (struct index_item **)malloc(sizeof(struct item_index *) * items_counter);

	clock_gettime(CLOCK_MONOTONIC, &a);
	ptr += sizeof(struct index_header);
	char *end_ptr = mmap_ptr + file_size;
	int item_no = 0;
	while(ptr < end_ptr) {
		struct index_item *ii = (struct index_item *)ptr;
		ptr += sizeof(struct index_item) + ROUND_UP(ii->key_sz, PADDING);
		
		if(ii->magic != INDEX_ITEM_MAGIC)
			goto error_freese;
		u32 read_checksum = ii->checksum;
		ii->checksum = 0;
		if(read_checksum != adler32(ii, sizeof(struct index_item) + ii->key_sz))
			goto error_freese;
	
		counter++;
		
		se[item_no] = ii;
		item_no++;
		if(item_no >= items_counter)
			break;
	}
	clock_gettime(CLOCK_MONOTONIC, &b);
	log_info("Reading index took %3llims, %i keys loaded.", TIMESPEC_SUBTRACT(b, a)/1000000, item_no);
	
	clock_gettime(CLOCK_MONOTONIC, &a);
	qsort(se, item_no, sizeof(struct index_item *), se_cmp);
	clock_gettime(CLOCK_MONOTONIC, &b);
	log_info("Sorting took %3llims", TIMESPEC_SUBTRACT(b, a)/1000000);
	
	int i;
	for(i = 0; i < item_no; i++) {
		struct index_item *ii = se[i];
		int ok = gc_item_move(db, ii->key, ii->key_sz, ii->logno, ii->value_offset);
		if(ok) {
			//log_info("moved logno:%i key:%.*s", ii->logno, ii->key_sz, ii->key);
			ok_counter += ok;
			freed_bytes +=  KEY_RECORD_SIZE(ii->key_sz) \
				      + VALUE_RECORD_SIZE(ii->value_sz);
			if(freed_bytes >= to_free_bytes)
				break;
		}
	}
	log_info("GC: allowed_bytes:%lli used_bytes:%lli to_free_bytes:%lli", allowed_bytes, db->loglist.total_bytes, to_free_bytes);


	if(munmap(mmap_ptr, file_size) < 0)
		log_perror("munmap()");
	free(se);
	close(fd);
	log_info("Finished GC thread, success, items/moved %i/%i", counter, ok_counter);
#ifndef NO_THREADS
	DB_LOCK(db);
#endif
	db->gc_finished = 1;
	db->gc_ok = 1;
#ifndef NO_THREADS
	DB_UNLOCK(db);
#endif
	return((void*)0);

error_freese:
	free(se);
error_unmap:
	if(munmap(mmap_ptr, file_size) < 0)
		log_perror("munmap()");
error_close:
	close(fd);
	log_info("Finished GC thread, error");
error_unlock:
#ifndef NO_THREADS
	DB_LOCK(db);
#endif
	db->gc_finished = 1;
#ifndef NO_THREADS
	DB_UNLOCK(db);
#endif
	return((void*)-1);
}






#define _GNU_SOURCE // for O_NOATIME
#define _FILE_OFFSET_BITS 64 // we like files > 2GB


#include "ydb_internal.h"

#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <glob.h>

#include <assert.h>

// open
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
// flock
#include <sys/file.h>
// close
#include <unistd.h>


#define DATA_FNAME "data"
#define DATA_EXT ".ydb"




#ifndef O_LARGEFILE
#define O_LARGEFILE 0
#endif
#ifndef O_NOATIME
#define O_NOATIME 0
#endif

static struct log* log_new(char *fname);


static inline void slot_set(struct loglist *llist, int logno, struct log *log) {
	rarr_set(llist->logs, logno, log);
}

inline struct log *slot_get(struct loglist *llist, int logno) {
	return (struct log *)rarr_get(llist->logs, logno);
}

int logno_from_fname(char *fname, int prefix_len, int suffix_len){
	char buf[256];
	memset(buf, 0, sizeof(buf));
	memcpy(buf, fname+prefix_len, strlen(fname)-(prefix_len+suffix_len) );
	return strtol(buf, NULL, 16);
}

char *log_filename(char *top_dir, int logno) {
	static char buf[256];
	snprintf(buf, sizeof(buf), "%s%s%s%04X%s",
			top_dir, PATH_DELIMITER, DATA_FNAME, logno, DATA_EXT);
	return buf;
}

static void touch_file(char *fname) {
	int fd = open(fname,  O_WRONLY|O_APPEND|O_CREAT, S_IRUSR|S_IWUSR);
	if(fd < 0) {
		log_perror("open()");
		log_error("Can't create file %s!", fname);
		return;
	}
	int r = fsync(fd);
	if(r < 0)
		log_perror("fsync()");
	close(fd);
}

int log_open(struct loglist *llist, int logno) {
	struct log *log = log_new(log_filename(llist->top_dir, logno));
	if(!log)
		return(-1);
	slot_set(llist, logno, log);
	llist->total_bytes += log->file_size;
	return(0);
}

static int log_create(struct loglist *llist, int logno) {
	touch_file(log_filename(llist->top_dir, logno));
	return log_open(llist, logno);
}

static int log_open_writer(struct loglist *llist, int write_logno) {
	struct log *log = slot_get(llist, write_logno);
	assert(log);
	int fd = open(log->fname, O_WRONLY|O_APPEND|O_LARGEFILE);
	if(fd < 0) {
		log_perror("open()");
		return(-1);
	}
	llist->write_fd = fd;
	llist->write_logno = write_logno;
	return fd;
}

static struct log* log_new(char *fname) {
	int fd = open(fname, O_RDONLY|O_LARGEFILE|O_NOATIME);
	if(fd < 0) {
		log_perror("open()");
		log_error("Unable to open file %s", fname);
		return(NULL);
	}
	if(flock(fd, LOCK_EX|LOCK_NB) < 0){
		log_perror("flock()");
		log_error("Can't get lock on %s - Other instance running?", fname);
		close(fd);
		return(NULL);
	}
	
	u64 size;
	if(get_fd_size(fd, &size) < 0) {
		log_error("File %s ignored - fstat", fname);
		close(fd);
		return(NULL);
	}
	
	if(0 != posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM)) {
		log_perror("posix_fadvise()");
	}

	struct log *log = (struct log *)zmalloc(sizeof(struct log));
	log->fname = strdup(fname);
	log->fd = fd;
	log->file_size = size;
	return log;
}

void log_del(struct log* log) {
	free(log->fname);
	close(log->fd);
	free(log);
}


int loglist_open(struct loglist *llist, struct db *db, char *top_dir, u64 min_log_size, int max_descriptors) {
	llist->db = db;
	llist->min_log_size = min_log_size;
	llist->logs = rarr_new();
	llist->top_dir = strdup(top_dir);
	char unlink_base[256];
	snprintf(unlink_base, sizeof(unlink_base), "%s%s%s%s.old",
				top_dir, PATH_DELIMITER, DATA_FNAME, DATA_EXT);
	llist->unlink_base = strdup(unlink_base);
	
	glob_t globbuf;
	globbuf.gl_offs = 1;
	char glob_str[256];
	char **off;
	int max_logno = -1;
	
	snprintf(glob_str, sizeof(glob_str), "%s%s%s*%s",
				top_dir, PATH_DELIMITER, DATA_FNAME, DATA_EXT);
	int prefix_len = strchr(glob_str, '*') - glob_str;
	int suffix_len = strlen(DATA_EXT);
	
	/* Load data files */
	glob(glob_str, 0, NULL, &globbuf);
	for(off=globbuf.gl_pathv; off && *off; off++) {
		int logno = logno_from_fname(*off, prefix_len, suffix_len);
		log_info("Opening log: %s (%04i)", *off, logno);
		if(log_open(llist, logno) < 0) {
			log_error("Unable to open log %5i/0x%04x", logno, logno);
			continue;
		}
		max_logno = MAX(max_logno, logno);
	}
	globfree(&globbuf);
	
	if(max_logno < 0) { /* empty directory yet*/
		if(log_create(llist, 0) < 0)
			return(-1);
		max_logno = 0;
	}
		
	if(log_open_writer(llist, max_logno) < 0)
		return(-1);
	return(0);
}

/* hey, this log should be unused... */
static void loglist_free_log(struct loglist *llist, int logno) {
	struct log *log = slot_get(llist, logno);
	assert(log);
	slot_set(llist, logno, NULL);
	llist->total_bytes -= log->file_size;
	log_del(log);
}

void loglist_close(struct loglist *llist) {
	/* TODO */
	int logno;
	int min = rarr_min(llist->logs);
	int max = rarr_max(llist->logs);
	for(logno=min; logno < max; logno++) {
		struct log *log = slot_get(llist, logno);
		if(log)
			loglist_free_log(llist, logno);
	}
	close(llist->write_fd);
	rarr_free(llist->logs);
	free(llist->top_dir);
	free(llist->unlink_base);
}




static int is_key_record_valid(struct ydb_key_record *yr) {
	if(yr->magic != YDB_KEY_MAGIC)
		return(0);
	u32 checksum_read = yr->checksum;
	yr->checksum = 0;
	u32 checksum_counted = adler32(yr, sizeof(struct ydb_key_record) + yr->key_sz);
	if(checksum_read != checksum_counted)
		return(0);
	return(1);
}

static int is_value_record_valid(char *value_buf, u32 value_sz) {
	int record_offset = ROUND_UP(value_sz, PADDING);
	struct ydb_value_record *yv = (struct ydb_value_record *) (value_buf + record_offset);
	
	u32 checksum_read = yv->checksum;
	yv->checksum = 0;
	u32 checksum_counted = adler32(value_buf, record_offset + sizeof(struct ydb_value_record));
	if(checksum_read != checksum_counted) {
		log_error("%i checksum 0x%x != 0x%x v=%c",record_offset,checksum_read, checksum_counted, *value_buf);
		return(0);
	}
	return(1);
}



/* Reads record metadata from log. Actually it's a poor mans iterator.
	returns:
		NO_MORE_DATA: stop
		END_OF_FILE: switch to next logno
		other: next_record_offset
	sets:
		key,
		key_sz,
		value_offet,
		value_sz,
		flags
  This design has it's flaws. We need to use seek for every record, which
  would not be a case if we'd use mmap(). But it's pretty simple that way.
*/
int loglist_get_record(struct loglist *llist, int logno, u64 record_offset,
			char *key, u16 *key_sz,
			u64 *value_offset, u32 *value_sz, int *flags) {
	char *reason = "unknown";
	if(logno > llist->write_logno)
		return(NO_MORE_DATA);
	
	struct log *log = slot_get(llist, logno);
	if(log == NULL) {
		if(record_offset != 0)
			log_error("Trying to read log that does not exist %i/0x%x!", logno, logno);
		return(END_OF_FILE);
	}
	if(log->file_size == record_offset)
		return(END_OF_FILE);
	
	assert(log->fd >= 0);
	
	int r;
	char buf[KEY_BUFFER_SIZE];
	struct ydb_key_record *yr = (struct ydb_key_record *)buf;
	r = preadall(log->fd, buf, sizeof(struct ydb_key_record), record_offset);
	if(r < 0) {
		reason = "preadall() error 1";
		goto record_error;
	}
	if(yr->magic != YDB_KEY_MAGIC){
		reason = "bad magic";
		goto record_error;
	}
	int key_record_sz = sizeof(struct ydb_key_record) \
		+ ROUND_UP(yr->key_sz, PADDING);
	int full_record_sz = key_record_sz;
	if(!(yr->flags & FLAG_NOVALUE))
	    full_record_sz += \
		+ ROUND_UP(yr->value_sz, PADDING) \
		+ sizeof(struct ydb_value_record);

	if(key_record_sz > sizeof(buf)) {
		reason = "key record too big";
		goto record_error;
	}
	/* we don't need to actually load value, just read the key */
	r = preadall(log->fd, &buf[sizeof(struct ydb_key_record)], yr->key_sz, record_offset + sizeof(struct ydb_key_record));
	if(r < 0) {
		reason = "readall() error 2";
		goto record_error;
	}
	if(!is_key_record_valid(yr)) {
		reason = "key checksum invalid";
		goto record_error;
	}
	
	int offset = sizeof(struct ydb_key_record) \
		    + yr->key_sz \
		    + ROUND_REMAINDER(yr->key_sz, PADDING);

	if(*key_sz < yr->key_sz){
		reason = "key value too long";
		goto record_error;
	}

	memcpy(key, yr->data, yr->key_sz);
	*key_sz = yr->key_sz;
	*value_offset = record_offset + offset;
	*value_sz = yr->value_sz;
	*flags = yr->flags;
	return(ROUND_UP(full_record_sz, PADDING));

record_error:
	log_error("Log %i/0x%x is invalid at offset %lli, ignoring it! error:%s", logno, logno, record_offset, reason);
	log_error("You have lost %lli bytes of data.",log->file_size - record_offset);
	return(END_OF_FILE);
}

void loglist_fsync(struct loglist *llist) {
	if(fsync(llist->write_fd))
		log_perror("fsync()");
}

static int llist_write_log_rotate(struct loglist *llist) {
	loglist_fsync(llist);

	log_info("New log created: %i", llist->write_logno+1);
	if(log_create(llist, llist->write_logno+1) < 0)
		return(-1);
	
	int previous_fd = llist->write_fd;
	if(log_open_writer(llist, llist->write_logno+1) < 0)
		return(-1);
	
	// Close only after new was opened with success
	if(close(previous_fd))
		log_perror("close()");
	return(0);
}

static s64 loglist_do_write(struct loglist *llist, char *buf, int buf_sz, u64 *record_offset_ptr) {
	struct log *log = slot_get(llist, llist->write_logno);
	assert(llist->write_fd >= 0);

	int r = writeall(llist->write_fd, buf, buf_sz);
	if(r < 1) {
		log_error("Unable to write to log %i, opening new log, just in case", llist->write_logno);
		/* never hurts to reply */
		if(llist_write_log_rotate(llist) >= 0) {
			r = writeall(llist->write_fd, buf, buf_sz);
			if(r < 1)
				return(-1);
		}
	}
	
	u64 offset = log->file_size;
	log->file_size += buf_sz;
	llist->total_bytes += buf_sz;
	llist->appended_bytes += buf_sz;
	
	if(log->file_size > llist->min_log_size) {
		if(llist_write_log_rotate(llist) < 0)
			log_error("Unable to write to new log!");
		/* hop context and save index */
		db_save_index(llist->db);
	}
	if(record_offset_ptr)
		*record_offset_ptr = offset;
	return(offset);
}

char buf[MAX_RECORD_SIZE];

struct append_info loglist_append(struct loglist *llist, char *key, u16 key_sz, char *value, u32 value_sz, int flags) {

	int sz, i;
	char *buf_ptr = buf;
	struct ydb_key_record *ar = (struct ydb_key_record *)buf_ptr;
	*ar = (struct ydb_key_record) {
		.magic = YDB_KEY_MAGIC,
		.checksum = 0,
		.flags = flags,
		.key_sz = key_sz,
		.value_sz = value_sz,
	};
	memcpy(ar->data, key, key_sz);
	sz = sizeof(struct ydb_key_record) + key_sz;
	ar->checksum = adler32(ar, sz);
	buf_ptr += sz;
	for(i=0; i < ROUND_REMAINDER(key_sz, PADDING); i++)
		*buf_ptr++ = 0;

	if(!(flags & FLAG_NOVALUE) ) {
		char *value_start = buf_ptr;
		memcpy(buf_ptr, value, value_sz);
		buf_ptr += value_sz;
		for(i=0; i < ROUND_REMAINDER(value_sz, PADDING); i++)
			*buf_ptr++ = 0;
		struct ydb_value_record *vr = (struct ydb_value_record *)buf_ptr;
		*vr = (struct ydb_value_record) {
			.checksum = 0
		};
		buf_ptr += sizeof(struct ydb_value_record);
		vr->checksum = adler32(value_start, buf_ptr-value_start);
	}
	sz = buf_ptr - buf;
	assert(sz <= MAX_RECORD_SIZE);
	
	int write_logno = llist->write_logno; // write log can be changed here
	u64 record_offset;
	int ret = loglist_do_write(llist, buf, sz, &record_offset);
	if(ret < 0)
		abort();

	struct append_info af;
	af.logno = write_logno;
	af.record_offset = record_offset;
	af.value_offset = record_offset + sizeof(struct ydb_key_record) + ROUND_UP(key_sz, PADDING);
	return af;
}

int loglist_is_writer(struct loglist *llist, int logno) {
	return(llist->write_logno == logno);
}

int loglist_unlink(struct loglist *llist, int logno) {
	char path[256];
	assert(!loglist_is_writer(llist, logno));
	
	struct log *log = slot_get(llist, logno);
	safe_strncpy(path, log->fname, sizeof(path));
	
	loglist_free_log(llist, logno);

	return unlink_with_history(path, llist->unlink_base, 4);
}


int loglist_get(struct loglist *llist, int logno,
		 u64 value_offset, u64 value_sz,
		 char *dst, u32 dst_sz) {
	if(dst_sz < value_sz+MAX_PADDING+sizeof(struct ydb_value_record)) {
		return(-1);
	}
	
	struct log *log = slot_get(llist, logno);
	if(log == NULL) /* not existing file, already informed user */
		return(-1);

	size_t count = ROUND_UP(value_sz, PADDING) + sizeof(struct ydb_value_record);
	int r = preadall(log->fd, dst, count, value_offset);
	if(r < 0){
		log_error("preadall() fd:%i  file:%s  offset:%llu  length:%i", log->fd, log->fname, value_offset, count);
		return(-1);
	}
	if(!is_value_record_valid(dst, value_sz)){
		log_error("Bad magic on item!");
		return(-1);
	}
	return(value_sz);
}


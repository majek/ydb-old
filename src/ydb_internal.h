#include "ydb.h" // the public interface

// for NULL
#include <stdio.h>
// for malloc
#include <stdlib.h>
// for memset
#include <string.h>
#include <stdint.h>
#include <assert.h>

/* mutex support */
#include <pthread.h>

#include "rbtree.h"

typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;
typedef int64_t s64;


#include "utils.h"

/* Linux? */
#define PATH_DELIMITER "/"
#define MAX_KEY_SIZE (64<<10) // 64KiB
#define MAX_VALUE_SIZE (4<<20) // 4 megabytes

#define MAX_PADDING 4
#define PADDING 4

#define KEY_BUFFER_SIZE (sizeof(struct ydb_key_record) + MAX_KEY_SIZE + MAX_PADDING)
#define VALUE_BUFFER_SIZE (MAX_VALUE_SIZE + MAX_PADDING + sizeof(struct ydb_value_record))
#define MAX_RECORD_SIZE (KEY_BUFFER_SIZE + VALUE_BUFFER_SIZE)

#define KEY_RECORD_SIZE(key_sz)	\
	(ROUND_UP(key_sz, PADDING) + sizeof(struct ydb_key_record))
#define VALUE_RECORD_SIZE(value_sz)	\
	(ROUND_UP(value_sz, PADDING) + sizeof(struct ydb_value_record))


typedef void* STDDEV;
typedef void* r_arr;


/* **** **** */
r_arr rarr_new();
void rarr_free(r_arr v);

void *rarr_get(r_arr v, int idx);
void rarr_set(r_arr v, int idx, void *value);
void rarr_clear(r_arr v, int idx);

int rarr_min(r_arr v);
int rarr_max(r_arr v);

int rarr_get_filled_items(r_arr v);


/* **** **** */
#define DB_LOCK(db)		\
	pthread_mutex_lock( &(db)->lock )

#define DB_UNLOCK(db)	\
	pthread_mutex_unlock( &(db)->lock )


#define FLAG_SET 0x00
#define FLAG_DELETE 0x01
#define FLAG_NOVALUE (FLAG_DELETE)

struct db;

/* **** **** */
struct tree {
	struct db *db;
	
	char *top_dir;		/* Base name of index file. */
	struct rb_root root;
	
	int commited_last_record_logno;
	u64 commited_last_record_offset;
	
	int last_record_logno;	
	u64 last_record_offset;
	
	u64 key_counter;
	u64 key_bytes;		/* used to store keys (incl header and padding) */
	u64 value_bytes;	/* used to store values (incl header and padding) */
	
	STDDEV keys_stddev;
	STDDEV values_stddev;
	
	r_arr refcnt;
};

#define INDEX_HEADER_MAGIC 0x43211234
struct index_header{
	u32	magic;
	int	last_record_logno;
	u64	last_record_offset;
	u64	key_counter; /* not required for parsing, but usefull for memory pre-allocation */
	u32	checksum;
};

#define INDEX_ITEM_MAGIC 0x12344321
struct index_item{
	u32	magic;
	u32	checksum;

	int	logno;
	u64	value_offset;

	u16	key_sz;
	u32	value_sz;

	char	key[];
};

int tree_load_index(struct tree *tree, int logno, int *last_record_logno, u64 *last_record_offset, int flags);
int tree_save_index(struct tree *tree, int logno);
char *tree_filename(char *buf, int buf_sz, char *top_dir, int logno);
int tree_get_keys(struct tree *tree, char *key, u16 key_sz, char *start_buf, int buf_sz);


#ifndef PACKED
#define PACKED __attribute__ ((packed))
#endif

struct PACKED item {
	u64 value_offset;
	u32 logno;
	u32 value_sz;
	struct rb_node node;

	u16 key_sz;
	char key[];
};

int tree_add(struct tree *tree, char *key, u16 key_sz, int logno,
			u64 value_offset, u32 value_size, u64 record_offset);
int tree_del(struct tree *tree, char *key, u16 key_sz, int logno, u64 record_offset);
struct item *tree_get(struct tree *tree, char *key, u16 key_sz);

struct tree_sig{
	int logno;
	u64 record_offset;
};
int tree_open(struct tree *tree, struct db *db, int logno, char *top_dir, int *last_record_logno, u64 *last_record_offset, int flags);
void tree_close(struct tree *tree);
int tree_get_max_fileno(char *top_dir);

unsigned long refcnt_get(struct tree *tree, int logno);

#define WRITE_FD_BUFFER_SZ 128

/* **** **** */
struct loglist {
	struct db *db;
	
	r_arr logs;
	char *top_dir;
	char *unlink_base;

	int write_logno;
	int write_fd;

	u64 min_log_size;
	u64 total_bytes; /* total size of all logs */
	
	u64 appended_bytes;
};

int loglist_open(struct loglist *llist, struct db *db, char *top_dir, u64 min_log_size, int max_descriptors);
void loglist_close(struct loglist *llist);
int loglist_get(struct loglist *llist, int logno, u64 value_offset, u64 value_size, char *dst, u32 dst_sz);
void loglist_sync(struct loglist *llist);
struct log *slot_get(struct loglist *llist, int logno);
int loglist_is_writer(struct loglist *llist, int logno);
int loglist_unlink(struct loglist *llist, int logno);

void loglist_fsync(struct loglist *llist);

/* returns */
struct append_info{
	int logno;
	u64 record_offset;
	u64 value_offset;
};
struct append_info loglist_append(struct loglist *llist, char *key, u16 key_sz, char *value, u32 value_sz, int flags);


#define END_OF_FILE (-1)
#define NO_MORE_DATA (-2)
/*
returns: record_sz or NO_MORE_DATA or END_OF_FILE
*/
int loglist_get_record(struct loglist *llist, int logno, u64 record_offset,
			char *key, u16 *key_sz,
			u64 *value_offset, u32 *value_sz, int *flags);



/* **** **** */
struct log {
	int fd;
	char *fname;
	u64 file_size;
};
int log_open(struct loglist *llist, int logno);
void log_close(struct log *log);
void log_get_value(struct log *log, u64 value_offset, u32 value_size, char *dst);



/* **** **** */
#define YDB_STRUCT_MAGIC (0x7DB8A61C)
struct db {
	u32	magic;
	char	*top_dir;

	struct tree	*tree;
	struct loglist	*loglist;

	int overcommit_ratio;
	int flags;
	
	int gc_enabled;
	int gc_running;
	int gc_finished;
	int gc_ok;
	struct timespec gc_spawned;
	pthread_t gc_thread;
	
	pthread_mutex_t lock;	/**/
};


int db_add(struct db *db, char *key, u16 key_sz, char *value, u32 value_sz);
void print_stats(struct db *db);


#define USED_BYTES(db)	\
	(db->tree->key_bytes + db->tree->value_bytes)

/* TODO: two adds instead of one */
#define DOUBLE_RATIO(db, default)	\
	(USED_BYTES(db) ? ((double)db->loglist->total_bytes / (double)USED_BYTES(db)) : default)
#define INT_RATIO(db, default)	\
	(USED_BYTES(db) ? (db->loglist->total_bytes / USED_BYTES(db)) : default)



/* **** **** */
/* key_record ... key ... padding ... value ... value_record ... padding */
#define YDB_KEY_MAGIC (0x7DB5EC5D)
struct ydb_key_record{
	u32	magic;
	u32	checksum;
	u16	flags;		// deleted?
	u16	key_sz;
	u32	value_sz;	/* length of value, without the header */
	char	data[];
};

struct ydb_value_record{
	u32	checksum;
};


/* **** **** */
void gc_spawn(struct db *db);
void gc_join(struct db *db);


/* **** sys_utils.c **** */
int safe_unlink(const char *old_path);
int unlink_with_history(const char *old_path, const char *new_base, int versions);
int max_descriptors();
int writeall(int fd, void *sbuf, size_t count);
int preadall(int fd, void *sbuf, size_t count, size_t offset);

int get_fd_size(int fd, u64 *size);


/* **** stats.c **** */
STDDEV stddev_new();
void stddev_free(STDDEV stddev);

void stddev_add(STDDEV stddev, int value);
void stddev_remove(STDDEV stddev, int old_value);
void stddev_modify(STDDEV stddev, int old_value, int new_value);
void stddev_get(STDDEV stddev, int *counter_ptr,
		double *avg_ptr, double *stddev_ptr);


/* **** buffer.c **** */
struct buffer{
	int fd;
	u_int64_t count;
	int start_pos;
	int end_pos;
	int buf_sz;
	char buf[];
};
typedef struct buffer *BUFFER;

BUFFER buf_writer_from_fd(int fd);
char *buf_writer(BUFFER b, int sz);
u_int64_t buf_writer_free(BUFFER b);

BUFFER buf_reader_from_fd(int fd);
char *buf_read(BUFFER b, int sz);
u_int64_t buf_reader_free(BUFFER b);


/* **** ydb_db.c **** */
int db_save_index(struct db *db);


/* **** Premature optimization is the root of all evil... **** */
#include "ydb_inline.h"


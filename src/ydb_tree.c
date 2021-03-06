#define _GNU_SOURCE // for O_NOATIME
#define _FILE_OFFSET_BITS 64 // we like files > 2GB


#include "ydb_internal.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <glob.h>

// open
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
// fsync
#include <unistd.h>
// mmap
#include <sys/mman.h>


#define DATA_FNAME "index"
#define DATA_EXT ".idx"


#ifndef O_LARGEFILE
#define O_LARGEFILE 0
#endif
#ifndef O_NOATIME
#define O_NOATIME 0
#endif


static struct item *item_new(struct tree *tree, char *key, u16 key_sz, \
				int logno, u64 value_offset, u32 value_sz);
static void item_del(struct tree *tree, struct item *item);


static inline void refcnt_incr(struct tree *tree, int logno) {
	ulong refcnt = (ulong)rarr_get(tree->refcnt, logno);
	refcnt += 1;
	rarr_set(tree->refcnt, logno, (void*)refcnt);
}

static inline void refcnt_decr(struct tree *tree, int logno) {
	ulong refcnt = (ulong)rarr_get(tree->refcnt, logno);
	refcnt -= 1;
	assert(refcnt >= 0);
	rarr_set(tree->refcnt, logno, (void*)refcnt);
}

static int key_cmp(char *a, int a_sz, char *b, int b_sz) {
	int r = memcmp(a, b, MIN(a_sz, b_sz));
	if(r == 0) {
		if(a_sz == b_sz)
			return(0);
		if(a_sz < b_sz)
			return(-1);
		return(1);
	}
	return(r);
}

ulong refcnt_get(struct tree *tree, int logno) {
	return (ulong)rarr_get(tree->refcnt, logno);
}

int tree_open(struct tree *tree, struct db *db, int logno, char *top_dir, int *last_record_logno, u64 *last_record_offset, int flags) {
	tree->db = db;
	tree->top_dir = strdup(top_dir);
	tree->refcnt = rarr_new();

	tree->root = RB_ROOT;
	tree->keys_stddev = stddev_new();
	tree->values_stddev = stddev_new();
	
	if(logno >= 0)
		return tree_load_index(tree, logno, last_record_logno, last_record_offset, flags);
	return(0);
}

void tree_close(struct tree *tree) {
	struct rb_node *node = rb_first(&tree->root);
	
	while (node) {
		struct item *item = container_of(node, struct item, node);
		node = rb_next(node);
		rb_erase(&item->node, &tree->root);
		item_del(tree, item);
	}
	
	assert(tree->key_counter == 0);
	assert(tree->key_bytes == 0);
	assert(tree->value_bytes == 0);
	
	rarr_free(tree->refcnt);
	free(tree->top_dir);
	stddev_free(tree->keys_stddev);
	stddev_free(tree->values_stddev);
}



struct item *tree_get(struct tree *tree, char *key, u16 key_sz) {
	int result = 0;
	struct rb_node *node = tree->root.rb_node;
	
	while (node) {
		struct item *item = container_of(node, struct item, node);
		result = key_cmp(key, key_sz, item->key, item->key_sz);
	
		if (result < 0)
			node = node->rb_left;
		else if (result > 0)
			node = node->rb_right;
		else
			return item;
	}
	return NULL;
}

static struct item *tree_get_next(struct tree *tree, char *key, u16 key_sz) {
	// TODO: implement properly!!! this is a dirty hack
	struct item *item = tree_get(tree, key, key_sz);
	if(!item)
		return NULL;
	struct rb_node *right = rb_next(&item->node);
	
	return container_of(right, struct item, node);
}

static int item_insert(struct tree *tree, struct item *data) {
	struct rb_node **new = &(tree->root.rb_node), *parent = NULL;

	/* Figure out where to put new node */
	while (*new) {
		struct item *item = container_of(*new, struct item, node);
		int result = key_cmp(data->key, data->key_sz, item->key, item->key_sz);

		parent = *new;
		if (result < 0)
			new = &((*new)->rb_left);
		else if (result > 0)
			new = &((*new)->rb_right);
		else
			return 0;
	}

	/* Add new node and rebalance tree. */
	rb_link_node(&data->node, parent, new);
	rb_insert_color(&data->node, &tree->root);

	return 1;
}

int tree_add(struct tree *tree, char *key, u16 key_sz, int logno,
			u64 value_offset, u32 value_sz, u64 record_offset) {
	struct item *old_item = tree_get(tree, key, key_sz);
	
	tree->last_record_logno = logno;
	tree->last_record_offset = record_offset;
	
	if(!old_item){ /* TODO: can be twice as fast here! */
		struct item *item = item_new(tree, key, key_sz, logno, value_offset, value_sz);
		item_insert(tree, item);
		return(-1);
	}else{ // keys match
		tree->value_bytes += VALUE_RECORD_SIZE(value_sz);
		tree->value_bytes -= VALUE_RECORD_SIZE(old_item->value_sz);
		stddev_modify(tree->values_stddev, old_item->value_sz, value_sz);
		
		refcnt_decr(tree, old_item->logno);
		refcnt_incr(tree, logno);
		
		old_item->logno = logno;
		old_item->value_offset = value_offset;
		old_item->value_sz = value_sz;
		return(0);
	}
}

static struct item *item_new(struct tree *tree, char *key, u16 key_sz, \
				int logno, u64 value_offset, u32 value_sz) {
	struct item *item = (struct item *)malloc(sizeof(struct item)+key_sz); // no need to zero
	item->logno = logno;
	item->value_offset = value_offset;
	item->value_sz = value_sz;
	item->key_sz = key_sz;
	memcpy(item->key, key, key_sz);

	tree->key_counter++;
	tree->value_bytes += VALUE_RECORD_SIZE(value_sz);
	tree->key_bytes += KEY_RECORD_SIZE(key_sz);
	
	stddev_add(tree->keys_stddev, key_sz);
	stddev_add(tree->values_stddev, value_sz);
	
	refcnt_incr(tree, logno);
	return(item);
}

static void item_del(struct tree *tree, struct item *item) {
	tree->key_counter--;
	tree->value_bytes -= VALUE_RECORD_SIZE(item->value_sz);
	tree->key_bytes -= KEY_RECORD_SIZE(item->key_sz);
	
	stddev_remove(tree->keys_stddev, item->key_sz);
	stddev_remove(tree->values_stddev, item->value_sz);
	
	refcnt_decr(tree, item->logno);
	free(item);
}


int tree_del(struct tree *tree, char *key, u16 key_sz, int logno, u64 record_offset) {
	tree->last_record_logno = logno;
	tree->last_record_offset = record_offset;

	struct item *item = tree_get(tree, key, key_sz);
	if(!item)
		return(-1);
	rb_erase(&item->node, &tree->root);
	item_del(tree, item);

	return(0);
}

char *tree_filename(char *buf, int buf_sz, char *top_dir, int logno) {
	snprintf(buf, buf_sz, "%s%s%s%04X%s",
			top_dir, PATH_DELIMITER, DATA_FNAME, logno, DATA_EXT);
	return buf;
}


/*
Dump index tree to the disk. This is pretty important so we can afford flush().
*/
int tree_save_index(struct tree *tree, int logno) {
	char fname_tmp[256], fname[256];
	tree_filename(fname, sizeof(fname), tree->top_dir, logno);
	snprintf(fname_tmp, sizeof(fname_tmp), "%s.new", fname);
	
	log_info("Saving index to %s", fname);
	/* we're overcommiting by a factor of sizeof(struct ydb_key_record) and padding*/
	u64 file_size =  sizeof(struct index_header) \
		  + sizeof(struct index_item) * tree->key_counter \
		  + PADDING * tree->key_counter \
		  + tree->key_bytes;
	
	/* we delete the previous contents! */
	int fd = open(fname_tmp,  O_RDWR|O_TRUNC|O_CREAT|O_LARGEFILE, S_IRUSR|S_IWUSR);
	if(fd < 0) {
		log_perror("open()");
		return(-1);
	}
	if(ftruncate(fd, file_size) < 0) {
		log_perror("ftrunctate()");
		goto error_close;
	}
	if(0 != posix_fallocate(fd, 0, file_size)) {
		log_perror("fallocate()");
	}
	
	BUFFER buf = buf_writer_from_fd(fd);
	int sz = sizeof(struct index_header);

	struct index_header *ih = (struct index_header *)buf_writer(buf, sz);

	*ih = (struct index_header) {
		.magic = INDEX_HEADER_MAGIC,
		.last_record_logno = tree->last_record_logno,
		.last_record_offset = tree->last_record_offset,
		.key_counter = tree->key_counter
	};
	ih->checksum = adler32(ih, sizeof(struct index_header));
	
	struct rb_node *node = rb_first(&tree->root);
	while (node) {
		struct item *item = container_of(node, struct item, node);
		node = rb_next(node);
		
		int ii_sz = sizeof(struct index_item) + ROUND_UP(item->key_sz, PADDING);
		struct index_item *ii = (struct index_item *)buf_writer(buf, ii_sz);
		
		*ii = (struct index_item) {
			.magic = INDEX_ITEM_MAGIC,
			.logno = item->logno,
			.value_offset = item->value_offset,
			.value_sz = item->value_sz,
			.key_sz = item->key_sz
		};
		memcpy(ii->key, item->key, item->key_sz);
		ii->checksum = adler32(ii, sizeof(struct index_item) + item->key_sz);
	}

	u64 written_bytes = buf_writer_free(buf);

	if(ftruncate(fd, written_bytes) < 0) {
		log_perror("ftrunctate()");
		goto error_close;
	}
	
	if(fsync(fd) < 0) {
		log_perror("fsync()");
		goto error_close;
	}
	if(close(fd) < 0) {
		log_perror("close()");
		return(-1);
	}
	/* no resources left to be closed */
	
	/* this is pretty important */
	if(rename(fname_tmp, fname) < 0) {
		log_perror("rename()");
		return(-1);
	}

	tree->commited_last_record_logno = tree->last_record_logno;
	tree->commited_last_record_offset = tree->last_record_offset;
	return(0);

error_close:
	close(fd);
	return(-1);
}

struct PACKED key_item {
	u_int64_t i_cas;
	u_int8_t key_sz;
	char key[];
};

int tree_get_keys(struct tree *tree, char *key, u16 key_sz, char *start_buf, int buf_sz) {
	char *buf = start_buf;
	char *buf_end = start_buf + buf_sz;
	
	struct rb_node *node;
	if(0 == key_sz) {
		node = rb_first(&tree->root);
	} else {
		struct item *item = tree_get_next(tree, key, key_sz);
		if(NULL == item) {
			return -1;
		}
		log_error("key not found");
		node = &item->node;
	}
	while (node) {
		struct item *item = container_of(node, struct item, node);
		node = rb_next(node);
		
		int sz = sizeof(struct key_item) + item->key_sz;
		if(buf_end - buf < sz) {
			break;
		}
		
		struct key_item *ki = (struct key_item *)buf;
		*ki = (struct key_item) {
			.i_cas = item->value_offset | (uint64_t)item->logno << 32,
			.key_sz = item->key_sz
			};
		memcpy(ki->key, item->key, item->key_sz);
		buf += sz;
	}
	return buf - start_buf;
}



int tree_load_index(struct tree *tree, int logno, int *last_record_logno, u64 *last_record_offset, int flags) {
	char fname[256];
	tree_filename(fname, sizeof(fname), tree->top_dir, logno);
	
	char *reason = "unknown";
	log_info("Loading index %s", fname);

	int fd = open(fname, O_RDONLY|O_LARGEFILE|O_NOATIME);
	if(fd < 0) {
		/* don't shout */
		if(flags & YDB_CREAT) return(-1);
		log_perror("open()");
		return(-1);
	}
	u64 file_size;
	if(get_fd_size(fd, &file_size) < 0)
		goto error_close;
	
	/* we're setting checksum to zero, unfortunatelly, so need of PROT_WRITE */
	char *mmap_ptr = mmap(NULL, file_size, PROT_READ|PROT_WRITE, MAP_PRIVATE, fd, 0);
	if(mmap_ptr == MAP_FAILED) {
		log_perror("mmap()");
		reason = "mmap failed";
		goto error_close;
	}

	char *buf = mmap_ptr;
	char *end_buf = mmap_ptr + file_size;
		
	struct index_header *ih = (struct index_header *)buf;
	buf += sizeof(struct index_header);
	
	if(ih->magic != INDEX_HEADER_MAGIC) {
		reason = "magic in the header broken";
		goto error_unmap;
	}
	u32 read_checksum = ih->checksum;
	ih->checksum = 0;
	if(read_checksum != adler32(ih, sizeof(struct index_header))) {
		reason = "checksum in the header broken";
		goto error_unmap;
	}
	*last_record_logno = ih->last_record_logno;
	*last_record_offset = ih->last_record_offset;
	tree->commited_last_record_logno = ih->last_record_logno;
	tree->commited_last_record_offset = ih->last_record_offset;

	while(buf < end_buf) {
		struct index_item *ii = (struct index_item *)buf;
		buf += sizeof(struct index_item) + ROUND_UP(ii->key_sz, PADDING);
		
		if(ii->magic != INDEX_ITEM_MAGIC) {
			reason = "magic in the record broken";
			goto error_unmap;
		}
		read_checksum = ii->checksum;
		ii->checksum = 0;
		if(read_checksum != adler32(ii, sizeof(struct index_item) + ii->key_sz)) {
			reason = "checksum in the record broken";
			goto error_unmap;
		}

		struct item *item = item_new(tree, ii->key, ii->key_sz, \
				ii->logno, ii->value_offset, ii->value_sz);
		item_insert(tree, item);
	}

	if(munmap(mmap_ptr, file_size) < 0)
		log_perror("munmap()");
	return(1);

error_unmap:
	if(munmap(mmap_ptr, file_size) < 0)
		log_perror("munmap()");
error_close:
	close(fd);
	log_error("Error loading index %s: %s, offet:%i", fname, reason, buf-mmap_ptr);
	return(-1);
}


int logno_from_fname(char *fname, int prefix_len, int suffix_len);

int tree_get_max_fileno(char *top_dir) {
	glob_t globbuf;
	globbuf.gl_offs = 1;
	char glob_str[256];
	char **off;
	snprintf(glob_str, sizeof(glob_str), "%s%s%s*%s",
				top_dir, PATH_DELIMITER, DATA_FNAME, DATA_EXT);
	int prefix_len = strchr(glob_str, '*') - glob_str;
	int suffix_len = strlen(DATA_EXT);
	
	int max_logno = -1;
	glob(glob_str, 0, NULL, &globbuf);
	for(off=globbuf.gl_pathv; off && *off; off++) {
		int logno = logno_from_fname(*off, prefix_len, suffix_len);
		max_logno = max_logno > logno ? max_logno: logno;
	}
	globfree(&globbuf);
	return(max_logno);
}


/*
int {
	glob_t globbuf;
	globbuf.gl_offs = 1;
	char glob_str[256];
	char **off;
	int max_logno = -1;
	snprintf(glob_str, sizeof(glob_str), "%s%s%s*%s",
				top_dir, PATH_DELIMITER, DATA_FNAME, DATA_EXT);
	int prefix_len = strchr(glob_str, '*') - glob_str;
	int suffix_len = strlen(DATA_EXT);
	
	glob(glob_str, 0, NULL, &globbuf);
	for(off=globbuf.gl_pathv; off && *off; off++) {
		int logno = logno_from_fname(*off, prefix_len, suffix_len);
		max_logno = MAX(max_logno, logno);
	}
	globfree(&globbuf);
	
	return(max_logno);
}
*/
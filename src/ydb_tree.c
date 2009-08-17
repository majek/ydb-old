#define _GNU_SOURCE // for O_NOATIME
#define _FILE_OFFSET_BITS 64 // we like files > 2GB


#include "ydb_internal.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>

// open
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
// fsync
#include <unistd.h>
// mmap
#include <sys/mman.h>


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
	uint refcnt = (uint)rarr_get(tree->refcnt, logno);
	refcnt += 1;
	rarr_set(tree->refcnt, logno, (void*)refcnt);
}

static inline void refcnt_decr(struct tree *tree, int logno) {
	uint refcnt = (uint)rarr_get(tree->refcnt, logno);
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

uint refcnt_get(struct tree *tree, int logno) {
	return (uint)rarr_get(tree->refcnt, logno);
}

int tree_open(struct tree *tree, char *fname, int *last_record_logno, u64 *last_record_offset, int flags) {
	char buf[256];
	
	tree->fname = strdup(fname);
	tree->refcnt = rarr_new();

	snprintf(buf, sizeof(buf), "%s.old", fname);
	tree->fname_old = strdup(buf);
	snprintf(buf, sizeof(buf), "%s.new", fname);
	tree->fname_new = strdup(buf);

	tree->root = RB_ROOT;
	
	return tree_load_index(tree, last_record_logno, last_record_offset, flags);
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
	free(tree->fname);
	free(tree->fname_old);
	free(tree->fname_new);
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
	refcnt_incr(tree, logno);
	return(item);
}

static void item_del(struct tree *tree, struct item *item) {
	tree->key_counter--;
	tree->value_bytes -= VALUE_RECORD_SIZE(item->value_sz);
	tree->key_bytes -= KEY_RECORD_SIZE(item->key_sz);
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


/*
Dump index tree to the disk. This is pretty important so we can afford flush().
*/
int tree_save_index(struct tree *tree) {
	log_info("Saving to index %s", tree->fname);
	/* we're overcommiting by a factor of sizeof(struct ydb_key_record) and padding*/
	u64 file_size =  sizeof(struct index_header) \
		  + sizeof(struct index_item) * tree->key_counter \
		  + PADDING * tree->key_counter \
		  + tree->key_bytes;
	//u64 mmap_size = MAX(file_size, MMAP_MIN_ADDR);
	
	/* we delete the previous contents! */
	int fd = open(tree->fname_new,  O_RDWR|O_TRUNC|O_APPEND|O_CREAT, S_IRUSR|S_IWUSR);
	if(fd < 0) {
		log_perror("open()");
		return(-1);
	}
	if(ftruncate(fd, file_size) < 0) {
		log_perror("ftrunctate()");
		goto error_close;
	}
	
	char *mmap_ptr = mmap(NULL, file_size, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	if(mmap_ptr == MAP_FAILED) {
		log_perror("mmap()");
		goto error_close;
	}
	char *buf = mmap_ptr;

	struct index_header *ih = (struct index_header *)buf;
	buf += sizeof(struct index_header);
	
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
		
		struct index_item *ii = (struct index_item *)buf;
		buf += sizeof(struct index_item) + ROUND_UP(item->key_sz, PADDING);
		
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

	u64 written_bytes = buf - mmap_ptr; /* actually written */
	if(ftruncate(fd, written_bytes) < 0) {
		log_perror("ftrunctate()");
		goto error_close;
	}
	
	if(msync(mmap_ptr, written_bytes, MS_SYNC) != 0) {
		log_perror("msync()");
		goto error_unmap;
	}
	if(munmap(mmap_ptr, file_size) < 0)
		log_perror("munmap()");
	
	if(fsync(fd) < 0) {
		log_perror("fsync()");
		goto error_close;
	}
	if(close(fd) < 0) {
		log_perror("close()");
		return(-1);
	}
	/* no resources left to be closed */
	
	/* TODO: check if tree->fname exists */
	/* TODO: move older indexes to backups :)*/
	
	/* ignore errors */
	rename(tree->fname, tree->fname_old);
	
	/* this is pretty important */
	if(rename(tree->fname_new, tree->fname) < 0) {
		log_perror("rename()");
		return(-1);
	}

	tree->commited_last_record_logno = tree->last_record_logno;
	tree->commited_last_record_offset = tree->last_record_offset;
	return(0);

error_unmap:
	if(munmap(mmap_ptr, file_size) < 0)
		log_perror("munmap()");
error_close:
	close(fd);
	return(-1);
}


int tree_load_index(struct tree *tree, int *last_record_logno, u64 *last_record_offset, int flags) {
	char *reason = "unknown";
	log_info("Loading index %s", tree->fname);

	int fd = open(tree->fname, O_RDONLY|O_LARGEFILE|O_NOATIME);
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
		goto error_unmap;
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
	log_error("Error loading index %s: %s, offet:%i", tree->fname, reason, buf-mmap_ptr);
	return(-1);
}

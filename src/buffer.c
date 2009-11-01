#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>

#include "ydb_internal.h"

#define DEFAULT_BUF_SIZE (4*1024*1024)



BUFFER buf_writer_from_fd(int fd) {
	BUFFER b = (BUFFER)malloc(sizeof(struct buffer) + DEFAULT_BUF_SIZE);
	b->fd = fd;
	b->count = 0;
	b->start_pos = 0;
	b->end_pos = 0;
	b->buf_sz = DEFAULT_BUF_SIZE;
	return(b);
}

char *buf_writer(BUFFER b, int sz) {
	/* won't fit? */
	if(b->end_pos + sz > b->buf_sz) {
		if(sz > b->buf_sz) {
			while(b->buf_sz < sz)
				b->buf_sz *= 2;
			b = realloc(b, b->buf_sz);
			assert(b);
		}
		if(b->end_pos) {
			int p = 0;
			while(p < b->end_pos) {
				int r = write(b->fd, b->buf + p, b->end_pos - p);
				assert(r > 0);
				p += r;
			}
			b->end_pos = 0;
		}
	}
	char *ptr = &b->buf[b->end_pos];
	b->end_pos += sz;
	b->count += sz;
	return(ptr);
}

u_int64_t buf_writer_free(BUFFER b) {
	if(b->end_pos) {
		int p = 0;
		while(p < b->end_pos) {
			int r = write(b->fd, b->buf + p, b->end_pos - p);
			assert(r > 0);
			p += r;
		}
		b->end_pos = 0;
	}
	u_int64_t count = b->count;
	free(b);
	return(count);
}



BUFFER buf_reader_from_fd(int fd) {
	return(buf_writer_from_fd(fd));
}

char *buf_read(BUFFER b, int sz) {
	if(b->start_pos + sz > b->end_pos) {
		if(sz > b->buf_sz) {
			while(b->buf_sz < sz)
				b->buf_sz *= 2;
			b = realloc(b, b->buf_sz);
			assert(b);
		}
		if(b->start_pos) {
			memmove(b->buf, b->buf + b->start_pos, b->end_pos - b->start_pos);
			b->end_pos -= b->start_pos;
			b->start_pos = 0;
		}
		
		/* fill the buffer */
		while(b->buf_sz - b->end_pos > 0) {
			int r = read(b->fd, b->buf+b->end_pos, b->buf_sz - b->end_pos);
			if(r <= 0) {
				break;
			}
			b->end_pos += r;
		}

		if(b->start_pos + sz > b->end_pos) {
			return(NULL);
		}
	}
	
	char *ptr = &b->buf[b->start_pos];
	b->start_pos += sz;
	b->count += sz;
	return(ptr);
}

u_int64_t buf_reader_free(BUFFER b) {
	u_int64_t count = b->count;
	free(b);
	return(count);
}

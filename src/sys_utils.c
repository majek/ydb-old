#define _XOPEN_SOURCE 500 // pread

#include "ydb_internal.h"


#include <stdio.h> // snprintf, rename
#include <sys/time.h> // getrlimit
#include <sys/resource.h> // getrlimit
#include <unistd.h> // pread
#include <errno.h>

// stat
#include <sys/stat.h>


int safe_unlink(const char *old_path) {
	char new_path[256];
	snprintf(new_path, sizeof(new_path), "%s~", old_path);
	if(rename(old_path, new_path) != 0) {
		perror("rename()");
		return(-1);
	}
	return(0);
}

int max_descriptors() {
	/* It's impossible to know the exact number.
	   We leave 128 descriptors to application. */
	struct rlimit rlimit;
	int r = getrlimit(RLIMIT_NOFILE, &rlimit);
	if(r != 0)
		return(1024 - 128); // guess the number
	int max_fd = rlimit.rlim_cur;
	return(max_fd - 128);
}

/* Write exactly `count` bytes to file. Returns -1 on error. */
int writeall(int fd, void *sbuf, size_t count) {
	char *buf = (char *)sbuf;
	size_t pos = 0;
	while(pos < count) {
		int r = write(fd, &buf[pos], count-pos);
		if(errno == EINTR)
			continue;
		if(r < 0) {
			perror("write()");
			return(r);
		}
		pos += r;
	}
	return(pos);
}

int preadall(int fd, void *sbuf, size_t count, size_t offset) {
	char *buf = (char *)sbuf;
	size_t pos = 0;
	while(pos < count) {
		ssize_t r = pread(fd, buf+pos, count-pos, offset+pos);
		if(r == 0) {
			if(errno == EINTR)
				continue;
			return(-1);
		}
		if(r < 0) {
			perror("pread()");
			return(-1);
		}
		pos += r;
	}
	return(pos);
}

#define MOD_ADLER 65521
u32 adler32(void *sdata, size_t len) {
	u8 *data = (u8*)sdata;
	u32 a = 1, b = 0;
	
	while (len != 0) {
		a = (a + *data++) % MOD_ADLER;
		b = (b + a) % MOD_ADLER;
		len--;
	}
	
	return (b << 16) | a;
}


int get_fd_size(int fd, u64 *size) {
	struct stat st;
	if(fstat(fd, &st) != 0){
		perror("stat()");
		return(-1);
	}
	if(size)
		*size = st.st_size;
	return(0);
}



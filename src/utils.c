#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <errno.h>

// open
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

// gettimeofday
#include <sys/time.h>
#include <time.h>

// write
#include <unistd.h>

int ydb_log_fd = -1;
char ydb_log_file[] = "ydb.log";


void ydb_log(char *file, int line, char *type, void *ptr, const char *fmt, ...) {
	char buf[1024];
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);
	
	if(ydb_log_fd < 0) {
		ydb_log_fd = open(ydb_log_file, O_WRONLY|O_APPEND|O_CREAT, S_IRUSR|S_IWUSR);
		if(ydb_log_fd < 0)
			return;
	}
	
	char fline[32];
	snprintf(fline, sizeof(fline), "%s:%i", file, line);
	
	struct timeval tv;
	gettimeofday(&tv, NULL);
	
	struct tm *tmp = localtime(&tv.tv_sec);
	
	char tb[32];
	strftime(tb, sizeof(tb), "%Y-%m-%d %H:%M:%S", tmp);
	
	char buf2[1024];

	if(!ptr) {
		snprintf(buf2, sizeof(buf2), "%s.%03li %-18s %-6s %s\n",
						tb,
						tv.tv_usec/1000,
						fline,
						type,
						buf);
	} else {
		snprintf(buf2, sizeof(buf2), "%s.%03li %-18s %-6s %p %s\n",
						tb,
						tv.tv_usec/1000,
						fline,
						type,
						ptr,
						buf);
	}
	
	write(ydb_log_fd, buf2, strlen(buf2));
	fsync(ydb_log_fd);
}

void ydb_log_perror(char *file, int line, const char *fmt, ...) {
	char errno_buf[512];
	char user_buf[1024];
	strerror_r(errno, errno_buf, sizeof(errno_buf));
	errno_buf[sizeof(errno_buf)-1] = '\0';
	
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(user_buf, sizeof(user_buf), fmt, ap);
	va_end(ap);
	
	ydb_log(file, line, "ERROR", NULL, "%s: %s", user_buf, errno_buf);
}




#include <stdio.h>
#include <stdarg.h>
#include <string.h>

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


void ydb_log(char *file, char *type, void *ptr, const char *fmt, ...) {
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
	
	struct timeval tv;
	gettimeofday(&tv, NULL);
	
	struct tm *tmp = localtime(&tv.tv_sec);
	
	char tb[32];
	strftime(tb, sizeof(tb), "%Y-%m-%d %H:%M:%S", tmp);
	
	char buf2[1024];

	if(!ptr) {
		snprintf(buf2, sizeof(buf2), "%s.%03li %-16s %-6s %s\n",
						tb,
						tv.tv_usec/1000,
						file,
						type,
						buf);
	} else {
		snprintf(buf2, sizeof(buf2), "%s.%03li %-16s %-6s %p %s\n",
						tb,
						tv.tv_usec/1000,
						file,
						type,
						ptr,
						buf);
	}
	
	write(ydb_log_fd, buf2, strlen(buf2));
	fsync(ydb_log_fd);
}


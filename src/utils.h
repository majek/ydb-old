#define MIN(a,b) ((a) <= (b)? (a) : (b))
#define MAX(a,b) ((a) >= (b)? (a) : (b))

// p _must_ be a power of 2
// ROUND_REMAINDER(0, 4) --> 0
// ROUND_REMAINDER(1, 4) --> 3
// ROUND_REMAINDER(2, 4) --> 2
// ROUND_REMAINDER(3, 4) --> 1

#define ROUND_REMAINDER(v, p)	\
	((p - (v) % p) & (p-1))
#define ROUND_UP(v, p)		\
	((v) + ROUND_REMAINDER((v), p))


/* **** **** */
static inline void *zmalloc(size_t size){
	void *ptr = malloc(size);
	memset(ptr, 0, size);
	return(ptr);
}

static inline char *safe_strncpy(char *dst, const char *src, size_t n) {
	assert(n > 0);
	dst[n-1] = '\0';
	return strncpy(dst, src, n-1);
}



#define log_debug(format, ...) ydb_log(__FILE__, __LINE__, "DEBUG", NULL, format, ##__VA_ARGS__)
#define log_info(format, ...)  ydb_log(__FILE__, __LINE__, "INFO",  NULL, format, ##__VA_ARGS__)
#define log_warn(format, ...)  ydb_log(__FILE__, __LINE__, "WARN",  NULL, format, ##__VA_ARGS__)
#define log_error(format, ...) ydb_log(__FILE__, __LINE__, "ERROR", NULL, format, ##__VA_ARGS__)

#define db_debug(format, ...) ydb_log(__FILE__, __LINE__, "DEBUG", db, format, ##__VA_ARGS__)
#define db_info(format, ...)  ydb_log(__FILE__, __LINE__, "INFO",  db, format, ##__VA_ARGS__)
#define db_warn(format, ...)  ydb_log(__FILE__, __LINE__, "WARN",  db, format, ##__VA_ARGS__)
#define db_error(format, ...) ydb_log(__FILE__, __LINE__, "ERROR", db, format, ##__VA_ARGS__)

void ydb_log(char *file, int line, char *type, void *ptr, const char *fmt, ...);


#define log_perror(format, ...) ydb_log_perror(__FILE__, __LINE__, format, ##__VA_ARGS__)
void ydb_log_perror(char *file, int line, const char *fmt, ...);



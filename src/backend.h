#define USE_STORAGE_BDB
#define USE_STORAGE_TC


#define VERSION_STRING "0.1.1"

#define MAX_KEY_SIZE (256)
#define POWER_BLOCK (1024*1024 + 32)
#define MAX_WIRE_VALUE_SIZE POWER_BLOCK
#define MAX_EXTRAS_SIZE (256)
#define MC_HEADER_SIZE 24

#define SWAP(a,b)		\
	do{			\
		__typeof (a) c;	\
		c = (a);	\
		(a) = (b);	\
		(b) = (c);	\
	}while(0)

#define MAX(a,b)	((a) >= (b) ? (a) : (b))
#define MIN(a,b)	((a) <= (b) ? (a) : (b))



#define MAX_REQUEST_SIZE \
	(MC_HEADER_SIZE + MAX_EXTRAS_SIZE + MAX_KEY_SIZE + MAX_WIRE_VALUE_SIZE)

typedef u_int64_t __u64;

#define ntohll(x) \
        ((__u64)( \
                (__u64)(((__u64)(x) & (__u64)0x00000000000000ffULL) << 56) | \
                (__u64)(((__u64)(x) & (__u64)0x000000000000ff00ULL) << 40) | \
                (__u64)(((__u64)(x) & (__u64)0x0000000000ff0000ULL) << 24) | \
                (__u64)(((__u64)(x) & (__u64)0x00000000ff000000ULL) <<  8) | \
                (__u64)(((__u64)(x) & (__u64)0x000000ff00000000ULL) >>  8) | \
                (__u64)(((__u64)(x) & (__u64)0x0000ff0000000000ULL) >> 24) | \
                (__u64)(((__u64)(x) & (__u64)0x00ff000000000000ULL) >> 40) | \
                (__u64)(((__u64)(x) & (__u64)0xff00000000000000ULL) >> 56) ))

#define htonll(x) ntohll(x)

typedef struct mc_storage_api MC_STORAGE_API;


struct mc_req{
	u_int8_t opcode;
	u_int16_t status;
	u_int32_t opaque;
	u_int64_t cas;

	char key[MAX_KEY_SIZE];
	u_int32_t key_sz;
	char extras[MAX_KEY_SIZE];
	u_int32_t extras_sz;
	char *value;
	u_int32_t value_sz;
	
	char *data_ptr;
};
typedef struct mc_req MC_REQ;

struct mc_header{
	// network byte order
	u_int8_t magic;
	u_int8_t opcode;
	u_int16_t key_length;
	u_int8_t extras_length;
	u_int8_t data_type;
	u_int16_t status;
	u_int32_t body_length;
	u_int32_t opaque;
	u_int64_t cas;
};

struct mc_metadata{
	u_int32_t flags;
	u_int32_t expiration;
	u_int64_t cas;
	u_int32_t checksum;
};
typedef struct mc_metadata MC_METADATA;

#define MC_GET_BODY_LENGTH(header) ntohl(header.body_length)

#define MAX_REAL_VALUE_SIZE (MAX_WIRE_VALUE_SIZE + sizeof(MC_METADATA)+4)

struct mc_conn {
	MC_STORAGE_API *api;
	char host[64];
	int port;
	int cd;
	struct event ev;
	union {
		char buf[MAX_REQUEST_SIZE];
		struct mc_header header;
	} data;
	MC_METADATA __reserved_for_metadata; // overflow
	char __reserved[128];
	
	int data_sz;	// size of valid data in the buffer
	int data_ptr;	// pointer
	
	MC_REQ req;
};

typedef struct mc_conn MC_CONN;


#define MC_CMD_GET	0x00 //   Get
#define MC_CMD_SET	0x01 //   Set
#define MC_CMD_ADD	0x02 //   Add
#define MC_CMD_REPLACE	0x03 //   Replace
#define MC_CMD_DELETE	0x04 //   Delete
#define MC_CMD_INCR	0x05 //   Increment
#define MC_CMD_DECR	0x06 //   Decrement
#define MC_CMD_QUIT	0x07 //   Quit
#define MC_CMD_FLUSH	0x08 //   Flush
#define MC_CMD_GETQ	0x09 //   GetQ
#define MC_CMD_NOOP	0x0A //   No-op
#define MC_CMD_VERSION	0x0B //   Version
#define MC_CMD_GETK	0x0C //   GetK
#define MC_CMD_GETKQ	0x0D //   GetKQ
#define MC_CMD_APPEND	0x0E //   Append
#define MC_CMD_PREPEND	0x0F //   Prepend
#define MC_CMD_STAT	0x10 //   Stat

#define MC_XCMD_QLIST_ADD	0xF0 //   add data to set
#define MC_XCMD_QLIST_DEL	0xF1 //   del data from set


#define MC_STATUS_OK			0x0000 // No error
#define MC_STATUS_KEY_NOT_FOUND		0x0001 // Key not found
#define MC_STATUS_KEY_EXISTS		0x0002 // Key exists
#define MC_STATUS_VALUE_TOO_BIG		0x0003 // Value too big
#define MC_STATUS_INVALID_ARGUMENTS	0x0004 // Invalid arguments
#define MC_STATUS_ITEM_NOT_STORED	0x0005 // Item not stored
#define MC_STATUS_UNKNOWN_COMMAND	0x0081 // Unknown command

void mc_close(MC_CONN *conn);


#define debug(format, ...)	gl_log("DEBUG", format, ##__VA_ARGS__)
#define info(format, ...)	gl_log("INFO", format, ##__VA_ARGS__)
#define warn(format, ...)	gl_log("WARN", format, ##__VA_ARGS__)
#define error(format, ...)	gl_log("ERROR", format, ##__VA_ARGS__)

void mc_log(MC_CONN *conn, const char *type, const char *s, ...);

#define mc_debug(conn, format, ...) mc_log(conn, "DEBUG", format, ##__VA_ARGS__)
#define mc_info(conn, format, ...)  mc_log(conn, "INFO", format, ##__VA_ARGS__)
#define mc_warn(conn, format, ...)  mc_log(conn, "WARNING", format, ##__VA_ARGS__)
#define mc_error(conn, format, ...) mc_log(conn, "ERROR", format, ##__VA_ARGS__)




/* net.c */
int net_bind(char *host, int port);
int net_accept(int sd, char **host, int *port);


/* utils.c */

void *zmalloc(size_t size);
unsigned long hash_djb2(char *s);
unsigned long hash_kr(char *p);
unsigned long hash_sum(char *s);
void gl_log(const char *type, const char *s, ...);
int key_escape(char *dst, int dst_sz, char *key, int key_sz);
uint32_t adler32(char *data, size_t len);
void perror_msg_and_die(const char *s, ...);


/* backend.c */
void do_event_loop(char *host, int port);

void client_callback(int cd, short event, void *userdata);
void mc_process(MC_CONN *conn, MC_REQ *req);

void mc_recv(MC_CONN *conn);
void mc_write(MC_CONN *conn, int after_read);

#define TRUE 1
#define FALSE 0


/* storage.c */
struct mc_storage_api {
	int (*get)(void *storage_data, char *dst, int size, char *key, int key_sz);
	int (*set)(void *storage_data, char *value, int value_sz, char *key, int key_sz);
	int (*del)(void *storage_data, char *key, int key_sz);
	void (*sync)(void *storage_data);
	void *storage_data;
};


int storage_get(MC_STORAGE_API *api, MC_METADATA *md, char *value, int value_sz, char *key, int key_sz);
int storage_set(MC_STORAGE_API *api, MC_METADATA *md, char *value, int value_sz, char *key, int key_sz);
int storage_delete(MC_STORAGE_API *api, char *key, int key_sz);
void storage_sync(MC_STORAGE_API *api);

/* sto_fs.c */
MC_STORAGE_API *storage_fs_create(char *dir);
void storage_fs_destroy(MC_STORAGE_API *api);

/* sto_fs.c */
MC_STORAGE_API *storage_dumb_create(char *dir);
void storage_dumb_destroy(MC_STORAGE_API *api);

/* sto_tc.c */
MC_STORAGE_API *storage_tc_create(char *dir);
void storage_tc_destroy(MC_STORAGE_API *api);

/* sto_bdb.c */
MC_STORAGE_API *storage_bdb_create(char *db_file);
void storage_bdb_destroy(MC_STORAGE_API *api);

/* sto_adb.c */
MC_STORAGE_API *storage_adb_create(char *db_file);
void storage_adb_destroy(MC_STORAGE_API *api);


/* command.c */
typedef MC_REQ* (*cmd_ptr_t)(MC_CONN *, MC_REQ *);

void commands_initialize();
void commands_destroy();

cmd_ptr_t get_cmd_pointer(u_int8_t opcode);
char* get_cmd_name(u_int8_t opcode);

MC_REQ *set_error_code(MC_REQ *req, unsigned char status);

MC_REQ *cmd_unknown(MC_CONN *conn, MC_REQ *req);
MC_REQ *cmd_get(MC_CONN *conn, MC_REQ *req);
MC_REQ *cmd_set(MC_CONN *conn, MC_REQ *req);
MC_REQ *cmd_delete(MC_CONN *conn, MC_REQ *req);
MC_REQ *cmd_noop(MC_CONN *conn, MC_REQ *req);
MC_REQ *cmd_version(MC_CONN *conn, MC_REQ *req);
MC_REQ *cmd_incr(MC_CONN *conn, MC_REQ *req);

#define FLAG_QLIST (0x04)
MC_REQ *xcmd_qlist_add(MC_CONN *conn, MC_REQ *req);
MC_REQ *xcmd_qlist_del(MC_CONN *conn, MC_REQ *req);


/* qlist2.c */
#define QLIST_TYPE_SIMPLE 0xDEADBABE

struct qlist {
	u_int32_t magic_type;
	u_int64_t start_item;

	u_int32_t  list_sz;
	u_int32_t  list_sz_max;

	u_int32_t  iter;
	u_int64_t  iter_item;
	
	//u_int32_t items;
	
	u_int8_t list[];
};
typedef struct qlist QLIST;


int qlist_initialize(QLIST *qlc, int used_bytes, int size);
void qlist_freeze(QLIST *qlc);
int qlist_or(QLIST *qlc, QLIST *qla, QLIST *qlb);



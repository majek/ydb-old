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



static inline void *zmalloc(size_t size){
	void *ptr = malloc(size);
	memset(ptr, 0, size);
	return(ptr);
}


typedef struct mc_storage_api MC_STORAGE_API;

struct mc_storage_api {
	int (*get)(void *storage_data, char *dst, int size, char *key, int key_sz);
	int (*set)(void *storage_data, char *value, int value_sz, char *key, int key_sz);
	int (*del)(void *storage_data, char *key, int key_sz);
	void (*sync)(void *storage_data);
	void *storage_data;
};


int storage_get(MC_STORAGE_API *api, char *value, int value_sz, char *key, int key_sz);
int storage_set(MC_STORAGE_API *api, char *value, int value_sz, char *key, int key_sz);
int storage_delete(MC_STORAGE_API *api, char *key, int key_sz);
void storage_sync(MC_STORAGE_API *api);

/* sto_tc.c */
MC_STORAGE_API *storage_tc_create(char *dir);
void storage_tc_destroy(MC_STORAGE_API *api);

/* sto_bdb.c */
MC_STORAGE_API *storage_bdb_create(char *db_file);
void storage_bdb_destroy(MC_STORAGE_API *api);

/* sto_ydb.c */
MC_STORAGE_API *storage_ydb_create(char *db_file);
void storage_ydb_destroy(MC_STORAGE_API *api);


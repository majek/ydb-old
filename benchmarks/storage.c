#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "backend.h"

int storage_get(MC_STORAGE_API *api, char *value, int value_sz, char *key, int key_sz) {
	int ret = api->get(api->storage_data, value, value_sz, key, key_sz);
	return(ret);
}

int storage_set(MC_STORAGE_API *api, char *value, int value_sz, char *key, int key_sz) {
	int ret = api->set(api->storage_data, value, value_sz, key, key_sz);
	return(ret);
}

int storage_delete(MC_STORAGE_API *api, char *key, int key_sz) {
	int ret = api->del(api->storage_data, key, key_sz);
	return(ret);
}

void storage_sync(MC_STORAGE_API *api) {
	api->sync(api->storage_data);
}

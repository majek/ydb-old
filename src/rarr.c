/*
Right-Array: Associative-like array abstraction.

Key assumptions:
   * linear access (ie. first 0th element, than 1st element)
   * relatively small window of used elements (around 1k?)
   * elements in front are lineary eptied
   * accessed often, modified rarely
   * once the window is moved right, it will never go back left
   * window is constant, once extended, there's no need to free

For example:
   * offset = 5
   * size = 10

relative indexes:   <-- offset --> [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
global indexes:     [0, 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13,14,15,16,17,18]


TODO:
   * when I start setting from a big index, rarr uses too much memory
*/

#include "ydb_internal.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define RARR_DEFAULT 1024
#define CLEAR_THRESHOLD 16
#define SHIFT_THRESHOLD 16

#define RARR_MAGIC 0xAA50BABE

struct rarr{
	u32 magic;
	int clear_count;
	int filled_items;
	int offset;
	int size;
	void **arr;
};

r_arr rarr_new() {
	struct rarr *a = (struct rarr *) zmalloc(sizeof(struct rarr));
	a->magic = RARR_MAGIC;
	a->offset = 0;
	a->size = RARR_DEFAULT;
	a->arr = (void**) zmalloc(RARR_DEFAULT * sizeof(void*));
	return a;
}

void rarr_free(r_arr v) {
	struct rarr *a = (struct rarr *)v;
	assert(a->magic == RARR_MAGIC);
	if(a->filled_items != 0)
		fprintf(stderr,"filled_items = %i\n",a->filled_items);
	assert(a->filled_items == 0);
	
	free(a->arr);
	free(a);
}

void rarr_free_and_clear(r_arr v) {
	struct rarr *a = (struct rarr *)v;
	assert(a->magic == RARR_MAGIC);
	
	free(a->arr);
	free(a);
}

int rarr_get_filled_items(r_arr v) {
	struct rarr *a = (struct rarr *)v;
	assert(a->magic == RARR_MAGIC);
	return(a->filled_items);
}


void *rarr_get(r_arr v, int idx) {
	struct rarr *a = (struct rarr *)v;
	assert(a->magic == RARR_MAGIC);
	
	if(idx >= a->offset && idx < (a->offset + a->size))
		return(a->arr[idx - a->offset]);
	return(NULL);
}

void rarr_shift(struct rarr *a, int off) {
	if(++a->clear_count < CLEAR_THRESHOLD)
		return;
	a->clear_count = 0;
	
	int i;
	assert(off < a->size);
	for(i=0; i<off; i++) {
		if(a->arr[i] != NULL)
			break;
	}
	if(i >= a->size || i < SHIFT_THRESHOLD) // all empty or too little - do nothing
		return;
	int left = i-1;
	int right = a->size - left;
	
	memmove(&a->arr[0], &a->arr[left], right*sizeof(void*));
	memset(&a->arr[right], 0, left*sizeof(void*));
	
	a->offset += left;
}


void rarr_extend(struct rarr *a, int new_size) {
	int prev_size = a->size;
	
	a->size = new_size;
	a->arr = (void**) realloc(a->arr, new_size * sizeof(void*));
	assert(a->arr);
	memset(&a->arr[prev_size], 0, (new_size - prev_size)*sizeof(void*));
}


void rarr_clear(r_arr v, int idx) {
	struct rarr *a = (struct rarr *)v;
	assert(a->magic == RARR_MAGIC);
	
	if(idx >= a->offset && idx < (a->offset + a->size)) {
		if(a->arr[idx - a->offset] != NULL)
			a->filled_items--;
		a->arr[idx - a->offset] = NULL;
		rarr_shift(v, idx - a->offset);
		return;
	}else{ // out of range
		return;
	}
}



void rarr_set(r_arr v, int idx, void *value) {
	struct rarr *a = (struct rarr *)v;
	assert(a->magic == RARR_MAGIC);

	if(value == NULL) {
		rarr_clear(v, idx);
		return;
	}

	// left
	if(idx < a->offset) {
		/* not supported at all*/
		assert(0);
	}
	// right
	if(idx >= (a->offset + a->size)) {
		int new_size = (((idx - a->offset) / RARR_DEFAULT) + 1) * RARR_DEFAULT;
		assert(idx < (a->offset + new_size));
		rarr_extend(a, new_size);
	}
	assert(idx >= a->offset && idx < (a->offset + a->size));
	if(a->arr[idx - a->offset] == NULL)
		a->filled_items++;
	a->arr[idx - a->offset] = value;
	return;
}

int rarr_min(r_arr v) {
	struct rarr *a = (struct rarr *)v;
	assert(a->magic == RARR_MAGIC);

	return(a->offset);
}
int rarr_max(r_arr v) {
	struct rarr *a = (struct rarr *)v;
	assert(a->magic == RARR_MAGIC);

	return(a->offset+a->size);
}

#ifdef TESTING
int main(int argc, char *argv[]) {
	r_arr rarr = rarr_new();
	int i;
	
	// check for the value in range
	assert(rarr_get(rarr, 0) == NULL);
	
	// clear out of range value
	rarr_set(rarr, 8000, NULL);
	assert(rarr_get(rarr, 8000) == NULL);

	// check for the value outside range
	assert(rarr_get(rarr, 9999) == NULL);
	
	// growing window
	rarr_set(rarr, 1, (void*)1);
	assert(rarr_get(rarr, 1) == (void*)1);
	rarr_set(rarr, 2048, (void*)2048);
	assert(rarr_get(rarr, 2048) == (void*)2048);
	assert(rarr_get(rarr, 1) == (void*)1);

	// moving window, slowly (actually can't test output...)
	for(i=0; i<32; i++)
		rarr_set(rarr, i, NULL);
	// just a sanity test
	assert(rarr_get(rarr, 2048) == (void*)2048);
	
	// moving window, big range
	for(i=0; i<32; i++)
		rarr_set(rarr, 2047, NULL);
	// just a sanity test
	assert(rarr_get(rarr, 2048) == (void*)2048);
	
	// free
	rarr_free_and_clear(rarr);
	
	// new one. go through a lot of elements with a filled window of about 4
	rarr = rarr_new();
	rarr_set(rarr, 0, (void*)1);
	rarr_set(rarr, 1, (void*)1);
	rarr_set(rarr, 2, (void*)1);
	rarr_set(rarr, 3, (void*)1);
	for(i=0; i<4096; i++) {
		assert(rarr_get_filled_items(rarr) == 4);
		rarr_set(rarr, i, NULL);
		rarr_set(rarr, i+4, (void*)1);
	}
	rarr_set(rarr, i+0, NULL);
	rarr_set(rarr, i+1, NULL);
	rarr_set(rarr, i+2, NULL);
	rarr_set(rarr, i+3, NULL);
	rarr_set(rarr, i+4, NULL);
	rarr_set(rarr, i+5, NULL);
	rarr_free(rarr);
	return(0);
}
#endif


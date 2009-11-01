#include <sys/types.h>
#include <assert.h>
#include <math.h>
#include <stdlib.h>

#define STDDEV_MAGIC (0xBABE057D)
struct stddev {
	u_int32_t magic;
	int64_t sum;
	int64_t sum_sq;
	int64_t count;
};

typedef void* STDDEV;

STDDEV stddev_new() {
	struct stddev *sd = (struct stddev *)malloc(sizeof(struct stddev));
	sd->magic = STDDEV_MAGIC;
	sd->sum = 0;
	sd->sum_sq = 0;
	sd->count = 0;
	return(sd);
}

void stddev_free(STDDEV stddev) {
	struct stddev *sd = (struct stddev *) stddev;
	assert(sd->magic == STDDEV_MAGIC);
	sd->magic = 0;
	free(sd);
}

void stddev_add(STDDEV stddev, int value) {
	struct stddev *sd = (struct stddev *) stddev;
	assert(sd->magic == STDDEV_MAGIC);
	sd->count ++;
	sd->sum += value;
	sd->sum_sq += value * value;
}

void stddev_remove(STDDEV stddev, int old_value) {
	struct stddev *sd = (struct stddev *) stddev;
	assert(sd->magic == STDDEV_MAGIC);
	sd->count --;
	sd->sum -= old_value;
	sd->sum_sq -= old_value * old_value;
}

void stddev_modify(STDDEV stddev, int old_value, int new_value) {
	stddev_remove(stddev, old_value);
	stddev_add(stddev, new_value);
}

void stddev_get(STDDEV stddev, int *counter_ptr,
		double *avg_ptr, double *stddev_ptr) {
	struct stddev *sd = (struct stddev *) stddev;
	assert(sd->magic == STDDEV_MAGIC);
	if(avg_ptr || stddev_ptr) {
		double avg, dev;
		if(sd->count == 0) {
			avg = 0.0;
			dev = 0.0;
		} else {
			avg = sd->sum / sd->count;
			dev = sqrt(  (sd->sum_sq / sd->count) - (avg*avg)  );
		}
		if(avg_ptr)
			*avg_ptr = avg;
		if(stddev_ptr)
			*stddev_ptr = dev;
	}
	if(counter_ptr)
		*counter_ptr = sd->count;
	return;
}



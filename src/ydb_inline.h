#define MOD_ADLER 65521

#define PREFETCH(x) __builtin_prefetch(x)
#define PREFETCHW(x) __builtin_prefetch(x, 1)

static inline u32 adler32(void *sdata, size_t len) {
	u8 *data = (u8*)sdata;
	u32 a = 1, b = 0;
	
	while (len != 0) {
		a = (a + *data++) % MOD_ADLER;
		b = (b + a) % MOD_ADLER;
		len--;
		
		if(0 == (((long int)data) & 0x7F) )
			PREFETCH(data + 128);
	}
	
	return (b << 16) | a;
}

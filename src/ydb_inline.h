#define MOD_ADLER 65521
static inline u32 adler32(void *sdata, size_t len) {
	u8 *data = (u8*)sdata;
	u32 a = 1, b = 0;
	
	while (len != 0) {
		a = (a + *data++) % MOD_ADLER;
		b = (b + a) % MOD_ADLER;
		len--;
	}
	
	return (b << 16) | a;
}

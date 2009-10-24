.PHONY: clean

O_FILES=$(patsubst %.c,%.o,$(wildcard *.c))
DEPS_FILES=$(patsubst %.o,.deps/%.d,$(O_FILES))
.SECONDARY: $(DEPS_FILES)

LIBNAME=libydb.so


CC=gcc
PYTHON=/usr/bin/python
C_FLAGS=$(CFLAGS) -pthread -fPIC -g -Wall -fstack-protector-all
L_FLAGS= -lc -lrt -pthread -fPIC -g -Wall -shared -Wl,-soname,$(LIBNAME)

REL_CFLAGS=$(C_FLAGS) -O2
COV_CFLAGS=$(C_FLAGS) -O0 --coverage -DTESTING
PROF_CFLAGS=$(C_FLAGS) -O2 -pg

REL_LFLAGS=$(L_FLAGS)
COV_LFLAGS=$(L_FLAGS) --coverage
PROF_LFLAGS=$(L_FLAGS) -pg


libydb.so: build-rel/libydb.so
	cp build-rel/libydb.so .

all: libydb.so build-cov/libydb.so build-prof/libydb.so

build-rel/libydb.so: $(patsubst %.o,build-rel/%.o,$(O_FILES))
	$(CC) $(REL_LFLAGS) -o $@ $^
	
build-cov/libydb.so: $(patsubst %.o,build-cov/%.o,$(O_FILES))
	$(CC) $(COV_LFLAGS) -o $@ $^

build-prof/libydb.so: $(patsubst %.o,build-prof/%.o,$(O_FILES))
	$(CC) $(PROF_LFLAGS) -o $@ $^


build-rel/%.o: %.c $(DEPS_FILES)
	@[ ! -e $(@D) ] && mkdir $(@D) || true
	$(CC) $(REL_CFLAGS) -o $@ -c $<

build-cov/%.o: %.c $(DEPS_FILES)
	@[ ! -e $(@D) ] && mkdir $(@D) || true
	$(CC) $(COV_CFLAGS) -o $@ -c $<

build-prof/%.o: %.c $(DEPS_FILES)
	@[ ! -e $(@D) ] && mkdir $(@D) || true
	$(CC) $(PROF_CFLAGS) -o $@ -c $<


clean:
	rm -f libydb.so
	rm -rf build-rel build-cov build-prof || true
	#rm -f *.gcno *.gcda *.gcov *.o *~ *.orig *.so *.d gmon.out


.deps/%.d: %.c
	@[ ! -e $(@D) ] && mkdir $(@D) || true
	@set -e; rm -f $@; \
	$(CC) -MM $(REL_CFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$
	
-include $(wildcard .deps/*d)



# rarr-test: test-rarr
# test-rarr:
# 	$(CC) $(C_FLAGS) -DTESTING rarr.c -o rarr-test
# 	./rarr-test
# 	rm rarr-test
# 	@gcov rarr.c > /dev/null 2>/dev/null
# 	@$(PYTHON) ../gcovst.py rarr.c.gcov

todo:
	@echo " [*] aborts:"
	@grep abort *c -ni --color=yes || true
	@echo " [*] exits:"
	@egrep "exit ?\(" *c -ni --color=yes || true
	@echo "\n [*] asserts:"
	@grep assert *c -n --color=yes|egrep -v "#include"  || true
	@echo " [*] prints:"
	@egrep "[[:space:]]f?printf" *c -ni --color=yes || true
	@echo "\n [*] perrors:"
	@egrep "[[:space:]]perror" *c -ni --color=yes || true
	@echo "\n [*] todo:"
	@grep todo *c -ni --color=yes || true




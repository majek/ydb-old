PYTHON=/usr/bin/python

C_FILES=\
	ydb_db.c \
	rbtree.c \
	ydb_loglist.c \
	ydb_tree.c \
	rarr.c \
	sys_utils.c \
	ydb_gc.c \
	utils.c
	
O_FILES=$(C_FILES:.c=.o)
D_FILES=$(C_FILES:.c=.d)
LIBNAME=libydb.so


CC=gcc

DEBUG_FLAGS=-pg -fprofile-arcs -ftest-coverage -O0
C_FLAGS=$(DEBUG_FLAGS) -fPIC -g -Wall -fstack-protector-all -pthread
LIB_FLAGS=$(DEBUG_FLAGS) -g -Wall -shared -Wl,-soname,$(LIBNAME) -lc

all: $(LIBNAME)

include $(D_FILES)

%.d: %.c
	@set -e; rm -f $@; \
	$(CC) -M $(CPPFLAGS) $< > $@.$$$$; \
	sed 's,\($*\)\.o[ :]*,\1.o $@ : ,g' < $@.$$$$ > $@; \
	rm -f $@.$$$$

%.o: %.c
	$(CC) $(C_FLAGS) -c $<

libydb.so: $(O_FILES)
	$(CC) $(LIB_FLAGS) -o $@ $(O_FILES)

clean:
	rm -f *.gcno *.gcda *.gcov *.o *~ *.orig *.so *.d gmon.out


rarr-test: test-rarr

test-rarr:
	$(CC) $(C_FLAGS) -DTESTING rarr.c -o rarr-test
	./rarr-test
	rm rarr-test
	@gcov rarr.c > /dev/null 2>/dev/null
	@$(PYTHON) ../gcovst.py rarr.c.gcov

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




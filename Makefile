TESTS=doctest unittest

PYTHON=/usr/bin/python

all:
	@$(MAKE) -C src all

src/build-cov/libydb.so:
	@$(MAKE) -C src build-cov/libydb.so

clean:
	$(MAKE) -C src clean
	rm -f tests/*.pyc
	rm -f test_leak
	rm -f ydb.log tests/ydb.log tests/.coverage

#strace -fFo /tmp/ydbtest.strace
test: src/build-cov/libydb.so
	@rm -f src/*.gcda src/*.gcov
	@echo -e "\n[*] **** starting tests ****"
	@cd tests && LIBYDB=../src/build-cov/ PYTHONPATH=.. $(PYTHON) run.py $(TESTS)
	@echo -e "\n[*] **** code coverage ****"
	@cd src && for i in *.c; do gcov -o build-cov -bc $$i; done > /dev/null 2>/dev/null
	@$(PYTHON) gcovst.py src
	


testleaks: all
	gcc -O0 -g -Wall -lydb -L./src -I./src -lpthread -Wl,-rpath=./src ./tests/test_leak.c -o test_leak
	mkdir /tmp/ydb-testleak || true
	valgrind --max-stackframe=932554432 --leak-check=full  --tool=memcheck  ./test_leak /tmp/ydb-testleak
	rm /tmp/ydb-testleak/*ydb
	rm -r /tmp/ydb-testleak
	rm test_leak
	

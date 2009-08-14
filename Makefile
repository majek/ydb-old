TESTS=unittest doctest

PYTHON=/usr/bin/python

all:
	@$(MAKE) -C src --no-print-directory

clean:
	$(MAKE) -C src clean
	rm -f tests/*.pyc
	rm -f test_leak
	rm -f ydb.log tests/ydb.log tests/.coverage

#strace -fFo /tmp/ydbtest.strace
test: all
	@rm -f src/*.gcda src/*.gcov
	@echo "\n[*] **** starting tests ****"
	@cd tests && PYTHONPATH=.. $(PYTHON) run.py $(TESTS)
	@echo "\n[*] **** code coverage ****"
	@cd src && gcov *.c > /dev/null 2>/dev/null
	@$(PYTHON) gcovst.py src/*.c.gcov


testleaks: all
	gcc -O0 -g -Wall -lydb -L./src -I./src -lpthread -Wl,-rpath=./src ./tests/test_leak.c -o test_leak
	mkdir /tmp/ydb-testleak || true
	valgrind --max-stackframe=932554432 --leak-check=full  --tool=memcheck  ./test_leak /tmp/ydb-testleak
	rm /tmp/ydb-testleak/*ydb
	rm -r /tmp/ydb-testleak
	#rm test_leak
	
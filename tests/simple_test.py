#!/usr/bin/env python
import os, sys
import ydatabase


def test1():
    directory = '/tmp/ydb-test'
    try:
        os.mkdir(directory)
    except OSError:
        pass
    ydb = ydatabase.YDB(directory)
    ydb.add('a', 'b')
    print '%r != %r' % ('b', ydb.get('a'))
    ydb.close()


if __name__ == '__main__':
    test1()
    sys.exit(0)


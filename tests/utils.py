import random
import unittest
import tempfile
import functools
import os, sys

import ydatabase

class YdbTest(unittest.TestCase):
    def ydb_reopen(self):
        self.ydb.close()
        self.ydb = ydatabase.YDB(self.ydb_dir, **self.ydb_kwargs)
        return self.ydb

"""
    def setUp(self):
        self.ydb_dir = tempfile.mktemp(prefix="ydb-")
        os.mkdir(self.ydb_dir)
        self.ydb = ydatabase.YDB(self.ydb_dir)

    def tearDown(self):
        if self.ydb:
            self.ydb.close()
        os.system("rm %s/*.ydb" % (self.ydb_dir,))
        #os.unlink(self.ydb_dir)
"""

def provide_ydb(**kwargs):
    def x(m):
        @functools.wraps(m)
        def wrapper(self):
            self.ydb_kwargs = kwargs
            self.ydb_dir = tempfile.mktemp(prefix="ydb-")
            os.mkdir(self.ydb_dir)
            self.ydb = ydatabase.YDB(self.ydb_dir, **kwargs)
            e = False
            try:
                try:
                    r = m(self, self.ydb)
                except:
                    e = True
                    raise
            finally:
                if self.ydb:
                    self.ydb.close()
                if not e:
                    os.system("rm %s/*.ydb" % (self.ydb_dir,))
                    os.system("rm -r %s" % (self.ydb_dir,))
                else:
                    print >> sys.stderr, "Ydb directory: %r" % (self.ydb_dir,)

            return r
        return wrapper
    return x



def run_unittests(g):
    test_args = {'verbosity': 1}
    for t in [t for t in g.keys()
                        if (t.startswith('Test') and issubclass(g[t], unittest.TestCase)) ]:
        suite = unittest.TestLoader().loadTestsFromTestCase(g[t])
        unittest.TextTestRunner(**test_args).run(suite)



#!/usr/bin/env python

import sys
import unittest
import glob
import time
import os, os.path
import doctest
import coverage


TEST_NAMES = [f.rpartition('.')[0] for f in glob.glob("test_*.py")]
TEST_NAMES.sort()

BAD_MODULES=['../gcovst.py']
MODULE_NAMES=[f[3:-3] for f in glob.glob("../*.py") if f not in BAD_MODULES]
MODULE_NAMES.sort()

VERBOSE=False

def my_import(name):
    mod = __import__(name)
    components = name.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod

def main_coverage(TESTS):
    modulenames = MODULE_NAMES

    coverage.erase()
    coverage.start()
    coverage.exclude('#pragma[: ]+[nN][oO] [cC][oO][vV][eE][rR]')

    modules = []
    for modulename in modulenames:
        print modulenames
        mod = my_import(modulename)
        modules.append(mod)

    if 'unittest' in TESTS:
        print "***** Unittest *****"
        test_args = {'verbosity': 1}
        suite = unittest.TestLoader().loadTestsFromNames(TEST_NAMES)
        unittest.TextTestRunner(**test_args).run(suite)

    if 'doctest' in TESTS:
        t0 = time.time()
        print "\n***** Doctest *****"
        for mod in modules:
            doctest.testmod(mod, verbose=VERBOSE)
        td = time.time() - t0
        print "      Tests took %.3f seconds" % (td, )

    print "\n***** Coverage Python *****"
    coverage.stop()
    coverage.report(modules, ignore_errors=1, show_missing=1)
    coverage.erase()


if __name__ == '__main__':
    main_coverage(sys.argv)

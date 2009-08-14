import ctypes
import os

LIBNAME="libydb.so"
PATHS=[".", "..", "../src/", "./src"]

for path in PATHS:
    path = os.path.join(path, LIBNAME)
    if os.path.exists(path):
        LIBPATH=path
        break
else:
    raise Exception("libydb.so not found!")


YDB_CREAT=0x01
YDB_RDONLY=0x02
YDB_GCDISABLE=0x04


def create_lib_instance(libfname):
    YDB = ctypes.c_void_p

    libydb = ctypes.cdll.LoadLibrary(libfname)
    libydb.ydb_open.argtypes= [ctypes.c_char_p, ctypes.c_int, ctypes.c_ulonglong, ctypes.c_int]
    libydb.ydb_open.restype = YDB

    libydb.ydb_sync.argtypes=[YDB]
    libydb.ydb_sync.restype = None

    libydb.ydb_close.argtypes=[YDB]
    libydb.ydb_close.restype = None

    libydb.ydb_add.argtypes=[YDB, ctypes.c_char_p, ctypes.c_ushort, ctypes.c_char_p, ctypes.c_uint]
    libydb.ydb_add.restype =ctypes.c_int

    libydb.ydb_del.argtypes=[YDB, ctypes.c_char_p, ctypes.c_ushort]
    libydb.ydb_del.restype =ctypes.c_int

    libydb.ydb_get.argtypes=[YDB, ctypes.c_char_p, ctypes.c_ushort, ctypes.c_char_p, ctypes.c_uint]
    libydb.ydb_get.restype =ctypes.c_int

    return libydb

libydb = create_lib_instance(LIBPATH)

class YDB:
    def __init__(self, directory, overcommit_factor=3, max_file_size=1*1024*1024*1024):
        self.ydb = libydb.ydb_open(directory, overcommit_factor, max_file_size, YDB_CREAT)
        assert self.ydb
        self.buf = ctypes.create_string_buffer(16*1024*1024)

    def sync(self):
        assert self.ydb
        libydb.ydb_sync(self.ydb)

    def close(self):
        assert self.ydb
        libydb.ydb_close(self.ydb)
        self.ydb = None

    def add(self, key, value):
        assert self.ydb
        assert isinstance(key, str)
        assert isinstance(value, str)
        r = libydb.ydb_add(self.ydb, key, len(key), value, len(value))
        return r >= 0

    def delete(self, key):
        assert self.ydb
        assert isinstance(key, str)
        r = libydb.ydb_del(self.ydb, key, len(key))
        return r >= 0

    def get(self, key):
        assert self.ydb
        assert isinstance(key, str)
        r = libydb.ydb_get(self.ydb, key, len(key), self.buf, len(self.buf))
        if r >= 0:
            return self.buf.raw[:r]
        return None

    def __nonzero__(self):
        if not self.ydb:
            return False
        return True




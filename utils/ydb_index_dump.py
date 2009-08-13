#!/usr/bin/env python
import struct
import sys

filename = sys.argv[1]
fd = open(filename, 'rb')

'''
#define INDEX_HEADER_MAGIC 0x43211234
struct index_header{
        u32     magic;
        int     last_record_logno;
        u64     last_record_offset;
        u32     checksum;
};

#define INDEX_ITEM_MAGIC 0x12344321
struct index_item{
        u32     magic;
        u32     checksum;

        int     logno;
        u64     value_offset;
        u32     value_sz;

        u16     key_sz;
        char    key[];
};

'''

import itertools

def round_reminder(v, p):
    return ((p - v % p) & (p-1))

def ROUND_UP(v, p):
    return v + round_reminder(v,p)


PADDING=4

data = fd.read()

def dump_record(s, items, format, data, pos, key_len=None, indent=0, magic=None):
    indent = ' ' * indent
    print '%s%s' % (indent, s,)
    values = struct.unpack_from(format, data, pos)
    assert(len(items) == len(values))
    for i in range(len(items)):
        mok = ''
        if items[i] == 'magic' and magic is not None:
            if values[i] == magic:
                mok = 'ok'
            else:
                mok = 'FAILED'
        item, value = items[i], values[i]
        if type(value) in [int, long]:
            print '%s    %-10s\t=0x%08x\t%s' % (indent, item, value, mok)
        else:
            print '%s    %-10s\t=%r\t%s' % (indent, item, value, mok)

    shift = len(struct.pack(format, *values))

    if key_len is not None:
        dd = dict(zip(items, values))
        key_sz = dd[key_len]
        key = data[pos+shift:pos+shift+key_sz]
        shift += key_sz + round_reminder(key_sz, PADDING)
        if len(key) > 32:
            print '%s    %s...%s' % (indent, repr(key[:16])[:-1], repr(key[-16:])[1:])
        else:
            print '%s    %r' % (indent, key)

    return shift

p = 0
p += dump_record('index header', 
        ['magic', 'last_record_logno', 'last_record_offset', 'checksum'],
        "<IiQI", data, p,
        magic = 0x43211234,
    )

i = itertools.count(1)
while p < len(data):
    p += dump_record('item %i' % i.next(),
            ['magic', 'checksum', 'logno', 'value_offset', 'key_sz', 'r','value_sz'],
            "<IIiQHHI", data, p,
            indent=4,
            key_len = 'key_sz',
            magic = 0x12344321,
        )


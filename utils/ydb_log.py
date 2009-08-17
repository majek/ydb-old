#!/usr/bin/env python
import sys
import itertools
import utils

'''
#define YDB_KEY_MAGIC (0x7DB5EC5D)
struct ydb_key_record{
        u32     magic;
        u32     checksum;
        u16     flags; // deleted
        u16     key_sz;
        u32     value_sz;       /* length of value, without the header */
        char    data[];
};

struct ydb_value_record{
        u32     checksum;
};
'''

filename = sys.argv[1]
fd = open(filename, 'rb')
data = fd.read()


p = 0
i = itertools.count(1)
while p < len(data):
    p += utils.dump_record('record',
            ['magic', 'checksum', 'flags', 'key_sz', 'value_sz', 'key'],
            "<IIHHI", data, p,
            magic = 0x7DB5EC5D,
        )

    
    p += utils.dump_record('item %i' % i.next(),
            ['magic', 'checksum', 'logno', 'value_offset', 'key_sz', 'r','value_sz'],
            "<IIiQHHI", data, p,
            indent=4,
            magic = 0x12344321,
        )





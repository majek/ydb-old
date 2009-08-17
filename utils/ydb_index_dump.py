#!/usr/bin/env python
import sys
import itertools
import utils

'''
#define INDEX_HEADER_MAGIC 0x43211234
struct index_header{
        u32     magic;
        int     last_record_logno;
        u64     last_record_offset;
        u64     keys_stored;
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

filename = sys.argv[1]
fd = open(filename, 'rb')
data = fd.read()


p = 0
p += utils.dump_record('index header', 
        ['magic', 'last_record_logno', 'last_record_offset', 'keys_stored', 'checksum'],
        "<IiQQI", data, p,
        magic = 0x43211234,
    )

i = itertools.count(1)
while p < len(data):
    p += utils.dump_record('item %i' % i.next(),
            ['magic', 'checksum', 'logno', 'value_offset', 'key_sz', 'r','value_sz'],
            "<IIiQHHI", data, p,
            indent=4,
            key_len = 'key_sz',
            magic = 0x12344321,
        )


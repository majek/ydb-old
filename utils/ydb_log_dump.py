#!/usr/bin/env python
import struct
import sys

filename = sys.argv[1]
fd = open(filename, 'rb')

'''
struct adb_key_record{
        uint32_t        magic;
        uint32_t        checksum;
        uint8_t         flags; // deleted
        uint8_t         key_sz;
        uint32_t        value_sz;
        uint8_t         data[]; // key ... padding ... value ... padding ... value_checksum
};
'''

def round_reminder(v, p):
    return ((p - v % p) & (p-1))


data = fd.read()
p = 0
while p < len(data):
    print "   %i - %i" % (p, len(data))
    magic, checksum, flags, key_sz, value_sz = struct.unpack_from("<IIHHI", data, p)
    print "magic=0x%08x" % (magic,), 
    print "key_checksum=0x%08x" % (checksum,), 
    print "flags=0x%x" % (flags,), 
    print "key_sz=0x%x" % (key_sz,), 
    print "value_sz=0x%x" % (value_sz,), 
    p += 16
    print "key=%.16r" % (data[p:p+key_sz]),
    p += key_sz
    p += round_reminder(key_sz, 4)
    if flags == 0:
        value = data[p:p+value_sz]
        if len(value) > 32:
            print "value=%r...%r" % (value[:16], value[-16:]),
        else:
            print "value=%r" % (value,),
        p += value_sz
        p += round_reminder(value_sz, 4)

        checksum, = struct.unpack_from("<I", data, p)
        print "value_checksum=0x%08x" % (checksum,)
        p += 4


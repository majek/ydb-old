import struct

def round_reminder(v, p):
    return ((p - v % p) & (p-1))

def ROUND_UP(v, p):
    return v + round_reminder(v,p)


PADDING=4


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


import sys

PY3 = sys.version_info[0] == 3

try:
    from itertools import izip
except ImportError:
    izip = zip

try:
    # Python 2
    buffer = buffer
    memoryview = buffer
except NameError:
    # Python 3 has no `buffer`; only `memoryview`
    def buffer(obj, offset, size=None):
        if size is None:
            return memoryview(obj)[offset:]
        else:
            return memoryview(obj[offset:offset+size])

    memoryview = memoryview

if PY3:
    MAXSIZE = sys.maxsize
else:
    # It's possible to have sizeof(long) != sizeof(Py_ssize_t).
    class X(object):
        def __len__(self):
            return 1 << 31
    try:
        len(X())
    except OverflowError:
        # 32-bit
        MAXSIZE = int((1 << 31) - 1)
    else:
        # 64-bit
        MAXSIZE = int((1 << 63) - 1)
    del X

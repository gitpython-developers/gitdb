import sys

PY3 = sys.version_info[0] == 3

try:
    from itertools import izip
    xrange = xrange
except ImportError:
    izip = zip
    xrange = range

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

try:
    MAXSIZE = sys.maxint
except AttributeError:
    MAXSIZE = sys.maxsize

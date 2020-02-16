import sys

PY3 = sys.version_info[0] == 3

try:
    # Python 2
    memoryview = buffer
    # Assume no memory view ...
    def to_bytes(i):
        return i
except NameError:
    # Python 3 has no `buffer`; only `memoryview`
    # smmap can return memory view objects, which can't be compared as buffers/bytes can ... 
    def to_bytes(i):
        if isinstance(i, memoryview):
            return i.tobytes()
        return i

    memoryview = memoryview

try:
    MAXSIZE = sys.maxint
except AttributeError:
    MAXSIZE = sys.maxsize

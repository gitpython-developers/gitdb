import sys

PY3 = sys.version_info[0] == 3

try:
    # Python 2
    def to_bytes(i):
        return i
except NameError:
    # smmap can return memory view objects, which can't be compared as buffers/bytes can ... 
    def to_bytes(i):
        if isinstance(i, memoryview):
            return i.tobytes()
        return i

try:
    MAXSIZE = sys.maxint
except AttributeError:
    MAXSIZE = sys.maxsize

import sys


PY3 = sys.version_info[0] >= 3

try:
    from itertools import izip
    xrange = xrange         # @UndefinedVariable
except ImportError:
    # py3
    izip = zip
    xrange = range
# end handle python version

try:
    # Python 2
    buffer = buffer         # @UndefinedVariable
    memoryview = buffer     # @ReservedAssignment

    # Assume no memory view ...
    def to_bytes(i):
        return i
except NameError:
    # Python 3 has no `buffer`; only `memoryview`
    # However, it's faster to just slice the object directly, maybe it keeps a view internally
    def buffer(obj, offset, size=None):
        if size is None:
            # return memoryview(obj)[offset:]
            return obj[offset:]
        else:
            # return memoryview(obj)[offset:offset+size]
            return obj[offset:offset + size]
    # end buffer reimplementation
    # smmap can return memory view objects, which can't be compared as buffers/bytes can ...

    def to_bytes(i):
        if isinstance(i, memoryview):
            return i.tobytes()
            ## NOTE: `memoryview` leak resources with memmaps & delayed destructors
            #  (i.e. PY3/Windows)
            i.release()
        return i

    memoryview = memoryview     # @ReservedAssignment

try:
    MAXSIZE = sys.maxint        # @UndefinedVariable
except AttributeError:
    MAXSIZE = sys.maxsize

try:
    from contextlib import ExitStack
except ImportError:
    from contextlib2 import ExitStack   # @UnusedImport

try:
    from struct import unpack_from      # @UnusedImport
except ImportError:
    from struct import unpack, calcsize
    __calcsize_cache = dict()

    def unpack_from(fmt, data, offset=0):
        try:
            size = __calcsize_cache[fmt]
        except KeyError:
            size = calcsize(fmt)
            __calcsize_cache[fmt] = size
        # END exception handling
        return unpack(fmt, data[offset: offset + size])
    # END own unpack_from implementation

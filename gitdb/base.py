# Copyright (C) 2010, 2011 Sebastian Thiel (byronimo@gmail.com) and contributors
#
# This module is part of GitDB and is released under
# the New BSD License: http://www.opensource.org/licenses/bsd-license.php
"""Module with basic data structures - they are designed to be lightweight and fast"""
from gitdb.util import bin_to_hex, suppress
from collections import namedtuple


from gitdb.fun import (
    type_id_to_type_map,
    type_to_type_id_map
)

__all__ = ('OInfo', 'OPackInfo', 'ODeltaPackInfo',
           'OStream', 'OPackStream', 'ODeltaPackStream',
           'IStream', 'InvalidOInfo', 'InvalidOStream')

#{ ODB Bases


class OInfo(namedtuple('OInfo', 'binsha, type, size')):

    """Carries information about an object in an ODB, providing information
    about the binary sha of the object, the type_string as well as the uncompressed size
    in bytes.

    It can be accessed using tuple notation and using attribute access notation::

        assert dbi[0] == dbi.binsha
        assert dbi[1] == dbi.type
        assert dbi[2] == dbi.size

    The type is designed to be as lightweight as possible."""
    __slots__ = ()

    @property
    def hexsha(self):
        """:return: our sha, hex encoded, 40 bytes"""
        return bin_to_hex(self.binsha)

    @property
    def type_id(self):
        return type_to_type_id_map[self.type]


class OPackInfo(namedtuple('OPackInfo', 'pack_offset, type_id, size')):

    """As OInfo, but provides a type_id property to retrieve the numerical type id, and
    does not include a sha.

    Additionally, the pack_offset is the absolute offset into the packfile at which
    all object information is located. The data_offset property points to the absolute
    location in the pack at which that actual data stream can be found."""
    __slots__ = ()

    @property
    def type(self):
        return type_id_to_type_map[self.type_id]


class ODeltaPackInfo(namedtuple('ODeltaPackInfo', 'pack_offset, type_id, size, delta_info')):

    """Adds delta specific information,
    Either the 20 byte sha which points to some object in the database,
    or the negative offset from the pack_offset, so that pack_offset - delta_info yields
    the pack offset of the base object"""
    __slots__ = ()

    @property
    def type(self):
        return type_id_to_type_map[self.type_id]


class OStream(namedtuple('OStream', 'binsha type size stream')):
    """Base for object streams retrieved from the database, providing additional
    information about the stream.
    Generally, ODB streams are read-only as objects are immutable

    .. Note:
       Is NOTE a :class:`OInfo` instance; for the effort required, see:
       see http://stackoverflow.com/questions/20794182/how-to-make-a-file-like-class-work-with-isinstancecls-io-iobase

    """
    __slots__ = ()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        with suppress(Exception):
            self.close()

    def close(self):
        self.stream.close()

    def read(self, size=-1):
        return self.stream.read(size)

    @property
    def hexsha(self):
        """:return: our sha, hex encoded, 40 bytes"""
        return bin_to_hex(self.binsha)

    @property
    def type_id(self):
        return type_to_type_id_map[self.type]


class ODeltaStream(OStream):
    @property
    def size(self):
        return self[3].size


class OPackStream(namedtuple('OPackStream', 'pack_offset, type_id, size, stream')):

    """Next to pack object information, a stream outputting an undeltified base object
    is provided"""
    __slots__ = ()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        with suppress(Exception):
            self.close()

    def close(self):
        self.stream.close()

    def read(self, size=-1):
        return self.stream.read(size)

    @property
    def type(self):
        return type_id_to_type_map[self.type_id]


class ODeltaPackStream(namedtuple('ODeltaPackStream', 'pack_offset, type_id, size, delta_info stream')):

    """Provides a stream outputting the uncompressed offset delta information"""
    __slots__ = ()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        with suppress(Exception):
            self.close()

    def close(self):
        self.stream.close()

    def read(self, size=-1):
        return self.stream.read(size)

    @property
    def type(self):
        return type_id_to_type_map[self.type_id]


class IStream(list):

    """Represents an input content stream to be fed into the ODB. It is mutable to allow
    the ODB to record information about the operations outcome right in this instance.

    It provides interfaces for the OStream and a StreamReader to allow the instance
    to blend in without prior conversion.

    The only method your content stream must support is 'read'"""
    __slots__ = ()

    def __new__(cls, type, size, stream, sha=None):
        return list.__new__(cls, (sha, type, size, stream, None))

    def __init__(self, type, size, stream, sha=None):
        list.__init__(self, (sha, type, size, stream, None))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        with suppress(Exception):
            self.close()

    def close(self):
        self._stream().close()

    #{ Interface
    @property
    def hexsha(self):
        """:return: our sha, hex encoded, 40 bytes"""
        return bin_to_hex(self[0])

    def _error(self):
        """:return: the error that occurred when processing the stream, or None"""
        return self[4]

    def _set_error(self, exc):
        """Set this input stream to the given exc, may be None to reset the error"""
        self[4] = exc

    error = property(_error, _set_error)

    #} END interface

    #{ Stream Reader Interface

    def read(self, size=-1):
        """Implements a simple stream reader interface, passing the read call on
            to our internal stream"""
        return self._stream().read(size)

    #} END stream reader interface

    #{  interface

    def _set_binsha(self, binsha):
        self[0] = binsha

    def _binsha(self):
        return self[0]

    binsha = property(_binsha, _set_binsha)

    def _type(self):
        return self[1]

    def _set_type(self, type):
        self[1] = type

    type = property(_type, _set_type)

    def _size(self):
        return self[2]

    def _set_size(self, size):
        self[2] = size

    size = property(_size, _set_size)

    def _stream(self):
        return self[3]

    def _set_stream(self, stream):
        self[3] = stream

    stream = property(_stream, _set_stream)

    #} END odb info interface


class InvalidOInfo(tuple):

    """Carries information about a sha identifying an object which is invalid in
    the queried database. The exception attribute provides more information about
    the cause of the issue"""
    __slots__ = ()

    def __new__(cls, sha, exc):
        return tuple.__new__(cls, (sha, exc))

    def __init__(self, sha, exc):
        tuple.__init__(self, (sha, exc))

    @property
    def binsha(self):
        return self[0]

    @property
    def hexsha(self):
        return bin_to_hex(self[0])

    @property
    def error(self):
        """:return: exception instance explaining the failure"""
        return self[1]


class InvalidOStream(InvalidOInfo):

    """Carries information about an invalid ODB stream"""
    __slots__ = ()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def close(self):
        pass

#} END ODB Bases

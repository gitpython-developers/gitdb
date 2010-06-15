"""Contains PackIndexFile and PackFile implementations"""
from util import (
					LockedFD,
					LazyMixin,
					file_contents_ro, 
					unpack_from
					)

from fun import (
					pack_object_header_info,
					OFS_DELTA, 
					REF_DELTA
				)

from base import (
						OPackInfo,
						OPackStream,
						ODeltaPackInfo,
						ODeltaPackStream,
					)
from stream import (
						DecompressMemMapReader,
					)

from struct import (
						pack,
					)

__all__ = ('PackIndexFile', 'PackFile')


	
#{ Utilities 

def pack_object_at(data, as_stream):
	"""
	:return: info or stream object of the correct type according to the type 
		of the object, REF_DELTAS will not be resolved in case a stream is desired.
		The resulting ODeltaPackStream will have None instead of a stream. 
	:param data: random accessable data at which the header of an object can be read
	:param as_stream: if True, a stream object will be returned that can read 
		the data, otherwise you receive an info object only
	:note: a bit redundant, but it needs to be as fast as possible !"""
	type_id, uncomp_size, data_offset = pack_object_header_info(data)
	
	if type_id == OFS_DELTA:
		i = 0
		delta_offset = 0
		s = 7
		while c & 0x80:
			c = ord(data[i])
			i += 1
			delta_offset += (c & 0x7f) << s
			s += 7
		# END character loop
		if as_stream:
			stream = DecompressMemMapReader(buffer(data, i), False)
			return ODeltaPackStream(type_id, uncomp_size, delta_offset, stream)
		else:
			return ODeltaPackInfo(type_id, uncomp_size, delta_offset)
		# END handle stream
	elif type_id == REF_DELTA:
		ref_sha = data[:20]
		if as_stream:
			stream = DecompressMemMapReader(buffer(data, 20), False)
			return ODeltaPackStream(type_id, uncomp_size, ref_sha, stream)
		else:
			return ODeltaPackInfo(type_id, uncomp_size, ref_sha)
		# END handle stream
	else:
		# assume its a base object
		if as_stream:
			# if no size is given, it will read the header on first access
			stream = DecompressMemMapReader(buffer(data, data_offset), False)
			return OPackStream(type_id, uncomp_size, stream)
		else:
			return OPackInfo(type_id, uncomp_size)
		# END handle as_stream
	# END handle type id
	

#} END utilities



class PackIndexFile(LazyMixin):
	"""A pack index provides offsets into the corresponding pack, allowing to find
	locations for offsets faster."""
	
	# Dont use slots as we dynamically bind functions for each version, need a dict for this
	# The slots you see here are just to keep track of our instance variables
	# __slots__ = ('_indexpath', '_fanout_table', '_data', '_version', 
	#				'_sha_list_offset', '_crc_list_offset', '_pack_offset', '_pack_64_offset')

	# used in v2 indices
	_sha_list_offset = 8 + 1024

	def __init__(self, indexpath):
		super(PackIndexFile, self).__init__()
		self._indexpath = indexpath
	
	def _set_cache_(self, attr):
		if attr == "_packfile_checksum":
			self._packfile_checksum = self._data[-40:-20]
		elif attr == "_packfile_checksum":
			self._packfile_checksum = self._data[-20:]
		elif attr == "_data":
			lfd = LockedFD(self._indexpath)
			fd = lfd.open()
			self._data = file_contents_ro(fd)
			lfd.rollback()
		else:
			# now its time to initialize everything - if we are here, someone wants
			# to access the fanout table or related properties
			
			# CHECK VERSION
			self._version = (self._data[:4] == '\377tOc' and 2) or 1
			if self._version == 2:
				version_id = unpack_from(">L", self._data, 4)[0] 
				assert version_id == self._version, "Unsupported index version: %i" % version_id
			# END assert version
			
			# SETUP FUNCTIONS
			# setup our functions according to the actual version
			for fname in ('entry', 'offset', 'sha', 'crc'):
				setattr(self, fname, getattr(self, "_%s_v%i" % (fname, self._version)))
			# END for each function to initialize
			
			
			# INITIALIZE DATA
			# byte offset is 8 if version is 2, 0 otherwise
			self._initialize()
		# END handle attributes
		

	#{ Access V1
	
	def _entry_v1(self, i):
		""":return: tuple(offset, binsha, 0)"""
		return unpack_from(">L20s", self._data, 1024 + i*24) + (0, ) 
	
	def _offset_v1(self, i):
		"""see ``_offset_v2``"""
		return unpack_from(">L", self._data, 1024 + i*24)[0]
	
	def _sha_v1(self, i):
		"""see ``_sha_v2``"""
		base = 1024 + (i*24)+4
		return self._data[base:base+20]
		
	def _crc_v1(self, i):
		"""unsupported"""
		return 0
		
	#} END access V1
	
	#{ Access V2
	def _entry_v2(self, i):
		""":return: tuple(offset, binsha, crc)"""
		return (self._offset_v2(i), self._sha_v2(i), self._crc_v2(i))
	
	def _offset_v2(self, i):
		""":return: 32 or 64 byte offset into pack files. 64 byte offsets will only 
			be returned if the pack is larger than 4 GiB, or 2^32"""
		offset = unpack_from(">L", self._data, self._pack_offset + i * 4)[0]
		
		# if the high-bit is set, this indicates that we have to lookup the offset
		# in the 64 bit region of the file. The current offset ( lower 31 bits )
		# are the index into it
		if offset & 0x80000000:
			offset = unpack_from(">Q", self._data, self._pack_64_offset + (self.offset & ~0x80000000) * 8)[0]
		# END handle 64 bit offset
		
		return offset
		
	def _sha_v2(self, i):
		""":return: sha at the given index of this file index instance"""
		base = self._sha_list_offset + i * 20
		return self._data[base:base+20]
		
	def _crc_v2(self, i):
		""":return: 4 bytes crc for the object at index i"""
		return unpack_from(">L", self._data, self._crc_list_offset + i * 4)[0] 
		
	#} END access V2
	
	#{ Initialization
	
	def _initialize(self):
		"""initialize base data"""
		self._fanout_table = self._read_fanout((self._version == 2) * 8)
		
		if self._version == 2:
			self._crc_list_offset = self._sha_list_offset + self.size() * 20
			self._pack_offset = self._crc_list_offset + self.size() * 4
			self._pack_64_offset = self._pack_offset + self.size() * 4
		# END setup base
		
	def _read_fanout(self, byte_offset):
		"""Generate a fanout table from our data"""
		d = self._data
		out = list()
		append = out.append
		for i in range(256):
			append(unpack_from('>L', d, byte_offset + i*4)[0])
		# END for each entry
		return out
	
	#} END initialization
		
	#{ Properties
	def version(self):
		return self._version
		
	def size(self):
		""":return: amount of objects referred to by this index"""
		return self._fanout_table[255]
		
	def packfile_checksum(self):
		""":return: 20 byte sha representing the sha1 hash of the pack file"""
		return self._data[-40:-20]
		
	def indexfile_checksum(self):
		""":return: 20 byte sha representing the sha1 hash of this index file"""
		return self._data[-20:]
		
	def sha_to_index(self, sha):
		"""
		:return: index usable with the ``offset`` or ``entry`` method, or None
			if the sha was not found in this pack index
		:param sha: 20 byte sha to lookup"""
		first_byte = ord(sha[0])
		lo = 0					# lower index, the left bound of the bisection
		if first_byte != 0:
			lo = self._fanout_table[first_byte-1]
		hi = self._fanout_table[first_byte]		# the upper, right bound of the bisection
		
		# bisect until we have the sha
		while lo < hi:
			mid = (lo + hi) / 2
			c = cmp(sha, self.sha(mid))
			if c < 0:
				hi = mid
			elif not c:
				return mid
			else:
				lo = mid
			# END handle midpoint
		# END bisect
		return None
	
	#} END properties
	
	
class PackFile(LazyMixin):
	"""A pack is a file written according to the Version 2 for git packs
	
	As we currently use memory maps, it could be assumed that the maximum size of
	packs therefor is 32 bit on 32 bit systems. On 64 bit systems, this should be 
	fine though.
	
	:note: at some point, this might be implemented using streams as well, or 
		streams are an alternate path in the case memory maps cannot be created
		for some reason - one clearly doesn't want to read 10GB at once in that 
		case"""
	
	__slots__ = ('_packpath', '_data', '_size', '_version')
	
	# offset into our data at which the first object starts
	_first_object_offset = 3*4 + 8
	
	def __init__(self, packpath):
		self._packpath = packpath
		
	def _set_cache_(self, attr):
		if attr == '_data':
			ldb = LockedFD(self._packpath)
			fd = ldb.open()
			self._data = file_contents_ro(fd)
			ldb.rollback()
			# TODO: figure out whether we should better keep the lock, or maybe
			# add a .keep file instead ?
		else:
			# read the header information
			type_id, self._version, self._size = unpack_from(">4sLL", self._data, 0)
			assert type_id == "PACK", "Pack file format is invalid: %r" % type_id
			assert self._version in (2, 3), "Cannot handle pack format version %i" % self._version
		# END handle header
		
	def _iter_objects(self, start_offset, as_stream):
		"""Handle the actual iteration of objects within this pack"""
		data = self._data
		size = len(data)
		cur_offset = start_offset or self._first_object_offset
		
		while cur_offset < size:
			ostream = pack_object_at(buffer(data, cur_offset), True)
			# TODO: Decompressor needs to track the size of bytes actually decompressed
			
		# END until we have read everything
		
	#{ Interface
	
	def size(self):
		""":return: The amount of objects stored in this pack""" 
		return self._size
		
	def version(self):
		""":return: the version of this pack"""
		return self._version
		
	def checksum(self):
		""":return: 20 byte sha1 hash on all object sha's contained in this file"""
		return self._data[-20:]
		
	#} END interface
	
	#{ Read-Database like Interface
	
	def info(self, offset):
		"""Retrieve information about the object at the given file-absolute offset
		:param offset: byte offset
		:return: OPackInfo instance, the actual type differs depending on the type_id attribute"""
		raise NotImplementedError()
		
	def stream(self, offset):
		"""Retrieve an object at the given file-relative offset as stream along with its information
		:param offset: byte offset
		:return: OPackStream instance, the actual type differs depending on the type_id attribute"""
		raise NotImplementedError()
		
	#} END Read-Database like Interface
	
	
class PackFileEntity(object):
	"""Combines the PackIndexFile and the PackFile into one, allowing the 
	actual objects to be resolved and iterated"""
	
	__slots__ = ('_index', '_pack')
	
	IndexFileCls = PackIndexFile
	PackFileCls = PackFile
	
	def __init__(self, basename):
		self._index = self.IndexFileCls("%s.idx" % basename)			# PackIndexFile instance
		self._pack = self.PackFileCls("%s.pack" % basename)			# corresponding PackFile instance
	
	
	def _iter_objects(self, as_stream):
		raise NotImplementedError
	
	#{ Read-Database like Interface
	
	def info(self, sha):
		"""Retrieve information about the object identified by the given sha
		:param sha: 20 byte sha1
		:return: OInfo instance"""
		raise NotImplementedError()
		
	def stream(self, sha):
		"""Retrieve an object stream along with its information as identified by the given sha
		:param sha: 20 byte sha1
		:return: OStream instance"""
		raise NotImplementedError()
		
	#} END Read-Database like Interface
	
	#{ Interface 
	
	def info_iter(self):
		""":return: Iterator over all objects in this pack. The iterator yields
			OInfo instances"""
		return self._iter_objects(as_stream=False)
		
	def stream_iter(self):
		""":return: iterator over all objects in this pack. The iterator yields
		OStream instances"""
		return self._iter_objects(as_stream=True)
		
	#} Interface

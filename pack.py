"""Contains PackIndex and PackFile implementations"""
from util import (
					LockedFD,
					LazyMixin,
					file_contents_ro, 
					unpack_from
					)

from struct import (
						pack,
					)

__all__ = ('PackIndex', 'Pack')


class PackIndex(LazyMixin):
	"""A pack index provides offsets into the corresponding pack, allowing to find
	locations for offsets faster."""
	
	# Dont use slots as we dynamically bind functions for each version, need a dict for this
	# The slots you see here are just to keep track of our instance variables
	# __slots__ = ('_indexpath', '_fanout_table', '_data', '_version', 
	#				'_sha_list_offset', '_crc_list_offset', '_pack_offset', '_pack_64_offset')

	# used in v2 indices
	_sha_list_offset = 8 + 1024

	def __init__(self, indexpath):
		super(PackIndex, self).__init__()
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
			self._crc_list_offset = self._sha_list_offset + self.size * 20
			self._pack_offset = self._crc_list_offset + self.size * 4
			self._pack_64_offset = self._pack_offset + self.size * 4
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
	@property
	def version(self):
		return self._version
		
	@property
	def size(self):
		""":return: amount of objects referred to by this index"""
		return self._fanout_table[255]
		
	@property
	def packfile_checksum(self):
		""":return: 20 byte sha representing the sha1 hash of the pack file"""
		return self._data[-40:-20]
		
	@property
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
	
	
class Pack(LazyMixin):
	"""A pack is a file written according to the Version 2 for git packs"""
	

"""Contains PackIndexFile and PackFile implementations"""
from gitdb.exc import (
						BadObject, 
						)
from util import (
					LockedFD,
					LazyMixin,
					file_contents_ro, 
					unpack_from
					)

from fun import (
					pack_object_header_info,
					stream_copy, 
					chunk_size,
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
						DeltaApplyReader,
						NullStream,
					)

from struct import (
						pack,
					)

__all__ = ('PackIndexFile', 'PackFile')

_delta_types = (OFS_DELTA, REF_DELTA) 

	
#{ Utilities 

def pack_object_at(data, offset, as_stream):
	"""
	:return: PackInfo|PackStream
		an object of the correct type according to the type_id  of the object.
		If as_stream is True, the object will contain a stream, allowing  the
		data to be read decompressed.
	:param data: random accessable data containing all required information
	:parma offset: offset in to the data at which the object information is located
	:param as_stream: if True, a stream object will be returned that can read 
		the data, otherwise you receive an info object only"""
	data = buffer(data, offset)
	type_id, uncomp_size, data_rela_offset = pack_object_header_info(data)
	total_rela_offset = None				# set later, actual offset until data stream begins
	delta_info = None
	
	# OFFSET DELTA
	if type_id == OFS_DELTA:
		i = data_rela_offset
		c = ord(data[i])
		i += 1
		delta_offset = c & 0x7f
		while c & 0x80:
			c = ord(data[i])
			i += 1
			delta_offset += 1
			delta_offset = (delta_offset << 7) + (c & 0x7f)
		# END character loop
		delta_info = delta_offset
		total_rela_offset = i
	# REF DELTA
	elif type_id == REF_DELTA:
		total_rela_offset = data_rela_offset+20
		ref_sha = data[data_rela_offset:total_rela_offset]
		delta_info = ref_sha
	# BASE OBJECT
	else:
		# assume its a base object
		total_rela_offset = data_rela_offset
	# END handle type id
	
	abs_data_offset = offset + total_rela_offset
	if as_stream:
		stream = DecompressMemMapReader(buffer(data, total_rela_offset), False, uncomp_size)
		if delta_info is None:
			return OPackStream(offset, abs_data_offset, type_id, uncomp_size, stream)
		else:
			return ODeltaPackStream(offset, abs_data_offset, type_id, uncomp_size, delta_info, stream)
	else:
		if delta_info is None:
			return OPackInfo(offset, abs_data_offset, type_id, uncomp_size)
		else:
			return ODeltaPackInfo(offset, abs_data_offset, type_id, uncomp_size, delta_info)
		# END handle info
	# END handle stream
		
			

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
	_first_object_offset = 3*4		# header bytes
	_footer_size = 20				# final sha
	
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
		
	def _iter_objects(self, start_offset, as_stream=True):
		"""Handle the actual iteration of objects within this pack"""
		data = self._data
		content_size = len(data) - self._footer_size
		cur_offset = start_offset or self._first_object_offset
		
		null = NullStream()
		while cur_offset < content_size:
			ostream = pack_object_at(data, cur_offset, True)
			# scrub the stream to the end - this decompresses the object, but yields
			# the amount of compressed bytes we need to get to the next offset
				
			stream_copy(ostream.read, null.write, ostream.size, chunk_size)
			cur_offset += (ostream.data_offset - ostream.pack_offset) + ostream.stream.compressed_bytes_read()
			
			
			# if a stream is requested, reset it beforehand
			# Otherwise return the Stream object directly, its derived from the 
			# info object
			if as_stream:
				ostream.stream.seek(0)
			yield ostream
		# END until we have read everything
		
	#{ Pack Information
	
	def size(self):
		""":return: The amount of objects stored in this pack""" 
		return self._size
		
	def version(self):
		""":return: the version of this pack"""
		return self._version
		
	def checksum(self):
		""":return: 20 byte sha1 hash on all object sha's contained in this file"""
		return self._data[-20:]
		
	#} END pack information
	
	#{ Pack Specific
	
	def collect_streams(self, offset):
		"""
		:return: list of pack streams which are required to build the object
			at the given offset. The first entry of the list is the object at offset, 
			the last one is either a full object, or a REF_Delta stream. The latter
			type needs its reference object to be locked up in an ODB to form a valid
			delta chain.
		:param offset: specifies the first byte of the object within this pack"""
		out = list()
		while True:
			ostream = pack_object_at(self._data, offset, True)
			out.append(ostream)
			if ostream.type_id == OFS_DELTA:
				offset = ostream.pack_offset - ostream.delta_info
			else:
				# the only thing we can lookup are OFFSET deltas. Everything
				# else is either an object, or a ref delta, in the latter 
				# case someone else has to find it
				break
			# END handle type
		# END while chaining streams
		return out
	
	def to_delta_stream(self, stream_list):
		"""Convert the given list of streams into a stream which resolves deltas
		(if availble) when reading from it.
		:param stream_list: one or more stream objects. If the first stream is a Delta, 
			there must be at least two streams in the list. The list's last stream
			must be a non-delta stream.
		:return: Non-Delta OPackStream object whose stream can be used to obtain 
			the decompressed resolved data
		:raise ValueError: if the stream list cannot be handled due to a missing base object"""
		if len(stream_list) == 1:
			if stream_list[0].type_id in _delta_types:
				raise ValueError("Cannot resolve deltas if only one stream is given", stream_list[0].type)
			# its an object, no need to resolve anything
			return stream_list[0]
		# END single object special handling
		
		if stream_list[-1].type_id in _delta_types:
			raise ValueError("Cannot resolve deltas if there is no base object stream, last one was type: %s" % stream_list[-1].type)
		# END check stream
		
		# just create the respective stream wrapper
		return DeltaApplyReader(stream_list)
		
	
	#} END pack specific
	
	#{ Read-Database like Interface
	
	def info(self, offset):
		"""Retrieve information about the object at the given file-absolute offset
		:param offset: byte offset
		:return: OPackInfo instance, the actual type differs depending on the type_id attribute"""
		return pack_object_at(self._data, offset or self._first_object_offset, False)
		
	def stream(self, offset):
		"""Retrieve an object at the given file-relative offset as stream along with its information
		:param offset: byte offset
		:return: OPackStream instance, the actual type differs depending on the type_id attribute"""
		return pack_object_at(self._data, offset or self._first_object_offset, True)
		
	def stream_iter(self, start_offset=0):
		""":return: iterator yielding OPackStream compatible instances, allowing 
		to access the data in the pack directly.
		:param start_offset: offset to the first object to iterate. If 0, iteration 
			starts at the very first object in the pack.
		:note: Iterating a pack directly is costly as the datastream has to be decompressed
			to determine the bounds between the objects"""
		return self._iter_objects(start_offset, as_stream=True)
		
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
		:raise BadObject:
		:return: OInfo instance"""
		raise NotImplementedError()
		
	def stream(self, sha):
		"""Retrieve an object stream along with its information as identified by the given sha
		:param sha: 20 byte sha1
		:raise BadObject: 
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

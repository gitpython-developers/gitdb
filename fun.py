"""Contains basic c-functions which usually contain performance critical code
Keeping this code separate from the beginning makes it easier to out-source
it into c later, if required"""

from exc import (
	BadObjectType
	)

from util import zlib
decompressobj = zlib.decompressobj

import mmap
from itertools import islice, izip

# INVARIANTS
OFS_DELTA = 6
REF_DELTA = 7
delta_types = (OFS_DELTA, REF_DELTA)

type_id_to_type_map = 	{
							0 : "",				# EXT 1
							1 : "commit",
							2 : "tree",
							3 : "blob",
							4 : "tag",
							5 : "",				# EXT 2
							OFS_DELTA : "OFS_DELTA", 	# OFFSET DELTA
							REF_DELTA : "REF_DELTA"		# REFERENCE DELTA
						}

type_to_type_id_map = dict(
							commit=1, 
							tree=2,
							blob=3,
							tag=4,
							OFS_DELTA=OFS_DELTA,
							REF_DELTA=REF_DELTA
						)

# used when dealing with larger streams
chunk_size = 1000*mmap.PAGESIZE

__all__ = ('is_loose_object', 'loose_object_header_info', 'msb_size', 'pack_object_header_info', 
			'write_object', 'loose_object_header', 'stream_copy', 'apply_delta_data', 
			'is_equal_canonical_sha', 'reverse_merge_deltas',
			'merge_deltas', 'DeltaChunkList')


#{ Structures

def _set_delta_rbound(d, size):
	"""Truncate the given delta to the given size
	:param size: size relative to our target offset, may not be 0, must be smaller or equal
		to our size"""
	if size == 0:
		raise ValueError("size to truncate to must not be 0")
	if d.ts == size:
		return
	if size > d.ts:
		raise ValueError("Cannot truncate delta 'larger'")
		
	d.ts = size
	
	# NOTE: data is truncated automatically when applying the delta
	# MUST NOT DO THIS HERE, see _split_delta
			
def _move_delta_lbound(d, bytes):
	"""Move the delta by the given amount of bytes, reducing its size so that its
	right bound stays static
	:param bytes: amount of bytes to move, must be smaller than delta size"""
	if bytes >= d.ts:
		raise ValueError("Cannot move offset that much")
		
	d.to += bytes
	d.so += bytes
	d.ts -= bytes
	if d.data:
		d.data = d.data[bytes:]
	# END handle data
	

class DeltaChunk(object):
	"""Represents a piece of a delta, it can either add new data, or copy existing
	one from a source buffer"""
	__slots__ = (
					'to',		# start offset in the target buffer in bytes 
					'ts',		# size of this chunk in the target buffer in bytes
					'so',		# start offset in the source buffer in bytes or None
					'data'		# chunk of bytes to be added to the target buffer,
								# DeltaChunkList to use as base, or None
				)
	
	def __init__(self, to, ts, so, data):
		self.to = to
		self.ts = ts
		self.so = sos
		self.data = data

	def __repr__(self):
		return "DeltaChunk(%i, %i, %s, %s)" % (self.to, self.ts, self.so, self.data or "")
	
	#{ Interface
		
	def rbound(self):
		return self.to + self.ts
		
	def has_data(self):
		""":return: True if the instance has data to add to the target stream"""
		return self.data is None or not isinstance(self.data, DeltaChunkList)
		
	def apply(self, source, write):
		"""Apply own data to the target buffer
		:param source: buffer providing source bytes for copy operations
		:param write: write method to call with data to write"""
		if self.has_data():
			# COPY DATA FROM SOURCE
			assert len(source) - self.so - self.ts > 0
			write(buffer(source, self.so, self.ts))
		else:
			# APPEND DATA
			# whats faster: if + 4 function calls or just a write with a slice ?
			if self.ts < len(self.data):
				write(self.data[:self.ts])
			else:
				write(self.data)
			# END handle truncation
		# END handle chunk mode
		
	#} END interface

def _closest_index(dcl, absofs):
	""":return: index at which the given absofs should be inserted. The index points
	to the DeltaChunk with a target buffer absofs that equals or is greater than
	absofs. 
	:note: global method for performance only, it belongs to DeltaChunkList"""
	# TODO: binary search !!
	for i,d in enumerate(dcl):
		if absofs < d.to:
			return i-1
		elif absofs == d.to:
			return i
	# END for each delta absofs
	return len(dcl)-1
	
def _split_delta(dcl, d, di, relofs, insert_offset=0):
	"""Split the delta at di into two deltas, adjusting their sizes, offsets and data 
	accordingly and adding the new part to the dcl
	:param relofs: relative offset at which to split the delta
	:param d: delta chunk to split
	:param di: index of d in dcl
	:param insert_offset: offset for the new split id
	:return: newly created DeltaChunk
	:note: belongs to DeltaChunkList"""
	if relofs > d.ts:
		raise ValueError("Cannot split behinds a chunks rbound")
		
	osize = d.ts - relofs
	_set_delta_rbound(d, relofs)
		
	# insert new one
	drb = d.rbound()
	
	nd = DeltaChunk(  	drb, 
						osize, 
						d.so + osize,
						(d.data and d.data[osize:]) or None	)
	
	self.insert(di+1+insert_offset, nd)
	return nd
	
def _handle_merge(ld, rd):
	"""Optimize the layout of the lhs delta and the rhs delta
	TODO: Once the default implementation is working""" 
	if d.has_data():
		if od.data:
			# OVERWRITE DATA
			pass
		else:
			# MERGE SOURCE AREA
			pass
		# END overwrite data
	else:
		if od.data:
			# MERGE DATA WITH DATA
			# overwrite the data at the respective spot
			pass
		else:
			# INSERT DATA INTO COPY AREA
			pass
		# END combine or insert data
	# END handle chunk mode
	
def _merge_delta(dcl, dc):
	"""Merge the given DeltaChunk instance into the dcl
	:param d: the DeltaChunk to merge"""
	if len(dcl) == 0:
		dcl.append(dc)
		return
	# END early return on empty list
	
	cdi = _closest_index(dcl, dc.to)		# current delta index
	cd = dcl[cdi]						# current delta
	
	# either we go at his spot, or after
	# cdi either moves one up, or stays
	#print "insert at %i" % (cdi + (dc.to > cd.to))
	#print cd, dc
	dcl.insert(cdi + (dc.to > cd.to), dc)
	cdi += dc.to == cd.to
	
	while True:
		# are we larger than the current block
		if dc.to < cd.to:
			if dc.rbound() >= cd.rbound():
				# xxx|xxx|x
				# remove the current item completely
				dcl.pop(cdi)
				cdi -= 1
			elif dc.rbound() > cd.to:
				# MOVE ITS LBOUND
				# xxx|x--|
				_move_delta_lbound(cd, dc.rbound() - cd.to)
				break
			else:
				# xx.|---|
				# WE DON'T OVERLAP IT
				# this can actually happen, once multiple streams are merged
				break
			# END rbound overlap handling
		# END lbound overlap handling
		else:
			if dc.to >= cd.rbound():
				#|---|xx
				break
			# END 
			
			if dc.rbound() >= cd.rbound():
				if dc.to == cd.to:
					#|xxx|x
					# REMOVE CD
					dcl.pop(cdi)
					cdi -= 1
				else:
					# TRUNCATE CD
					#|-xx|
					_set_delta_rbound(cd, dc.to - cd.to)
				# END handle offset special case
			elif dc.to == cd.to:
				#|x--|
				# we shift it by our size
				_move_delta_lbound(cd, dc.ts)
			else:
				#|-x-|
				# SPLIT CD AND LBOUND MOVE ITS SECOND PART
				# insert offset is required to insert it after us
				nd = _split_delta(dcl, cd, cdi, 1)
				_move_delta_lbound(nd, dc.ts)
				break
			# END handle rbound overlap
		# END handle overlap
		
		cdi += 1
		if cdi < len(dcl):
			cd = dcl[cdi]
		else:
			break
		# END check for end of list
	# while our chunk is not completely done
	
	## DEBUG ## 
	dcl.check_integrity()
	
	

class DeltaChunkList(list):
	"""List with special functionality to deal with DeltaChunks"""
	
	def init(self, size):
		"""Intialize this instance with chunks defining to fill up size from a base
		buffer of equal size
		:return: self"""
		if len(self) != 0:
			return
		# pretend we have one huge delta chunk, which just copies everything
		# from source to destination
		maxint32 = 2**32
		for x in range(0, size, maxint32):
			self.append(DeltaChunk(x, maxint32, x, None))
		# END create copy chunks
		offset = x*maxint32
		remainder = size-offset
		if remainder:
			self.append(DeltaChunk(offset, remainder, offset, None))
		# END handle all done in loop
		
		return self
		
	def set_rbound(self, size):
		"""Chops the list at the given size, splitting and removing DeltaNodes 
		as required
		:return: self"""
		di = _closest_index(self, size)
		d = self[di]
		rsize = size - d.to
		if rsize:
			_set_delta_rbound(d, rsize)
		# END truncate last node if possible
		del(self[di+(rsize!=0):])
		
		## DEBUG ##
		self.check_integrity(size)
		
		return self
		
	def connect_with(self, bdlc):
		"""Connect this instance's delta chunks virtually with the given base.
		This means that all copy deltas will simply apply to the given region 
		of the given base. Afterwards, the base is optimized so that add-deltas
		will be truncated to the region actually used, or removed completely where
		adequate. This way, memory usage is reduced.
		:param bdlc: DeltaChunkList to serve as base"""
		raise NotImplementedError("todo")
		
	def apply(self, bbuf, write):
		"""Apply the chain's changes and write the final result using the passed
		write function.
		:param bbuf: base buffer containing the base of all deltas contained in this
			list. It will only be used if the chunk in question does not have a base
			chain.
		:param write: function taking a string of bytes to write to the output"""
		raise NotImplementedError("todo")
		
	def check_integrity(self, target_size=-1):
		"""Verify the list has non-overlapping chunks only, and the total size matches
		target_size
		:param target_size: if not -1, the total size of the chain must be target_size
		:raise AssertionError: if the size doen't match"""
		if target_size > -1:
			assert self[-1].rbound() == target_size
			assert reduce(lambda x,y: x+y, (d.ts for d in self), 0) == target_size
		# END target size verification
		
		if len(self) < 2:
			return
			
		# check data
		for dc in self:
			if dc.data:
				assert len(dc.data) >= dc.ts
		# END for each dc
			
		left = islice(self, 0, len(self)-1)
		right = iter(self)
		right.next()
		# this is very pythonic - we might have just use index based access here, 
		# but this could actually be faster
		for lft,rgt in izip(left, right):
			assert lft.rbound() == rgt.to
			assert lft.to + lft.ts == rgt.to
		# END for each pair
		
#} END structures

#{ Routines

def is_loose_object(m):
	"""
	:return: True the file contained in memory map m appears to be a loose object.
		Only the first two bytes are needed"""
	b0, b1 = map(ord, m[:2])
	word = (b0 << 8) + b1
	return b0 == 0x78 and (word % 31) == 0

def loose_object_header_info(m):
	"""
	:return: tuple(type_string, uncompressed_size_in_bytes) the type string of the 
		object as well as its uncompressed size in bytes.
	:param m: memory map from which to read the compressed object data"""
	decompress_size = 8192		# is used in cgit as well
	hdr = decompressobj().decompress(m, decompress_size)
	type_name, size = hdr[:hdr.find("\0")].split(" ")
	return type_name, int(size)
	
def pack_object_header_info(data):
	"""
	:return: tuple(type_id, uncompressed_size_in_bytes, byte_offset)
		The type_id should be interpreted according to the ``type_id_to_type_map`` map
		The byte-offset specifies the start of the actual zlib compressed datastream
	:param m: random-access memory, like a string or memory map"""
	c = ord(data[0])				# first byte
	i = 1							# next char to read
	type_id = (c >> 4) & 7			# numeric type
	size = c & 15					# starting size
	s = 4							# starting bit-shift size
	while c & 0x80:
		c = ord(data[i])
		i += 1
		size += (c & 0x7f) << s
		s += 7
	# END character loop
	
	try:
		return (type_id, size, i)
	except KeyError:
		# invalid object type - we could try to be smart now and decode part 
		# of the stream to get the info, problem is that we had trouble finding 
		# the exact start of the content stream
		raise BadObjectType(type_id)
	# END handle exceptions
	
def msb_size(data, offset=0):
	"""
	:return: tuple(read_bytes, size) read the msb size from the given random 
		access data starting at the given byte offset"""
	size = 0
	i = 0
	l = len(data)
	hit_msb = False
	while i < l:
		c = ord(data[i+offset])
		size |= (c & 0x7f) << i*7
		i += 1
		if not c & 0x80:
			hit_msb = True
			break
		# END check msb bit
	# END while in range
	if not hit_msb:
		raise AssertionError("Could not find terminating MSB byte in data stream")
	return i+offset, size 
	
def loose_object_header(type, size):
	"""
	:return: string representing the loose object header, which is immediately
		followed by the content stream of size 'size'"""
	return "%s %i\0" % (type, size)
		
def write_object(type, size, read, write, chunk_size=chunk_size):
	"""
	Write the object as identified by type, size and source_stream into the 
	target_stream
	
	:param type: type string of the object
	:param size: amount of bytes to write from source_stream
	:param read: read method of a stream providing the content data
	:param write: write method of the output stream
	:param close_target_stream: if True, the target stream will be closed when
		the routine exits, even if an error is thrown
	:return: The actual amount of bytes written to stream, which includes the header and a trailing newline"""
	tbw = 0												# total num bytes written
	
	# WRITE HEADER: type SP size NULL
	tbw += write(loose_object_header(type, size))
	tbw += stream_copy(read, write, size, chunk_size)
	
	return tbw

def stream_copy(read, write, size, chunk_size):
	"""
	Copy a stream up to size bytes using the provided read and write methods, 
	in chunks of chunk_size
	
	:note: its much like stream_copy utility, but operates just using methods"""
	dbw = 0												# num data bytes written
	
	# WRITE ALL DATA UP TO SIZE
	while True:
		cs = min(chunk_size, size-dbw)
		# NOTE: not all write methods return the amount of written bytes, like
		# mmap.write. Its bad, but we just deal with it ... perhaps its not 
		# even less efficient
		# data_len = write(read(cs))
		# dbw += data_len
		data = read(cs)
		data_len = len(data)
		dbw += data_len
		write(data)
		if data_len < cs or dbw == size:
			break
		# END check for stream end
	# END duplicate data
	return dbw
	
def reverse_merge_deltas(dcl, dstreams):
	"""Read the condensed delta chunk information from dstream and merge its information
	into a list of existing delta chunks
	:param dcl: see 3
	:param dstreams: iterable of delta stream objects. They must be ordered latest first, 
		hence the delta to be applied last comes first, then its ancestors
	:return: None"""
	raise NotImplementedError("This is left out up until we actually iterate the dstreams - they are prefetched right now")
	
def merge_deltas(dstreams):
	"""Read the condensed delta chunk information from dstream and merge its information
	into a list of existing delta chunks
	:param dstreams: iterable of delta stream objects. They must be ordered latest last, 
		hence the delta to be applied last comes last, its oldest ancestor first
	:return: DeltaChunkList, containing all operations to apply"""
	bdcl = None							# data chunk list for initial base
	dcl = DeltaChunkList()
	for dsi, ds in enumerate(dstreams):
		# print "Stream", dsi
		db = ds.read()
		delta_buf_size = ds.size
		
		# read header
		i, base_size = msb_size(db)
		i, target_size = msb_size(db, i)
		
		# interpret opcodes
		tbw = 0						# amount of target bytes written
		while i < delta_buf_size:
			c = ord(db[i])
			i += 1
			if c & 0x80:
				cp_off, cp_size = 0, 0
				if (c & 0x01):
					cp_off = ord(db[i])
					i += 1
				if (c & 0x02):
					cp_off |= (ord(db[i]) << 8)
					i += 1
				if (c & 0x04):
					cp_off |= (ord(db[i]) << 16)
					i += 1
				if (c & 0x08):
					cp_off |= (ord(db[i]) << 24)
					i += 1
				if (c & 0x10):
					cp_size = ord(db[i])
					i += 1
				if (c & 0x20):
					cp_size |= (ord(db[i]) << 8)
					i += 1
				if (c & 0x40):
					cp_size |= (ord(db[i]) << 16)
					i += 1
					
				if not cp_size: 
					cp_size = 0x10000
				
				rbound = cp_off + cp_size
				if (rbound < cp_size or
					rbound > base_size):
					break
				
				# _merge_delta(dcl, DeltaChunk(tbw, cp_size, cp_off, None))
				dcl.append(DeltaChunk(tbw, cp_size, cp_off, None))
				tbw += cp_size
			elif c:
				# TODO: Concatenate multiple deltachunks 
				# _merge_delta(dcl, DeltaChunk(tbw, c, 0, db[i:i+c]))
				dcl.append(DeltaChunk(tbw, c, 0, db[i:i+c]))
				i += c
				tbw += c
			else:
				raise ValueError("unexpected delta opcode 0")
			# END handle command byte
		# END while processing delta data
		
		# merge the lists !
		if base is not None:
			dcl.connect_with(base)
		# END handle merge
		
		# prepare next base
		base = dcl
		dcl = DeltaChunkList()
	# END for each delta stream
	
	# print dcl
	
	
def apply_delta_data(src_buf, src_buf_size, delta_buf, delta_buf_size, write):
	"""
	Apply data from a delta buffer using a source buffer to the target file
	
	:param src_buf: random access data from which the delta was created
	:param src_buf_size: size of the source buffer in bytes
	:param delta_buf_size: size fo the delta buffer in bytes
	:param delta_buf: random access delta data
	:param write: write method taking a chunk of bytes
	:note: transcribed to python from the similar routine in patch-delta.c"""
	i = 0
	db = delta_buf
	while i < delta_buf_size:
		c = ord(db[i])
		i += 1
		if c & 0x80:
			cp_off, cp_size = 0, 0
			if (c & 0x01):
				cp_off = ord(db[i])
				i += 1
			if (c & 0x02):
				cp_off |= (ord(db[i]) << 8)
				i += 1
			if (c & 0x04):
				cp_off |= (ord(db[i]) << 16)
				i += 1
			if (c & 0x08):
				cp_off |= (ord(db[i]) << 24)
				i += 1
			if (c & 0x10):
				cp_size = ord(db[i])
				i += 1
			if (c & 0x20):
				cp_size |= (ord(db[i]) << 8)
				i += 1
			if (c & 0x40):
				cp_size |= (ord(db[i]) << 16)
				i += 1
				
			if not cp_size: 
				cp_size = 0x10000
			
			rbound = cp_off + cp_size
			if (rbound < cp_size or
			    rbound > src_buf_size):
				break
			write(buffer(src_buf, cp_off, cp_size))
		elif c:
			write(db[i:i+c])
			i += c
		else:
			raise ValueError("unexpected delta opcode 0")
		# END handle command byte
	# END while processing delta data
	
	# yes, lets use the exact same error message that git uses :)
	assert i == delta_buf_size, "delta replay has gone wild"
	
	
def is_equal_canonical_sha(canonical_length, match, sha1):
	"""
	:return: True if the given lhs and rhs 20 byte binary shas
		The comparison will take the canonical_length of the match sha into account, 
		hence the comparison will only use the last 4 bytes for uneven canonical representations
	:param match: less than 20 byte sha
	:param sha1: 20 byte sha"""
	binary_length = canonical_length/2
	if match[:binary_length] != sha1[:binary_length]:
		return False
		
	if canonical_length - binary_length and \
		(ord(match[-1]) ^ ord(sha1[len(match)-1])) & 0xf0:
		return False
	# END handle uneven canonnical length
	return True
	
#} END routines


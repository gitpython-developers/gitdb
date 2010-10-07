"""Contains basic c-functions which usually contain performance critical code
Keeping this code separate from the beginning makes it easier to out-source
it into c later, if required"""

from exc import (
	BadObjectType
	)

from util import zlib
decompressobj = zlib.decompressobj

import mmap

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

def _trunc_delta(d, size):
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
			
def _move_delta_offset(d, bytes):
	"""Move the delta by the given amount of bytes, reducing its size so that its
	right bound stays static
	:param bytes: amount of bytes to move, must be smaller than delta size"""
	if bytes >= d.ts:
		raise ValueError("Cannot move offset that much")
		
	d.to += bytes
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
					'data'		# chunk of bytes to be added to the target buffer or None 
				)
	
	def __init__(self, to, ts, so, data):
		self.to = to
		self.ts = ts
		self.so = so
		self.data = data
		
	#{ Interface
		
	def abssize(self):
		return self.to + self.ts
		
	def apply(self, source, write):
		"""Apply own data to the target buffer
		:param source: buffer providing source bytes for copy operations
		:param write: write method to call with data to write"""
		if self.data is None:
			# COPY DATA FROM SOURCE
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
	absofs
	:note: global method for performance only, it belongs to DeltaChunkList"""
	# TODO: binary search !!
	for i,d in enumerate(dcl):
		if absofs >= d.to:
			return i
	# END for each delta absofs
	raise AssertionError("Should never be here")
	
def _split_delta(dcl, absofs, di=None):
	"""Split the delta at di into two deltas, adjusting their sizes, absofss and data 
	accordingly and adding them to the dcl.
	:param absofs: absolute absofs at which to split the delta
	:param di: a pre-determined delta-index, or None if it should be retrieved
	:note: it will not split if it
	:return: the closest index which has been split ( usually di if given)
	:note: belongs to DeltaChunkList"""
	if di is None:
		di = _closest_index(dcl, absofs)
	
	d = dcl[di]
	if d.to == absofs or d.abssize() == absofs:
		return di
		
	_trunc_delta(d, absofs - d.to)
		
	# insert new one
	ds = d.abssize()
	relsize = absofs - ds
	
	self.insert(di+1, DeltaChunk(  ds, 
									relsize, 
									(d.so and ds) or None,
									(d.data and d.data[relsize:]) or None))
	# END adjust next one
	return di
	
def _merge_delta(dcl, d):
	"""Merge the given DeltaChunk instance into the dcl"""
	index = _closest_index(dcl, d.to)
	od = dcl[index]
	
	if d.data is None:
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
			pass
		else:
			# INSERT DATA INTO COPY AREA
			pass
		# END combine or insert data
	# END handle chunk mode
	

class DeltaChunkList(list):
	"""List with special functionality to deal with DeltaChunks"""
	
	def init(self, size):
		"""Intialize this instance with chunks defining to fill up size from a base
		buffer of equal size"""
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
		
	def terminate_at(self, size):
		"""Chops the list at the given size, splitting and removing DeltaNodes 
		as required"""
		di = _closest_index(self, size)
		d = self[di]
		rsize = size - d.to
		if rsize:
			_trunc_delta(d, rsize)
		# END truncate last node if possible
		del(self[di+(rsize!=0):])
		
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
	:param dcl: see merge_deltas
	:param dstreams: iterable of delta stream objects. They must be ordered latest first, 
		hence the delta to be applied last comes first, then its ancestors
	:return: None"""
	raise NotImplementedError("This is left out up until we actually iterate the dstreams - they are prefetched right now")
	
def merge_deltas(dcl, dstreams):
	"""Read the condensed delta chunk information from dstream and merge its information
	into a list of existing delta chunks
	:param dcl: DeltaChunkList, may be empty initially, and will be changed
		during the merge process
	:param dstreams: iterable of delta stream objects. They must be ordered latest last, 
		hence the delta to be applied last comes last, its oldest ancestor first
	:return: None"""
	for ds in dstreams:
		db = ds.read()
		delta_buf_size = ds.size
		
		# read header
		i, src_size = msb_size(db)
		i, target_size = msb_size(db, i)
		
		if len(dcl) == 0:
			dcl.init(target_size)
		# END handle empty list
		
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
					rbound > src_size):
					break
				
				_merge_delta(dcl, DeltaChunk(tbw, cp_size, cp_off, None))
				tbw += cp_size
			elif c:
				# TODO: Concatenate multiple deltachunks 
				_merge_delta(dcl, DeltaChunk(tbw, c, None, db[i:i+c]))
				i += c
				tbw += c
			else:
				raise ValueError("unexpected delta opcode 0")
			# END handle command byte
		# END while processing delta data
		
		dcl.terminate_at(target_size)
		
	# END for each delta stream
	
	
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


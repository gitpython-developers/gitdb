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

from cStringIO import StringIO

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
			'is_equal_canonical_sha', 'reverse_connect_deltas',
			'connect_deltas', 'DeltaChunkList')


#{ Structures

def _set_delta_rbound(d, size):
	"""Truncate the given delta to the given size
	:param size: size relative to our target offset, may not be 0, must be smaller or equal
		to our size
	:return: d"""
	if d.ts == size:
		return
		
	d.ts = size
	
	# NOTE: data is truncated automatically when applying the delta
	# MUST NOT DO THIS HERE
	
	return d
			
def _move_delta_lbound(d, bytes):
	"""Move the delta by the given amount of bytes, reducing its size so that its
	right bound stays static
	:param bytes: amount of bytes to move, must be smaller than delta size
	:return: d"""
	if bytes == 0:
		return
		
	d.to += bytes
	d.so += bytes
	d.ts -= bytes
	if d.has_data():
		d.data = d.data[bytes:]
	# END handle data
	
	return d
	
def delta_duplicate(src):
	return DeltaChunk(src.to, src.ts, src.so, src.data, src.flags)
	
def delta_chunk_apply(dc, bbuf, write):
	"""Apply own data to the target buffer
	:param bbuf: buffer providing source bytes for copy operations
	:param write: write method to call with data to write"""
	if dc.data is None:
		# COPY DATA FROM SOURCE
		write(buffer(bbuf, dc.so, dc.ts))
	elif isinstance(dc.data, DeltaChunkList):
		delta_list_apply(dc.data, bbuf, write, dc.so, dc.ts)
	else:
		# APPEND DATA
		# whats faster: if + 4 function calls or just a write with a slice ?
		# Considering data can be larger than 127 bytes now, it should be worth it
		if dc.ts < len(dc.data):
			write(dc.data[:dc.ts])
		else:
			write(dc.data)
		# END handle truncation
	# END handle chunk mode

class DeltaChunk(object):
	"""Represents a piece of a delta, it can either add new data, or copy existing
	one from a source buffer"""
	__slots__ = (
					'to',		# start offset in the target buffer in bytes 
					'ts',		# size of this chunk in the target buffer in bytes
					'so',		# start offset in the source buffer in bytes or None
					'data',		# chunk of bytes to be added to the target buffer,
								# DeltaChunkList to use as base, or None
					'flags'		# currently only True or False
				)
	
	def __init__(self, to, ts, so, data, flags):
		self.to = to
		self.ts = ts
		self.so = so
		self.data = data
		self.flags = flags

	def __repr__(self):
		return "DeltaChunk(%i, %i, %s, %s, %i)" % (self.to, self.ts, self.so, self.data or "", self.flags)
	
	#{ Interface
		
	def copy_offset(self):
		""":return: offset to apply when copying from a base buffer, or 0 
			if this is not a copying delta chunk"""
		
		if self.data is not None:
			if isinstance(self.data, DeltaChunkList):
				return self.data.lbound() + self.so
			else:
				return self.so
		# END handle data type
		return 0
		
	def rbound(self):
		return self.to + self.ts
		
	def has_data(self):
		""":return: True if the instance has data to add to the target stream"""
		return self.data is not None and not isinstance(self.data, DeltaChunkList)
		
	def has_copy_chunklist(self):
		""":return: True if we copy our data from a chunklist"""
		return self.data is not None and isinstance(self.data, DeltaChunkList)
		
	def set_copy_chunklist(self, dcl):
		"""Set the deltachunk list to be used as basis for copying.
		:note: only works if this chunk is a copy delta chunk"""
		self.data = dcl
		self.so = 0				# allows lbound moves to be virtual
		
	
		
	#} END interface

def _closest_index(dcl, absofs):
	""":return: index at which the given absofs should be inserted. The index points
	to the DeltaChunk with a target buffer absofs that equals or is greater than
	absofs. 
	:note: global method for performance only, it belongs to DeltaChunkList"""
	lo = 0
	hi = len(dcl)
	while lo < hi:
		mid = (lo + hi) / 2
		dc = dcl[mid]
		if dc.to > absofs:
			hi = mid
		elif dc.rbound() > absofs or dc.to == absofs:
			return mid
		else:
			lo = mid + 1
		# END handle bound
	# END for each delta absofs
	return len(dcl)-1
	
def delta_list_apply(dcl, bbuf, write, lbound_offset=0, size=0):
	"""Apply the chain's changes and write the final result using the passed
	write function.
	:param bbuf: base buffer containing the base of all deltas contained in this
		list. It will only be used if the chunk in question does not have a base
		chain.
	:param lbound_offset: offset at which to start applying the delta, relative to 
		our lbound
	:param size: if larger than 0, only the given amount of bytes will be applied
	:param write: function taking a string of bytes to write to the output"""
	slen = len(dcl)
	if slen == 0:
		return
	# END early abort
	absofs = dcl.lbound() + lbound_offset
	if size == 0:
		size = dcl.rbound() - absofs
	# END initialize size
	
	if lbound_offset or absofs + size != dcl.rbound():
		cdi = _closest_index(dcl, absofs)
		cd = dcl[cdi]
		if cd.to != absofs:
			tcd = delta_duplicate(cd)
			_move_delta_lbound(tcd, absofs - cd.to)
			_set_delta_rbound(tcd, min(tcd.ts, size)) 
			delta_chunk_apply(tcd, bbuf, write)
			size -= tcd.ts
			cdi += 1
		# END handle first chunk
		
		# here we have to either apply full chunks, or smaller ones, but 
		# we always start at the chunks target offset
		while cdi < slen and size:
			cd = dcl[cdi]
			if cd.ts <= size:
				delta_chunk_apply(cd, bbuf, write)
				size -= cd.ts
			else:
				tcd = delta_duplicate(cd)
				_set_delta_rbound(tcd, size)
				delta_chunk_apply(tcd, bbuf, write)
				size -= tcd.ts
				break
			# END handle bytes to apply
			cdi += 1
		# END handle rest
	else:
		for dc in dcl:
			delta_chunk_apply(dc, bbuf, write)
		# END for each dc
	# END handle application values

	
class DeltaChunkList(list):
	"""List with special functionality to deal with DeltaChunks.
	There are two types of lists we represent. The one was created bottom-up, working
	towards the latest delta, the other kind was created top-down, working from the 
	latest delta down to the earliest ancestor. This attribute is queryable 
	after all processing with is_reversed."""
	
	__slots__ = tuple()
	
	def rbound(self):
		""":return: rightmost extend in bytes, absolute"""
		if len(self) == 0:
			return 0
		return self[-1].rbound()
		
	def lbound(self):
		""":return: leftmost byte at which this chunklist starts"""
		if len(self) == 0:
			return 0
		return self[0].to
		
	def size(self):
		""":return: size of bytes as measured by our delta chunks"""
		return self.rbound() - self.lbound()
		
	def connect_with(self, bdcl):
		"""Connect this instance's delta chunks virtually with the given base.
		This means that all copy deltas will simply apply to the given region 
		of the given base. Afterwards, the base is optimized so that add-deltas
		will be truncated to the region actually used, or removed completely where
		adequate. This way, memory usage is reduced.
		:param bdcl: DeltaChunkList to serve as base"""
		for dc in self:
			if not dc.has_data():
				dc.set_copy_chunklist(bdcl[dc.so:dc.ts])
			# END handle overlap
		# END for each dc
		
	def apply(self, bbuf, write, lbound_offset=0, size=0):
		"""Only used by public clients, internally we only use the global routines
		for performance"""
		return delta_list_apply(self, bbuf, write, lbound_offset, size)
		
	def compress(self):
		"""Alter the list to reduce the amount of nodes. Currently we concatenate
		add-chunks
		:return: self"""
		slen = len(self)
		if slen < 2:
			return self
		i = 0
		slen_orig = slen
		
		first_data_index = None
		while i < slen:
			dc = self[i]
			i += 1
			if not dc.has_data():
				if first_data_index is not None and i-2-first_data_index > 1:
				#if first_data_index is not None:
					nd = StringIO()						# new data
					so = self[first_data_index].to		# start offset in target buffer
					for x in xrange(first_data_index, i-1):
						xdc = self[x]
						nd.write(xdc.data[:xdc.ts])
					# END collect data
					
					del(self[first_data_index:i-1])
					buf = nd.getvalue()
					self.insert(first_data_index, DeltaChunk(so, len(buf), 0, buf, False)) 
					
					slen = len(self)
					i = first_data_index + 1
					
				# END concatenate data
				first_data_index = None
				continue
			# END skip non-data chunks
			
			if first_data_index is None:
				first_data_index = i-1
		# END iterate list
		
		#if slen_orig != len(self):
		#	print "INFO: Reduced delta list len to %f %% of former size" % ((float(len(self)) / slen_orig) * 100)
		return self
		
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
			assert dc.ts > 0
			if dc.has_data():
				assert len(dc.data) >= dc.ts
			if dc.has_copy_chunklist():
				assert dc.ts <= dc.data.size()
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
		
	def __getslice__(self, absofs, size):
		""":return: Subsection of this  list at the given absolute  offset, with the given 
			size in bytes.
		:return: DeltaChunkList (copy) which represents the given chunk"""
		if len(self) == 0:
			return DeltaChunkList()
			
		absofs = max(absofs, self.lbound())
		size = min(self.rbound() - self.lbound(), size)
		cdi = _closest_index(self, absofs)	# delta start index
		cd = self[cdi]
		slen = len(self)
		ndcl = self.__class__()
		
		if cd.to != absofs:
			tcd = delta_duplicate(cd)
			_move_delta_lbound(tcd, absofs - cd.to)
			_set_delta_rbound(tcd, min(tcd.ts, size))
			ndcl.append(tcd)
			size -= tcd.ts
			cdi += 1
		# END lbound overlap handling
		
		while cdi < slen and size:
			# are we larger than the current block
			cd = self[cdi]
			if cd.ts <= size:
				ndcl.append(delta_duplicate(cd))
				size -= cd.ts
			else:
				tcd = delta_duplicate(cd)
				_set_delta_rbound(tcd, size)
				ndcl.append(tcd)
				size -= tcd.ts
				break
			# END hadle size
			cdi += 1
		# END for each chunk
		
		# ndcl.check_integrity()
		return ndcl

	
class TopdownDeltaChunkList(DeltaChunkList):
	"""Represents a list which is generated by feeding its ancestor streams one by 
	one"""
	__slots__ = ('frozen', )	# if True, the list is frozen and can reproduce all data
								# Will only be set in lists which where processed top-down 
	
	def __init__(self):
		self.frozen = False
	
	def connect_with_next_base(self, bdcl):
		"""Connect this chain with the next level of our base delta chunklist.
		The goal in this game is to mark as many of our chunks rigid, hence they
		cannot be changed by any of the upcoming bases anymore. Once all our 
		chunks are marked like that, we can stop all processing
		:param bdcl: data chunk list being one of our bases. They must be fed in 
			consequtively and in order, towards the earliest ancestor delta
		:return: True if processing was done. Use it to abort processing of
			remaining streams"""
		if self.frozen == 1:
			# Can that ever be hit ?
			return False
		# END early abort
		# mark us so that the is_reversed method returns True, without us thinking
		# we are frozen
		self.frozen = -1
		
		raise NotImplementedError("todo")
		return True
		
		
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
	
def reverse_connect_deltas(dcl, dstreams):
	"""Read the condensed delta chunk information from dstream and merge its information
	into a list of existing delta chunks
	:param dcl: see 3
	:param dstreams: iterable of delta stream objects. They must be ordered latest first, 
		hence the delta to be applied last comes first, then its ancestors
	:return: None"""
	raise NotImplementedError("This is left out up until we actually iterate the dstreams - they are prefetched right now")
	
def connect_deltas(dstreams, reverse):
	"""Read the condensed delta chunk information from dstream and merge its information
	into a list of existing delta chunks
	:param dstreams: iterable of delta stream objects. They must be ordered latest last, 
		hence the delta to be applied last comes last, its oldest ancestor first
	:param reverse: If False, the given iterable of delta-streams returns
		items in from latest ancestor to the last delta.
		If True, deltas are ordered so that the one to be applied last comes first.
	:return: DeltaChunkList, containing all operations to apply"""
	bdcl = None							# data chunk list for initial base
	tdcl = None							# topmost dcl, only effective if reverse is True
	
	if reverse:
		dcl = tdcl = TopdownDeltaChunkList()
	else:
		dcl = DeltaChunkList()
	# END handle type of first chunk list
	
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
				
				dcl.append(DeltaChunk(tbw, cp_size, cp_off, None, False))
				tbw += cp_size
			elif c:
				# NOTE: in C, the data chunks should probably be concatenated here.
				# In python, we do it as a post-process
				dcl.append(DeltaChunk(tbw, c, 0, db[i:i+c], False))
				i += c
				tbw += c
			else:
				raise ValueError("unexpected delta opcode 0")
			# END handle command byte
		# END while processing delta data
		
		dcl.compress()
		
		# merge the lists !
		if bdcl is not None:
			if tdcl:
				if not tdcl.connect_with_next_base(dcl):
					break
				# END early abort
			else:
				dcl.connect_with(bdcl)
		# END handle merge
		
		# dcl.check_integrity()
		
		# prepare next base
		bdcl = dcl
		dcl = DeltaChunkList()
	# END for each delta stream
	
	if tdcl:
		return tdcl
	else:
		return bdcl
	
	
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



from cStringIO import StringIO
import errno
import mmap
import os

from util import (
		LazyMixin,
		make_sha,
		write, 
		close,
		zlib
	)

__all__ = ('DecompressMemMapReader', 'FDCompressedSha1Writer')


#{ RO Streams

class DecompressMemMapReader(LazyMixin):
	"""Reads data in chunks from a memory map and decompresses it. The client sees 
	only the uncompressed data, respective file-like read calls are handling on-demand
	buffered decompression accordingly
	
	A constraint on the total size of bytes is activated, simulating 
	a logical file within a possibly larger physical memory area
	
	To read efficiently, you clearly don't want to read individual bytes, instead, 
	read a few kilobytes at least.
	
	:note: The chunk-size should be carefully selected as it will involve quite a bit 
		of string copying due to the way the zlib is implemented. Its very wasteful, 
		hence we try to find a good tradeoff between allocation time and number of 
		times we actually allocate. An own zlib implementation would be good here
		to better support streamed reading - it would only need to keep the mmap
		and decompress it into chunks, thats all ... """
	__slots__ = ('_m', '_zip', '_buf', '_buflen', '_br', '_cws', '_cwe', '_s', '_close', 
				'_cbr', '_phi')
	
	max_read_size = 512*1024		# currently unused
	
	def __init__(self, m, close_on_deletion, size=None):
		"""Initialize with mmap for stream reading
		:param m: must be content data - use new if you have object data and no size"""
		self._m = m
		self._zip = zlib.decompressobj()
		self._buf = None						# buffer of decompressed bytes
		self._buflen = 0						# length of bytes in buffer
		if size is not None:
			self._s = size						# size of uncompressed data to read in total
		self._br = 0							# num uncompressed bytes read
		self._cws = 0							# start byte of compression window
		self._cwe = 0							# end byte of compression window
		self._cbr = 0							# number of compressed bytes read
		self._phi = False						# is True if we parsed the header info
		self._close = close_on_deletion			# close the memmap on deletion ?
		
	def _set_cache_(self, attr):
		assert attr == '_s'
		# only happens for size, which is a marker to indicate we still 
		# have to parse the header from the stream
		self._parse_header_info()
		
	def __del__(self):
		if self._close:
			self._m.close()
		# END handle resource freeing
		
	def _parse_header_info(self):
		"""If this stream contains object data, parse the header info and skip the 
		stream to a point where each read will yield object content
		:return: parsed type_string, size"""
		# read header
		maxb = 512				# should really be enough, cgit uses 8192 I believe
		self._s = maxb
		hdr = self.read(maxb)
		hdrend = hdr.find("\0")
		type, size = hdr[:hdrend].split(" ")
		size = int(size)
		self._s = size
		
		# adjust internal state to match actual header length that we ignore
		# The buffer will be depleted first on future reads
		self._br = 0
		hdrend += 1									# count terminating \0
		self._buf = StringIO(hdr[hdrend:])
		self._buflen = len(hdr) - hdrend
		
		self._phi = True
		
		return type, size
		
	@classmethod
	def new(self, m, close_on_deletion=False):
		"""Create a new DecompressMemMapReader instance for acting as a read-only stream
		This method parses the object header from m and returns the parsed 
		type and size, as well as the created stream instance.
		:param m: memory map on which to oparate. It must be object data ( header + contents )
		:param close_on_deletion: if True, the memory map will be closed once we are 
			being deleted"""
		inst = DecompressMemMapReader(m, close_on_deletion, 0)
		type, size = inst._parse_header_info()
		return type, size, inst

	def compressed_bytes_read(self):
		""":return: number of compressed bytes read. This includes the bytes it 
		took to decompress the header ( if there was one )"""
		# ABSTRACT: When decompressing a byte stream, it can be that the first
		# x bytes which were requested match the first x bytes in the loosely 
		# compressed datastream. This is the worst-case assumption that the reader
		# does, it assumes that it will get at least X bytes from X compressed bytes
		# in call cases.
		# The caveat is that the object, according to our known uncompressed size, 
		# is already complete, but there are still some bytes left in the compressed
		# stream that contribute to the amount of compressed bytes.
		# How can we know that we are truly done, and have read all bytes we need
		# to read ? 
		# Without help, we cannot know, as we need to obtain the status of the 
		# decompression. If it is not finished, we need to decompress more data
		# until it is finished, to yield the actual number of compressed bytes
		# belonging to the decompressed object
		# We are using a custom zlib module for this, if its not present, 
		# we try to put in additional bytes up for decompression if feasible
		# and check for the unused_data.
		
		# Only scrub the stream forward if we are officially done with the
		# bytes we were to have.
		if self._br == self._s and not self._zip.unused_data:
			# manipulate the bytes-read to allow our own read method to coninute
			# but keep the window at its current position
			self._br = 0
			if hasattr(self._zip, 'status'):
				while self._zip.status == zlib.Z_OK:
					self.read(mmap.PAGESIZE)
				# END scrub-loop custom zlib
			else:
				# pass in additional pages, until we have unused data
				while not self._zip.unused_data and self._cbr != len(self._m):
					self.read(mmap.PAGESIZE)
				# END scrub-loop default zlib
			# END handle stream scrubbing
			
			# reset bytes read, just to be sure
			self._br = self._s
		# END handle stream scrubbing
		
		return self._cbr - len(self._zip.unused_data)
		
	def seek(self, offset, whence=os.SEEK_SET):
		"""Allows to reset the stream to restart reading
		:raise ValueError: If offset and whence are not 0"""
		if offset != 0 or whence != os.SEEK_SET:
			raise ValueError("Can only seek to position 0")
		# END handle offset
		
		self._zip = zlib.decompressobj()
		self._br = self._cws = self._cwe = self._cbr = 0
		if self._phi:
			self._phi = False
			del(self._s)		# trigger header parsing on first access
		# END skip header
	
	def read(self, size=-1):
		if size < 1:
			size = self._s - self._br
		else:
			size = min(size, self._s - self._br)
		# END clamp size
		
		if size == 0:
			return str()
		# END handle depletion
	
	
		# deplete the buffer, then just continue using the decompress object 
		# which has an own buffer. We just need this to transparently parse the 
		# header from the zlib stream
		dat = str()
		if self._buf:
			if self._buflen >= size:
				# have enough data
				dat = self._buf.read(size)
				self._buflen -= size
				self._br += size
				return dat
			else:
				dat = self._buf.read()		# ouch, duplicates data
				size -= self._buflen
				self._br += self._buflen
				
				self._buflen = 0
				self._buf = None
			# END handle buffer len
		# END handle buffer
		
		# decompress some data
		# Abstract: zlib needs to operate on chunks of our memory map ( which may 
		# be large ), as it will otherwise and always fill in the 'unconsumed_tail'
		# attribute which possible reads our whole map to the end, forcing 
		# everything to be read from disk even though just a portion was requested.
		# As this would be a nogo, we workaround it by passing only chunks of data, 
		# moving the window into the memory map along as we decompress, which keeps 
		# the tail smaller than our chunk-size. This causes 'only' the chunk to be
		# copied once, and another copy of a part of it when it creates the unconsumed
		# tail. We have to use it to hand in the appropriate amount of bytes durin g
		# the next read.
		tail = self._zip.unconsumed_tail
		if tail:
			# move the window, make it as large as size demands. For code-clarity, 
			# we just take the chunk from our map again instead of reusing the unconsumed
			# tail. The latter one would safe some memory copying, but we could end up
			# with not getting enough data uncompressed, so we had to sort that out as well.
			# Now we just assume the worst case, hence the data is uncompressed and the window
			# needs to be as large as the uncompressed bytes we want to read.
			self._cws = self._cwe - len(tail)
			self._cwe = self._cws + size
		else:
			cws = self._cws
			self._cws = self._cwe
			self._cwe = cws + size 
		# END handle tail
		
		
		# if window is too small, make it larger so zip can decompress something
		if self._cwe - self._cws < 8:
			self._cwe = self._cws + 8
		# END adjust winsize
		
		# takes a slice, but doesn't copy the data, it says ... 
		indata = buffer(self._m, self._cws, self._cwe - self._cws)
		
		# get the actual window end to be sure we don't use it for computations
		self._cwe = self._cws + len(indata)
		
		dcompdat = self._zip.decompress(indata, size)
		
		# update the amount of compressed bytes read
		# We feed possibly overlapping chunks, which is why the unconsumed tail
		# has to be taken into consideration, as well as the unused data
		# if we hit the end of the stream
		self._cbr += len(indata) - len(self._zip.unconsumed_tail)
		self._br += len(dcompdat)
		
		if dat:
			dcompdat = dat + dcompdat
			
		return dcompdat
		
#} END RO streams


#{ W Streams

class Sha1Writer(object):
	"""Simple stream writer which produces a sha whenever you like as it degests
	everything it is supposed to write"""
	__slots__ = "sha1"
	
	def __init__(self):
		self.sha1 = make_sha("")

	#{ Stream Interface

	def write(self, data):
		""":raise IOError: If not all bytes could be written
		:return: lenght of incoming data"""
		self.sha1.update(data)
		return len(data)

	# END stream interface 

	#{ Interface
	
	def sha(self, as_hex = False):
		""":return: sha so far
		:param as_hex: if True, sha will be hex-encoded, binary otherwise"""
		if as_hex:
			return self.sha1.hexdigest()
		return self.sha1.digest()
	
	#} END interface 

class FDCompressedSha1Writer(Sha1Writer):
	"""Digests data written to it, making the sha available, then compress the 
	data and write it to the file descriptor
	:note: operates on raw file descriptors
	:note: for this to work, you have to use the close-method of this instance"""
	__slots__ = ("fd", "sha1", "zip")
	
	# default exception
	exc = IOError("Failed to write all bytes to filedescriptor")
	
	def __init__(self, fd):
		super(FDCompressedSha1Writer, self).__init__()
		self.fd = fd
		self.zip = zlib.compressobj(zlib.Z_BEST_SPEED)

	#{ Stream Interface

	def write(self, data):
		""":raise IOError: If not all bytes could be written
		:return: lenght of incoming data"""
		self.sha1.update(data)
		cdata = self.zip.compress(data)
		bytes_written = write(self.fd, cdata)
		if bytes_written != len(cdata):
			raise self.exc
		return len(data)

	def close(self):
		remainder = self.zip.flush()
		if write(self.fd, remainder) != len(remainder):
			raise self.exc
		return close(self.fd)

	#} END stream interface

#} END W streams

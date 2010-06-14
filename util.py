import binascii
import os
import sys
import errno

try:
	import async.mod.zlib as zlib
except ImportError:
	import zlib
# END try async zlib

from async import ThreadPool

try:
    import hashlib
except ImportError:
    import sha

#{ Globals

# A pool distributing tasks, initially with zero threads, hence everything 
# will be handled in the main thread
pool = ThreadPool(0)

#} END globals


#{ Aliases

hex_to_bin = binascii.a2b_hex
bin_to_hex = binascii.b2a_hex

# errors
ENOENT = errno.ENOENT

# os shortcuts
exists = os.path.exists
mkdir = os.mkdir
isdir = os.path.isdir
rename = os.rename
dirname = os.path.dirname
join = os.path.join
read = os.read
write = os.write
close = os.close

# constants
NULL_HEX_SHA = "0"*40

#} END Aliases


#{ Routines

def make_sha(source=''):
    """A python2.4 workaround for the sha/hashlib module fiasco 
    :note: From the dulwich project """
    try:
        return hashlib.sha1(source)
    except NameError:
        sha1 = sha.sha(source)
        return sha1

def stream_copy(source, destination, chunk_size=512*1024):
	"""Copy all data from the source stream into the destination stream in chunks
	of size chunk_size
	
	:return: amount of bytes written"""
	br = 0
	while True:
		chunk = source.read(chunk_size)
		destination.write(chunk)
		br += len(chunk)
		if len(chunk) < chunk_size:
			break
	# END reading output stream
	return br

def to_hex_sha(sha):
	""":return: hexified version  of sha"""
	if len(sha) == 40:
		return sha
	return bin_to_hex(sha)
	
def to_bin_sha(sha):
	if len(sha) == 20:
		return sha
	return hex_to_bin(sha)


#} END routines


#{ Utilities


class FDStreamWrapper(object):
	"""A simple wrapper providing the most basic functions on a file descriptor 
	with the fileobject interface. Cannot use os.fdopen as the resulting stream
	takes ownership"""
	__slots__ = ("_fd", '_pos')
	def __init__(self, fd):
		self._fd = fd
		self._pos = 0
		
	def write(self, data):
		self._pos += len(data)
		os.write(self._fd, data)
	
	def read(self, count=0):
		if count == 0:
			count = os.path.getsize(self._filepath)
		# END handle read everything
			
		bytes = os.read(self._fd, count)
		self._pos += len(bytes)
		return bytes
		
	def fileno(self):
		return self._fd
		
	def tell(self):
		return self._pos
			
	
class LockedFD(object):
	"""This class facilitates a safe read and write operation to a file on disk.
	If we write to 'file', we obtain a lock file at 'file.lock' and write to 
	that instead. If we succeed, the lock file will be renamed to overwrite 
	the original file.
	
	When reading, we obtain a lock file, but to prevent other writers from 
	succeeding while we are reading the file.
	
	This type handles error correctly in that it will assure a consistent state 
	on destruction.
	
	:note: with this setup, parallel reading is not possible"""
	__slots__ = ("_filepath", '_fd', '_write')
	
	def __init__(self, filepath):
		"""Initialize an instance with the givne filepath"""
		self._filepath = filepath
		self._fd = None
		self._write = None			# if True, we write a file
	
	def __del__(self):
		# will do nothing if the file descriptor is already closed
		if self._fd is not None:
			self.rollback()
		
	def _lockfilepath(self):
		return "%s.lock" % self._filepath
		
	def open(self, write=False, stream=False):
		"""Open the file descriptor for reading or writing, both in binary mode.
		:param write: if True, the file descriptor will be opened for writing. Other
			wise it will be opened read-only.
		:param stream: if True, the file descriptor will be wrapped into a simple stream 
			object which supports only reading or writing
		:return: fd to read from or write to. It is still maintained by this instance
			and must not be closed directly
		:raise IOError: if the lock could not be retrieved
		:raise OSError: If the actual file could not be opened for reading
		:note: must only be called once"""
		if self._write is not None:
			raise AssertionError("Called %s multiple times" % self.open)
		
		self._write = write
		
		# try to open the lock file
		binary = getattr(os, 'O_BINARY', 0)
		lockmode = 	os.O_WRONLY | os.O_CREAT | os.O_EXCL | binary
		try:
			fd = os.open(self._lockfilepath(), lockmode)
			if not write:
				os.close(fd)
			else:
				self._fd = fd
			# END handle file descriptor
		except OSError:
			raise IOError("Lock at %r could not be obtained" % self._lockfilepath())
		# END handle lock retrieval
		
		# open actual file if required
		if self._fd is None:
			# we could specify exlusive here, as we obtained the lock anyway
			self._fd = os.open(self._filepath, os.O_RDONLY | binary)
		# END open descriptor for reading
		
		if stream:
			return FDStreamWrapper(self._fd)
		else:
			return self._fd
		# END handle stream
		
	def commit(self):
		"""When done writing, call this function to commit your changes into the 
		actual file. 
		The file descriptor will be closed, and the lockfile handled.
		:note: can be called multiple times"""
		self._end_writing(successful=True)
		
	def rollback(self):
		"""Abort your operation without any changes. The file descriptor will be 
		closed, and the lock released.
		:note: can be called multiple times"""
		self._end_writing(successful=False)
		
	def _end_writing(self, successful=True):
		"""Handle the lock according to the write mode """
		if self._write is None:
			raise AssertionError("Cannot end operation if it wasn't started yet")
		
		if self._fd is None:
			return
		
		os.close(self._fd)
		self._fd = None
		
		lockfile = self._lockfilepath()
		if self._write and successful:
			# on windows, rename does not silently overwrite the existing one
			if sys.platform == "win32":
				if os.path.isfile(self._filepath):
					os.remove(self._filepath)
				# END remove if exists
			# END win32 special handling
			os.rename(lockfile, self._filepath)
		else:
			# just delete the file so far, we failed
			os.remove(lockfile)
		# END successful handling

#} END utilities

"""Utilities used in ODB testing"""
from gitdb import (
	OStream, 
	)
from gitdb.stream import Sha1Writer

import sys
import zlib
import random
from array import array
from cStringIO import StringIO

import unittest
import tempfile
import shutil
import os


#{ Bases

class TestBase(unittest.TestCase):
	"""Base class for all tests"""
	

#} END bases

#{ Decorators

def with_rw_directory(func):
	"""Create a temporary directory which can be written to, remove it if the 
	test suceeds, but leave it otherwise to aid additional debugging"""
	def wrapper(self):
		path = tempfile.mktemp(prefix=func.__name__)
		os.mkdir(path)
		try:
			return func(self, path)
		except Exception:
			print >> sys.stderr, "Test %s.%s failed, output is at %r" % (type(self).__name__, func.__name__, path)
			raise
		else:
			shutil.rmtree(path)
		# END handle exception
	# END wrapper
	
	wrapper.__name__ = func.__name__
	return wrapper


#} END decorators

#{ Routines

def make_bytes(size_in_bytes, randomize=False):
	""":return: string with given size in bytes
	:param randomize: try to produce a very random stream"""
	actual_size = size_in_bytes / 4
	producer = xrange(actual_size)
	if randomize:
		producer = list(producer)
		random.shuffle(producer)
	# END randomize
	a = array('i', producer)
	return a.tostring()

def make_object(type, data):
	""":return: bytes resembling an uncompressed object"""
	odata = "blob %i\0" % len(data)
	return odata + data
	
def make_memory_file(size_in_bytes, randomize=False):
	""":return: tuple(size_of_stream, stream)
	:param randomize: try to produce a very random stream"""
	d = make_bytes(size_in_bytes, randomize)
	return len(d), StringIO(d)

#} END routines

#{ Stream Utilities

class DummyStream(object):
		def __init__(self):
			self.was_read = False
			self.bytes = 0
			self.closed = False
			
		def read(self, size):
			self.was_read = True
			self.bytes = size
			
		def close(self):
			self.closed = True
			
		def _assert(self):
			assert self.was_read


class DeriveTest(OStream):
	def __init__(self, sha, type, size, stream, *args, **kwargs):
		self.myarg = kwargs.pop('myarg')
		self.args = args
		
	def _assert(self):
		assert self.args
		assert self.myarg


class ZippedStoreShaWriter(Sha1Writer):
	"""Remembers everything someone writes to it"""
	__slots__ = ('buf', 'zip')
	def __init__(self):
		Sha1Writer.__init__(self)
		self.buf = StringIO()
		self.zip = zlib.compressobj(1)	# fastest
	
	def __getattr__(self, attr):
		return getattr(self.buf, attr)
	
	def write(self, data):
		alen = Sha1Writer.write(self, data)
		self.buf.write(self.zip.compress(data))
		return alen
		
	def close(self):
		self.buf.write(self.zip.flush())


#} END stream utilitiess


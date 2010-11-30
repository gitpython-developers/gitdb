"""Utilities used in ODB testing"""
from gitdb import (
	OStream, 
	)
from gitdb.stream import ( 
							Sha1Writer, 
							ZippedStoreShaWriter
						)

from gitdb.util import zlib

import sys
import random
from array import array
from cStringIO import StringIO

import glob
import unittest
import tempfile
import shutil
import os
import gc


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
			try:
				return func(self, path)
			except Exception:
				print >> sys.stderr, "Test %s.%s failed, output is at %r" % (type(self).__name__, func.__name__, path)
				raise
		finally:
			# Need to collect here to be sure all handles have been closed. It appears
			# a windows-only issue. In fact things should be deleted, as well as 
			# memory maps closed, once objects go out of scope. For some reason
			# though this is not the case here unless we collect explicitly.
			gc.collect()
			shutil.rmtree(path)
		# END handle exception
	# END wrapper
	
	wrapper.__name__ = func.__name__
	return wrapper


def with_packs_rw(func):
	"""Function that provides a path into which the packs for testing should be 
	copied. Will pass on the path to the actual function afterwards"""
	def wrapper(self, path):
		src_pack_glob = fixture_path('packs/*')
		copy_files_globbed(src_pack_glob, path, hard_link_ok=True)
		return func(self, path)
	# END wrapper
	
	wrapper.__name__ = func.__name__
	return wrapper

#} END decorators

#{ Routines

def fixture_path(relapath=''):
	""":return: absolute path into the fixture directory
	:param relapath: relative path into the fixtures directory, or ''
		to obtain the fixture directory itself"""
	return os.path.join(os.path.dirname(__file__), 'fixtures', relapath)
	
def copy_files_globbed(source_glob, target_dir, hard_link_ok=False):
	"""Copy all files found according to the given source glob into the target directory
	:param hard_link_ok: if True, hard links will be created if possible. Otherwise 
		the files will be copied"""
	for src_file in glob.glob(source_glob):
		if hard_link_ok and hasattr(os, 'link'):
			target = os.path.join(target_dir, os.path.basename(src_file))
			try:
				os.link(src_file, target)
			except OSError:
				shutil.copy(src_file, target_dir)
			# END handle cross device links ( and resulting failure )
		else:
			shutil.copy(src_file, target_dir)
		# END try hard link
	# END for each file to copy
	

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

#} END stream utilitiess


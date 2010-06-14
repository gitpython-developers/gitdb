"""Module containing a database to deal with packs"""
from base import (
						FileDBBase, 
						ObjectDBR
				)

from gitdb.exc import (
							UnsupportedOperation,
						)

__all__ = ('PackedDB', )

class PackedDB(FileDBBase, ObjectDBR):
	"""A database operating on a set of object packs"""
	
	def __init__(self, root_path):
		super(PackedDB, self).__init__(root_path)
		
	
	#{ Object DB Read 
	
	def has_object(self, sha):
		raise NotImplementedError()
		
	def info(self, sha):
		raise NotImplementedError()
	
	def stream(self, sha):
		raise NotImplementedError()
	
	#} END object db read
	
	#{ object db write
	
	def store(self, istream):
		"""Storing individual objects is not feasible as a pack is designed to 
		hold multiple objects. Writing or rewriting packs for single objects is
		inefficient"""
		raise UnsupportedOperation()
		
	def store_async(self, reader):
		raise NotImplementedError()
	
	#} END object db write

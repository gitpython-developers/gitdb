"""Module containing a database to deal with packs"""
from base import (
						FileDBBase, 
						ObjectDBR
				)

__all__ = ('PackedDB', )

class PackedDB(FileDBBase, ObjectDBR):
	"""A database operating on a set of object packs"""
	

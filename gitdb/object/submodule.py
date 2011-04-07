from base import IndexObject

class Submodule(IndexObject):
	"""Dummy type representing submodules. At some point an implemenation might be add
	( it currently is located in GitPython )"""
	
	# this is a bogus type for base class compatability
	type = 'submodule'
	# this type doesn't really have a type id
	type_id = 0
	


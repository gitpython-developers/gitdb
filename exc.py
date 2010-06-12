"""Module with common exceptions"""

class ODBError(Exception):
	"""All errors thrown by the object database"""
	
class InvalidDBRoot(ODBError):
	"""Thrown if an object database cannot be initialized at the given path"""
	
class BadObject(ODBError):
	"""The object with the given SHA does not exist"""
	
class BadObjectType(ODBError):
	"""The object had an unsupported type"""


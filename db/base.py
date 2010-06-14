"""Contains implementations of database retrieveing objects"""
from gitdb.util import (
		pool,
		join
	)

from async import (
		ChannelThreadTask
	)


__all__ = ('ObjectDBR', 'ObjectDBW', 'FileDBBase', 'CompoundDB')


class ObjectDBR(object):
	"""Defines an interface for object database lookup.
	Objects are identified either by hex-sha (40 bytes) or 
	by sha (20 bytes)"""
	
	def __contains__(self, sha):
		return self.has_obj
	
	#{ Query Interface 
	def has_object(self, sha):
		"""
		:return: True if the object identified by the given 40 byte hexsha or 20 bytes
			binary sha is contained in the database"""
		raise NotImplementedError("To be implemented in subclass")
		
	def has_object_async(self, reader):
		"""Return a reader yielding information about the membership of objects
		as identified by shas
		:param reader: Reader yielding 20 byte or 40 byte shas.
		:return: async.Reader yielding tuples of (sha, bool) pairs which indicate
			whether the given sha exists in the database or not"""
		task = ChannelThreadTask(reader, str(self.has_object_async), lambda sha: (sha, self.has_object(sha)))
		return pool.add_task(task) 
		
	def info(self, sha):
		""" :return: OInfo instance
		:param sha: 40 bytes hexsha or 20 bytes binary sha
		:raise BadObject:"""
		raise NotImplementedError("To be implemented in subclass")
		
	def info_async(self, reader):
		"""Retrieve information of a multitude of objects asynchronously
		:param reader: Channel yielding the sha's of the objects of interest
		:return: async.Reader yielding OInfo|InvalidOInfo, in any order"""
		task = ChannelThreadTask(reader, str(self.info_async), self.info)
		return pool.add_task(task)
		
	def stream(self, sha):
		""":return: OStream instance
		:param sha: 40 bytes hexsha or 20 bytes binary sha
		:raise BadObject:"""
		raise NotImplementedError("To be implemented in subclass")
		
	def stream_async(self, reader):
		"""Retrieve the OStream of multiple objects
		:param reader: see ``info``
		:param max_threads: see ``ObjectDBW.store``
		:return: async.Reader yielding OStream|InvalidOStream instances in any order
		:note: depending on the system configuration, it might not be possible to 
			read all OStreams at once. Instead, read them individually using reader.read(x)
			where x is small enough."""
		# base implementation just uses the stream method repeatedly
		task = ChannelThreadTask(reader, str(self.stream_async), self.stream)
		return pool.add_task(task)
			
	#} END query interface
	
	
class ObjectDBW(object):
	"""Defines an interface to create objects in the database"""
	
	def __init__(self, *args, **kwargs):
		self._ostream = None
	
	#{ Edit Interface
	def set_ostream(self, stream):
		"""Adjusts the stream to which all data should be sent when storing new objects
		:param stream: if not None, the stream to use, if None the default stream
			will be used.
		:return: previously installed stream, or None if there was no override
		:raise TypeError: if the stream doesn't have the supported functionality"""
		cstream = self._ostream
		self._ostream = stream
		return cstream
		
	def ostream(self):
		""":return: overridden output stream this instance will write to, or None
			if it will write to the default stream"""
		return self._ostream
	
	def store(self, istream):
		"""Create a new object in the database
		:return: the input istream object with its sha set to its corresponding value
		:param istream: IStream compatible instance. If its sha is already set 
			to a value, the object will just be stored in the our database format, 
			in which case the input stream is expected to be in object format ( header + contents ).
		:raise IOError: if data could not be written"""
		raise NotImplementedError("To be implemented in subclass")
	
	def store_async(self, reader):
		"""Create multiple new objects in the database asynchronously. The method will 
		return right away, returning an output channel which receives the results as 
		they are computed.
		
		:return: Channel yielding your IStream which served as input, in any order.
			The IStreams sha will be set to the sha it received during the process, 
			or its error attribute will be set to the exception informing about the error.
		:param reader: async.Reader yielding IStream instances.
			The same instances will be used in the output channel as were received
			in by the Reader.
		:note:As some ODB implementations implement this operation atomic, they might 
			abort the whole operation if one item could not be processed. Hence check how 
			many items have actually been produced."""
		# base implementation uses store to perform the work
		task = ChannelThreadTask(reader, str(self.store_async), self.store) 
		return pool.add_task(task)
	
	#} END edit interface
	

class FileDBBase(object):
	"""Provides basic facilities to retrieve files of interest, including 
	caching facilities to help mapping hexsha's to objects"""
	
	def __init__(self, root_path):
		"""Initialize this instance to look for its files at the given root path
		All subsequent operations will be relative to this path
		:raise InvalidDBRoot: 
		:note: The base will not perform any accessablity checking as the base
			might not yet be accessible, but become accessible before the first 
			access."""
		super(FileDBBase, self).__init__()
		self._root_path = root_path
		
		
	#{ Interface 
	def root_path(self):
		""":return: path at which this db operates"""
		return self._root_path
	
	def db_path(self, rela_path):
		"""
		:return: the given relative path relative to our database root, allowing 
			to pontentially access datafiles"""
		return join(self._root_path, rela_path)
	#} END interface
		

class CompoundDB(ObjectDBR):
	"""A database which delegates calls to sub-databases"""
	# TODO

# Copyright (C) 2010, 2011 Sebastian Thiel (byronimo@gmail.com) and contributors
#
# This module is part of GitDB and is released under
# the New BSD License: http://www.opensource.org/licenses/bsd-license.php
"""Contains implementations of database retrieveing objects"""
from gitdb.util import (
		pool,
		join,
		normpath,
		dirname,
		LazyMixin, 
		hex_to_bin
	)

from gitdb.config import GitConfigParser
from gitdb.exc import (
						BadObject, 
						AmbiguousObjectName
						)

from async import (
		ChannelThreadTask
	)

from itertools import chain
import sys
import os


__all__ = (	'ObjectDBR', 'ObjectDBW', 'FileDBBase', 'CompoundDB', 'CachingDB', 
			'TransportDBMixin', 'RefParseMixin', 'ConfigurationMixin', 'RepositoryPathsMixin',  
			'RefSpec', 'FetchInfo', 'PushInfo')


class ObjectDBR(object):
	"""Defines an interface for object database lookup.
	Objects are identified either by their 20 byte bin sha"""
	
	def __contains__(self, sha):
		return self.has_obj
	
	#{ Query Interface 
	def has_object(self, sha):
		"""
		:return: True if the object identified by the given 20 bytes
			binary sha is contained in the database"""
		raise NotImplementedError("To be implemented in subclass")
		
	def has_object_async(self, reader):
		"""Return a reader yielding information about the membership of objects
		as identified by shas
		:param reader: Reader yielding 20 byte shas.
		:return: async.Reader yielding tuples of (sha, bool) pairs which indicate
			whether the given sha exists in the database or not"""
		task = ChannelThreadTask(reader, str(self.has_object_async), lambda sha: (sha, self.has_object(sha)))
		return pool.add_task(task) 
		
	def info(self, sha):
		""" :return: OInfo instance
		:param sha: bytes binary sha
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
		:param sha: 20 bytes binary sha
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
	
	def size(self):
		""":return: amount of objects in this database"""
		raise NotImplementedError()
		
	def sha_iter(self):
		"""Return iterator yielding 20 byte shas for all objects in this data base"""
		raise NotImplementedError()
			
	#} END query interface
	
	
class ObjectDBW(object):
	"""Defines an interface to create objects in the database"""
	
	def __init__(self, *args, **kwargs):
		self._ostream = None
	
	#{ Edit Interface
	def set_ostream(self, stream):
		"""
		Adjusts the stream to which all data should be sent when storing new objects
		
		:param stream: if not None, the stream to use, if None the default stream
			will be used.
		:return: previously installed stream, or None if there was no override
		:raise TypeError: if the stream doesn't have the supported functionality"""
		cstream = self._ostream
		self._ostream = stream
		return cstream
		
	def ostream(self):
		"""
		:return: overridden output stream this instance will write to, or None
			if it will write to the default stream"""
		return self._ostream
	
	def store(self, istream):
		"""
		Create a new object in the database
		:return: the input istream object with its sha set to its corresponding value
		
		:param istream: IStream compatible instance. If its sha is already set 
			to a value, the object will just be stored in the our database format, 
			in which case the input stream is expected to be in object format ( header + contents ).
		:raise IOError: if data could not be written"""
		raise NotImplementedError("To be implemented in subclass")
	
	def store_async(self, reader):
		"""
		Create multiple new objects in the database asynchronously. The method will 
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
		

class CachingDB(object):
	"""A database which uses caches to speed-up access"""
	
	#{ Interface 
	def update_cache(self, force=False):
		"""
		Call this method if the underlying data changed to trigger an update
		of the internal caching structures.
		
		:param force: if True, the update must be performed. Otherwise the implementation
			may decide not to perform an update if it thinks nothing has changed.
		:return: True if an update was performed as something change indeed"""
		
	# END interface




def _databases_recursive(database, output):
	"""Fill output list with database from db, in order. Deals with Loose, Packed 
	and compound databases."""
	if isinstance(database, CompoundDB):
		compounds = list()
		dbs = database.databases()
		output.extend(db for db in dbs if not isinstance(db, CompoundDB))
		for cdb in (db for db in dbs if isinstance(db, CompoundDB)):
			_databases_recursive(cdb, output)
	else:
		output.append(database)
	# END handle database type
	

class CompoundDB(ObjectDBR, LazyMixin, CachingDB):
	"""A database which delegates calls to sub-databases.
	
	Databases are stored in the lazy-loaded _dbs attribute.
	Define _set_cache_ to update it with your databases"""
	def _set_cache_(self, attr):
		if attr == '_dbs':
			self._dbs = list()
		elif attr == '_db_cache':
			self._db_cache = dict()
		else:
			super(CompoundDB, self)._set_cache_(attr)
	
	def _db_query(self, sha):
		""":return: database containing the given 20 byte sha
		:raise BadObject:"""
		# most databases use binary representations, prevent converting 
		# it everytime a database is being queried
		try:
			return self._db_cache[sha]
		except KeyError:
			pass
		# END first level cache
		
		for db in self._dbs:
			if db.has_object(sha):
				self._db_cache[sha] = db
				return db
		# END for each database
		raise BadObject(sha)
	
	#{ ObjectDBR interface 
	
	def has_object(self, sha):
		try:
			self._db_query(sha)
			return True
		except BadObject:
			return False
		# END handle exceptions
		
	def info(self, sha):
		return self._db_query(sha).info(sha)
		
	def stream(self, sha):
		return self._db_query(sha).stream(sha)

	def size(self):
		""":return: total size of all contained databases"""
		return reduce(lambda x,y: x+y, (db.size() for db in self._dbs), 0)
		
	def sha_iter(self):
		return chain(*(db.sha_iter() for db in self._dbs))
		
	#} END object DBR Interface
	
	#{ Interface
	
	def databases(self):
		""":return: tuple of database instances we use for lookups"""
		return tuple(self._dbs)

	def update_cache(self, force=False):
		# something might have changed, clear everything
		self._db_cache.clear()
		stat = False
		for db in self._dbs:
			if isinstance(db, CachingDB):
				stat |= db.update_cache(force)
			# END if is caching db
		# END for each database to update
		return stat
		
	def partial_to_complete_sha_hex(self, partial_hexsha):
		"""
		:return: 20 byte binary sha1 from the given less-than-40 byte hexsha
		:param partial_hexsha: hexsha with less than 40 byte
		:raise AmbiguousObjectName: """
		databases = list()
		_databases_recursive(self, databases)
		
		len_partial_hexsha = len(partial_hexsha)
		if len_partial_hexsha % 2 != 0:
			partial_binsha = hex_to_bin(partial_hexsha + "0")
		else:
			partial_binsha = hex_to_bin(partial_hexsha)
		# END assure successful binary conversion 
		
		candidate = None
		for db in databases:
			full_bin_sha = None
			try:
				if hasattr(db, 'partial_to_complete_sha_hex'):
					full_bin_sha = db.partial_to_complete_sha_hex(partial_hexsha)
				else:
					full_bin_sha = db.partial_to_complete_sha(partial_binsha, len_partial_hexsha)
				# END handle database type
			except BadObject:
				continue
			# END ignore bad objects
			if full_bin_sha:
				if candidate and candidate != full_bin_sha:
					raise AmbiguousObjectName(partial_hexsha)
				candidate = full_bin_sha
			# END handle candidate
		# END for each db
		if not candidate:
			raise BadObject(partial_binsha)
		return candidate
		
	#} END interface
	

class RefSpec(object):
	"""A refspec is a simple container which provides information about the way
	something should be fetched or pushed. It requires to use symbols to describe
	the actual objects which is done using reference names (or respective instances
	which resolve to actual reference names)."""
	__slots__ = ('source', 'destination', 'force')
	
	def __init__(self, source, destination, force=False):
		"""initalize the instance with the required values
		:param source: reference name or instance. If None, the Destination 
			is supposed to be deleted."""
		self.source = source
		self.destination = destination
		self.force = force
		if self.destination is None:
			raise ValueError("Destination must be set")
		
	def __str__(self):
		""":return: a git-style refspec"""
		s = str(self.source)
		if self.source is None:
			s = ''
		#END handle source
		d = str(self.destination)
		p = ''
		if self.force:
			p = '+'
		#END handle force
		res = "%s%s:%s" % (p, s, d)
		
	def delete_destination(self):
		return self.source is None
		
		
class PushInfo(object):
	"""A type presenting information about the result of a push operation for exactly
	one refspec

	flags				# bitflags providing more information about the result
	local_ref			# Reference pointing to the local reference that was pushed
						# It is None if the ref was deleted.
	remote_ref_string 	# path to the remote reference located on the remote side
	remote_ref 			# Remote Reference on the local side corresponding to 
						# the remote_ref_string. It can be a TagReference as well.
	old_commit 			# commit at which the remote_ref was standing before we pushed
						# it to local_ref.commit. Will be None if an error was indicated
	summary				# summary line providing human readable english text about the push
	"""
	__slots__ = tuple()
	
	NEW_TAG, NEW_HEAD, NO_MATCH, REJECTED, REMOTE_REJECTED, REMOTE_FAILURE, DELETED, \
	FORCED_UPDATE, FAST_FORWARD, UP_TO_DATE, ERROR = [ 1 << x for x in range(11) ]
		
		
class FetchInfo(object):
	"""A type presenting information about the fetch operation on exactly one refspec
	
	The following members are defined:
	ref				# name of the reference to the changed 
					# remote head or FETCH_HEAD. Implementations can provide
					# actual class instance which convert to a respective string
	flags			# additional flags to be & with enumeration members, 
					# i.e. info.flags & info.REJECTED 
					# is 0 if ref is FETCH_HEAD
	note				# additional notes given by the fetch-pack implementation intended for the user
	old_commit		# if info.flags & info.FORCED_UPDATE|info.FAST_FORWARD, 
					# field is set to the previous location of ref as hexsha or None
					# Implementors may use their own type too, but it should decay into a
					# string of its hexadecimal sha representation"""
	__slots__ = tuple()
	
	NEW_TAG, NEW_HEAD, HEAD_UPTODATE, TAG_UPDATE, REJECTED, FORCED_UPDATE, \
	FAST_FORWARD, ERROR = [ 1 << x for x in range(8) ]


class TransportDBMixin(object):
	"""A database which allows to transport objects from and to different locations
	which are specified by urls (location) and refspecs (what to transport, 
	see http://www.kernel.org/pub/software/scm/git/docs/git-fetch.html).
	
	At the beginning of a transport operation, it will be determined which objects
	have to be sent (either by this or by the other side).
	
	Afterwards a pack with the required objects is sent (or received). If there is 
	nothing to send, the pack will be empty.
	
	The communication itself if implemented using a protocol instance which deals
	with the actual formatting of the lines sent.
	
	As refspecs involve symbolic names for references to be handled, we require
	RefParse functionality. How this is done is up to the actual implementation."""
	# The following variables need to be set by the derived class
	#{Configuration
	protocol = None
	#}end configuraiton
	
	#{ Interface
	
	def fetch(self, url, refspecs, progress=None, **kwargs):
		"""Fetch the objects defined by the given refspec from the given url.
		:param url: url identifying the source of the objects. It may also be 
			a symbol from which the respective url can be resolved, like the
			name of the remote. The implementation should allow objects as input
			as well, these are assumed to resovle to a meaningful string though.
		:param refspecs: iterable of reference specifiers or RefSpec instance, 
			identifying the references to be fetch from the remote.
		:param progress: callable which receives progress messages for user consumption
		:param kwargs: may be used for additional parameters that the actual implementation could 
			find useful.
		:return: List of FetchInfo compatible instances which provide information about what 
			was previously fetched, in the order of the input refspecs.
		:note: even if the operation fails, one of the returned FetchInfo instances
			may still contain errors or failures in only part of the refspecs.
		:raise: if any issue occours during the transport or if the url is not 
			supported by the protocol.
		"""
		raise NotImplementedError()
		
	def push(self, url, refspecs, progress=None, **kwargs):
		"""Transport the objects identified by the given refspec to the remote
		at the given url.
		:param url: Decribes the location which is to receive the objects
			see fetch() for more details
		:param refspecs: iterable of refspecs strings or RefSpec instances
			to identify the objects to push
		:param progress: see fetch() 
		:param kwargs: additional arguments which may be provided by the caller
			as they may be useful to the actual implementation
		:todo: what to return ?
		:raise: if any issue arises during transport or if the url cannot be handled"""
		raise NotImplementedError()
		
	#}end interface
	

class RefParseMixin(object):
	"""Interface allowing to resolve symbolic names or partial hexadecimal shas into
	actual binary shas. The actual feature set depends on the implementation though, 
	but should follow git-rev-parse."""
	
	def resolve(self, name):
		"""Resolve the given name into a binary sha. Valid names are as defined 
		in the rev-parse documentation http://www.kernel.org/pub/software/scm/git/docs/git-rev-parse.html"""
		raise NotImplementedError()
		
		
class RepositoryPathsMixin(object):
	"""Represents basic functionality of a full git repository. This involves an 
	optional working tree, a git directory with references and an object directory.
	
	This type collects the respective paths and verifies the provided base path 
	truly is a git repository.
	
	If the underlying type provides the config_reader() method, we can properly determine 
	whether this is a bare repository as well."""
	# slots has no effect here, its just to keep track of used attrs
	__slots__  = ("_git_path", '_bare')
	
	#{ Configuration 
	objs_dir = 'objects'
	#} END configuration
	
	#{ Interface
	
	def is_bare(self):
		""":return: True if this is a bare repository
		:note: this value is cached upon initialization"""
		return self._bare
		
	def git_path(self):
		""":return: path to directory containing this actual git repository (which 
		in turn provides access to objects and references"""
		return self._git_path
		
	def working_tree_path(self):
		""":return: path to directory containing the working tree checkout of our 
		git repository.
		:raise AssertionError: If this is a bare repository"""
		if self.is_bare():
			raise AssertionError("Repository at %s is bare and does not have a working tree directory" % self.git_path())
		#END assertion
		return dirname(self.git_path())
		
	def working_dir(self):
		""":return: working directory of the git process or related tools, being 
		either the working_tree_path if available or the git_path"""
		if self.is_bare():
			return self.git_path()
		else:
			return self.working_tree_dir()
		#END handle bare state
		
	#} END interface
		
		
class ConfigurationMixin(object):
	"""Interface providing configuration handler instances, which provide locked access
	to a single git-style configuration file (ini like format, using tabs as improve readablity).
	
	Configuration readers can be initialized with multiple files at once, whose information is concatenated
	when reading. Lower-level files overwrite values from higher level files, i.e. a repository configuration file 
	overwrites information coming from a system configuration file
	
	:note: for this mixin to work, a git_path() compatible type is required"""
	config_level = ("system", "global", "repository")
	
	#{ Configuration
	system_config_file_name = "gitconfig"
	repo_config_file_name = "config"
	#} END 
	
	def _path_at_level(self, level ):
		# we do not support an absolute path of the gitconfig on windows , 
		# use the global config instead
		if sys.platform == "win32" and level == "system":
			level = "global"
		#END handle windows
			
		if level == "system":
			return "/etc/%s" % self.system_config_file_name
		elif level == "global":
			return normpath(os.path.expanduser("~/.%s" % self.system_config_file_name))
		elif level == "repository":
			return join(self.git_path(), self.repo_config_file_name)
		#END handle level
		
		raise ValueError("Invalid configuration level: %r" % level)
		
	#{ Interface
	
	def config_reader(self, config_level=None):
		"""
		:return:
			GitConfigParser allowing to read the full git configuration, but not to write it
			
			The configuration will include values from the system, user and repository 
			configuration files.
			
		:param config_level:
			For possible values, see config_writer method
			If None, all applicable levels will be used. Specify a level in case 
			you know which exact file you whish to read to prevent reading multiple files for 
			instance
		:note: On windows, system configuration cannot currently be read as the path is 
			unknown, instead the global path will be used."""
		files = None
		if config_level is None:
			files = [ self._path_at_level(f) for f in self.config_level ]
		else:
			files = [ self._path_at_level(config_level) ]
		#END handle level
		return GitConfigParser(files, read_only=True)
		
	def config_writer(self, config_level="repository"):
		"""
		:return:
			GitConfigParser allowing to write values of the specified configuration file level.
			Config writers should be retrieved, used to change the configuration ,and written 
			right away as they will lock the configuration file in question and prevent other's
			to write it.
			
		:param config_level:
			One of the following values
			system = sytem wide configuration file
			global = user level configuration file
			repository = configuration file for this repostory only"""
		return GitConfigParser(self._path_at_level(config_level), read_only=False)
	
	#} END interface
	

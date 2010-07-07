from base import (
						CompoundDB, 
						ObjectDBW, 
						FileDBBase
					)

from loose import LooseObjectDB
from pack import PackedDB
from ref import ReferenceDB

from gitdb.util import LazyMixin
from gitdb.exc import (
						InvalidDBRoot, 
						BadObject, 
						AmbiguousObjectName
						)
import os

from gitdb.util import hex_to_bin

__all__ = ('GitDB', )


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
	


class GitDB(FileDBBase, ObjectDBW, CompoundDB):
	"""A git-style object database, which contains all objects in the 'objects'
	subdirectory"""
	# Configuration
	PackDBCls = PackedDB
	LooseDBCls = LooseObjectDB
	ReferenceDBCls = ReferenceDB
	
	# Directories
	packs_dir = 'pack'
	loose_dir = ''
	alternates_dir = os.path.join('info', 'alternates')
	
	def __init__(self, root_path):
		"""Initialize ourselves on a git objects directory"""
		super(GitDB, self).__init__(root_path)
		
	def _set_cache_(self, attr):
		if attr == '_dbs' or attr == '_loose_db':
			self._dbs = list()
			loose_db = None
			for subpath, dbcls in ((self.packs_dir, self.PackDBCls), 
									(self.loose_dir, self.LooseDBCls),
									(self.alternates_dir, self.ReferenceDBCls)):
				path = self.db_path(subpath)
				if os.path.exists(path):
					self._dbs.append(dbcls(path))
					if dbcls is self.LooseDBCls:
						loose_db = self._dbs[-1]
					# END remember loose db
				# END check path exists
			# END for each db type
			
			# should have at least one subdb
			if not self._dbs:
				raise InvalidDBRoot(self.root_path())
			# END handle error
			
			# we the first one should have the store method
			assert loose_db is not None and hasattr(loose_db, 'store'), "First database needs store functionality"
			
			# finally set the value
			self._loose_db = loose_db
		else:
			super(GitDB, self)._set_cache_(attr)
		# END handle attrs
		
	#{ ObjectDBW interface
		
	def store(self, istream):
		return self._loose_db.store(istream)
		
	def ostream(self):
		return self._loose_db.ostream()
	
	def set_ostream(self, ostream):
		return self._loose_db.set_ostream(ostream)
		
	#} END objectdbw interface
	
	#{ Interface 
	
	def partial_to_complete_sha_hex(self, partial_hexsha):
		"""
		:return: 20 byte binary sha1 from the given less-than-40 byte hexsha
		:param partial_hexsha: hexsha with less than 40 byte
		:raise AmbiguousObjectName: """
		databases = list()
		_databases_recursive(self, databases)
		
		if len(partial_hexsha) % 2 != 0:
			partial_binsha = hex_to_bin(partial_hexsha + "0")
		else:
			partial_binsha = hex_to_bin(partial_hexsha)
		# END assure successful binary conversion 
		
		candidate = None
		for db in databases:
			full_bin_sha = None
			try:
				if isinstance(db, LooseObjectDB):
					full_bin_sha = db.partial_to_complete_sha_hex(partial_hexsha)
				else:
					full_bin_sha = db.partial_to_complete_sha(partial_binsha)
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

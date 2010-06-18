from base import (
					CompoundDB,
					FileDBBase,
				)

from loose import LooseObjectDB
from pack import PackedDB
from ref import ReferenceDB

from gitdb.util import LazyMixin
from gitdb.exc import InvalidDBRoot
import os

__all__ = ('GitDB', )

class GitDB(FileDBBase, CompoundDB):
	"""A git-style object database, which contains all objects in the 'objects'
	subdirectory"""
	# Configuration
	PackDBCls = PackedDB
	LooseDBCls = LooseObjectDB
	ReferenceDBCls = ReferenceDB
	
	# Directories
	packs_dir = 'packs'
	loose_dir = ''
	alternates_dir = os.path.join('info', 'alternates')
	
	def __init__(self, root_path):
		"""Initialize ourselves on a git objects directory"""
		super(GitDB, self).__init__(root_path)
		
	def _set_cache_(self, attr):
		if attr == '_dbs':
			self._dbs = list()
			for subpath, dbcls in ((self.packs_dir, self.PackDBCls), 
									(self.loose_dir, self.LooseDBCls),
									(self.alternates_dir, self.ReferenceDBCls)):
				path = self.db_path(subpath)
				if os.path.exists(path):
					self._dbs.append(dbcls(path))
				# END check path exists
			# END for each db type
			
			# should have at least one subdb
			if not self._dbs:
				raise InvalidDBRoot(self.root_path())
		# END handle dbs
		

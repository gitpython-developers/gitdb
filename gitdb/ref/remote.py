from head import Head
from gitdb.util import (
						join, 
						join_path
						)



__all__ = ["RemoteReference"]

	
class RemoteReference(Head):
	"""Represents a reference pointing to a remote head."""
	_common_path_default = "refs/remotes"
	
	
	@classmethod
	def iter_items(cls, repo, common_path = None, remote=None):
		"""Iterate remote references, and if given, constrain them to the given remote"""
		common_path = common_path or cls._common_path_default
		if remote is not None:
			common_path = join_path(common_path, str(remote))
		# END handle remote constraint
		return super(RemoteReference, cls).iter_items(repo, common_path)
	
	@property
	def remote_name(self):
		"""
		:return:
			Name of the remote we are a reference of, such as 'origin' for a reference
			named 'origin/master'"""
		tokens = self.path.split('/')
		# /refs/remotes/<remote name>/<branch_name>
		return tokens[2]
		
	@property
	def remote_head(self):
		""":return: Name of the remote head itself, i.e. master.
		:note: The returned name is usually not qualified enough to uniquely identify
			a branch"""
		tokens = self.path.split('/')
		return '/'.join(tokens[3:])
		
	@classmethod
	def create(cls, *args, **kwargs):
		"""Used to disable this method"""
		raise TypeError("Cannot explicitly create remote references")

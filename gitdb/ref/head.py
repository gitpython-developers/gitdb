from symbolic import SymbolicReference
from reference import Reference
from gitdb.config import SectionConstraint
from gitdb.util import join_path

__all__ = ["HEAD", "Head"]


	
class HEAD(SymbolicReference):
	"""Special case of a Symbolic Reference as it represents the repository's 
	HEAD reference."""
	_HEAD_NAME = 'HEAD'
	_ORIG_HEAD_NAME = 'ORIG_HEAD'
	__slots__ = tuple()
	
	def __init__(self, repo, path=_HEAD_NAME):
		if path != self._HEAD_NAME:
			raise ValueError("HEAD instance must point to %r, got %r" % (self._HEAD_NAME, path))
		super(HEAD, self).__init__(repo, path)
	
	def orig_head(self):
		"""
		:return: SymbolicReference pointing at the ORIG_HEAD, which is maintained 
			to contain the previous value of HEAD"""
		return SymbolicReference(self.repo, self._ORIG_HEAD_NAME)
		

class Head(Reference):
	"""A Head is a named reference to a Commit"""
	_common_path_default = "refs/heads"
	k_config_remote = "remote"
	k_config_remote_ref = "merge"			# branch to merge from remote
	
	# will be set by init method !
	RemoteReferenceCls = None
	
	#{ Configuration
	
	def set_tracking_branch(self, remote_reference):
		"""
		Configure this branch to track the given remote reference. This will alter
			this branch's configuration accordingly.
		
		:param remote_reference: The remote reference to track or None to untrack 
			any references
		:return: self"""
		if remote_reference is not None and not isinstance(remote_reference, self.RemoteReferenceCls):
			raise ValueError("Incorrect parameter type: %r" % remote_reference)
		# END handle type
		
		writer = self.config_writer()
		if remote_reference is None:
			writer.remove_option(self.k_config_remote)
			writer.remove_option(self.k_config_remote_ref)
			if len(writer.options()) == 0:
				writer.remove_section()
			# END handle remove section
		else:
			writer.set_value(self.k_config_remote, remote_reference.remote_name)
			writer.set_value(self.k_config_remote_ref, Head.to_full_path(remote_reference.remote_head))
		# END handle ref value
		
		return self
		
	def tracking_branch(self):
		"""
		:return: The remote_reference we are tracking, or None if we are 
			not a tracking branch"""
		reader = self.config_reader()
		if reader.has_option(self.k_config_remote) and reader.has_option(self.k_config_remote_ref):
			ref = Head(self.repo, Head.to_full_path(reader.get_value(self.k_config_remote_ref)))
			remote_refpath = self.RemoteReferenceCls.to_full_path(join_path(reader.get_value(self.k_config_remote), ref.name))
			return self.RemoteReferenceCls(self.repo, remote_refpath)
		# END handle have tracking branch
		
		# we are not a tracking branch
		return None
	
		
	#{ Configruation
	
	def _config_parser(self, read_only):
		if read_only:
			parser = self.repo.config_reader()
		else:
			parser = self.repo.config_writer()
		# END handle parser instance
		
		return SectionConstraint(parser, 'branch "%s"' % self.name)
	
	def config_reader(self):
		"""
		:return: A configuration parser instance constrained to only read 
			this instance's values"""
		return self._config_parser(read_only=True)
		
	def config_writer(self):
		"""
		:return: A configuration writer instance with read-and write acccess
			to options of this head"""
		return self._config_parser(read_only=False)
	
	#} END configuration
		


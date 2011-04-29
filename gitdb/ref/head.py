from symbolic import SymbolicReference

__all__ = ["HEAD"]

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


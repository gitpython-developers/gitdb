"""Contains library functions"""
import os
from gitdb.test.lib import *
import shutil
import tempfile


#{ Invvariants
k_env_git_repo = "GITDB_TEST_GIT_REPO_BASE"
#} END invariants


#{ Utilities
def resolve_or_fail(env_var):
	""":return: resolved environment variable or raise EnvironmentError"""
	try:
		return os.environ[env_var]
	except KeyError:
		raise EnvironmentError("Please set the %r envrionment variable and retry" % env_var)
	# END exception handling

#} END utilities


#{ Base Classes 

class TestBigRepoR(TestBase):
	"""TestCase providing access to readonly 'big' repositories using the following 
	member variables:
	
	* gitrepopath
	
	 * read-only base path of the git source repository, i.e. .../git/.git"""
	 
	#{ Invariants
	head_sha_2k = '235d521da60e4699e5bd59ac658b5b48bd76ddca'
	head_sha_50 = '32347c375250fd470973a5d76185cac718955fd5'
	#} END invariants 
	
	@classmethod
	def setUpAll(cls):
		try:
			super(TestBigRepoR, cls).setUpAll()
		except AttributeError:
			pass
		cls.gitrepopath = resolve_or_fail(k_env_git_repo)
		assert cls.gitrepopath.endswith('.git')

		
#} END base classes

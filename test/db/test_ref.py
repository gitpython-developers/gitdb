from lib import *
from gitdb.db import ReferenceDB
		
class TestReferenceDB(TestBase):
	
	@with_rw_directory
	def test_writing(self, path):
		# TODO: setup alternate file
		alternates = 
		ldb = ReferenceDB(path)
		
		# try empty, non-existing
		
		# add two, one is invalid
		
		# remove valid
		
		# add valid
		
		self.fail("todo")
		

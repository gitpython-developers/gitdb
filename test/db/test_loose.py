from lib import *
from gitdb.db import LooseObjectDB
		
class TestLooseDB(TestDBBase):
	
	@with_rw_directory
	def test_writing(self, path):
		ldb = LooseObjectDB(path)
		
		# write data
		self._assert_object_writing(ldb)
		self._assert_object_writing_async(ldb)
	

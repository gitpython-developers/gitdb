from lib import *
from gitdb.db import LooseObjectDB
		
class TestLooseDB(TestDBBase):
	
	@with_rw_directory
	def test_writing(self, path):
		ldb = LooseObjectDB(path)
		
		# write data
		self._assert_object_writing(ldb)
		self._assert_object_writing_async(ldb)
	
		# verify sha iteration and size
		shas = list(ldb.sha_iter())
		assert shas and len(shas[0]) == 20
		
		assert len(shas) == ldb.size()

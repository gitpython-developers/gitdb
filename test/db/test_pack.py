from lib import *
from gitdb.db import PackedDB
		
class TestPackDB(TestDBBase):
	
	@with_rw_directory
	@with_packs_rw
	def test_writing(self, path):
		ldb = PackedDB(path)
		# TODO
		
	

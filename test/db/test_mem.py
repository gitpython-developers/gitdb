from lib import *
from gitdb.db import MemoryDB
		
class TestMemoryDB(TestDBBase):
	
	def test_writing(self):
		mdb = MemoryDB()
		
		# write data
		self._assert_object_writing_simple(mdb)

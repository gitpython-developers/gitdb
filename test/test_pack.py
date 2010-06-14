"""Test everything about packs reading and writing"""

from lib import (
					TestBase,
					with_rw_directory, 
					with_packs
				)
					

class TestPack(TestBase):
	
	@with_rw_directory
	@with_packs
	def test_reading(self, pack_dir):
		# initialze a pack file for reading
		pass

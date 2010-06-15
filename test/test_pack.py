"""Test everything about packs reading and writing"""
from lib import (
					TestBase,
					with_rw_directory, 
					with_packs_rw,
					fixture_path
				)
from gitdb.pack import (
							PackIndex
						)
import os


class TestPack(TestBase):
	
	packindexfile_v2 = fixture_path('packs/pack-11fdfa9e156ab73caae3b6da867192221f2089c2.idx')
	packindexfile_v1 = fixture_path('packs/pack-c0438c19fb16422b6bbcce24387b3264416d485b.idx')
	
	def _assert_index_file(self, index, version, size):
		assert index.packfile_checksum != index.indexfile_checksum
		assert index.version == version
		assert index.size == size
		
		# get all data of all objects
		for oidx in xrange(index.size):
			sha = index.sha(oidx)
			assert oidx == index.sha_to_index(sha)
			
			entry = index.entry(oidx)
			assert len(entry) == 3
			
			assert entry[0] == index.offset(oidx)
			assert entry[1] == sha
			assert entry[2] == index.crc(oidx)
		# END for each object index in indexfile
		
	
	def test_pack_index(self):
		# check version 1 and 2
		index = PackIndex(self.packindexfile_v1)
		self._assert_index_file(index, 1, 67)
		
		index = PackIndex(self.packindexfile_v2)
		self._assert_index_file(index, 2, 30)
		
		
		

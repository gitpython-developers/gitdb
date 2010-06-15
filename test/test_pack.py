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
	
	def test_pack_index(self):
		# read v2 index information
		index_file = fixture_path('packs/pack-11fdfa9e156ab73caae3b6da867192221f2089c2.idx')
		index = PackIndex(index_file)
		
		assert index.packfile_checksum != index.indexfile_checksum
		assert index.version == 2
		assert index.size == 30
		
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
		
		

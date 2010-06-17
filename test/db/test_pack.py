from lib import *
from gitdb.db import PackedDB
from gitdb.test.lib import fixture_path

import os
import random

class TestPackDB(TestDBBase):
	
	@with_rw_directory
	@with_packs_rw
	def test_writing(self, path):
		pdb = PackedDB(path)
		
		# on demand, we init our pack cache
		num_packs = 2
		assert len(pdb._entities) == num_packs
		assert pdb._st_mtime != 0
		
		# test pack directory changed: 
		# packs removed - rename a file, should affect the glob
		pack_path = pdb._entities[0][1].pack().path()
		new_pack_path = pack_path + "renamed"
		os.rename(pack_path, new_pack_path)
		
		pdb.update_pack_entity_cache(force=True)
		assert len(pdb._entities) == num_packs - 1
		
		# packs added
		os.rename(new_pack_path, pack_path)
		pdb.update_pack_entity_cache(force=True)
		assert len(pdb._entities) == num_packs
	
		# bang on the cache
		# access the Entities directly, as there is no iteration interface
		# yet ( or required for now )
		sha_list = list()
		for entity in (item[1] for item in pdb._entities):
			for index in xrange(entity.index().size()):
				
				sha_list.append(entity.index().sha(index))
			# END for each index
		# END for each entity
		
		# hit all packs in random order
		random.shuffle(sha_list)
		
		for sha in sha_list:
			info = pdb.info(sha)
			stream = pdb.stream(sha)
		# END for each sha to query

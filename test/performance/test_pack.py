"""Performance tests for object store"""
from lib import (
	TestBigRepoR 
	)

from gitdb.db.pack import PackedDB

import sys
import os
from time import time
import random

class TestPackedDBPerformance(TestBigRepoR):
	
	def test_pack_random_access(self):
		pdb = PackedDB(os.path.join(self.gitrepopath, "objects/pack"))
		
		# sha lookup
		st = time()
		sha_list = list(pdb.sha_iter())
		elapsed = time() - st
		ns = len(sha_list)
		print >> sys.stderr, "PDB: looked up %i shas by index in %f s ( %f shas/s )" % (ns, elapsed, ns / elapsed)
		
		# sha lookup: best-case and worst case access
		pdb_pack_info = pdb._pack_info
		access_times = list()
		for rand in range(2):
			if rand:
				random.shuffle(sha_list)
			# END shuffle shas
			st = time()
			for sha in sha_list:
				pdb_pack_info(sha)
			# END for each sha to look up
			elapsed = time() - st
			access_times.append(elapsed)
			
			# discard cache
			del(pdb._entities)
			pdb.entities()
			print >> sys.stderr, "PDB: looked up %i sha (random=%i) in %f s ( %f shas/s )" % (ns, rand, elapsed, ns / elapsed)
		# END for each random mode
		elapsed_order, elapsed_rand = access_times
		
		# well, its never really sequencial regarding the memory patterns, but it 
		# shows how well the prioriy cache performs
		print >> sys.stderr, "PDB: sequential access is %f %% faster than random-access" % (100 - ((elapsed_order / elapsed_rand) * 100))
		
		
		# query info and streams only
		max_items = 10000			# can wait longer when testing memory
		for pdb_fun in (pdb.info, pdb.stream):
			st = time()
			for sha in sha_list[:max_items]:
				pdb_fun(sha)
			elapsed = time() - st
			print >> sys.stderr, "PDB: Obtained %i object %s by sha in %f s ( %f info/s )" % (max_items, pdb_fun.__name__.upper(), elapsed, max_items / elapsed)
		# END for each function
		
		# retrieve stream and read all
		max_items = 5000
		pdb_stream = pdb.stream
		total_size = 0
		st = time()
		for sha in sha_list[:max_items]:
			stream = pdb_stream(sha)
			stream.read()
			total_size += stream.size
		elapsed = time() - st
		total_kib = total_size / 1000
		print >> sys.stderr, "PDB: Obtained %i streams by sha and read all bytes totallying %i KiB ( %f KiB / s ) in %f s ( %f streams/s )" % (max_items, total_kib, total_kib/elapsed , elapsed, max_items / elapsed)
		
	
		print >> sys.stderr, "Endurance run: verify streaming of %i objects (crc and sha)" % ns
		for crc in range(2):
			count = 0
			st = time()
			for entity in pdb.entities():
				pack_verify = entity.is_valid_stream
				sha_by_index = entity.index().sha
				for index in xrange(entity.index().size()):
					try:
						assert pack_verify(sha_by_index(index), use_crc=crc)
					except UnsupportedOperation:
						pass
					# END ignore old indices
					count += 1
				# END for each index
			# END for each entity
			elapsed = time() - st
			print >> sys.stderr, "PDB: verified %i objects (crc=%i) in %f s ( %f objects/s )" % (count, crc, elapsed, count / elapsed)
		# END for each verify mode
		

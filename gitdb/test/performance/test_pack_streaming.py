"""Specific test for pack streams only"""
from lib import (
	TestBigRepoR 
	)

from gitdb.db.pack import PackedDB

import os
import sys
from time import time

class TestPackStreamingPerformance(TestBigRepoR):
	
	def test_stream_reading(self):
		pdb = PackedDB(os.path.join(self.gitrepopath, "objects/pack"))
		
		# streaming only, meant for --with-profile runs
		ni = 5000
		count = 0
		pdb_stream = pdb.stream
		total_size = 0
		st = time()
		for sha in pdb.sha_iter():
			if count == ni:
				break
			stream = pdb_stream(sha)
			stream.read()
			total_size += stream.size
			count += 1
		elapsed = time() - st
		total_kib = total_size / 1000
		print >> sys.stderr, "PDB Streaming: Got %i streams by sha and read all bytes totallying %i KiB ( %f KiB / s ) in %f s ( %f streams/s )" % (ni, total_kib, total_kib/elapsed , elapsed, ni / elapsed)
		

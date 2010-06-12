"""Performance data streaming performance"""

from lib import TestBigRepoR
from gitdb.db import *
from gitdb.stream import *

from cStringIO import StringIO
from time import time
import os
import sys
import stat
import subprocess


from lib import (
	TestBigRepoR,
	make_memory_file,
	with_rw_directory
	)


class TestObjDBPerformance(TestBigRepoR):
	
	large_data_size_bytes = 1000*1000*10		# some MiB should do it
	moderate_data_size_bytes = 1000*1000*1		# just 1 MiB
	
	@with_rw_directory
	def test_large_data_streaming(self, path):
		ldb = LooseObjectDB(path)
		
		for randomize in range(2):
			desc = (randomize and 'random ') or ''
			print >> sys.stderr, "Creating %s data ..." % desc
			st = time()
			size, stream = make_memory_file(self.large_data_size_bytes, randomize)
			elapsed = time() - st
			print >> sys.stderr, "Done (in %f s)" % elapsed
			
			# writing - due to the compression it will seem faster than it is 
			st = time()
			sha = ldb.store(IStream('blob', size, stream)).sha
			elapsed_add = time() - st
			assert ldb.has_object(sha)
			db_file = ldb.readable_db_object_path(sha)
			fsize_kib = os.path.getsize(db_file) / 1000
			
			
			size_kib = size / 1000
			print >> sys.stderr, "Added %i KiB (filesize = %i KiB) of %s data to loose odb in %f s ( %f Write KiB / s)" % (size_kib, fsize_kib, desc, elapsed_add, size_kib / elapsed_add)
			
			# reading all at once
			st = time()
			ostream = ldb.stream(sha)
			shadata = ostream.read()
			elapsed_readall = time() - st
			
			stream.seek(0)
			assert shadata == stream.getvalue()
			print >> sys.stderr, "Read %i KiB of %s data at once from loose odb in %f s ( %f Read KiB / s)" % (size_kib, desc, elapsed_readall, size_kib / elapsed_readall)
			
			
			# reading in chunks of 1 MiB
			cs = 512*1000
			chunks = list()
			st = time()
			ostream = ldb.stream(sha)
			while True:
				data = ostream.read(cs)
				chunks.append(data)
				if len(data) < cs:
					break
			# END read in chunks
			elapsed_readchunks = time() - st
			
			stream.seek(0)
			assert ''.join(chunks) == stream.getvalue()
			
			cs_kib = cs / 1000
			print >> sys.stderr, "Read %i KiB of %s data in %i KiB chunks from loose odb in %f s ( %f Read KiB / s)" % (size_kib, desc, cs_kib, elapsed_readchunks, size_kib / elapsed_readchunks)
			
			# del db file so git has something to do
			os.remove(db_file)
			
		# END for each randomization factor

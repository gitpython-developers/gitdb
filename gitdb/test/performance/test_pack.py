# Copyright (C) 2010, 2011 Sebastian Thiel (byronimo@gmail.com) and contributors
#
# This module is part of GitDB and is released under
# the New BSD License: http://www.opensource.org/licenses/bsd-license.php
"""Performance tests for object store"""
from lib import (
    TestBigRepoR
)

from gitdb.exc import UnsupportedOperation
from gitdb.db.pack import PackedDB

import sys
import os
from time import time

from nose import SkipTest


class TestPackedDBPerformance(TestBigRepoR):

    def test_pack_random_access(self):
        pdb = PackedDB(os.path.join(self.gitrepopath, "objects/pack"))

        # sha lookup
        st = time()
        sha_list = list(pdb.sha_iter())
        elapsed = time() - st
        ns = len(sha_list)
        print >> sys.stderr, "PDB: looked up %i shas by index in %f s ( %f shas/s )" % (
            ns, elapsed, ns / elapsed)

        # sha lookup: best-case and worst case access
        pdb_pack_info = pdb._pack_info
        # END shuffle shas
        st = time()
        for sha in sha_list:
            pdb_pack_info(sha)
        # END for each sha to look up
        elapsed = time() - st

        # discard cache
        del(pdb._entities)
        pdb.entities()
        print >> sys.stderr, "PDB: looked up %i sha in %i packs in %f s ( %f shas/s )" % (
            ns, len(pdb.entities()), elapsed, ns / elapsed)
        # END for each random mode

        # query info and streams only
        max_items = 10000           # can wait longer when testing memory
        for pdb_fun in (pdb.info, pdb.stream):
            st = time()
            for sha in sha_list[:max_items]:
                pdb_fun(sha)
            elapsed = time() - st
            print >> sys.stderr, "PDB: Obtained %i object %s by sha in %f s ( %f items/s )" % (
                max_items, pdb_fun.__name__.upper(), elapsed, max_items / elapsed)
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
        print >> sys.stderr, "PDB: Obtained %i streams by sha and read all bytes totallying %i KiB ( %f KiB / s ) in %f s ( %f streams/s )" % (
            max_items, total_kib, total_kib / elapsed, elapsed, max_items / elapsed)

    def test_correctness(self):
        raise SkipTest(
            "Takes too long, enable it if you change the algorithm and want to be sure you decode packs correctly")
        pdb = PackedDB(os.path.join(self.gitrepopath, "objects/pack"))
        # disabled for now as it used to work perfectly, checking big
        # repositories takes a long time
        print >> sys.stderr, "Endurance run: verify streaming of objects (crc and sha)"
        for crc in range(2):
            count = 0
            st = time()
            for entity in pdb.entities():
                pack_verify = entity.is_valid_stream
                sha_by_index = entity.index().sha
                for index in xrange(entity.index().size()):
                    try:
                        assert pack_verify(sha_by_index(index), use_crc=crc)
                        count += 1
                    except UnsupportedOperation:
                        pass
                    # END ignore old indices
                # END for each index
            # END for each entity
            elapsed = time() - st
            print >> sys.stderr, "PDB: verified %i objects (crc=%i) in %f s ( %f objects/s )" % (
                count, crc, elapsed, count / elapsed)
        # END for each verify mode

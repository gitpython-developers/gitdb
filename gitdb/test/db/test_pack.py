# Copyright (C) 2010, 2011 Sebastian Thiel (byronimo@gmail.com) and contributors
#
# This module is part of GitDB and is released under
# the New BSD License: http://www.opensource.org/licenses/bsd-license.php
from gitdb.test.db.lib import (
    TestDBBase,
    with_rw_directory,
    with_packs_rw
)
from gitdb.db import PackedDB

from gitdb.exc import BadObject, AmbiguousObjectName

import os
import random
from gitdb.util import mman
from gitdb.test import HIDE_WINDOWS_KNOWN_ERRORS


class TestPackDB(TestDBBase):

    ## Unless HIDE_WINDOWS_KNOWN_ERRORS, on Windows fails with:
    # File "D:\Work\gitdb.git\gitdb\test\db\test_pack.py", line 41, in test_writing
    #    os.rename(pack_path, new_pack_path)
    # PermissionError: [WinError 32] The process cannot access the file
    #    because it is being used by another process: 'pack-c0438c19fb16422b6bbcce24387b3264416d485b.packrenamed'
    @with_rw_directory
    @with_packs_rw
    def test_writing(self, path):
        pdb = PackedDB(path)

        # on demand, we init our pack cache
        num_packs = len(pdb.entities())
        assert pdb._st_mtime != 0

        # test pack directory changed:
        # packs removed - rename a file, should affect the glob
        pack_path = pdb.entities()[0].pack().path()
        new_pack_path = pack_path + "renamed"
        ## FIXME: Had to manually collect leaked files!!
        if HIDE_WINDOWS_KNOWN_ERRORS:
            leaked_mmaps = mman.collect()
            self.assertEqual(leaked_mmaps, 6)
        os.rename(pack_path, new_pack_path)

        pdb.update_cache(force=True)
        assert len(pdb.entities()) == num_packs - 1

        # packs added
        os.rename(new_pack_path, pack_path)
        pdb.update_cache(force=True)
        assert len(pdb.entities()) == num_packs

        # bang on the cache
        # access the Entities directly, as there is no iteration interface
        # yet ( or required for now )
        sha_list = list(pdb.sha_iter())
        assert len(sha_list) == pdb.size()

        # hit all packs in random order
        random.shuffle(sha_list)

        for sha in sha_list:
            pdb.info(sha)
            with pdb.stream(sha):
                pass
        # END for each sha to query

        # test short finding - be a bit more brutal here
        max_bytes = 19
        min_bytes = 2
        num_ambiguous = 0
        for i, sha in enumerate(sha_list):
            short_sha = sha[:max((i % max_bytes), min_bytes)]
            try:
                assert pdb.partial_to_complete_sha(short_sha, len(short_sha) * 2) == sha
            except AmbiguousObjectName:
                num_ambiguous += 1
                pass  # valid, we can have short objects
            # END exception handling
        # END for each sha to find

        # we should have at least one ambiguous, considering the small sizes
        # but in our pack, there is no ambigious ...
        # assert num_ambiguous

        # non-existing
        self.failUnlessRaises(BadObject, pdb.partial_to_complete_sha, b'\0\0', 4)

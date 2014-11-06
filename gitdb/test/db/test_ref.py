# Copyright (C) 2010, 2011 Sebastian Thiel (byronimo@gmail.com) and contributors
#
# This module is part of GitDB and is released under
# the New BSD License: http://www.opensource.org/licenses/bsd-license.php
from .lib import *
from gitdb.db import ReferenceDB

from gitdb.util import (
    NULL_BIN_SHA,
    hex_to_bin
)

import os


class TestReferenceDB(TestDBBase):

    def make_alt_file(self, alt_path, alt_list):
        """Create an alternates file which contains the given alternates.
        The list can be empty"""
        alt_file = open(alt_path, "wb")
        for alt in alt_list:
            alt_file.write(alt + "\n")
        alt_file.close()

    @with_rw_directory
    def test_writing(self, path):

        alt_path = os.path.join(path, 'alternates')
        rdb = ReferenceDB(alt_path)
        assert len(rdb.databases()) == 0
        assert rdb.size() == 0
        assert len(list(rdb.sha_iter())) == 0

        # try empty, non-existing
        assert not rdb.has_object(NULL_BIN_SHA)

        # setup alternate file
        # add two, one is invalid
        own_repo_path = fixture_path(
            '../../../.git/objects')       # use own repo
        self.make_alt_file(alt_path, [own_repo_path, "invalid/path"])
        rdb.update_cache()
        assert len(rdb.databases()) == 1

        # we should now find a default revision of ours
        gitdb_sha = hex_to_bin("5690fd0d3304f378754b23b098bd7cb5f4aa1976")
        assert rdb.has_object(gitdb_sha)

        # remove valid
        self.make_alt_file(alt_path, ["just/one/invalid/path"])
        rdb.update_cache()
        assert len(rdb.databases()) == 0

        # add valid
        self.make_alt_file(alt_path, [own_repo_path])
        rdb.update_cache()
        assert len(rdb.databases()) == 1

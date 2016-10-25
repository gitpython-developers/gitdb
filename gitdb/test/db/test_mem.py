# Copyright (C) 2010, 2011 Sebastian Thiel (byronimo@gmail.com) and contributors
#
# This module is part of GitDB and is released under
# the New BSD License: http://www.opensource.org/licenses/bsd-license.php
from gitdb.db import (
    MemoryDB,
    LooseObjectDB
)
from gitdb.test.db.lib import (
    TestDBBase,
    with_rw_directory
)


class TestMemoryDB(TestDBBase):

    @with_rw_directory
    def test_writing(self, path):
        mdb = MemoryDB()

        # write data
        self._assert_object_writing_simple(mdb)

        # test stream copy
        ldb = LooseObjectDB(path)
        assert ldb.size() == 0
        num_streams_copied = mdb.stream_copy(mdb.sha_iter(), ldb)
        assert num_streams_copied == mdb.size()

        assert ldb.size() == mdb.size()
        for sha in mdb.sha_iter():
            assert ldb.has_object(sha)
            with ldb.stream(sha) as st1:
                with mdb.stream(sha) as st2:
                    self.assertEqual(st1.read(), st2.read())
        # END verify objects where copied and are equal

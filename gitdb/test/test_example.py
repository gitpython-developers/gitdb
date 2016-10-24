# Copyright (C) 2010, 2011 Sebastian Thiel (byronimo@gmail.com) and contributors
#
# This module is part of GitDB and is released under
# the New BSD License: http://www.opensource.org/licenses/bsd-license.php
"""Module with examples from the tutorial section of the docs"""
import os
from gitdb.test.lib import TestBase
from gitdb import IStream
from gitdb.db import LooseObjectDB

from io import BytesIO


class TestExamples(TestBase):

    def test_base(self):
        ldb = LooseObjectDB(os.path.join(self.gitrepopath, 'objects'))

        for sha1 in ldb.sha_iter():
            oinfo = ldb.info(sha1)
            with ldb.stream(sha1) as ostream:
                assert oinfo[:3] == ostream[:3]

                assert len(ostream.read()) == ostream.size
            assert ldb.has_object(oinfo.binsha)
        # END for each sha in database

        data = "my data".encode("ascii")
        istream = IStream("blob", len(data), BytesIO(data))

        # the object does not yet have a sha
        assert istream.binsha is None
        with ldb.store(istream):
            # now the sha is set
            assert len(istream.binsha) == 20
            assert ldb.has_object(istream.binsha)

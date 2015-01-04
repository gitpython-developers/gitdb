# Copyright (C) 2010, 2011 Sebastian Thiel (byronimo@gmail.com) and contributors
#
# This module is part of GitDB and is released under
# the New BSD License: http://www.opensource.org/licenses/bsd-license.php
"""Contains library functions"""
import os
import logging
from gitdb.test.lib import TestBase


#{ Invvariants
k_env_git_repo = "GITDB_TEST_GIT_REPO_BASE"
#} END invariants


#{ Base Classes

class TestBigRepoR(TestBase):

    """TestCase providing access to readonly 'big' repositories using the following 
    member variables:

    * gitrepopath

     * read-only base path of the git source repository, i.e. .../git/.git"""

    def setUp(self):
        try:
            super(TestBigRepoR, self).setUp()
        except AttributeError:
            pass

        self.gitrepopath = os.environ.get(k_env_git_repo)
        if not self.gitrepopath:
            logging.info(
                "You can set the %s environment variable to a .git repository of your choice - defaulting to the gitdb repository", k_env_git_repo)
            ospd = os.path.dirname
            self.gitrepopath = os.path.join(ospd(ospd(ospd(ospd(__file__)))), '.git')
        # end assure gitrepo is set
        assert self.gitrepopath.endswith('.git')


#} END base classes

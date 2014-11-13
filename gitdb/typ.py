# Copyright (C) 2010, 2011 Sebastian Thiel (byronimo@gmail.com) and contributors
#
# This module is part of GitDB and is released under
# the New BSD License: http://www.opensource.org/licenses/bsd-license.php
"""Module containing information about types known to the database"""

from gitdb.utils.encoding import force_bytes

str_blob_type = force_bytes("blob")
str_commit_type = force_bytes("commit")
str_tree_type = force_bytes("tree")
str_tag_type = force_bytes("tag")

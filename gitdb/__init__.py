# Copyright (C) 2010, 2011 Sebastian Thiel (byronimo@gmail.com) and contributors
#
# This module is part of GitDB and is released under
# the New BSD License: http://www.opensource.org/licenses/bsd-license.php
"""Initialize the object database module"""

import sys
import os

__author__ = "Sebastian Thiel"
__contact__ = "byronimo@gmail.com"
__homepage__ = "https://github.com/gitpython-developers/gitdb"
version_info = (2, 1, 0, 'dev1')
__version__ = '.'.join(str(i) for i in version_info)


# default imports
from gitdb.base import *
from gitdb.db import *
from gitdb.stream import *

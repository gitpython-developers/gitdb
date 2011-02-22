# Copyright (C) 2010, 2011 Sebastian Thiel (byronimo@gmail.com) and contributors
#
# This module is part of GitDB and is released under
# the New BSD License: http://www.opensource.org/licenses/bsd-license.php
"""Initialize the object database module"""

import sys
import os

#{ Initialization
def _init_externals():
	"""Initialize external projects by putting them into the path"""
	sys.path.append(os.path.join(os.path.dirname(__file__), 'ext', 'async'))
	
	try:
		import async
	except ImportError:
		raise ImportError("'async' could not be imported, assure it is located in your PYTHONPATH")
	#END verify import
	
#} END initialization

_init_externals()

# default imports
from db import *
from base import *
from stream import *


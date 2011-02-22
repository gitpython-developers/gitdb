# Copyright (C) 2010, 2011 Sebastian Thiel (byronimo@gmail.com) and contributors
#
# This module is part of GitDB and is released under
# the New BSD License: http://www.opensource.org/licenses/bsd-license.php

import gitdb.util

#{ Initialization 
def _init_pool():
	"""Assure the pool is actually threaded"""
	size = 2
	print "Setting ThreadPool to %i" % size
	gitdb.util.pool.set_size(size)


#} END initialization

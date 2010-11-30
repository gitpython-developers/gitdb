
import gitdb.util

#{ Initialization 
def _init_pool():
	"""Assure the pool is actually threaded"""
	size = 2
	print "Setting ThreadPool to %i" % size
	gitdb.util.pool.set_size(size)


#} END initialization

"""Performance tests for object store"""

import sys
from time import time

from lib import (
	TestBigRepoR
	)

class TestGitDBPerformance(TestBigRepoR):
	
	def test_random_access(self):
		pass
		# TODO: use the actual db for this
		

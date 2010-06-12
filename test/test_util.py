"""Test for object db"""
from lib import TestBase
from gitdb.util import (
	to_hex_sha, 
	to_bin_sha, 
	NULL_HEX_SHA
	)

	
class TestUtils(TestBase):
	def test_basics(self):
		assert to_hex_sha(NULL_HEX_SHA) == NULL_HEX_SHA
		assert len(to_bin_sha(NULL_HEX_SHA)) == 20
		assert to_hex_sha(to_bin_sha(NULL_HEX_SHA)) == NULL_HEX_SHA


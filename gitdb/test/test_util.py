# Copyright (C) 2010, 2011 Sebastian Thiel (byronimo@gmail.com) and contributors
#
# This module is part of GitDB and is released under
# the New BSD License: http://www.opensource.org/licenses/bsd-license.php
"""Test for object db"""
import tempfile
import os

from lib import TestBase
from gitdb.util import (
	to_hex_sha, 
	to_bin_sha, 
	NULL_HEX_SHA, 
	LockedFD
	)

	
class TestUtils(TestBase):
	def test_basics(self):
		assert to_hex_sha(NULL_HEX_SHA) == NULL_HEX_SHA
		assert len(to_bin_sha(NULL_HEX_SHA)) == 20
		assert to_hex_sha(to_bin_sha(NULL_HEX_SHA)) == NULL_HEX_SHA
		
	def _cmp_contents(self, file_path, data):
		# raise if data from file at file_path 
		# does not match data string
		fp = open(file_path, "rb")
		try:
			assert fp.read() == data
		finally:
			fp.close()
		
	def test_lockedfd(self):
		my_file = tempfile.mktemp()
		orig_data = "hello"
		new_data = "world"
		my_file_fp = open(my_file, "wb")
		my_file_fp.write(orig_data)
		my_file_fp.close()
		
		try:
			lfd = LockedFD(my_file)
			lockfilepath = lfd._lockfilepath() 
			
			# cannot end before it was started
			self.failUnlessRaises(AssertionError, lfd.rollback)
			self.failUnlessRaises(AssertionError, lfd.commit)
			
			# open for writing
			assert not os.path.isfile(lockfilepath)
			wfd = lfd.open(write=True)
			assert lfd._fd is wfd
			assert os.path.isfile(lockfilepath)
			
			# write data and fail
			os.write(wfd, new_data)
			lfd.rollback()
			assert lfd._fd is None
			self._cmp_contents(my_file, orig_data)
			assert not os.path.isfile(lockfilepath)
			
			# additional call doesnt fail
			lfd.commit()
			lfd.rollback()
			
			# test reading
			lfd = LockedFD(my_file)
			rfd = lfd.open(write=False)
			assert os.read(rfd, len(orig_data)) == orig_data
			
			assert os.path.isfile(lockfilepath)
			# deletion rolls back
			del(lfd)
			assert not os.path.isfile(lockfilepath)
			
			
			# write data - concurrently
			lfd = LockedFD(my_file)
			olfd = LockedFD(my_file)
			assert not os.path.isfile(lockfilepath)
			wfdstream = lfd.open(write=True, stream=True)		# this time as stream
			assert os.path.isfile(lockfilepath)
			# another one fails
			self.failUnlessRaises(IOError, olfd.open)
			
			wfdstream.write(new_data)
			lfd.commit()
			assert not os.path.isfile(lockfilepath)
			self._cmp_contents(my_file, new_data)
			
			# could test automatic _end_writing on destruction
		finally:
			os.remove(my_file)
		# END final cleanup
		
		# try non-existing file for reading
		lfd = LockedFD(tempfile.mktemp())
		try:
			lfd.open(write=False)
		except OSError:
			assert not os.path.exists(lfd._lockfilepath())
		else:
			self.fail("expected OSError")
		# END handle exceptions


class TestActor(TestBase):
    def test_from_string_should_separate_name_and_email(self):
        a = Actor._from_string("Michael Trier <mtrier@example.com>")
        assert_equal("Michael Trier", a.name)
        assert_equal("mtrier@example.com", a.email)
        
        # base type capabilities
        assert a == a
        assert not ( a != a )
        m = set()
        m.add(a)
        m.add(a)
        assert len(m) == 1

    def test_from_string_should_handle_just_name(self):
        a = Actor._from_string("Michael Trier")
        assert_equal("Michael Trier", a.name)
        assert_equal(None, a.email)

    def test_should_display_representation(self):
        a = Actor._from_string("Michael Trier <mtrier@example.com>")
        assert_equal('<git.Actor "Michael Trier <mtrier@example.com>">', repr(a))

    def test_str_should_alias_name(self):
        a = Actor._from_string("Michael Trier <mtrier@example.com>")
        assert_equal(a.name, str(a))

"""Test for object db"""
from lib import (
		TestBase,
		DummyStream,
		DeriveTest, 
	)

from gitdb import *
from gitdb.util import (
	NULL_HEX_SHA
	)

from gitdb.typ import (
	str_blob_type
	)


class TestBaseTypes(TestBase):
	
	def test_streams(self):
		# test info
		sha = NULL_HEX_SHA
		s = 20
		info = OInfo(sha, str_blob_type, s)
		assert info.sha == sha
		assert info.type == str_blob_type
		assert info.size == s
		
		# test pack info
		# provides type_id
		blob_id = 3
		pinfo = OPackInfo(blob_id, s)
		assert pinfo.type == str_blob_type
		assert pinfo.type_id == blob_id
		
		dpinfo = ODeltaPackInfo(blob_id, s, sha)
		assert dpinfo.type == str_blob_type
		assert dpinfo.type_id == blob_id
		assert dpinfo.delta_info == sha
		
		
		# test ostream
		stream = DummyStream()
		ostream = OStream(*(info + (stream, )))
		assert ostream.stream is stream
		ostream.read(15)
		stream._assert()
		assert stream.bytes == 15
		ostream.read(20)
		assert stream.bytes == 20
		
		# test packstream
		postream = OPackStream(*(pinfo + (stream, )))
		assert postream.stream is stream
		postream.read(10)
		stream._assert()
		assert stream.bytes == 10
		
		# test deltapackstream
		dpostream = ODeltaPackStream(*(dpinfo + (stream, )))
		dpostream.stream is stream
		dpostream.read(5)
		stream._assert()
		assert stream.bytes == 5
		
		# derive with own args
		DeriveTest(sha, str_blob_type, s, stream, 'mine',myarg = 3)._assert()
		
		# test istream
		istream = IStream(str_blob_type, s, stream)
		assert istream.sha == None
		istream.sha = sha
		assert istream.sha == sha
		
		assert len(istream.binsha) == 20
		assert len(istream.hexsha) == 40
		
		assert istream.size == s
		istream.size = s * 2
		istream.size == s * 2
		assert istream.type == str_blob_type
		istream.type = "something"
		assert istream.type == "something"
		assert istream.stream is stream
		istream.stream = None
		assert istream.stream is None
		
		assert istream.error is None
		istream.error = Exception()
		assert isinstance(istream.error, Exception)

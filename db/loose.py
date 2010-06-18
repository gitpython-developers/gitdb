from base import (
						FileDBBase, 
						ObjectDBR, 
						ObjectDBW
				)


from gitdb.exc import (
	InvalidDBRoot, 
	BadObject, 
	)

from gitdb.stream import (
		DecompressMemMapReader,
		FDCompressedSha1Writer,
		Sha1Writer
	)

from gitdb.base import (
							OStream,
							OInfo
						)

from gitdb.util import (
		ENOENT,
		to_hex_sha,
		exists,
		isdir,
		mkdir,
		rename,
		dirname,
		join
	)

from gitdb.fun import ( 
	chunk_size,
	loose_object_header_info, 
	write_object,
	stream_copy
	)

import tempfile
import mmap
import os


__all__ = ( 'LooseObjectDB', )


class LooseObjectDB(FileDBBase, ObjectDBR, ObjectDBW):
	"""A database which operates on loose object files"""
	
	# CONFIGURATION
	# chunks in which data will be copied between streams
	stream_chunk_size = chunk_size
	
	
	def __init__(self, root_path):
		super(LooseObjectDB, self).__init__(root_path)
		self._hexsha_to_file = dict()
		# Additional Flags - might be set to 0 after the first failure
		# Depending on the root, this might work for some mounts, for others not, which
		# is why it is per instance
		self._fd_open_flags = getattr(os, 'O_NOATIME', 0)
	
	#{ Interface 
	def object_path(self, hexsha):
		"""
		:return: path at which the object with the given hexsha would be stored, 
			relative to the database root"""
		return join(hexsha[:2], hexsha[2:])
	
	def readable_db_object_path(self, hexsha):
		"""
		:return: readable object path to the object identified by hexsha
		:raise BadObject: If the object file does not exist"""
		try:
			return self._hexsha_to_file[hexsha]
		except KeyError:
			pass
		# END ignore cache misses 
			
		# try filesystem
		path = self.db_path(self.object_path(hexsha))
		if exists(path):
			self._hexsha_to_file[hexsha] = path
			return path
		# END handle cache
		raise BadObject(hexsha)
		
	#} END interface
	
	def _map_loose_object(self, sha):
		"""
		:return: memory map of that file to allow random read access
		:raise BadObject: if object could not be located"""
		db_path = self.db_path(self.object_path(to_hex_sha(sha)))
		try:
			fd = os.open(db_path, os.O_RDONLY|self._fd_open_flags)
		except OSError,e:
			if e.errno != ENOENT:
				# try again without noatime
				try:
					fd = os.open(db_path, os.O_RDONLY)
				except OSError:
					raise BadObject(to_hex_sha(sha))
				# didn't work because of our flag, don't try it again
				self._fd_open_flags = 0
			else:
				raise BadObject(to_hex_sha(sha))
			# END handle error
		# END exception handling
		try:
			return mmap.mmap(fd, 0, access=mmap.ACCESS_READ)
		finally:
			os.close(fd)
		# END assure file is closed
		
	def set_ostream(self, stream):
		""":raise TypeError: if the stream does not support the Sha1Writer interface"""
		if stream is not None and not isinstance(stream, Sha1Writer):
			raise TypeError("Output stream musst support the %s interface" % Sha1Writer.__name__)
		return super(LooseObjectDB, self).set_ostream(stream)
			
	def info(self, sha):
		m = self._map_loose_object(sha)
		try:
			type, size = loose_object_header_info(m)
			return OInfo(sha, type, size)
		finally:
			m.close()
		# END assure release of system resources
		
	def stream(self, sha):
		m = self._map_loose_object(sha)
		type, size, stream = DecompressMemMapReader.new(m, close_on_deletion = True)
		return OStream(sha, type, size, stream)
		
	def has_object(self, sha):
		try:
			self.readable_db_object_path(to_hex_sha(sha))
			return True
		except BadObject:
			return False
		# END check existance
	
	def store(self, istream):
		"""note: The sha we produce will be hex by nature"""
		tmp_path = None
		writer = self.ostream()
		if writer is None:
			# open a tmp file to write the data to
			fd, tmp_path = tempfile.mkstemp(prefix='obj', dir=self._root_path)
			writer = FDCompressedSha1Writer(fd)
		# END handle custom writer
	
		try:
			try:
				if istream.sha is not None:
					stream_copy(istream.read, writer.write, istream.size, self.stream_chunk_size)
				else:
					# write object with header, we have to make a new one
					write_object(istream.type, istream.size, istream.read, writer.write,
									chunk_size=self.stream_chunk_size)
				# END handle direct stream copies
			except:
				if tmp_path:
					os.remove(tmp_path)
				raise
			# END assure tmpfile removal on error
		finally:
			if tmp_path:
				writer.close()
		# END assure target stream is closed
		
		sha = istream.sha or writer.sha(as_hex=True)
		
		if tmp_path:
			obj_path = self.db_path(self.object_path(sha))
			obj_dir = dirname(obj_path)
			if not isdir(obj_dir):
				mkdir(obj_dir)
			# END handle destination directory
			rename(tmp_path, obj_path)
		# END handle dry_run
		
		istream.sha = sha
		return istream
	

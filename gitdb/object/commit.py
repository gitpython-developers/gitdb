# commit.py
# Copyright (C) 2008, 2009 Michael Trier (mtrier@gmail.com) and contributors
#
# This module is part of GitPython and is released under
# the BSD License: http://www.opensource.org/licenses/bsd-license.php
from gitdb.typ import ObjectType
from tree import Tree
from cStringIO import StringIO

import base
from gitdb.util import (
						hex_to_bin,
						Actor,
						)
from util import (
						Traversable,
						Serializable,
						altz_to_utctz_str,
						parse_actor_and_date
					)
import sys

__all__ = ('Commit', )

class Commit(base.Object, Traversable, Serializable):
	"""Wraps a git Commit object.
	
	This class will act lazily on some of its attributes and will query the 
	value on demand only if it involves calling the git binary."""
	
	# ENVIRONMENT VARIABLES
	# read when creating new commits
	env_author_date = "GIT_AUTHOR_DATE"
	env_committer_date = "GIT_COMMITTER_DATE"
	
	# CONFIGURATION KEYS
	conf_encoding = 'i18n.commitencoding'
	
	# INVARIANTS
	default_encoding = "UTF-8"
	
	
	# object configuration 
	type = ObjectType.commit
	type_id = ObjectType.commit_id
	
	__slots__ = ("tree",
				 "author", "authored_date", "author_tz_offset",
				 "committer", "committed_date", "committer_tz_offset",
				 "message", "parents", "encoding")
	_id_attribute_ = "binsha"
	
	def __init__(self, odb, binsha, tree=None, author=None, authored_date=None, author_tz_offset=None,
				 committer=None, committed_date=None, committer_tz_offset=None, 
				 message=None,  parents=None, encoding=None):
		"""Instantiate a new Commit. All keyword arguments taking None as default will 
		be implicitly set on first query. 
		
		:param binsha: 20 byte sha1
		:param parents: tuple( Commit, ... ) 
			is a tuple of commit ids or actual Commits
		:param tree: Tree
			Tree object
		:param author: Actor
			is the author string ( will be implicitly converted into an Actor object )
		:param authored_date: int_seconds_since_epoch
			is the authored DateTime - use time.gmtime() to convert it into a 
			different format
		:param author_tz_offset: int_seconds_west_of_utc
			is the timezone that the authored_date is in
		:param committer: Actor
			is the committer string
		:param committed_date: int_seconds_since_epoch
			is the committed DateTime - use time.gmtime() to convert it into a 
			different format
		:param committer_tz_offset: int_seconds_west_of_utc
			is the timezone that the authored_date is in
		:param message: string
			is the commit message
		:param encoding: string
			encoding of the message, defaults to UTF-8
		:param parents:
			List or tuple of Commit objects which are our parent(s) in the commit 
			dependency graph
		:return: git.Commit
		
		:note: Timezone information is in the same format and in the same sign 
			as what time.altzone returns. The sign is inverted compared to git's 
			UTC timezone."""
		super(Commit,self).__init__(odb, binsha)
		if tree is not None:
			assert isinstance(tree, Tree), "Tree needs to be a Tree instance, was %s" % type(tree)
		if tree is not None:
			self.tree = tree
		if author is not None:
			self.author = author
		if authored_date is not None:
			self.authored_date = authored_date
		if author_tz_offset is not None:
			self.author_tz_offset = author_tz_offset
		if committer is not None:
			self.committer = committer
		if committed_date is not None:
			self.committed_date = committed_date
		if committer_tz_offset is not None:
			self.committer_tz_offset = committer_tz_offset
		if message is not None:
			self.message = message
		if parents is not None:
			self.parents = parents
		if encoding is not None:
			self.encoding = encoding
		
	@classmethod
	def _get_intermediate_items(cls, commit):
		return commit.parents

	def _set_cache_(self, attr):
		if attr in Commit.__slots__:
			# read the data in a chunk, its faster - then provide a file wrapper
			binsha, typename, self.size, stream = self.odb.odb.stream(self.binsha)
			self._deserialize(StringIO(stream.read()))
		else:
			super(Commit, self)._set_cache_(attr)
		# END handle attrs

	@property
	def summary(self):
		""":return: First line of the commit message"""
		return self.message.split('\n', 1)[0]
		
	@classmethod
	def _iter_from_process_or_stream(cls, odb, proc_or_stream):
		"""Parse out commit information into a list of Commit objects
		We expect one-line per commit, and parse the actual commit information directly
		from our lighting fast object database

		:param proc: git-rev-list process instance - one sha per line
		:return: iterator returning Commit objects"""
		stream = proc_or_stream
		if not hasattr(stream,'readline'):
			stream = proc_or_stream.stdout
			
		readline = stream.readline
		while True:
			line = readline()
			if not line:
				break
			hexsha = line.strip()
			if len(hexsha) > 40:
				# split additional information, as returned by bisect for instance
				hexsha, rest = line.split(None, 1)
			# END handle extra info
			
			assert len(hexsha) == 40, "Invalid line: %s" % hexsha
			yield cls(odb, hex_to_bin(hexsha))
		# END for each line in stream
	
	#{ Serializable Implementation
	
	def _serialize(self, stream):
		write = stream.write
		write("tree %s\n" % self.tree)
		for p in self.parents:
			write("parent %s\n" % p)
			
		a = self.author
		aname = a.name
		if isinstance(aname, unicode):
			aname = aname.encode(self.encoding)
		# END handle unicode in name
		
		c = self.committer
		fmt = "%s %s <%s> %s %s\n"
		write(fmt % ("author", aname, a.email, 
						self.authored_date, 
						altz_to_utctz_str(self.author_tz_offset)))
			
		# encode committer
		aname = c.name
		if isinstance(aname, unicode):
			aname = aname.encode(self.encoding)
		# END handle unicode in name
		write(fmt % ("committer", aname, c.email, 
						self.committed_date,
						altz_to_utctz_str(self.committer_tz_offset)))
		
		if self.encoding != self.default_encoding:
			write("encoding %s\n" % self.encoding)
		
		write("\n")
		
		# write plain bytes, be sure its encoded according to our encoding
		if isinstance(self.message, unicode):
			write(self.message.encode(self.encoding))
		else:
			write(self.message)
		# END handle encoding
		return self
	
	def _deserialize(self, stream):
		""":param from_rev_list: if true, the stream format is coming from the rev-list command
		Otherwise it is assumed to be a plain data stream from our object"""
		readline = stream.readline
		self.tree = Tree(self.odb, hex_to_bin(readline().split()[1]), Tree.tree_id<<12, '')

		self.parents = list()
		next_line = None
		while True:
			parent_line = readline()
			if not parent_line.startswith('parent'):
				next_line = parent_line
				break
			# END abort reading parents
			self.parents.append(type(self)(self.odb, hex_to_bin(parent_line.split()[-1])))
		# END for each parent line
		self.parents = tuple(self.parents)
		
		self.author, self.authored_date, self.author_tz_offset = parse_actor_and_date(next_line)
		self.committer, self.committed_date, self.committer_tz_offset = parse_actor_and_date(readline())
		
		
		# now we can have the encoding line, or an empty line followed by the optional
		# message.
		self.encoding = self.default_encoding
		# read encoding or empty line to separate message
		enc = readline()
		enc = enc.strip()
		if enc:
			self.encoding = enc[enc.find(' ')+1:]
			# now comes the message separator 
			readline()
		# END handle encoding
		
		# decode the authors name
		try:
			self.author.name = self.author.name.decode(self.encoding) 
		except UnicodeDecodeError:
			print >> sys.stderr, "Failed to decode author name '%s' using encoding %s" % (self.author.name, self.encoding)
		# END handle author's encoding
		
		# decode committer name
		try:
			self.committer.name = self.committer.name.decode(self.encoding) 
		except UnicodeDecodeError:
			print >> sys.stderr, "Failed to decode committer name '%s' using encoding %s" % (self.committer.name, self.encoding)
		# END handle author's encoding
		
		# a stream from our data simply gives us the plain message
		# The end of our message stream is marked with a newline that we strip
		self.message = stream.read()
		try:
			self.message = self.message.decode(self.encoding)
		except UnicodeDecodeError:
			print >> sys.stderr, "Failed to decode message '%s' using encoding %s" % (self.message, self.encoding)
		# END exception handling 
		return self
		
	#} END serializable implementation

from reference import Reference

__all__ = ["TagReference", "Tag"]


	
class TagReference(Reference):
	"""Class representing a lightweight tag reference which either points to a commit 
	,a tag object or any other object. In the latter case additional information, 
	like the signature or the tag-creator, is available.
	
	This tag object will always point to a commit object, but may carray additional
	information in a tag object::
	
	 tagref = TagReference.list_items(repo)[0]
	 print tagref.commit.message
	 if tagref.tag is not None:
		print tagref.tag.message"""
	
	__slots__ = tuple()
	_common_path_default = "refs/tags"
	
	@property
	def commit(self):
		""":return: Commit object the tag ref points to"""
		obj = self.object
		if obj.type == "commit":
			return obj
		elif obj.type == "tag":
			# it is a tag object which carries the commit as an object - we can point to anything
			return obj.object
		else:
			raise ValueError( "Tag %s points to a Blob or Tree - have never seen that before" % self )	

	@property
	def tag(self):
		"""
		:return: Tag object this tag ref points to or None in case 
			we are a light weight tag"""
		obj = self.object
		if obj.type == "tag":
			return obj
		return None
		
	# make object read-only
	# It should be reasonably hard to adjust an existing tag
	object = property(Reference._get_object)
		
# provide an alias
Tag = TagReference

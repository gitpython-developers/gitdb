import binascii
import os
import errno

try:
	import async.mod.zlib as zlib
except ImportError:
	import zlib
# END try async zlib

from async import ThreadPool

try:
    import hashlib
except ImportError:
    import sha

#{ Globals

# A pool distributing tasks, initially with zero threads, hence everything 
# will be handled in the main thread
pool = ThreadPool(0)

#} END globals


#{ Aliases

hex_to_bin = binascii.a2b_hex
bin_to_hex = binascii.b2a_hex

# errors
ENOENT = errno.ENOENT

# os shortcuts
exists = os.path.exists
mkdir = os.mkdir
isdir = os.path.isdir
rename = os.rename
dirname = os.path.dirname
join = os.path.join
read = os.read
write = os.write
close = os.close

# constants
NULL_HEX_SHA = "0"*40

#} END Aliases


#{ Routines

def make_sha(source=''):
    """A python2.4 workaround for the sha/hashlib module fiasco 
    :note: From the dulwich project """
    try:
        return hashlib.sha1(source)
    except NameError:
        sha1 = sha.sha(source)
        return sha1

def stream_copy(source, destination, chunk_size=512*1024):
	"""Copy all data from the source stream into the destination stream in chunks
	of size chunk_size
	
	:return: amount of bytes written"""
	br = 0
	while True:
		chunk = source.read(chunk_size)
		destination.write(chunk)
		br += len(chunk)
		if len(chunk) < chunk_size:
			break
	# END reading output stream
	return br

def to_hex_sha(sha):
	""":return: hexified version  of sha"""
	if len(sha) == 40:
		return sha
	return bin_to_hex(sha)
	
def to_bin_sha(sha):
	if len(sha) == 20:
		return sha
	return hex_to_bin(sha)


#} END routines


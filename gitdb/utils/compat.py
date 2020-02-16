import sys

PY3 = sys.version_info[0] == 3

try:
    MAXSIZE = sys.maxint
except AttributeError:
    MAXSIZE = sys.maxsize

# Copyright (C) 2010, 2011 Sebastian Thiel (byronimo@gmail.com) and contributors
#
# This module is part of GitDB and is released under
# the New BSD License: http://www.opensource.org/licenses/bsd-license.php
import os

from gitdb.util import is_win


#: We need an easy way to see if Appveyor TCs start failing,
#: so the errors marked with this var are considered "acknowledged" ones, awaiting remedy,
#: till then, we wish to hide them.
HIDE_WINDOWS_KNOWN_ERRORS = is_win and os.environ.get('HIDE_WINDOWS_KNOWN_ERRORS', True)


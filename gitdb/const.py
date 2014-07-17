from gitdb.utils.encoding import force_bytes

NULL_BYTE = force_bytes("\0")
NULL_HEX_SHA = "0" * 40
NULL_BIN_SHA = NULL_BYTE * 20

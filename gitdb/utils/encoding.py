from gitdb.utils import compat

if compat.PY3:
    string_types = (str, )
    text_type = str
else:
    string_types = (basestring, )
    text_type = unicode

def force_bytes(data, encoding="utf-8"):
    if isinstance(data, bytes):
        return data

    if isinstance(data, compat.memoryview):
        return bytes(data)

    if isinstance(data, string_types):
        return data.encode(encoding)

    return data

def force_text(data, encoding="utf-8"):
    if isinstance(data, text_type):
        return data

    if isinstance(data, string_types):
        return data.decode(encoding)

    if not isinstance(data, bytes):
        data = force_bytes(data, encoding)

    if compat.PY3:
        return text_type(data, encoding)
    else:
        return text_type(data)

def _safe_int(string):
    try:
        return int(string)
    except ValueError:
        return string

__version__ = '1.0.1'
VERSION = tuple(_safe_int(x) for x in __version__.split('.'))

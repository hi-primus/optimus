def _safe_int(string):
    try:
        return int(string)
    except ValueError:
        return string

version__ = '0.5.2'
VERSION = tuple(_safe_int(x) for x in __version__.split('.'))

def _safe_int(string):
    try:
        return int(string)
    except ValueError:
        return string


__version__ = '2.0.6'
VERSION = tuple(_safe_int(x) for x in __version__.split('.'))

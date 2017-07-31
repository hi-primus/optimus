def _safe_int(string):
    try:
        return int(string)
    except ValueError:
        return string


__version__ = '0.3.4'
VERSION = tuple(_safe_int(x) for x in __version__.split('.'))

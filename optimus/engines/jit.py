import numpy as np
from numba import njit


@njit(fastmath=True)
def min_max(arr):
    # Reference https://stackoverflow.com/questions/12200580/numpy-function-for-simultaneous-max-and-min
    n = arr.size
    odd = n % 2
    if not odd:
        n -= 1
    max_val = min_val = arr[0]
    i = 1
    while i < n:
        x = arr[i]
        y = arr[i + 1]
        if x > y:
            x, y = y, x
        min_val = min(x, min_val)
        max_val = max(y, max_val)
        i += 2
    if not odd:
        x = arr[n]
        min_val = min(x, min_val)
        max_val = max(y, max_val)
    return max_val, min_val


@njit(fastmath=True)
def histogram1d(v, bins, range):
    # Reference https://iscinumpy.gitlab.io/post/histogram-speeds-in-python/
    return np.histogram(v, bins, range)


@njit(fastmath=True)
def count_na_j(df, series):
    return np.count_nonzero(df[series].isnull().values.ravel())

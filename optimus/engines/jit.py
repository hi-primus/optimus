import numpy as np
from numba import njit

# Reference https://stackoverflow.com/questions/52673285/performance-of-pandas-apply-vs-np-vectorize-to-create-new-column-from-existing-c
# Reference https://stackoverflow.com/questions/12200580/numpy-function-for-simultaneous-max-and-min

@njit(fastmath=True)
def min_max(arr):
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
        min_val = np.nanmin([x, min_val])
        max_val = np.nanmax([y, max_val])
        i += 2
    if not odd:
        x = arr[n]
        min_val = np.nanmin([x, min_val])
        max_val = np.nanmax([y, max_val])
    return min_val, max_val


# Histogram Implementation
# Reference https://numba.pydata.org/numba-examples/examples/density_estimation/histogram/results.html
# Reference https://iscinumpy.gitlab.io/post/histogram-speeds-in-python/

# Numpy
def histogram1d(v, bins, range):
    return np.histogram(v, bins, range)


# Numba
@njit(fastmath=True)
def get_bin_edges(a, bins):
    """
    Get edges from a array
    :param a:
    :param bins:
    :return:
    """
    bin_edges = np.zeros((bins + 1,), dtype=np.float64)
    a_min, a_max = min_max(a)
    delta = (a_max - a_min) / bins
    for i in range(bin_edges.shape[0]):
        bin_edges[i] = a_min + i * delta

    bin_edges[-1] = a_max  # Avoid round off error on last point
    return bin_edges


@njit(fastmath=True)
def get_bin_edges_min_max(a_min, a_max, bins):
    bin_edges = np.zeros((bins + 1,), dtype=np.float64)
    # a_min, a_max = min , max
    # a_min = a.min()
    # a_max = a.max()
    delta = (a_max - a_min) / bins
    for i in range(bin_edges.shape[0]):
        bin_edges[i] = a_min + i * delta

    bin_edges[-1] = a_max  # Avoid roundoff error on last point
    return bin_edges


@njit(fastmath=True)
def compute_bin(x, bin_edges):
    # assuming uniform bins for now
    n = bin_edges.shape[0] - 1
    a_min = bin_edges[0]
    a_max = bin_edges[-1]

    # special case to mirror NumPy behavior for last bin
    if x == a_max:
        return n - 1  # a_max always in last bin

    bin = int(n * (x - a_min) / (a_max - a_min))

    if bin < 0 or bin >= n:
        return None
    else:
        return bin


# Dask. This NOT work faster than histogram1 library. Require more testing
# numba_histogram1 = delayed(numba_histogram)
# # print(func(df["OFFENSE_CODE"],10).compute())
# %timeit numba_histogram1(np.array(df["OFFENSE_CODE"]),10).compute()

@njit(fastmath=True)
def numba_histogram(a, bins):

    hist = np.zeros((bins,), dtype=np.intp)
    bin_edges = get_bin_edges(a, bins)

    for x in a.flat:
        bin = compute_bin(x, bin_edges)
        if bin is not None:
            hist[int(bin)] += 1

    return hist, bin_edges

@njit
def numba_count_uniques(a):
    q = np.zeros(len(a), dtype=np.uint8)
    count = 0
    for x in a.flat:
        if q[x] == 0:
            q[x] = 1
            count += 1
    return count

@njit(fastmath=True)
def numba_histogram_edges(a, bin_edges):
    hist = np.zeros(len(bin_edges), dtype=np.intp)

    for x in a.flat:
        bin = compute_bin(x, bin_edges)
        if bin is not None:
            hist[int(bin)] += 1

    return list(hist)


def count_na_j(df, series):
    return np.count_nonzero(df[series].isnull().values.ravel())


# Reference https://stackoverflow.com/questions/10741346/numpy-most-efficient-frequency-counts-for-unique-values-in-an-array
# https://stackoverflow.com/a/21124789/581858
def bincount(a, n):
    y = np.bincount(a)
    ii = np.nonzero(y)[0]
    r = np.vstack((ii, y[ii])).T
    r = r[r[:, 1].argsort()[::-1]][:n]
    i, j = np.hsplit(r, 2)
    i, j = np.concatenate(i), np.concatenate(j)

    return i, j

    # Old implementation
    # i, j = np.unique(df[col_name], return_counts=True)
    # count_sort_ind = np.argsort(-j)

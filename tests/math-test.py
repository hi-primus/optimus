def square(x):
    """Return the square of `x`.
    Parameters
    ----------
    x : int or float
        The input number.
    Returns
    -------
    x2 : same type as `x`
        The square of the input value.
    Examples
    --------
    >>> square(5)
    25
    """
    return x ** 2

def test_square():
    x = 4
    assert square(x) == 16

def is_pyodide():
    """
    Check if we are running over pyodide
    :return:
    """
    try:
        import pyodide
        have_pyodide = True
    except ImportError as e:
        have_pyodide = False
    return have_pyodide


def is_pyarrow_installed():
    """
    Check if pyarrow is installed
    :return:
    """
    try:
        import pyarrow
        have_arrow = True
    except ImportError as e:
        print(e)
        have_arrow = False
    return have_arrow


def is_notebook():
    try:
        shell = get_ipython().__class__.__name__
        if shell in ['ZMQInteractiveShell', 'Shell']:
            return True
        else:
            return False
    except NameError:
        return False

class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class ColumnNotFound(Error):
    """Exception raised for errors in the input.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, column_name, message):
        self.column_name = column_name
        self.message = message
        super().__init__(self.message)


class PyodideNotFound(Error):
    """Exception raised when pyodide is not found.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
        super().__init__(self.message)

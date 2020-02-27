from abc import abstractmethod, ABC


# This implementation works for Spark, Dask, dask_cudf

class BaseRows(ABC):
    """Base class for all Rows implementations"""

    def __init__(self, df):
        self.df = df

    @staticmethod
    @abstractmethod
    def create_id(column="id"):
        pass

    @staticmethod
    @abstractmethod
    def append(rows):
        pass

    @staticmethod
    @abstractmethod
    def select(condition):
        pass

    @staticmethod
    @abstractmethod
    def select_by_dtypes(self, input_cols, data_type=None):
        pass

    @staticmethod
    @abstractmethod
    def count(self) -> int:
        pass

    @staticmethod
    @abstractmethod
    def to_list(input_cols):
        pass

    @staticmethod
    @abstractmethod
    def sort(input_cols):
        pass

    # @staticmethod
    # @abstractmethod
    # def sort(columns, order="desc"):
    #     pass
    #
    # @staticmethod
    # @abstractmethod
    # def sort(col_sort):
    #     pass

    @staticmethod
    @abstractmethod
    def drop(where=None):
        pass

    @staticmethod
    @abstractmethod
    def between(columns, lower_bound=None, upper_bound=None, invert=False, equal=False,
                bounds=None):
        pass

    @staticmethod
    @abstractmethod
    def drop_by_dtypes(input_cols, data_type=None):
        pass

    @staticmethod
    @abstractmethod
    def drop_na(input_cols, how="any", *args, **kwargs):
        """
        Removes rows with null values. You can choose to drop the row if 'all' values are nulls or if
        'any' of the values is null.
        :param input_cols:
        :param how:
        :return:
        """
        pass

    @staticmethod
    @abstractmethod
    def drop_duplicates(input_cols=None):
        """
        Drop duplicates values in a dataframe
        :param input_cols: List of columns to make the comparison, this only  will consider this subset of columns,
        :return: Return a new DataFrame with duplicate rows removed
        :param input_cols:
        :return:
        """
        pass

    @staticmethod
    @abstractmethod
    def limit(count):
        """
        Limit the number of rows
        :param count:
        :return:
        """

        pass

    @staticmethod
    @abstractmethod
    def is_in(input_cols, values):
        pass

    @staticmethod
    @abstractmethod
    def unnest(input_cols):
        pass

    @staticmethod
    @abstractmethod
    def approx_count():
        pass

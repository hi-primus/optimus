from abc import abstractmethod, ABC
from enum import Enum


class AbstractColumns(ABC):
    """Base class for all database implementations"""

    @staticmethod
    @abstractmethod
    def append(*args, **kwarsg) -> Enum:
        """
        Append a column or a Dataframe to a Dataframe
        :return: en enum of type ``DriverProperties``
        """
        pass

    @staticmethod
    @abstractmethod
    def select(columns="*", regex=None, data_type=None, invert=False) -> str:
        """
        Returns a string connection to the underlying database implementation.
        :param kwargs: connection properties
        :return: the connection string
        """
        pass

    @staticmethod
    @abstractmethod
    def copy(self, *args, **kwargs) -> str:
        """
        Returns a string connection to the underlying database implementation.
        :param kwargs: connection properties
        :return: the connection string
        """
        pass

    @staticmethod
    @abstractmethod
    def to_timestamp(self, *args, **kwargs) -> str:
        """
        Returns the query for all table names in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
        """
        pass

    @staticmethod
    @abstractmethod
    def apply_expr(self, *args, **kwargs) -> str:
        """
        Returns the query for a table name in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
       """
        pass

    @staticmethod
    @abstractmethod
    def apply(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def apply_by_dtypes(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def set(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def rename(self) -> Enum:
        """
        Returns an enum containing the following database properties:

            * name
            * port
            * java_class
            * table_name

        :return: en enum of type ``DriverProperties``
        """
        pass

    @staticmethod
    @abstractmethod
    def cast(self, *args, **kwargs) -> str:
        """
        Returns a string connection to the underlying database implementation.
        :param kwargs: connection properties
        :return: the connection string
        """
        pass

    @staticmethod
    @abstractmethod
    def astype(self, *args, **kwargs) -> str:
        """
        Returns the query for all table names in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
        """
        pass

    @staticmethod
    @abstractmethod
    def move(self, *args, **kwargs) -> str:
        """
        Returns the query for a table name in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
       """
        pass

    @staticmethod
    @abstractmethod
    def keep(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def sort(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def drop(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def create_exprs(self) -> Enum:
        """
        Returns an enum containing the following database properties:

            * name
            * port
            * java_class
            * table_name

        :return: en enum of type ``DriverProperties``
        """
        pass

    @staticmethod
    @abstractmethod
    def agg_exprs(self, *args, **kwargs) -> str:
        """
        Returns a string connection to the underlying database implementation.
        :param kwargs: connection properties
        :return: the connection string
        """
        pass

    @staticmethod
    @abstractmethod
    def min(self, *args, **kwargs) -> str:
        """
        Returns the query for all table names in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
        """
        pass

    @staticmethod
    @abstractmethod
    def max(columns) -> str:
        pass

    @staticmethod
    @abstractmethod
    def range(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def median(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def percentile(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def mad(columns, relative_error, more) -> Enum:
        """
        Returns an enum containing the following database properties:

            * name
            * port
            * java_class
            * table_name

        :return: en enum of type ``DriverProperties``
        """
        pass

    @staticmethod
    @abstractmethod
    def std(columns) -> str:
        """
        Returns a string connection to the underlying database implementation.
        :param kwargs: connection properties
        :return: the connection string
        """
        pass

    @staticmethod
    @abstractmethod
    def kurt(self, *args, **kwargs) -> str:
        """
        Returns the query for all table names in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
        """
        pass

    @staticmethod
    @abstractmethod
    def mean(self, *args, **kwargs) -> str:
        """
        Returns the query for a table name in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
       """
        pass

    @staticmethod
    @abstractmethod
    def skewness(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def sum(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def variance(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def abs(self) -> Enum:
        """
        Returns an enum containing the following database properties:

            * name
            * port
            * java_class
            * table_name

        :return: en enum of type ``DriverProperties``
        """
        pass

    @staticmethod
    @abstractmethod
    def mode(self, *args, **kwargs) -> str:
        """
        Returns a string connection to the underlying database implementation.
        :param kwargs: connection properties
        :return: the connection string
        """
        pass

    @staticmethod
    @abstractmethod
    def lower(self, *args, **kwargs) -> str:
        """
        Returns the query for all table names in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
        """
        pass

    @staticmethod
    @abstractmethod
    def upper(self, *args, **kwargs) -> str:
        """
        Returns the query for a table name in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
       """
        pass

    @staticmethod
    @abstractmethod
    def trim(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def reverse(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def remove(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def remove_accents(self) -> Enum:
        """
        Returns an enum containing the following database properties:

            * name
            * port
            * java_class
            * table_name

        :return: en enum of type ``DriverProperties``
        """
        pass

    @staticmethod
    @abstractmethod
    def remove_special_chars(self, *args, **kwargs) -> str:
        """
        Returns a string connection to the underlying database implementation.
        :param kwargs: connection properties
        :return: the connection string
        """
        pass

    @staticmethod
    @abstractmethod
    def remove_white_spaces(self, *args, **kwargs) -> str:
        """
        Returns the query for all table names in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
        """
        pass

    @staticmethod
    @abstractmethod
    def date_transform(self, *args, **kwargs) -> str:
        """
        Returns the query for a table name in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
       """
        pass

    @staticmethod
    @abstractmethod
    def years_between(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def replace(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def replace_regex(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def impute(self) -> Enum:
        """
        Returns an enum containing the following database properties:

            * name
            * port
            * java_class
            * table_name

        :return: en enum of type ``DriverProperties``
        """
        pass

    @staticmethod
    @abstractmethod
    def fill_na(self, *args, **kwargs) -> str:
        """
        Returns a string connection to the underlying database implementation.
        :param kwargs: connection properties
        :return: the connection string
        """
        pass

    @staticmethod
    @abstractmethod
    def is_na(self, *args, **kwargs) -> str:
        """
        Returns the query for all table names in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
        """
        pass

    @staticmethod
    @abstractmethod
    def count(self, *args, **kwargs) -> str:
        """
        Returns the query for a table name in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
       """
        pass

    @staticmethod
    @abstractmethod
    def count_na(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def count_zeros(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def count_uniques(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def value_counts(self) -> Enum:
        """
        Returns an enum containing the following database properties:

            * name
            * port
            * java_class
            * table_name

        :return: en enum of type ``DriverProperties``
        """
        pass

    @staticmethod
    @abstractmethod
    def unique(self, *args, **kwargs) -> str:
        """
        Returns a string connection to the underlying database implementation.
        :param kwargs: connection properties
        :return: the connection string
        """
        pass

    @staticmethod
    @abstractmethod
    def select_by_dtypes(self, *args, **kwargs) -> str:
        """
        Returns the query for all table names in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
        """
        pass

    @staticmethod
    @abstractmethod
    def _math(self, *args, **kwargs) -> str:
        """
        Returns the query for a table name in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
       """
        pass

    @staticmethod
    @abstractmethod
    def add(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def sub(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def mul(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def div(self) -> Enum:
        """
        Returns an enum containing the following database properties:

            * name
            * port
            * java_class
            * table_name

        :return: en enum of type ``DriverProperties``
        """
        pass

    @staticmethod
    @abstractmethod
    def z_score(self, *args, **kwargs) -> str:
        """
        Returns a string connection to the underlying database implementation.
        :param kwargs: connection properties
        :return: the connection string
        """
        pass

    @staticmethod
    @abstractmethod
    def min_max_scaler(self, *args, **kwargs) -> str:
        """
        Returns the query for all table names in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
        """
        pass

    @staticmethod
    @abstractmethod
    def max_abs_scaler(self, *args, **kwargs) -> str:
        """
        Returns the query for a table name in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
       """
        pass

    @staticmethod
    @abstractmethod
    def iqr(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def nest(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def unnest(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def cell(self) -> Enum:
        """
        Returns an enum containing the following database properties:

            * name
            * port
            * java_class
            * table_name

        :return: en enum of type ``DriverProperties``
        """
        pass

    @staticmethod
    @abstractmethod
    def scatter(self, *args, **kwargs) -> str:
        """
        Returns a string connection to the underlying database implementation.
        :param kwargs: connection properties
        :return: the connection string
        """
        pass

    @staticmethod
    @abstractmethod
    def hist(self, *args, **kwargs) -> str:
        """
        Returns the query for all table names in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
        """
        pass

    @staticmethod
    @abstractmethod
    def frequency_by_group(self, *args, **kwargs) -> str:
        """
        Returns the query for a table name in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
       """
        pass

    @staticmethod
    @abstractmethod
    def count_mismatch(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def count_by_dtypes(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def frequency(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def correlation(self) -> Enum:
        """
        Returns an enum containing the following database properties:

            * name
            * port
            * java_class
            * table_name

        :return: en enum of type ``DriverProperties``
        """
        pass

    @staticmethod
    @abstractmethod
    def schema_dtype(self, *args, **kwargs) -> str:
        """
        Returns a string connection to the underlying database implementation.
        :param kwargs: connection properties
        :return: the connection string
        """
        pass

    @staticmethod
    @abstractmethod
    def dtypes(self, *args, **kwargs) -> str:
        """
        Returns the query for all table names in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
        """
        pass

    @staticmethod
    @abstractmethod
    def names(self, *args, **kwargs) -> str:
        """
        Returns the query for a table name in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
       """
        pass

    @staticmethod
    @abstractmethod
    def qcut(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def clip(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def values_to_cols(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @staticmethod
    @abstractmethod
    def string_to_index(self) -> Enum:
        """
        Returns an enum containing the following database properties:

            * name
            * port
            * java_class
            * table_name

        :return: en enum of type ``DriverProperties``
        """
        pass

    @staticmethod
    @abstractmethod
    def index_to_string(self, *args, **kwargs) -> str:
        """
        Returns a string connection to the underlying database implementation.
        :param kwargs: connection properties
        :return: the connection string
        """
        pass

    @staticmethod
    @abstractmethod
    def bucketizer(self, *args, **kwargs) -> str:
        """
        Returns the query for all table names in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
        """
        pass

    @staticmethod
    @abstractmethod
    def set_meta(self, *args, **kwargs) -> str:
        """
        Returns the query for a table name in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
       """
        pass

    @staticmethod
    @abstractmethod
    def get_meta(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

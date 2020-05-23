from abc import abstractmethod, ABC
from enum import Enum


class AbstractDriver(ABC):
    """Base class for all database implementations"""

    @abstractmethod
    def properties(self) -> Enum:
        """
        Returns an enum containing the following database properties:

            * name
            * port
            * java_class
            * table_name

        :return: en enum of type ``DriverProperties``
        """
        pass

    @abstractmethod
    def url(self, *args, **kwargs) -> str:
        """
        Returns a string connection to the underlying database implementation.
        :param kwargs: connection properties
        :return: the connection string
        """
        pass

    @abstractmethod
    def table_names_query(self, *args, **kwargs) -> str:
        """
        Returns the query for all table names in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
        """
        pass

    @abstractmethod
    def table_name_query(self, *args, **kwargs) -> str:
        """
        Returns the query for a table name in the database.
        :param kwargs: query parameters
        :return: a query for all the tables in a given database
       """
        pass

    @abstractmethod
    def count_query(self, *args, **kwargs) -> str:
        """
        Returns the query for counting the rows in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @abstractmethod
    def primary_key_query(self, *args, **kwargs) -> str:
        """
        Returns the query the primary keys in a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

    @abstractmethod
    def min_max_query(self, *args, **kwargs) -> str:
        """
        Returns the query with the min and max values in a columns from a given table.
        :param kwargs: query parameters
        :return: a query to count the number of rows in a table
        """
        pass

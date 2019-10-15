from enum import Enum

from optimus.io.abstract_driver import AbstractDriver


class DriverContext:
    """Driver context holding a reference to the underlying driver implementation"""

    def __init__(self, driver: AbstractDriver) -> None:
        self._driver = driver

    @property
    def driver(self) -> AbstractDriver:
        return self._driver

    @driver.setter
    def driver(self, driver: AbstractDriver) -> None:
        self._driver = driver

    def properties(self) -> Enum:
        return self._driver.properties()

    def url(self, *args, **kwargs) -> str:
        return self._driver.url(*args, **kwargs)

    def table_names_query(self, *args, **kwargs) -> str:
        return self._driver.table_names_query(*args, **kwargs)

    def table_name_query(self, *args, **kwargs) -> str:
        return self._driver.table_name_query(*args, **kwargs)

    def count_query(self, *args, **kwargs) -> str:
        return self._driver.count_query(*args, **kwargs)

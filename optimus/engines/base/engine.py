from abc import abstractmethod

from optimus.engines.base.io.connect import Connect
from optimus.helpers.logger import logger


class BaseEngine:

    @staticmethod
    def verbose(verbose):
        """
        Enable verbose mode
        :param verbose:
        :return:
        """

        logger.active(verbose)

    @property
    def connect(self):
        """
        Create the JDBC string connection
        :return: JDBC object
        """
        return Connect()

    @abstractmethod
    def create(self):
        pass

    @abstractmethod
    def load(self):
        pass

    @abstractmethod
    def engine(self):
        pass

    @abstractmethod
    def remote_run(self, callback, *args, **kwargs):
        pass

    @abstractmethod
    def remote_submit(self, callback, *args, **kwargs):
        pass

    @abstractmethod
    def submit(self, func, *args, **kwargs):
        pass

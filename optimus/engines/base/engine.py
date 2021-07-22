from abc import abstractmethod
from optimus.engines.base.contants import BaseConstants

from optimus.engines.base.io.connect import Connect
from optimus.helpers.logger import logger
import numpy as np

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

    @property
    def nan(self):
        """
        Create the JDBC string connection
        :return: JDBC object
        """
        return np.nan

    @abstractmethod
    def create(self):
        pass

    @property
    def constants(self):
        return BaseConstants()

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
    def submit(self, func, *args, priority=0, pure=False, **kwargs):
        pass

from abc import abstractmethod
from optimus.engines.base.constants import BaseConstants
from optimus.engines.base.functions import BaseFunctions

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
        return Connect(self)

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

    @property
    def F(self):
        return BaseFunctions()

    @abstractmethod
    def load(self):
        pass

    @abstractmethod
    def engine(self):
        pass

    @property
    def engine_label(self):
        return self.__class__.__name__

    @abstractmethod
    def remote_run(self, callback, *args, **kwargs):
        pass

    @abstractmethod
    def remote_submit(self, callback, *args, **kwargs):
        pass

    @abstractmethod
    def submit(self, func, *args, priority=0, pure=False, **kwargs):
        pass

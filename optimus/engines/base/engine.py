from abc import abstractmethod

import numpy as np

from optimus import Optimus
from optimus.engines.base.constants import BaseConstants
from optimus.engines.base.functions import BaseFunctions
from optimus.engines.base.io.connect import Connect
from optimus.engines.dask.io.jdbc import JDBC
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

    def __getstate__(self):
        return self.engine

    def __setstate__(self, state):
        return Optimus(state)


    def connect(self, driver=None, host=None, database=None, user=None, password=None, port=None, schema="public",
                oracle_tns=None, oracle_service_name=None, oracle_sid=None, presto_catalog=None,
                cassandra_keyspace=None, cassandra_table=None, bigquery_project=None, bigquery_dataset=None):
        """
        Create the JDBC string connection
        :return: JDBC object
        """

        return JDBC(host=host, database=database, user=user, password=password,
                    port=port, driver=driver, schema=schema,
                    oracle_tns=oracle_tns,
                    oracle_service_name=oracle_service_name,
                    oracle_sid=oracle_sid, presto_catalog=presto_catalog,
                    cassandra_keyspace=cassandra_keyspace,
                    cassandra_table=cassandra_table,
                    bigquery_project=bigquery_project,
                    bigquery_dataset=bigquery_dataset, op=self)

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

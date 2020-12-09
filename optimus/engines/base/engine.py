from abc import abstractmethod

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

    @staticmethod
    def connect(driver=None, host=None, database=None, user=None, password=None, port=None, schema="public",
                oracle_tns=None, oracle_service_name=None, oracle_sid=None, presto_catalog=None,
                cassandra_keyspace=None, cassandra_table=None, bigquery_project=None, bigquery_dataset=None):
        """
        Create the JDBC string connection
        :return: JDBC object
        """

        return JDBC(host, database, user, password, port, driver, schema, oracle_tns, oracle_service_name, oracle_sid,
                    presto_catalog, cassandra_keyspace, cassandra_table, bigquery_project, bigquery_dataset)

    @abstractmethod
    def create(self):
        pass

    @abstractmethod
    def load(self):
        pass

    @abstractmethod
    def engine(self):
        pass
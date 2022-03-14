import ibis

from optimus._version import __version__
from optimus.engines.base.engine import BaseEngine
from optimus.engines.ibis.io.jdbc import JDBC
from optimus.optimus import Engine, EnginePretty


class IbisEngine(BaseEngine):
    __version__ = __version__

    def __init__(self, verbose=False, op=None, comm=None, *args, **kwargs):
        self.engine = Engine.IBIS.value

        self.verbose(verbose)
        self.client = ibis

    @property
    def F(self):
        from optimus.engines.ibis.functions import IbisFunctions
        return IbisFunctions(self)

    def connect(self, driver=None, host=None, database=None, user=None, password=None, port=None, schema="public",
                oracle_tns=None, oracle_service_name=None, oracle_sid=None, presto_catalog=None,
                cassandra_keyspace=None, cassandra_table=None, bigquery_project=None, bigquery_dataset=None):
        """
        Create the JDBC string connection
        :return: JDBC object
        """

        return JDBC(host, database, user, password, port, driver, schema, oracle_tns, oracle_service_name, oracle_sid,
                    presto_catalog, cassandra_keyspace, cassandra_table, bigquery_project, bigquery_dataset, op=self)

    @property
    def engine_label(self):
        return EnginePretty.IBIS.value

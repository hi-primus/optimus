import math

import cudf
import fastnumbers
import numpy as np

from optimus.bumblebee import Comm
from optimus.engines.cudf.cudf import CUDF
from optimus.engines.cudf.io.jdbc import JDBC
from optimus.engines.cudf.io.load import Load
from optimus.helpers.check import is_cudf_series
from optimus.helpers.logger import logger
from optimus.profiler.profiler import Profiler
from optimus.version import __version__

CUDF.instance = None
Profiler.instance = None
Comm.instance = None


class CUDFEngine:
    __version__ = __version__

    def __init__(self, verbose=False, comm=None, *args, **kwargs):
        if comm is True:
            Comm.instance = Comm()
        else:
            Comm.instance = comm

        self.engine = 'cudf'
        # self.create = Create()
        self.load = Load()
        # self.read = self.spark.read
        self.verbose(verbose)

        CUDF.instance = cudf

        self.client = CUDF.instance

        Profiler.instance = Profiler()
        self.profiler = Profiler.instance

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
                cassandra_keyspace=None, cassandra_table=None):
        """
        Create the JDBC string connection
        :return: JDBC object
        """

        return JDBC(host, database, user, password, port, driver, schema, oracle_tns, oracle_service_name, oracle_sid,
                    presto_catalog, cassandra_keyspace, cassandra_table)

    op_to_series_func = {
        "sqrt": {
            "cudf": "sqrt",
            "python": "sqrt",
        },
        "abs": {
            "cudf": "abs",
            "python": "fabs"

        },
        "pow": {
            "cudf": "pow",
            "python": "pow"
        },
        "exp": {
            "cudf": "exp",
            "python": "exp"
        }

    }

    @staticmethod
    def call(value, *args, method_name=None):
        """

        :param value:
        :param args:
        :param method_name:
        :return:
        """

        if is_cudf_series(value):
            method = getattr(value, CUDFEngine.op_to_series_func[method_name]["cudf"])
            result = method(*args)
        elif fastnumbers.isreal(value):  # Numeric
            method = getattr(math, CUDFEngine.op_to_series_func[method_name]["python"])
            result = method(fastnumbers.fast_real(value), *args)
        else:  # string
            result = np.nan
        return result

    @staticmethod
    def sqrt(value):
        return CUDFEngine.call(value, method_name="sqrt")

    @staticmethod
    def abs(value):
        return CUDFEngine.call(value, method_name="abs")

    @staticmethod
    def pow(value, n):
        return CUDFEngine.call(value, n, method_name="pow")

    @staticmethod
    def exp(value):
        return CUDFEngine.call(value, method_name="exp")

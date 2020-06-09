
# CUDF
# cudf.sqrt(df.Lat)
# cudf.sin(df.Lat)
# cudf.cos(df.Lat)
# cudf.tan(df.Lat)
# cudf.arcsin(df.Lat)
# cudf.arccos(df.Lat)
# cudf.arctan(df.Lat)
# df.Lat.abs()
# df.Lat.exp()
# df.Lat.log()
# df.Lat.pow(2)
# df.Lat.round(2)
# df.Lat.floor()
# df.Lat.mod(1)
# df.Lat.sqrt()
# df.Lat.log()
from abc import abstractmethod

from optimus.engines.dask.io.jdbc import JDBC
from optimus.helpers.logger import logger

op_to_series_func = {
    "abs": {
        "cudf": "abs",
        "numpy": "abs",
    },
    "exp": {
        "cudf": "exp",
        "numpy": "exp",
    },
    "sqrt": {
        "cudf": "sqrt",
        "numpy": "sqrt",
    },
    "mod": {
        "cudf": "mod",
        "numpy": "mod"

    },
    "pow": {
        "cudf": "pow",
        "numpy": "power"
    },
    "ceil": {
        "cudf": "ceil",
        "numpy": "ceil"
    },
    "floor": {
        "cudf": "floor",
        "numpy": "floor"
    },
    "trunc": {
        "cudf": "trunc",
        "numpy": "trunc"
    },
    "radians": {
        "cudf": "radians",
        "numpy": "radians"
    },
    "degrees": {
        "cudf": "degrees",
        "numpy": "degrees"
    },
    "ln": {
        "cudf": "log",
        "numpy": "log"
    },
    "log": {
        "cudf": "log10",
        "numpy": "log10"
    },
    "sin": {
        "cudf": "sin",
        "numpy": "sin"
    },
    "cos": {
        "cudf": "cos",
        "numpy": "cos"
    },
    "tan": {
        "cudf": "tan",
        "numpy": "tan"
    },
    "asin": {
        "cudf": "asin",
        "numpy": "arcsin"
    },
    "acos": {
        "cudf": "acos",
        "numpy": "arccos"
    },
    "atan": {
        "cudf": "atan",
        "numpy": "arctan"
    },
    "sinh": {
        "cudf": "sinh",
        "numpy": "sinh"
    },
    "asinh": {
        "cudf": "arcsinh",
        "numpy": "arcsinh"
    },
    "cosh": {
        "cudf": "cosh",
        "numpy": "cosh"
    }
    ,
    "acosh": {
        "cudf": "arccosh",
        "numpy": "arccosh"
    },
    "tanh": {
        "cudf": "tanh",
        "numpy": "tanh"
    },
    "atanh": {
        "cudf": "arctanh",
        "numpy": "arctanh"
    }

}


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
                cassandra_keyspace=None, cassandra_table=None):
        """
        Create the JDBC string connection
        :return: JDBC object
        """

        return JDBC(host, database, user, password, port, driver, schema, oracle_tns, oracle_service_name, oracle_sid,
                    presto_catalog, cassandra_keyspace, cassandra_table)

    @abstractmethod
    def call(self, *args, method_name):
        pass

    def abs(self, series):
        return self.call(series, method_name="abs")

    def exp(self, series):
        return self.call(series, method_name="exp")

    def mod(self, series, *args):
        return self.call(series, *args, method_name="mod")

    def pow(self, series, *args):
        return self.call(series, *args, method_name="pow")

    def ceil(self, series):
        return self.call(series, method_name="ceil")

    def sqrt(self, series):
        return self.call(series, method_name="sqrt")

    def floor(self, series):
        return self.call(series, method_name="floor")

    def trunc(self, series):
        return self.call(series, method_name="trunc")

    def radians(self, series):
        return self.call(series, method_name="radians")

    def degrees(self, series):
        return self.call(series, method_name="degrees")

    def ln(self, series):
        return self.call(series, method_name="ln")

    def log(self, series):
        return self.call(series, method_name="log")

    # Trigonometrics
    def sin(self, series):
        return self.call(series, method_name="sin")

    def cos(self, series):
        return self.call(series, method_name="cos")

    def tan(self, series):
        return self.call(series, method_name="tan")

    def asin(self, series):
        return self.call(series, method_name="asin")

    def acos(self, series):
        return self.call(series, method_name="acos")

    def atan(self, series):
        return self.call(series, method_name="atan")

    def sinh(self, series):
        return self.call(series, method_name="sinh")

    def asinh(self, series):
        return self.call(series, method_name="asinh")

    def cosh(self, series):
        return self.call(series, method_name="cosh")

    def tanh(self, series):
        return self.call(series, method_name="tanh")

    def acosh(self, series):
        return self.call(series, method_name="acosh")

    def atanh(self, series):
        return self.call(series, method_name="atanh")

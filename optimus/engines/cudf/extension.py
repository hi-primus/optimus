from cudf.core import DataFrame
from cudf.core import Series

from optimus.engines.base.extension import BaseExt


def ext(self: DataFrame):
    class Ext(BaseExt):
        _name = None

        def __init__(self, df):
            super().__init__(df)

        @staticmethod
        def cache():
            return self

        @staticmethod
        def sample(n=10, random=False):
            pass

        @staticmethod
        def pivot(index, column, values):
            pass

        @staticmethod
        def melt(id_vars, value_vars, var_name="variable", value_name="value", data_type="str"):
            pass

        @staticmethod
        def query(sql_expression):
            pass

        @staticmethod
        def partitions():
            pass

        @staticmethod
        def partitioner():
            pass

        @staticmethod
        def repartition(n=None, *args, **kwargs):
            pass

        @staticmethod
        def show():
            df = self
            return df

        @staticmethod
        def debug():
            pass

        @staticmethod
        def create_id(column="id"):
            pass

    return Ext(self)


def ext_series(self: Series):
    class Ext:
        def __init__(self, series):
            self.series = series

        def to_dict(self, index=True):
            series = self.series
            if index is True:
                return series.to_pandas().to_dict()
            else:
                return series.to_pandas().to_list()

    return Ext(self)


DataFrame.ext = property(ext)
Series.ext = property(ext_series)

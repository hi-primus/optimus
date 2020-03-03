import pandas as pd

DataFrame = pd.DataFrame

from optimus.engines.base.extension import BaseExt


def ext(self: DataFrame):
    class Ext(BaseExt):
        @staticmethod
        def cache():
            pass

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
        def repartition(partitions_number=None, col_name=None):
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

        def __init__(self, df):
            super().__init__(df)

    return Ext(self)

print("ASDFasldkjfah")
DataFrame.ext = property(ext)

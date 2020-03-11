import json

import numpy as np
import pandas as pd

from optimus.engines.base.extension import BaseExt
from optimus.helpers.columns import parse_columns
from optimus.helpers.json import dump_json
from optimus.helpers.json import json_converter

DataFrame = pd.DataFrame


def ext(self: DataFrame):
    class Ext(BaseExt):

        def __init__(self, df):
            super().__init__(df)

        @staticmethod
        def to_json(columns):
            df = self

            columns = parse_columns(df, columns)
            # print(df)
            result = {"sample": {"columns": [{"title": col_name} for col_name in df.cols.select(columns).cols.names()],
                                 "value": df.rows.to_list(columns)}}

            # for col_name in columns:
            #     print("to_json AAAAAA",col_name)
            #     if df[col_name].dtype == np.float64 or df[col_name].dtype == np.int64:
            #         result.update(df.cols.hist(col_name))
            #     else:
            #         result.update(df.cols.frequency(col_name))
            # print("ASDFASdf", len(df))
            return json.dumps(result, ensure_ascii=False, default=json_converter)

        @staticmethod
        def profile(columns, lower_bound=None, upper_bound=None, bins=10, output=None):
            """

            :param lower_bound:
            :param upper_bound:
            :param columns:
            :param bins:
            :param output:
            :return:
            """

            df = self
            if lower_bound is None:
                lower_bound = 0
            if upper_bound is None:
                upper_bound = len(df)

            df = self[lower_bound:upper_bound]
            # columns = parse_columns(df, columns)
            # result = {}

            columns = parse_columns(df, columns)
            # print(df)
            result = {"sample": {"columns": [{"title": col_name} for col_name in df.cols.select(columns).cols.names()],
                                 "value": df.rows.to_list(columns)}}

            # df = df.dropna()
            df = self
            result["columns"] = {}
            for col_name in columns:
                stats = {}
                if df[col_name].dtype == np.float64 or df[col_name].dtype == np.int64:
                    # stats.update()
                    stats = {"hist": df.cols.hist(col_name, buckets=bins)}
                    r = {col_name: stats}

                else:
                    # df[col_name] = df[col_name].astype("str").dropna()
                    stats["stats"] = {"frequency": df.cols.frequency(col_name, n=bins),
                                      "count_uniques": len(df[col_name].value_counts())}

                    r = {col_name: stats}

                result["columns"].update(r)

            result["stats"] = {"rows_count": len(df)}
            if output == "json":
                result = dump_json(result)

            return result

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

    return Ext(self)


DataFrame.ext = property(ext)

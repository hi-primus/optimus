import json
import unicodedata

import numpy as np
import pandas as pd

from optimus.engines.base.extension import BaseExt
from optimus.helpers.columns import parse_columns
from optimus.helpers.json import dump_json
from optimus.helpers.json import json_converter
from optimus.helpers.raiseit import RaiseIt

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
            df_length = len(df)

            if lower_bound is None:
                lower_bound = 0

            if lower_bound < 0:
                lower_bound = 0

            if upper_bound is None:
                upper_bound = len(df)

            if upper_bound > df_length:
                upper_bound = df_length

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

                stats["stats"] = {"missing": 3, "mismatch": 4, "null": df.cols.count_na(col_name)[col_name]}
                col_dtype = df[col_name].dtype
                if col_dtype == np.float64 or df[col_name].dtype == np.int64:
                    stats["stats"].update({"hist": df.cols.hist(col_name, buckets=bins)})
                    r = {col_name: stats}

                elif col_dtype == "object":

                    # df[col_name] = df[col_name].astype("str").dropna()
                    stats["stats"].update({"frequency": df.cols.frequency(col_name, n=bins)[col_name],
                                           "count_uniques": len(df[col_name].value_counts())})
                    r = {col_name: stats}
                else:
                    
                    RaiseIt.type_error(col_dtype, [np.float64, np.int64, np.object_])

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

        def remove_accents(self, input_cols, output_cols=None):
            def _remove_accents(value):
                cell_str = str(value)

                # first, normalize strings:
                nfkd_str = unicodedata.normalize('NFKD', cell_str)

                # Keep chars that has no other char combined (i.e. accents chars)
                with_out_accents = u"".join([c for c in nfkd_str if not unicodedata.combining(c)])
                return with_out_accents

            df = self.df
            return df.cols.apply(input_cols, _remove_accents, func_return_type=str,
                                 filter_col_by_dtypes=df.constants.STRING_TYPES,
                                 output_cols=output_cols)

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
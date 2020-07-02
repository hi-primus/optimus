from abc import ABC

import numpy as np

from optimus.engines.base.extension import BaseExt
from optimus.helpers.columns import parse_columns
from optimus.helpers.json import dump_json
from optimus.helpers.raiseit import RaiseIt
from optimus.profiler.constants import MAX_BUCKETS


class DataFrameBaseExt(BaseExt):

    def __init__(self, df):
        super(DataFrameBaseExt, self).__init__(df)

    def profile(self, columns, lower_bound=None, upper_bound=None, bins=MAX_BUCKETS, output=None, infer=False):
        """

        :param lower_bound:
        :param upper_bound:
        :param columns:
        :param bins:
        :param output:
        :return:
        """

        df = self.df
        df_length = len(df)

        if lower_bound is None:
            lower_bound = 0

        if lower_bound < 0:
            lower_bound = 0

        if upper_bound is None:
            upper_bound = len(df)

        if upper_bound > df_length:
            upper_bound = df_length

        df = df[lower_bound:upper_bound]

        columns = parse_columns(df, columns)
        result = {"sample": {"columns": [{"title": col_name} for col_name in df.cols.select(columns).cols.names()]}}

        # df = self
        result["columns"] = {}
        dtypes = df.cols.dtypes("*")

        for col_name in columns:
            stats = {}

            # stats["stats"] = {"missing": 3, "mismatch": 4, "null": df.cols.count_na(col_name)[col_name]}
            stats["stats"] = df.cols.count_by_dtypes(col_name, "int")[col_name]

            col_dtype = df[col_name].dtype
            if col_dtype == np.float64 or df[col_name].dtype == np.int64:
                stats["stats"].update({"hist": df.cols.hist(col_name, buckets=bins)[col_name]["hist"]})
                r = {col_name: stats}

            elif col_dtype == "object" or "category":

                stats["stats"].update({"frequency": df.cols.frequency(col_name, n=bins)[col_name]["frequency"],
                                       "count_uniques": len(df[col_name].value_counts())})
                r = {col_name: stats}
            else:
                RaiseIt.type_error(col_dtype, [np.float64, np.int64, np.object_])

            r[col_name]["dtype"] = dtypes[col_name]
            result["columns"].update(r)
        result["summary"] = {"rows_count": df_length}

        if output == "json":
            result = dump_json(result)

        return result

    def cache(self):
        return self.df

    def sample(self, n=10, random=False):
        pass

    def pivot(self, index, column, values):
        pass

    def melt(self, id_vars, value_vars, var_name="variable", value_name="value", data_type="str"):
        pass

    def query(self, sql_expression):
        pass

    def partitions(self):
        pass

    def partitioner(self):
        pass

    def repartition(self, n=None, *args, **kwargs):
        pass

    def show(self):
        df = self
        return df

    def debug(self):
        pass

    def compute(self):
        return self.df

    @staticmethod
    def create_id(column="id"):
        pass


class DataFrameSeriesBaseExt(ABC):

    def __init__(self, series):
        self.series = series
        # super(SeriesBaseExt, self).__init__(series)

    def to_dict(self, index=True):
        """
        Create a dict
        :param index: Return the series index
        :return:
        """
        series = self.series
        if index is True:
            return series.to_dict()
        else:
            return series.to_list()

    def export(self):
        """
        Helper function to export all the dataframe in text format. Aimed to be used in test functions
        :return:
        """
        series = self.series
        df_data = series.to_json()
        df_schema = series.dtype.type

        return f"{df_schema}, {df_data}"

    def table(self):
        pass


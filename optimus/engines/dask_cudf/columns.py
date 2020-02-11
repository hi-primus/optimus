# import cudf as DataFrame

import cudf
import pandas as pd
from dask.dataframe.core import DataFrame
from dask.distributed import as_completed

from optimus.engines.base.dask.columns import DaskBaseColumns
from optimus.engines.dask_cudf.dask_cudf import DaskCUDF
from optimus.helpers.check import equal_function
from optimus.helpers.columns import parse_columns
from optimus.helpers.converter import val_to_list
from optimus.infer import is_, is_future, Infer
from optimus.infer import is_list_of_futures
from optimus.profiler.functions import fill_missing_var_types


def cols(self: DataFrame):
    class Cols(DaskBaseColumns):
        def __init__(self, df):
            super(DaskBaseColumns, self).__init__(df)

        def append(*args, **kwargs):
            return self

        @staticmethod
        def sort(order="asc", columns=None):
            """
            :param order:
            :param columns:
            :return:
            """
            df = self
            columns = val_to_list(columns)

            df.sort_values(by=columns, ascending=True if order == "asc" else False)
            return df

        @staticmethod
        def mode(columns):
            # See https://github.com/rapidsai/cudf/issues/3677
            raise NotImplementedError

        @staticmethod
        def abs(columns):
            pass

        def exec_agg(self, exprs):
            """
            Execute and aggregation
            :param exprs:
            :return:
            """

            # 'scheduler' param values
            # "threads": a scheduler backed by a thread pool
            # "processes": a scheduler backed by a process pool (preferred option on local machines as it uses all CPUs)
            # "single-threaded" (aka “sync”): a synchronous scheduler, good for debugging
            agg_list = DaskCUDF.instance.compute(exprs, scheduler="processes")

            # agg_list = val_to_list(agg_list)

            agg_results = []
            # Distributed mode return a list of Futures objects, Single mode not.
            # TODO: Maybe use .gather
            if is_list_of_futures(agg_list):
                for future in as_completed(agg_list):
                    agg_results.append(future.result())
            elif is_future(agg_list):
                agg_results = agg_list.result()
            else:
                agg_results = agg_list[0]
            print("AGG_RESULTS", agg_results)
            result = {}

            # Parsing results
            def parse_percentile(value):
                _result = {}
                if is_(value, pd.core.series.Series):
                    _result.setdefault(value.name,
                                       {"percentile": {str(i): float(j) for i, j in dict(value).items()}})

                else:
                    for (p_col_name, p_result) in value.iteritems():
                        if is_(p_result, pd.core.series.Series):
                            p_result = dict(p_result)
                        _result.setdefault(p_col_name,
                                           {"percentile": {str(i): float(j) for i, j in p_result.items()}})
                return _result

            def parse_hist(value):
                x = value["count"]
                y = value["bins"]
                _result = []
                for idx, v in enumerate(y):
                    if idx < len(y) - 1:
                        _result.append({"count": x[idx], "lower": y[idx], "upper": y[idx + 1]})
                return _result

            # print("AGG_RESULTS", agg_results)
            print("RESULTS", type(agg_results))
            for agg_name, col_name_result in agg_results:
                if agg_name == "percentile":
                    col_name_result = parse_percentile(col_name_result)
                elif agg_name == "hist":
                    col_name_result = parse_hist(col_name_result)
                # if is_(col_name_result, cudf.core.series.Series):
                #     print(col_name_result)
                # print("RESULT",type(col_name_result))
                if is_(col_name_result, cudf.core.series.Series):
                    # print("*****", col_name_result)
                    for cols_name in col_name_result.index:
                        # print("-----COL_NAME", col_name_result)
                        # print("-----cols_name", cols_name)
                        result[cols_name] = {agg_name: col_name_result[col_name_result.index == cols_name][0]}

                elif is_(col_name_result, dict):
                    print(col_name_result)
                    result = col_name_result
                    # for cols_name, j in col_name_result.iteritems():
                    #     result[cols_name] = {agg_name: j}

            return result

        def create_exprs(self, columns, funcs, *args):
            df = self.df
            # Std, kurtosis, mean, skewness and other agg functions can not process date columns.
            filters = {"object": [df.functions.min],
                       }

            def _filter(_col_name, _func):
                for data_type, func_filter in filters.items():
                    for f in func_filter:
                        if equal_function(func, f) and \
                                df.cols.dtypes(col_name)[col_name] == data_type:
                            return True
                return False

            columns = parse_columns(df, columns)
            funcs = val_to_list(funcs)
            exprs = {}

            # This functions can process all the series at the same time
            multi = [df.functions.min, df.functions.max, df.functions.stddev,
                     df.functions.mean, df.functions.variance, df.functions.percentile_agg, df.functions.kurtosis]
            result = {}
            print("FUNCS", funcs)
            for func in funcs:
                # Create expression for functions that accepts multiple columns
                if equal_function(func, multi):
                    exprs.update(func(columns, args)(df))
                    for k, v in exprs.items():
                        if k in result:
                            result[k].update(v)
                        else:
                            result[k] = {}
                            result[k] = v
                    result = list(result.items())
                # If not process by column
                else:
                    for col_name in columns:
                        # If the key exist update it
                        if not _filter(col_name, func):

                            if col_name in exprs:
                                result[col_name].update(func(col_name, args)(df))
                            else:
                                print("COL_NAME", col_name)
                                result[col_name] = func(col_name, args)(df)
                print("FUNCS", result)

            # Convert to list
            return result

        def count_by_dtypes(self, columns, infer=False, str_funcs=None, int_funcs=None, mismatch=None):
            df = self.df
            columns = parse_columns(df, columns)
            dtypes = df.cols.dtypes()

            result = {}
            for col_name in columns:
                df_result = df[col_name].map_partitions(Infer.parse_dask, col_name, infer, dtypes, str_funcs,
                                                        int_funcs, meta=str).compute()

                result[col_name] = dict(df_result.value_counts())

            if infer is True:
                for k in result.keys():
                    result[k] = fill_missing_var_types(result[k])
            else:
                result = self.parse_profiler_dtypes(result)

            return result

    return Cols(self)


DataFrame.cols = property(cols)

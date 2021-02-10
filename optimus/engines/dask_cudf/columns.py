import dask
from dask_ml import preprocessing

from optimus.engines.base.commons.functions import string_to_index, index_to_string, impute
from optimus.engines.base.dask.columns import DaskBaseColumns
from optimus.helpers.columns import parse_columns
from optimus.infer import Infer, is_dict
from optimus.profiler.functions import fill_missing_var_types


class Cols(DaskBaseColumns):
    def __init__(self, df):
        super(DaskBaseColumns, self).__init__(df)

    def _names(self):
        return list(self.root.data.columns)

    def _map(self, df, input_col, output_col, func, args, kw_columns):
        kw_columns[output_col] = df[input_col].map_partitions(func, *args)
        kw_columns[output_col] = df[input_col].map_partitions(func, *args)
        return kw_columns

    def _series_to_dict(self, series):
        return series.to_pandas().to_dict()

    def string_to_index(self, input_cols=None, output_cols=None, columns=None):
        df = self.root
        le = preprocessing.LabelEncoder()
        return string_to_index(df, input_cols, output_cols, le)

    def index_to_string(self, input_cols=None, output_cols=None, columns=None):
        df = self.root
        le = preprocessing.LabelEncoder()
        return index_to_string(df, input_cols, output_cols, le)

    def count_by_dtypes(self, columns, infer=False, str_funcs=None, int_funcs=None, mismatch=None):
        df = self.root
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

    def impute(self, input_cols, data_type="continuous", strategy="mean", output_cols=None):
        df = self.root
        return impute(df, input_cols, data_type="continuous", strategy="mean", output_cols=None)

    def hist(self, columns="*", buckets=20, compute=True):

        df = self.root
        columns = parse_columns(df, columns)
        import cupy as cp

        def _bins_col(_columns, _min, _max):
            return {col_name: cp.linspace(_min["min"][col_name], _max["max"][col_name], num=buckets) for
                    col_name in _columns}

        _min = df.cols.min(columns, compute=False, tidy=False)
        _max = df.cols.max(columns, compute=False, tidy=False)
        _bins = _bins_col(columns, _min, _max)

        def chunk(pdf, _bins):
            _count, _bins = cp.histogram(cp.array(pdf.to_gpu_array()), bins=_bins)
            return _count

        reductions = []

        @dask.delayed
        def format(_count, _bins, _col_name):
            result = {}
            result[_col_name] = [
                {"lower": float(_bins[i]), "upper": float(_bins[i + 1]), "count": int(_count[i])}
                for i in range(len(_bins) - 1)]
            return result

        for col_name in columns:
            r = df.cols.to_float(col_name).data[col_name].reduction(chunk, aggregate=lambda x: x.sum(),
                                                                     chunk_kwargs={
                                                                         '_bins': _bins[col_name]})
            reductions.append(format(r, _bins[col_name], col_name))

        d = dask.delayed(reductions)

        if compute:
            result = d.compute()
        else:
            result = d
        
        _res = {}

        for res in result:
            _res.update(res)
            
        return {"hist": _res}

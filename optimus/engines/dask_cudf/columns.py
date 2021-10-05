import dask
import fastnumbers
from dask_ml import preprocessing

from optimus.engines.base.commons.functions import string_to_index, index_to_string, impute
from optimus.engines.base.dask.columns import DaskBaseColumns
from optimus.engines.base.cudf.columns import CUDFBaseColumns
from optimus.helpers.columns import parse_columns
from optimus.helpers.raiseit import RaiseIt
from optimus.profiler.functions import fill_missing_var_types


class Cols(CUDFBaseColumns, DaskBaseColumns):
    def __init__(self, df):
        super().__init__(df)

    def _series_to_pandas(self, series):
        return series.compute().to_pandas()

    def _names(self):
        return list(self.root.data.columns)

    def _map(self, df, input_col, output_col, func, args):
        return df[input_col].map_partitions(func, *args)
        # kw_columns[output_col] = df[input_col].map_partitions(func, *args)
        # return kw_columns

    def string_to_index(self, cols=None, output_cols=None):
        df = self.root
        df.le = df.le or preprocessing.LabelEncoder()
        return string_to_index(df, cols, output_cols, df.le)

    def index_to_string(self, cols=None, output_cols=None):
        df = self.root
        df.le = df.le or preprocessing.LabelEncoder()
        return index_to_string(df, cols, output_cols, df.le)

    def hist(self, columns="*", buckets=20, compute=True):

        df = self.root
        columns = parse_columns(df, columns)
        import cupy as cp

        def _bins_col(_columns, _min, _max):
            # In some cases a string can be passed as min or max values. Try to convert them to numeric if not nan
            return {col_name: cp.linspace(fastnumbers.fast_float(_min["min"][col_name], default=cp.nan),
                                          fastnumbers.fast_float(_max["max"][col_name], default=cp.nan),
                                          num=buckets) for
                    col_name in _columns}

        _min = df.cols.min(columns, numeric=True, compute=False, tidy=False)
        _max = df.cols.max(columns, numeric=True, compute=False, tidy=False)

        _bins = _bins_col(columns, _min, _max)

        def chunk(pdf, _bins):
            _count, _bins = cp.histogram(cp.array(pdf.to_gpu_array()), bins=_bins)
            return _count

        reductions = []

        @dask.delayed
        def _format_dict(_col_name, _count, _bins):
            result = {}
            result[_col_name] = [
                {"lower": float(_bins[i]), "upper": float(_bins[i + 1]), "count": int(_count[i])}
                for i in range(len(_bins) - 1)]
            return result

        for col_name in columns:
            count = df.cols.to_float(col_name).data[col_name].reduction(chunk, aggregate=lambda x: x.sum(),
                                                                        chunk_kwargs={
                                                                            '_bins': _bins[col_name]})
            reductions.append(_format_dict(col_name, count, _bins[col_name]))

        d = dask.delayed(reductions)

        if compute:
            result = d.compute()
        else:
            result = d

        _res = {}

        for res in result:
            _res.update(res)

        return {"hist": _res}

    def _unnest(self, dfd, input_col, final_columns, separator, splits, mode, output_cols):

        modes = {
            "string": lambda series: series.astype(str).str.split(separator, expand=True, n=splits-1),
            "array": lambda series: series.apply(pd.Series)
        }

        if mode in list(modes.keys()):
            partitions = self.F.to_delayed(dfd[input_col])
            partitions = [modes[mode](part) for part in partitions]
            dfd_new = self.F.from_delayed(partitions)
            
        else:
            RaiseIt.value_error(mode, list(modes.keys()))

        return dfd_new

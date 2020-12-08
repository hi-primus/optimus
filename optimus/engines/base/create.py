import pandas as pd

from optimus.engines.base.meta import Meta


class Create:
    def __init__(self, root):
        self.root = root

    def dataframe(self, dict, cols=None, rows=None, pdf=None, n_partitions=1, *args, **kwargs):
        """
        Helper to create dataframe:
        :param cols: List of Tuple with name, data type and a flag to accept null
        :param rows: List of Tuples with the same number and types that cols
        :param pdf: a pandas dataframe
        :param n_partitions:
        :return: Dataframe
        """
        creator = self.creator
        if pdf is None:

            # Process the rows
            if not is_list_of_tuples(rows):
                rows = [(i,) for i in rows]

            if is_list_of_tuples(cols):
                _columns = [c[0] for c in cols]
                _dtypes = [c[1] for c in cols]
            else:
                _columns = cols

            # Process the columns
            if creator.__name__ == "dask_cudf":
                import cudf
                pdf = cudf.DataFrame(columns=_columns, data=rows)
            else:
                pdf = pd.DataFrame(columns=_columns, data=rows)

            for col, dtype in zip(_columns, _dtypes):
                pdf[col].astype(dtype)

        creator = self.creator
        if creator.__name__ == "pandas" or creator.__name__ == "cudf":
            df = creator.DataFrame(pdf, *args, **kwargs)
        elif creator.__name__ == "dask.dataframe":
            df = self.creator.from_pandas(pdf, npartitions=n_partitions, *args, **kwargs)
        elif creator.__name__ == "dask_cudf":
            df = self.creator.from_cudf(pdf, npartitions=n_partitions, *args, **kwargs)

        df.meta = Meta.set(df.meta, value=df.meta.columns(df.cols.names()).get())
        return df

    df = data_frame

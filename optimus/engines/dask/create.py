import pandas as pd
from dask import dataframe as dd

from optimus.infer import is_, is_list_of_tuples


class Create:
    @staticmethod
    def data_frame(cols=None, rows=None, infer_schema=True, pdf=None):
        """
        Helper to create a Spark dataframe:
        :param cols: List of Tuple with name, data type and a flag to accept null
        :param rows: List of Tuples with the same number and types that cols
        :param infer_schema: Try to infer the schema data type.
        :param pdf: a pandas dataframe
        :return: Dataframe
        """

        if is_(pdf, pd.DataFrame):
            df = dd.from_pandas(pdf, npartitions=1)
        else:

            # Process the rows
            if not is_list_of_tuples(rows):
                rows = [(i,) for i in rows]

            if is_list_of_tuples(cols):
                _columns = [c[0] for c in cols]
                _dtypes = [c[1] for c in cols]
            else:
                _columns = cols

            # Process the columns
            pdf = pd.DataFrame(columns=_columns, data=rows)

            for col, dtype in zip(_columns, _dtypes):
                pdf[col].astype(dtype)

            df = dd.from_pandas(pdf, npartitions=1)
        df = df.meta.columns(df.cols.names())
        return df

    df = data_frame

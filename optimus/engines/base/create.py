import pandas as pd

from optimus.infer import is_list_of_tuples, is_


class Create:
    def __init__(self, creator):
        self.creator = creator

    def data_frame(self, cols=None, rows=None, pdf=None, n_partitions =1 ,*args, **kwargs):
        """
        Helper to create dataframe:
        :param cols: List of Tuple with name, data type and a flag to accept null
        :param rows: List of Tuples with the same number and types that cols
        :param pdf: a pandas dataframe
        :return: Dataframe
        """

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
            pdf = pd.DataFrame(columns=_columns, data=rows)

            for col, dtype in zip(_columns, _dtypes):
                pdf[col].astype(dtype)

        # df = dd.from_pandas(pdf, npartitions=1)
        creator = self.creator

        if creator == pd:
            df = self.creator.DataFrame(pdf, *args, **kwargs)
        else:
            df = self.creator.from_pandas(pdf, npartitions=n_partitions, *args, **kwargs)

        df = df.meta.columns(df.cols.names())
        return df

    df = data_frame

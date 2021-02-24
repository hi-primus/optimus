from optimus.engines.base.dataframe.dataframe import Ext as BaseDataFrame
from optimus.engines.cudf.io.save import Save


class CUDFDataFrame(BaseDataFrame):
    def __init__(self, data):
        super().__init__(self, data)

    @property
    def rows(self):
        from optimus.engines.cudf.rows import Rows
        return Rows(self)

    @property
    def cols(self):
        from optimus.engines.cudf.columns import Cols
        return Cols(self)

    @property
    def save(self):
        return Save(self)

    @property
    def functions(self):
        from optimus.engines.cudf.functions import CUDFFunctions
        return CUDFFunctions()

    @property
    def constants(self):
        from optimus.engines.cudf.constants import constants
        return constants(self)

    def _create_buffer_df(self, input_cols, n):
        pass

    def _buffer_window(self, input_cols, lower_bound, upper_bound):
        return PandasDataFrame(self.data[input_columns][lower_bound: upper_bound].to_pandas())

    def get_buffer(self):
        return self

    def buffer_window(self, columns=None, lower_bound=None, upper_bound=None, n=None):
        # TODO: This is the same method that the one in PandasDataFrame
        if lower_bound is None:
            lower_bound = 0

        if lower_bound < 0:
            lower_bound = 0

        df_length = self.rows.count()

        if upper_bound is None:
            upper_bound = df_length

        input_columns = parse_columns(self, columns)

        if lower_bound == 0 and upper_bound == df_length:
            result = self[input_columns]
        else:
            if upper_bound > df_length:
                upper_bound = df_length

            if lower_bound >= df_length:
                diff = upper_bound - lower_bound
                lower_bound = df_length - diff
                upper_bound = df_length

            result = self._buffer_window(input_columns, lower_bound, upper_bound)

        return result

    def head(self, columns="*", n=10):
        """

        :return:
        """
        df = self.root
        columns = parse_columns(df, columns)
        return df.data[columns].head(n).to_pandas()

    def to_pandas(self):
        return self.data.to_pandas()

    def to_dict(self, orient="records", index=True):
        """
        Create a dict
        :param orient:
        :param index: Return the series index
        :return:
        """

        series = self.data
        if index is True:
            return series.to_pandas().to_dict(orient)
        else:
            return series.to_pandas().to_list()

    def repartition(self, n=None, *args, **kwargs):
        return self.root

from abc import ABC

from optimus.engines.base.extension import BaseExt


class DataFrameBaseExt(BaseExt):

    def __init__(self, df):
        super(DataFrameBaseExt, self).__init__(df)

    @staticmethod
    def delayed(func):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

    def cache(self):
        return self.df

    def sample(self, n=10, random=False):
        pass

    def pivot(self, index, column, values):
        pass

    def melt(self, id_vars, value_vars, var_name="variable", value_name="value", data_type="str"):
        pass

    def to_delayed(self):
        return [self.df]

    def query(self, sql_expression):
        pass

    def partitions(self):
        pass

    def partitioner(self):
        pass

    def repartition(self, n=None, *args, **kwargs):
        return self.df

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

    @staticmethod
    def delayed(func):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return wrapper

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

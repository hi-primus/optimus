from abc import abstractmethod, ABC

# This implementation works for Spark, Dask, dask_cudf
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import Actions
from optimus.infer import is_str


class BaseRows(ABC):
    """Base class for all Rows implementations"""

    def __init__(self, df):
        self.df = df

    @staticmethod
    @abstractmethod
    def create_id(column="id"):
        pass

    @staticmethod
    @abstractmethod
    def append(rows):
        pass

    def apply(self, func, args=None, output_cols=None):
        """
        This will aimed to handle vectorized and not vectorized operations
        :param output_cols:
        :param func:
        :return:
        """
        df = self.df
        kw_columns = {}

        for output_col in output_cols:
            result = func(df, *args)
            kw_columns = {output_col: result}

        return df.assign(**kw_columns)

    def find(self, condition):
        """

        :param condition: a condition like (df.A > 0) & (df.B <= 10)
        :return:
        """
        df = self
        if is_str(condition):
            condition = eval(condition)

        df["__match__"] = condition
        return df

    def select(self, condition):
        """

        :param condition: a condition like (df.A > 0) & (df.B <= 10)
        :return:
        """

        df = self.df
        if is_str(condition):
            condition = eval(condition)
        df = df[condition]
        df = df.meta.preserve(df, Actions.SORT_ROW.value, df.cols.names())
        return df

    def count(self, compute=True) -> int:
        """
        Count dataframe rows
        """
        df = self.df
        if compute is True:
            result = len(df.ext.compute())
        else:
            result = len(df)
        return result

    @staticmethod
    @abstractmethod
    def to_list(input_cols):
        pass

    @staticmethod
    @abstractmethod
    def sort(input_cols):
        pass

    def drop(self, where=None):
        """
        Drop a row depending on a dataframe expression
        :param where: Expression used to drop the row, For Ex: (df.A > 3) & (df.A <= 1000)
        :return: Spark DataFrame
        :return:
        """
        df = self.df
        if is_str(where):
            where = eval(where)

        df = df[~where]
        df = df.meta.preserve(df, Actions.DROP_ROW.value, df.cols.names())
        return df

    @staticmethod
    @abstractmethod
    def between(columns, lower_bound=None, upper_bound=None, invert=False, equal=False,
                bounds=None):
        pass

    @staticmethod
    @abstractmethod
    def drop_by_dtypes(input_cols, data_type=None):
        pass

    def drop_na(self, subset=None, how="any", *args, **kwargs):
        """
        Removes rows with null values. You can choose to drop the row if 'all' values are nulls or if
        'any' of the values is null.
        :param subset:
        :param how:
        :return:
        """
        df = self.df
        subset = parse_columns(df, subset)
        df = df.meta.preserve(df, Actions.DROP_ROW.value, df.cols.names())
        return df.dropna(how=how, subset=subset)

    @staticmethod
    @abstractmethod
    def drop_duplicates(input_cols=None):
        """
        Drop duplicates values in a dataframe
        :param input_cols: List of columns to make the comparison, this only  will consider this subset of columns,
        :return: Return a new DataFrame with duplicate rows removed
        :param input_cols:
        :return:
        """
        pass

    @staticmethod
    @abstractmethod
    def limit(count):
        """
        Limit the number of rows
        :param count:
        :return:
        """

        pass

    @staticmethod
    @abstractmethod
    def is_in(input_cols, values):
        pass

    @staticmethod
    @abstractmethod
    def unnest(input_cols):
        pass

    def approx_count(self):
        """
        Aprox count
        :return:
        """
        return self.count()

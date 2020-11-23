from abc import abstractmethod, ABC

# This implementation works for Spark, Dask, dask_cudf
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import Actions
from optimus.infer import is_str, Infer


class BaseRows(ABC):
    """Base class for all Rows implementations"""

    def __init__(self, parent):
        self.parent = parent

    @staticmethod
    @abstractmethod
    def create_id(column="id"):
        pass

    @staticmethod
    @abstractmethod
    def append(rows):
        pass

    #
    def greater_than(self, input_col, value):

        df = self.parent.data
        return self.parent.new(df[df[input_col] > value])

    def greater_than_equal(self, input_col, value):

        df = self.parent.data
        return self.parent.new(df[df[input_col] >= value])

    def less_than(self, input_col, value):

        df = self.parent.data
        return self.parent.new(df[df[input_col] < value])

    def less_than_equal(self, input_col, value):

        df = self.parent.data
        return self.parent.new(df[df[input_col] <= value])

    def equal(self, input_col, value):

        df = self.parent.data
        return self.parent.new(df[df[input_col] == value])

    def not_equal(self, input_col, value):

        df = self.parent.data
        return self.parent.new(df[df[input_col] != value])

    def missing(self, col_name):
        """
        Return missing values
        :param col_name:
        :return:
        """
        df = self.parent.data

        mask_null = df[col_name].isnull()
        return self.parent.new(df[~mask_null])

    def mismatch(self, col_name, dtype):
        """
        Return mismatches values
        :param col_name:
        :param dtype:
        :return:
        """
        df = self.parent.data
        mask = df[col_name].astype("str").str.match(Infer.ProfilerDataTypesFunctions[dtype])
        mask_null = df[col_name].isnull()
        return self.parent.new(df[~mask & ~mask_null])

    def match(self, col_name, dtype):
        """
        Return Match values
        :param col_name:
        :param dtype:
        :return:
        """
        df = self.parent.data
        mask = df[col_name].astype("str").str.match(Infer.ProfilerDataTypesFunctions[dtype])
        return self.parent.new(df[mask])

    def apply(self, func, args=None, output_cols=None):
        """
        This will aimed to handle vectorized and not vectorized operations
        :param output_cols:
        :param func:
        :return:
        """
        df = self.parent.data
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
        df = self.parent.data
        if is_str(condition):
            condition = eval(condition)

        df["__match__"] = condition
        return self.parent.new(df)

    def select(self, condition):
        """

        :param condition: a condition like (df.A > 0) & (df.B <= 10)
        :return:
        """

        df = self.parent.data
        if is_str(condition):
            condition = eval(condition)
        df = df[condition]
        odf = self.parent.new(df)
        odf.meta.action(Actions.SORT_ROW.value, odf.cols.names())
        return odf

    def count(self, compute=True) -> int:
        """
        Count dataframe rows
        """
        df = self.parent.data
        # TODO: Be sure that we need the compute param
        if compute is True:
            result = len(df)
        else:
            result = len(df)
        return result

    def to_list(self, input_cols):
        """

        :param input_cols:
        :return:
        """
        odf = self.parent
        input_cols = parse_columns(odf, input_cols)
        df = odf.cols.select(input_cols).to_pandas().values.tolist()

        return df

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
        df = self.parent.data
        if is_str(where):
            where = eval(where)

        df = df[~where]
        odf = self.parent.new(df)
        odf.meta.action(Actions.DROP_ROW.value, odf.cols.names())
        return odf

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
        df = self.parent
        subset = parse_columns(df.data, subset)
        df = df.meta.preserve(df, Actions.DROP_ROW.value, df.cols.names())
        return self.parent.new(df.dropna(how=how, subset=subset))

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
    def is_in(input_cols, values, output_cols=None):
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

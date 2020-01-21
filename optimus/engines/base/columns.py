from abc import abstractmethod, ABC
from enum import Enum

from optimus.helpers.columns import parse_columns, check_column_numbers
from optimus.helpers.constants import RELATIVE_ERROR
from optimus.helpers.converter import format_dict


# This implementation works for Spark, Dask, dask_cudf

class BaseColumns(ABC):
    """Base class for all Cols implementations"""

    def __init__(self, df):
        self.df = df

    @staticmethod
    @abstractmethod
    def append(*args, **kwarsg):
        pass

    @staticmethod
    @abstractmethod
    def select(columns="*", regex=None, data_type=None, invert=False) -> str:
        pass

    @staticmethod
    @abstractmethod
    def copy(input_cols, output_cols=None, columns=None):
        pass

    @staticmethod
    @abstractmethod
    def to_timestamp(input_cols, date_format=None, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def apply_expr(input_cols, func=None, args=None, filter_col_by_dtypes=None, output_cols=None,
                   meta=None):
        pass

    @staticmethod
    @abstractmethod
    def apply(input_cols, func=None, func_return_type=None, args=None, func_type=None, when=None,
              filter_col_by_dtypes=None, output_cols=None, skip_output_cols_processing=False,
              meta="apply"):
        pass

    @staticmethod
    @abstractmethod
    def apply_by_dtypes(columns, func, func_return_type, args=None, func_type=None, data_type=None):
        pass

    @staticmethod
    @abstractmethod
    def set(output_col, value=None):
        pass

    @staticmethod
    @abstractmethod
    def rename(*args, **kwargs) -> Enum:
        pass

    def parse_profiler_dtypes(self, col_data_type):
        """
        Parse a spark data type to a profiler data type
        :return:
        """
        df = self.df
        columns = {}
        for k, v in col_data_type.items():
            # Initialize values to 0
            result_default = {data_type: 0 for data_type in df.constants.DTYPES_TO_PROFILER.keys()}
            for k1, v1 in v.items():
                for k2, v2 in df.constants.DTYPES_TO_PROFILER.items():
                    if k1 in df.constants.DTYPES_TO_PROFILER[k2]:
                        result_default[k2] = result_default[k2] + v1
            columns[k] = result_default
        return columns

    @staticmethod
    @abstractmethod
    def cast(input_cols=None, dtype=None, output_cols=None, columns=None):
        pass

    @staticmethod
    @abstractmethod
    def astype(*args, **kwargs):
        pass

    @staticmethod
    @abstractmethod
    def move(column, position, ref_col=None):
        pass

    @staticmethod
    @abstractmethod
    def keep(columns=None, regex=None):
        pass

    @staticmethod
    @abstractmethod
    def sort(order="asc", columns=None):
        pass

    @staticmethod
    @abstractmethod
    def drop(columns=None, regex=None, data_type=None):
        pass

    def dtypes(self, columns="*"):
        """
        Return the column(s) data type as string
        :param columns: Columns to be processed
        :return:
        """
        df = self.df
        columns = parse_columns(df, columns)
        data_types = ({k: str(v) for k, v in dict(df.dtypes).items()})
        return {col_name: data_types[col_name] for col_name in columns}

    @staticmethod
    @abstractmethod
    def create_exprs(columns, funcs, *args):
        pass

    def agg_exprs(self, columns, funcs, *args):
        """
        Create and run aggregation
        :param columns:
        :param funcs:
        :param args:
        :return:
        """
        return self.exec_agg(self.create_exprs(columns, funcs, *args))

    @staticmethod
    @abstractmethod
    def exec_agg(exprs):
        pass

    def min(self, columns):
        df = self.df
        return self.agg_exprs(columns, df.functions.min)

    def max(self, columns):
        df = self.df
        return self.agg_exprs(columns, df.functions.min)

    def range(self, columns):
        df = self.df
        return self.agg_exprs(columns, df.functions.range_agg)

    def percentile(self, columns, values=None, relative_error=RELATIVE_ERROR):
        df = self.df
        # values = [str(v) for v in values]
        if values is None:
            values = [0.5]
        return self.agg_exprs(columns, df.functions.percentile_agg, df, values, relative_error)

    def median(self, columns, relative_error=RELATIVE_ERROR):
        return format_dict(self.percentile(columns, [0.5], relative_error))

    # Descriptive Analytics
    # TODO: implement double MAD http://eurekastatistics.com/using-the-median-absolute-deviation-to-find-outliers/

    def mad(self, columns, relative_error=RELATIVE_ERROR, more=None):
        df = self.df
        columns = parse_columns(df, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        result = {}
        for col_name in columns:
            funcs = [df.functions.mad_agg]

            result[col_name] = self.agg_exprs(columns, funcs, more)

        return format_dict(result)

    def std(self, columns):
        df = self.df
        columns = parse_columns(df, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")
        return format_dict(self.agg_exprs(columns, df.functions.stddev))

    def kurt(self, columns):
        df = self.df
        columns = parse_columns(self, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return format_dict(self.agg_exprs(columns, df.functions.kurtosis))

    def mean(self, columns):
        df = self.df
        columns = parse_columns(df, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return format_dict(self.agg_exprs(columns, df.functions.mean))

    def skewness(self, columns):
        df = self.df
        columns = parse_columns(self, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return format_dict(self.agg_exprs(columns, df.functions.skewness))

    def sum(self, columns):
        df = self.df
        columns = parse_columns(self, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return format_dict(self.agg_exprs(columns, df.functions.sum))

    def variance(self, columns):
        df = self.df
        columns = parse_columns(self, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return format_dict(self.agg_exprs(columns, df.functions.variance))

    @staticmethod
    @abstractmethod
    def abs(columns):
        pass

    @staticmethod
    @abstractmethod
    def mode(columns):
        pass

    @staticmethod
    @abstractmethod
    def lower(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def upper(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def trim(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def reverse(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def remove(columns, search=None, search_by="chars", output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def remove_accents(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def remove_special_chars(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def remove_white_spaces(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def date_transform(input_cols, current_format=None, output_format=None, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def years_between(input_cols, date_format=None, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def replace(input_cols, search=None, replace_by=None, search_by="chars", output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def replace_regex(input_cols, regex=None, value=None, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def impute(input_cols, data_type="continuous", strategy="mean", output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def fill_na(input_cols, value=None, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def is_na(input_cols, output_cols=None):
        pass

    @staticmethod
    def count(self):
        pass

    @staticmethod
    @abstractmethod
    def count_na(columns):
        pass

    @staticmethod
    @abstractmethod
    def count_zeros(columns):
        pass

    @staticmethod
    @abstractmethod
    def count_uniques(columns, estimate=True):
        pass

    @staticmethod
    @abstractmethod
    def value_counts(columns):
        pass

    @staticmethod
    @abstractmethod
    def unique(columns):
        pass

    @staticmethod
    @abstractmethod
    def nunique(*args, **kwargs):
        pass

    @staticmethod
    @abstractmethod
    def select_by_dtypes(data_type):
        pass

    @staticmethod
    @abstractmethod
    def _math(columns, operator, new_column):
        """
        Helper to process arithmetic operation between columns. If a
        :param columns: Columns to be used to make the calculation
        :param operator: A lambda function
        :return:
        """

        # df = self
        # columns = parse_columns(df, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
        # check_column_numbers(columns, "*")
        #
        # for col_name in columns:
        #     df = df.cols.cast(col_name, "float")
        #
        # if len(columns) < 2:
        #     raise Exception("Error: 2 or more columns needed")
        #
        # columns = list(map(lambda x: F.col(x), columns))
        # expr = reduce(operator, columns)
        #
        # return df.withColumn(new_column, expr)
        pass

    @staticmethod
    def add(columns, col_name="sum"):
        """
        Add two or more columns
        :param columns: '*', list of columns names or a single column name
        :param col_name:
        :return:
        """

        return BaseColumns._math(columns, lambda x, y: x + y, col_name)

    @staticmethod
    def sub(columns, col_name="sub"):
        """
        Subs two or more columns
        :param columns: '*', list of columns names or a single column name
        :param col_name:
        :return:
        """
        return BaseColumns._math(columns, lambda x, y: x - y, col_name)

    @staticmethod
    def mul(columns, col_name="mul"):
        """
        Multiply two or more columns
        :param columns: '*', list of columns names or a single column name
        :param col_name:
        :return:
        """
        return BaseColumns._math(columns, lambda x, y: x * y, col_name)

    @staticmethod
    def div(columns, col_name="div"):
        """
        Divide two or more columns
        :param columns: '*', list of columns names or a single column name
        :param col_name:
        :return:
        """
        return BaseColumns._math(columns, lambda x, y: x / y, col_name)

    @staticmethod
    @abstractmethod
    def z_score(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def min_max_scaler(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def max_abs_scaler(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def iqr(columns, more=None, relative_error=None):
        pass

    @staticmethod
    @abstractmethod
    def nest(input_cols, shape="string", separator="", output_col=None):
        pass

    @staticmethod
    @abstractmethod
    def unnest(input_cols, separator=None, splits=None, index=None, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def cell(column):
        pass

    @staticmethod
    @abstractmethod
    def scatter(columns, buckets=10):
        pass

    def hist(self, columns, buckets=20):
        df = self.df
        result = self.agg_exprs(columns, df.functions.hist_agg, df, buckets, None)
        return result

    @staticmethod
    @abstractmethod
    def frequency_by_group(columns, n=10, percentage=False, total_rows=None):
        pass

    @staticmethod
    @abstractmethod
    def count_mismatch(columns_mismatch: dict = None):
        pass

    @staticmethod
    @abstractmethod
    def count_by_dtypes(columns, infer=False, str_funcs=None, int_funcs=None):
        pass

    @staticmethod
    @abstractmethod
    def frequency(columns, n=10, percentage=False, total_rows=None):
        pass

    @staticmethod
    @abstractmethod
    def correlation(input_cols, method="pearson", output="json"):
        pass

    @staticmethod
    @abstractmethod
    def boxplot(columns):
        # """
        # Output values frequency in json format
        # :param columns: Columns to be processed
        # :return:
        # """
        # df = self
        # columns = parse_columns(df, columns)
        #
        # for col_name in columns:
        #     iqr = df.cols.iqr(col_name, more=True)
        #     lb = iqr["q1"] - (iqr["iqr"] * 1.5)
        #     ub = iqr["q3"] + (iqr["iqr"] * 1.5)
        #
        #     _mean = df.cols.mean(columns)
        #
        #     query = ((F.col(col_name) < lb) | (F.col(col_name) > ub))
        #     fliers = collect_as_list(df.rows.select(query).cols.select(col_name).limit(1000))
        #     stats = [{'mean': _mean, 'med': iqr["q2"], 'q1': iqr["q1"], 'q3': iqr["q3"], 'whislo': lb, 'whishi': ub,
        #               'fliers': fliers, 'label': one_list_to_val(col_name)}]
        #
        #     return stats
        pass

    @staticmethod
    @abstractmethod
    def schema_dtype(columns="*"):
        pass

    def names(self, col_names="*", filter_by_column_dtypes=None, invert=False):
        columns = parse_columns(self.df, col_names, filter_by_column_dtypes=filter_by_column_dtypes, invert=invert)
        return columns

    @staticmethod
    @abstractmethod
    def qcut(columns, num_buckets, handle_invalid="skip"):
        pass

    @staticmethod
    @abstractmethod
    def clip(columns, lower_bound, upper_bound):
        pass

    @staticmethod
    @abstractmethod
    def values_to_cols(input_cols):
        pass

    @staticmethod
    @abstractmethod
    def string_to_index(input_cols=None, output_cols=None, columns=None):
        pass

    @staticmethod
    @abstractmethod
    def index_to_string(input_cols=None, output_cols=None, columns=None):
        pass

    @staticmethod
    @abstractmethod
    def bucketizer(input_cols, splits, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def set_meta(col_name, spec=None, value=None, missing=dict):
        pass

    @staticmethod
    @abstractmethod
    def get_meta(col_name, spec=None):
        pass

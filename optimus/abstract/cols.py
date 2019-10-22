from abc import abstractmethod, ABC


class AbstractCols(ABC):

    # Basics
    @abstractmethod
    def schema_dtype(self):
        pass

    @abstractmethod
    def dtypes(self):
        pass

    @abstractmethod
    def names(self):
        pass

    @abstractmethod
    def append(self):
        pass

    @abstractmethod
    def select(self):
        pass

    @abstractmethod
    def copy(self):
        pass

    @abstractmethod
    def to_timestamp(self):
        pass

    @abstractmethod
    def apply_expr(self):
        pass

    @abstractmethod
    def apply(self):
        pass

    @abstractmethod
    def apply_by_dtypes(self):
        pass

    @abstractmethod
    def rename(self):
        pass

    @abstractmethod
    def cast(self):
        pass

    @abstractmethod
    def astype(self):
        pass

    @abstractmethod
    def move(self):
        pass

    @abstractmethod
    def keep(self):
        pass

    @abstractmethod
    def sort(self):
        pass

    @abstractmethod
    def drop(self):
        pass

    # Aggregations
    @abstractmethod
    def create_exprs(self):
        pass

    @abstractmethod
    def agg_exprs(self):
        pass

    @abstractmethod
    def exec_agg(self):
        pass

    @staticmethod
    def min(columns):
        return Cols.agg_exprs(columns, self.functions.min)

    @abstractmethod
    def max(self):
        pass

    @abstractmethod
    def range(self):
        pass

    @abstractmethod
    def median(self):
        pass

    @abstractmethod
    def percentile(self):
        pass

    @abstractmethod
    def std(self):
        pass

    @staticmethod
    @abstractmethod
    def kurt(columns):
        pass

    @staticmethod
    @abstractmethod
    def mean(columns):
        pass

    @staticmethod
    @abstractmethod
    def skewness(columns):
        pass

    @staticmethod
    @abstractmethod
    def sum(columns):
        pass

    @staticmethod
    @abstractmethod
    def variance(columns):
        pass

    @staticmethod
    @abstractmethod
    def abs(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def mode(columns):
        pass

    # Chars Operations
    @staticmethod
    @abstractmethod
    def lower(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def upper(input_cols, output_cols=None):
        pass

    # String Operations
    @staticmethod
    @abstractmethod
    def trim(input_cols, output_cols=None):
        pass

    @staticmethod
    @abstractmethod
    def reverse(input_cols, output_cols=None):
        pass

    @abstractmethod
    def remove(self):
        pass

    @abstractmethod
    def remove_accents(self):
        pass

    @abstractmethod
    def remove_special_chars(self):
        pass

    @abstractmethod
    def remove_white_spaces(self):
        pass

    @abstractmethod
    def date_transform(self):
        pass

    @abstractmethod
    def years_between(self):
        pass

    @abstractmethod
    def replace(self):
        pass

    @abstractmethod
    def replace_regex(self):
        pass

    @abstractmethod
    def impute(self):
        pass

    @abstractmethod
    def fill_na(self):
        pass

    @abstractmethod
    def is_na(self):
        pass

    # Counts
    @abstractmethod
    def count(self):
        pass

    @abstractmethod
    def count_na(self):
        pass

    @abstractmethod
    def count_zeros(self):
        pass

    @abstractmethod
    def count_uniques(self):
        pass

    @abstractmethod
    def value_counts(self):
        pass

    @abstractmethod
    def unique(self):
        pass

    @abstractmethod
    def nunique(self):
        pass

    @abstractmethod
    def add(self):
        pass

    @abstractmethod
    def sub(self):
        pass

    @abstractmethod
    def mul(self):
        pass

    @abstractmethod
    def div(self):
        pass

    @abstractmethod
    def z_score(self):
        pass

    @abstractmethod
    def iqr(self):
        pass

    @abstractmethod
    def nest(self):
        pass

    @abstractmethod
    def unnest(self):
        pass

    @abstractmethod
    def cell(self):
        pass

    @abstractmethod
    def scatter(self):
        pass

    @abstractmethod
    def hist(self):
        pass

    @abstractmethod
    def frequency(self):
        pass

    @abstractmethod
    def correlation(self):
        pass

    @abstractmethod
    def boxplot(self):
        pass

    @abstractmethod
    def count_by_dtypes(self):
        pass

    @abstractmethod
    def parse_profiler_dtypes(self):
        pass

    @abstractmethod
    def qcut(self):
        pass

    @abstractmethod
    def clip(self):
        pass

    @abstractmethod
    def bucketizer(self):
        pass

    # Meta data
    @abstractmethod
    def set_meta(self):
        pass

    @abstractmethod
    def get_meta(self):
        pass

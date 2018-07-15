from pyspark.sql import DataFrame
from pyspark.sql.dataframe import *

from pyspark.sql import functions as F
# from pyspark.sql.functions import *
from pyspark.sql.functions import Column
import builtins
import re
import unicodedata
import string

# Library used for method overloading using decorators
from multipledispatch import dispatch

from pyspark.ml.feature import Imputer

# Helpers
from optimus.helpers.constants import *
from optimus.helpers.decorators import *
from optimus.helpers.functions import *

@add_attr(DataFrame)
def cells():
    @add_attr(cells)
    def print():
        print("hola")


@add_method(DataFrame)
def cols(self):
    @add_attr(cols)
    @dispatch(str, object)
    def create(name=None, value=None):
        """

        :param name:
        :param value:
        :return:
        """

        assert isinstance(name, str), "name param must be a string"
        assert isinstance(value, (int, float, str, Column)), "object param must be a number, str or a Column "

        if isinstance(value, (int, str, float)):
            value = F.lit(value)

        return self.withColumn(name, value)

    @add_attr(cols)
    @dispatch(list)
    def create(col_names=None):
        """

        :param col_names:
        :return:
        """
        df = self
        for c in col_names:
            value = c[1]
            name = c[0]

            # Create a column if necessary
            if isinstance(value, (int, str, float)):
                value = F.lit(value)
            df = df.withColumn(name, value)
        return df

    @add_attr(cols)
    def select(columns=None, regex=None):
        """
        SElect columns using index or column name
        :param columns:
        :param regex:
        :return:
        """

        if regex:
            r = re.compile(regex)
            columns = list(filter(r.match, self.columns))

        def get_column(column):
            if isinstance(column, int):
                col_name = self.columns[column]
            elif isinstance(column, str):
                col_name = column
            return col_name

        return self.select(list(map(get_column, columns)))

    @add_attr(cols)
    def apply(columns, func):
        """
        Operation as rename or cast need to reconstruct the columns with new types or names.
        This is a helper function handle it
        :param columns:
        :param func:
        :return:
        """

        columns = parse_columns(self, columns)

        udf_function = F.udf(func)

        df = self
        for col_name in columns:
            df = df.withColumn(col_name, udf_function(df[col_name]))
        return df

    @add_attr(cols)
    def rename(columns_old_new=None, func=None):
        """"
        This functions change the name of a column(s) dataFrame.
        :param columns_old_new: List of tuples. Each tuple has de following form: (oldColumnName, newColumnName).
        :param func: can be lower, upper or any string transformation function
        """

        df = self
        # Apply a transformation function to the param string
        if isfunction(func):
            exprs = [
                F.col(c).alias(func(c)) for c in df.columns
            ]
            df = df.select(*exprs)
        else:
            # Check that the 1st element in the tuple is a valid set of columns
            assert validate_columns_names(self, columns_old_new, 0)
            for c in columns_old_new:
                df = df.withColumnRenamed(c[0], c[1])

        return df

    @add_attr(cols)
    def cast(cols_and_types):
        """
        Cast a column to a var type
        :param self:
        :param cols_and_types:
                List of tuples of column names and types to be casted. This variable should have the
                following structure:

                colsAndTypes = [('columnName1', 'integer'), ('columnName2', 'float'), ('columnName3', 'string')]

                The first parameter in each tuple is the column name, the second is the final datatype of column after
                the transformation is made.
        :return:
        """

        # assert validate_columns_names(self, cols_and_types, 0)

        def _cast(attr):
            col_name = attr[0]
            return F.col(col_name).cast(DICT_TYPES[TYPES[attr[1]]])

        df = apply(cols_and_types, _cast)

        return df

    @add_attr(cols)
    def move(column, ref_col, position):
        """
        Move a column to specific position
        :param column:
        :param ref_col:
        :param position:
        :return:
        """
        # Check that column is a string or a list
        column = parse_columns(self, column)
        ref_col = parse_columns(self, ref_col)

        # Asserting if position is 'after' or 'before'
        assert (position == 'after') or (
                position == 'before'), "Error: Position parameter only can be 'after' or 'before' actually" % position

        # Get dataframe columns
        columns = self.columns

        # Get source and reference column index position
        new_index = columns.index(ref_col[0])
        old_index = columns.index(column[0])

        # if position is 'after':
        if position == 'after':
            # Check if the movement is from right to left:
            if new_index >= old_index:
                columns.insert(new_index, columns.pop(old_index))  # insert and delete a element
            else:  # the movement is form left to right:
                columns.insert(new_index + 1, columns.pop(old_index))
        else:  # If position if before:
            if new_index[0] >= old_index:  # Check if the movement if from right to left:
                columns.insert(new_index - 1, columns.pop(old_index))
            elif new_index[0] < old_index:  # Check if the movement if from left to right:
                columns.insert(new_index, columns.pop(old_index))

        return self[columns]

    @add_attr(cols)
    def keep(columns, regex=None):
        """
        Just Keep the columns and drop.
        :param columns:
        :param regex:
        :return:
        """

        if regex:
            r = re.compile(regex)
            columns = list(filter(r.match, self.columns))

        columns = parse_columns(self, columns)
        return self.select(*columns)

    @add_attr(cols)
    def sort(reverse=False):
        columns = sorted(self.columns, reverse=reverse)
        return self.select(columns)
        pass

    @add_attr(cols)
    def drop(columns=None, regex=None):
        df = self
        if regex:
            r = re.compile(regex)
            columns = list(filter(r.match, self.columns))

        columns = parse_columns(self, columns)

        for column in columns:
            df = df.drop(column)
        return df

    @add_attr(cols)
    # Quantile statistics
    @add_attr(cols)
    def _agg(agg, columns):
        """
        Helper function to manage aggregation functions
        :param agg: Aggregation function from spark
        :param columns: list of columns names or a string (a column name).
        :return:
        """
        columns = parse_columns(self, columns)

        # Aggregate
        result = list(map(lambda c: self.agg({c: agg}).collect()[0][0], columns))

        column_result = dict(zip(columns, result))

        # if the list has one elment return just a single element
        return column_result

    @add_attr(cols)
    def min(columns):
        """
        Return the min value from a column dataframe
        :param columns: '*', list of columns names or a string (a column name).
        :return:
        """
        return _agg("min", columns)

    @add_attr(cols)
    def max(columns):
        """
        Return the max value from a column dataframe
        :param columns: '*', list of columns names or a string (a column name).
        :return:
        """
        return _agg("max", columns)

    @add_attr(cols)
    def range(columns):
        """
        Return the range form the min to the max value
        :param columns:
        :return:
        """

        columns = parse_columns(self, columns)

        range = []
        for c in columns:
            max_val = max(c)[c]
            min_val = min(c)[c]
            range.append({c: {'min': min_val, 'max': max_val}})

        return range

    @add_attr(cols)
    def median(columns):
        """
        Return the median of a column dataframe
        :param columns:
        :return:
        """

        result = list(map(lambda c: self.approxQuantile(columns, [0.5], 0)[0][0], columns))

        result = val_to_list(result)
        columns = val_to_list(columns)

        return dict(zip(columns, result))

    # Descriptive Analytics
    @add_attr(cols)
    def stddev(columns):
        """
        Return the standard deviation of a column dataframe
        :param columns:
        :return:
        """
        return _agg("stddev", columns)

    @add_attr(cols)
    def kurt(columns):
        """
        Return the kurtosis of a column dataframe
        :param columns:
        :return:
        """
        return _agg("kurtosis", columns)

    @add_attr(cols)
    def mean(columns):
        """
        Return the mean of a column dataframe
        :param columns:
        :return:
        """
        return _agg("mean", columns)

    @add_attr(cols)
    def skewness(columns):
        """
        Return the skewness of a column dataframe
        :param columns:
        :return:
        """
        return _agg("skewness", columns)

    @add_attr(cols)
    def sum(columns):
        """
        Return the sum of a column dataframe
        :param columns:
        :return:
        """
        return _agg("skewness", columns)

    @add_attr(cols)
    def variance(columns):
        """
        Return the column variance
        :param columns:
        :return:
        """
        return _agg("variance", columns)

    @add_attr(cols)
    def mode(columns):
        """
        Return the the column mode
        :param columns:
        :return:
        """

        columns = parse_columns(self, columns)
        mode_result = []

        for c in columns:
            cnts = self.groupBy(c).count()
            mode_df = cnts.join(
                cnts.agg(F.max("count").alias("max_")), F.col("count") == F.col("max_")
            )

            # if none of the values are repeated we not have mode
            mode_list = mode_df.filter(mode_df["count"] > 1).select(c).collect()

            mode_result.append({c: filter_list(mode_list)})

        return mode_result

    # String Operations
    @add_attr(cols)
    def lower(columns):
        """
        Lowercase all the string in a column
        :param columns:
        :return:
        """
        return apply(columns, F.lower)

    @add_attr(cols)
    def upper(columns):
        """
        Uppercase all the strings column
        :param columns:
        :return:
        """
        return apply(columns, F.upper)

    @add_attr(cols)
    def trim(columns):
        """
        Trim the string in a column
        :param columns:
        :return:
        """
        return apply(columns, F.trim)

    @add_attr(cols)
    def reverse(columns):
        """
        Reverse the order of all the string in a column
        :param columns:
        :return:
        """
        return apply(columns, F.reverse)

    @add_attr(cols)
    def remove_accents(columns):
        """
        Remove accents in specific columns
        :param columns:
        :return:
        """

        columns = parse_columns(self, columns)

        def _remove_accents(attr):
            cell_str = attr
            # first, normalize strings:
            nfkd_str = unicodedata.normalize('NFKD', cell_str)
            # Keep chars that has no other char combined (i.e. accents chars)
            with_out_accents = u"".join([c for c in nfkd_str if not unicodedata.combining(c)])
            return with_out_accents

        udf_function = F.udf(_remove_accents)
        df = self

        for c in columns:
            df = df.withColumn(c, udf_function(df[c]))
        return df

    @add_attr(cols)
    def remove_special_chars(columns):
        """
        Remove accents in specific columns
        :param columns:
        :return:
        """

        columns = parse_columns(self, columns)

        def _remove_special_chars(attr):
            # Remove all punctuation and control characters
            for punct in (set(attr) & set(string.punctuation)):
                attr = attr.replace(punct, "")
            return attr

        udf_function = F.udf(_remove_special_chars)
        df = self
        for c in columns:
            df = df.withColumn(c, udf_function(df[c]))
        return df

    @add_attr(cols)
    def split(column, mark, get=None, n=None):
        df = self
        split_col = F.split(df[column], mark)

        # assert isinstance(get, int), "Error: get must be an integer"
        if get:
            df = df.withColumn('COL_' + str(get), split_col.getItem(get))
        if n:
            for p in builtins.range(n):
                df = df.withColumn('COL_' + str(p), split_col.getItem(p))

        return df

    @add_attr(cols)
    def date_transform(columns, current_format, output_format):
        """
        Tranform a column date format
        :param  columns: Name date columns to be transformed. Columns ha
        :param  current_format: current_format is the current string dat format of columns specified. Of course,
                                all columns specified must have the same format. Otherwise the function is going
                                to return tons of null values because the transformations in the columns with
                                different formats will fail.
        :param  output_format: output date string format to be expected.
        """
        # Check if current_format argument a string datatype:
        assert isinstance(current_format, str)

        # Check if output_format argument a string datatype:
        assert isinstance(output_format, str)

        # Check if columns argument must be a string or list datatype:
        columns = parse_columns(self, columns)

        df = self

        exprs = [F.date_format(F.unix_timestamp(c, current_format).cast("timestamp"), output_format).alias(
            c) if c in columns else c for c in df.columns]

        return df.select(*exprs)

    @add_attr(cols)
    def age_from_date(name_col_age, dates_format, column):
        """
        This method compute the age based on a born date.
        :param  column: Name of the column born dates column.
        :param  dates_format: String format date of the column provided.
        :param  name_col_age: Name of the new column, the new columns is the resulting column of ages.
        """
        # Check if column argument a string datatype:
        assert isinstance(dates_format, str)

        # Check if dates_format argument a string datatype:
        assert isinstance(name_col_age, str)

        # Asserting if column if in dataFrame:
        # validate_columns_names(self, cols_and_types, 2)
        validate_columns_names(self, column)

        # Output format date
        format_dt = "yyyy-MM-dd"  # Some SimpleDateFormat string

        df = self

        # df.withColumn("date", exprs)

        def _age(attr):
            name_col_age = attr[0]
            dates_format = attr[1]
            col_name = attr[2]

            return F.format_number(
                F.abs(
                    F.months_between(
                        F.date_format(
                            F.unix_timestamp(
                                col_name,
                                dates_format).cast("timestamp"),
                            format_dt),
                        F.current_date()) / 12), 4) \
                .alias(
                name_col_age)

        df = apply([(name_col_age, "yyyyMMdd", "date")], _age)

        return df

    @add_attr(cols)
    def impute(columns, out_cols, strategy):
        """
        Imputes missing data from specified columns using the mean or median.
        :param self:
        :param columns: List of columns to be analyze.
        :param out_cols: List of output columns with missing values imputed.
        :param strategy: String that specifies the way of computing missing data. Can be "mean" or "median"
        :return: Transformer object (DF with columns that has the imputed values).
        """

        # Check if columns to be process are in dataframe
        columns = parse_columns(self, columns)

        assert isinstance(columns, list), "Error: columns argument must be a list"

        assert isinstance(out_cols, list), "Error: out_cols argument must be a list"

        # Check if columns argument a string datatype:
        assert isinstance(strategy, str)

        assert (strategy == "mean" or strategy == "median"), "Error: strategy has to be 'mean' or 'median'."

        # def _impute(cols):

        imputer = Imputer(inputCols=columns, outputCols=out_cols)

        df = self
        model = imputer.setStrategy(strategy).fit(df)
        df = model.transform(df)

        return df

    @add_attr(cols)
    def count_na(columns):
        """
        Return the NAN and Null count in a Column
        :param columns:
        :param type: Accepts integer, float, string or None
        :return:
        """
        columns = parse_columns(self, columns)
        df = self
        # df = df.select([F.count(F.when(F.isnan(c), c)).alias(c) for c in columns]) Just count Nans
        return collect_to_dict(df.select([F.count(F.when(F.isnan(c) | F.col(c).isNull(), c)).alias(c) for c in columns]) \
            .collect())

    @add_attr(cols)
    def count_zeros(columns):
        """
        Return the NAN and Null count in a Column
        :param columns:
        :param type: Accepts integer, float, string or None
        :return:
        """
        columns = parse_columns(self, columns)
        df = self
        # df = df.select([F.count(F.when(F.isnan(c), c)).alias(c) for c in columns]) Just count Nans
        return collect_to_dict(df.select([F.count(F.when(F.col(c) == 0, c)).alias(c) for c in columns]) \
            .collect())

    @add_attr(cols)
    def count_uniques(columns):
        """
        Return how many unique items exist in a columns
        :param columns:
        :param type: Accepts integer, float, string or None
        :return:
        """
        columns = parse_columns(self, columns)
        df = self
        return [{c: df.select(c).distinct().count()} for c in columns]

    @add_attr(cols)
    def get_column_names_by_type(data_type):
        """
        This function returns column names of dataFrame which have the same
        datatype provided. It analyses column datatype by dataFrame.dtypes method.

        :return    List of column names of a type specified.

        :param df:
        :param data_type:
        :return:
        """
        assert (data_type in ['string', 'integer', 'float', 'date', 'double']), \
            "Error, data_type only can be one of the following values: 'string', 'integer', 'float', 'date', 'double'"

        return list(y[0] for y in filter(lambda x: x[1] == TYPES[data_type], self.dtypes))

    @add_attr(cols)
    def unique():
        """
        Just a wrapper for Apache Spark distinct function
        :return:
        """
        return self.distinct()

    return cols

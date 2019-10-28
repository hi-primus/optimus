from optimus.abstract.abstract_cols import AbstractCols
from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import RELATIVE_ERROR


class BaseCols(AbstractCols):
    def __init__(self, df, functions, exec_agg, create_exprs):
        self.df = df
        self.functions = functions
        self.exec_agg = exec_agg
        self.create_exprs = create_exprs
        # print(self.df)

    def min(self, columns):
        """
        Return the min value from a Dask dataframe column
        :param columns: '*', list of columns names or a single column name.
        :return:
        """

        return self.agg_exprs(columns, self.functions.min)

    def max(self, columns):
        """
        Return the max value from a Dask dataframe column
        :param columns: '*', list of columns names or a single column name.
        :return:
        """

        return self.agg_exprs(columns, self.functions.max)

    def range(self, columns):
        """
        Return the range form the min to the max value
        :param columns: '*', list of columns names or a single column name.
        :return:
        """

        return self.agg_exprs(columns, self.functions.range_agg)

    def median(self, columns, relative_error=RELATIVE_ERROR):
        """
        Return the median of a column spark
        :param columns: '*', list of columns names or a single column name.
        :param relative_error: If set to zero, the exact median is computed, which could be very expensive. 0 to 1
        :return:
        """

        return format_dict(self.percentile(columns, ["0.5"], relative_error))

    def percentile(self, columns, values=None, relative_error=RELATIVE_ERROR):
        """
        Return the percentile of a spark
        :param columns:  '*', list of columns names or a single column name.
        :param values: list of percentiles to be calculated
        :param relative_error:  If set to zero, the exact percentiles are computed, which could be very expensive.
        :return: percentiles per columns
        """
        # values = [str(v) for v in values]
        if values is None:
            values = [0.5]
        return self.agg_exprs(columns, self.functions.percentile_agg, self.df, values, relative_error)

    # Descriptive Analytics
    # TODO: implement double MAD http://eurekastatistics.com/using-the-median-absolute-deviation-to-find-outliers/
    @staticmethod
    def mad(columns, relative_error=RELATIVE_ERROR, more=None):
        """
        Return the Median Absolute Deviation
        :param columns: Column to be processed
        :param more: Return some extra computed values (Median).
        :param relative_error: Relative error calculating the media
        :return:
        """
        columns = parse_columns(self, columns, filter_by_column_dtypes=self.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        result = {}
        for col_name in columns:

            _mad = {}
            median_value = self.cols.median(col_name, relative_error)
            mad_value = self.withColumn(col_name, F.abs(F.col(col_name) - median_value)) \
                .cols.median(col_name, relative_error)

            if more:
                _mad = {"mad": mad_value, "median": median_value}
            else:
                _mad = {"mad": mad_value}

            result[col_name] = _mad

        return format_dict(result)

    def std(self, columns):
        """
        Return the standard deviation of a column spark
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        columns = parse_columns(self, columns, filter_by_column_dtypes=self.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return format_dict(Cols.agg_exprs(columns, F.stddev))

    @staticmethod
    def kurt(columns):
        """
        Return the kurtosis of a column spark
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        columns = parse_columns(self, columns, filter_by_column_dtypes=self.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return format_dict(Cols.agg_exprs(columns, F.kurtosis))

    @staticmethod
    def mean(columns):
        """
        Return the mean of a column spark
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        columns = parse_columns(self, columns, filter_by_column_dtypes=self.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return format_dict(Cols.agg_exprs(columns, F.mean))

    @staticmethod
    def skewness(columns):
        """
        Return the skewness of a column spark
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        columns = parse_columns(self, columns, filter_by_column_dtypes=self.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return format_dict(Cols.agg_exprs(columns, F.skewness))

    @staticmethod
    def sum(columns):
        """
        Return the sum of a column spark
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        columns = parse_columns(self, columns, filter_by_column_dtypes=self.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return format_dict(Cols.agg_exprs(columns, F.sum))

    @staticmethod
    def variance(columns):
        """
        Return the column variance
        :param columns: '*', list of columns names or a single column name.
        :return:
        """
        columns = parse_columns(self, columns, filter_by_column_dtypes=self.constants.NUMERIC_TYPES)
        check_column_numbers(columns, "*")

        return format_dict(Cols.agg_exprs(columns, F.variance))

    @staticmethod
    def abs(input_cols, output_cols=None):
        """
        Apply abs to the values in a column
        :param input_cols:
        :param output_cols:
        :return:
        """
        input_cols = parse_columns(self, input_cols, filter_by_column_dtypes=self.constants.NUMERIC_TYPES)
        output_cols = get_output_cols(input_cols, output_cols)

        check_column_numbers(output_cols, "*")
        # Abs not accepts column's string names. Convert to Spark Column

        # TODO: make this in one pass.
        df = self
        for col_name in output_cols:
            df = df.withColumn(col_name, F.abs(F.col(col_name)))
        return df

    @staticmethod
    def mode(columns):
        """
        Return the column mode
        :param columns: '*', list of columns names or a single column name.
        :return:
        """

        columns = parse_columns(self, columns)
        mode_result = []

        for col_name in columns:
            count = self.groupBy(col_name).count()
            mode_df = count.join(
                count.agg(F.max("count").alias("max_")), F.col("count") == F.col("max_")
            )
            if SparkEngine.cache:
                mode_df = mode_df.cache()
            # if none of the values are repeated we not have mode
            mode_list = (mode_df
                         .rows.select(mode_df["count"] > 1)
                         .cols.select(col_name)
                         .collect())

            mode_result.append({col_name: filter_list(mode_list)})

        return format_dict(mode_result)

    def agg_exprs(self, columns, funcs, *args):
        """
        Create and run aggregation
        :param columns:
        :param funcs:
        :param args:
        :return:
        """
        # print(args)
        return self.exec_agg(self.create_exprs(columns, funcs, *args))

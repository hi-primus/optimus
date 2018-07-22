# Reference http://nbviewer.jupyter.org/github/julioasotodv/spark-df-profiling/blob/master/examples/Demo.ipynb


import pyspark.sql.functions as F
from optimus.helpers.functions import *
from optimus.profiler.functions import *

from pathlib import Path


class Profiler:
    def __init__(self):
        pass

    @staticmethod
    def dataset_info(df):

        columns = parse_columns(df, df.columns)

        cols_count = len(df.columns)
        rows_count = df.count()
        missing = df.cols().count_na(columns)

        return (
            {'cols_count': cols_count,
             'rows_count': rows_count,
             'missing': missing}
        )

    @staticmethod
    def var_type_with_max_count(df, columns):
        """
        Return the count of columns by type
        :return:
        """
        columns = parse_columns(df, columns)

        result = Profiler.count_data_types(columns)

        col_type = {}
        for key, value in result.items():
            max_key = max(value, key=value.get)
            col_type[key] = ({max_key: value[max_key]})

        return col_type

    @staticmethod
    def count_data_types(df, columns):
        """
        Count the number of int, float, string and bool in a column
        :param df:
        :param columns:
        :return:
        """

        def _count_data_types(col_name):
            """
            Function for determine if register value is float or int or string.
            :param col_name:
            :return:
            """
            types = df.cols().apply(col_name, check_data_type, "udf") \
                .groupBy(col_name) \
                .count()

            # Convert the collect result to a list
            # TODO: check if collect_to_dict function can be used here

            count_by_data_type = {}
            for row in types.collect():
                count_by_data_type[row[0]] = row[1]

            # Fill missing data types with 0
            count_by_data_type = fill_missing_var_types(count_by_data_type)

            # Subtract white spaces to the total string count
            count_empty_strings = df.where(F.col(col_name) == '').count()
            count_by_data_type['string'] = count_by_data_type['string'] - count_empty_strings

            data_types_count = {"string": count_by_data_type['string'],
                                "boolean": count_by_data_type['boolean'],
                                "integer": count_by_data_type['integer'],
                                "float": count_by_data_type['float']
                                }

            null_missed_count = {"null": count_by_data_type['null'],
                                 "missing": count_empty_strings,
                                 }

            col = {}
            col['type'] = max(data_types_count, key=data_types_count.get)
            col['details'] = {**data_types_count, **null_missed_count}

            return col

        columns = parse_columns(df, columns)

        return {c: _count_data_types(c) for c in columns}

    @staticmethod
    def columns(df, columns):
        """
        Get statistical information about a column
        :param df:
        :param columns: Columns that you w
        :return: json object
        """
        columns = parse_columns(df, columns)

        # Initialize Objects
        column_info = {}
        column_info['columns'] = {}

        rows_count = df.count()
        column_info['rows_count'] = rows_count

        count_dtype = Profiler.count_data_types(df, columns)

        column_info['size'] = human_readable_bytes(df.size())

        for col_name in columns:
            col = {}

            # Get if a column is numerical or categorical
            column_type = count_dtype[col_name]['type']

            # Get uniques
            uniques = df.cols().count_uniques(col_name)

            na = df.cols().count_na(col_name)

            # Uniques
            col['uniques_count'] = uniques[col_name]
            col['p_uniques'] = uniques[col_name] / rows_count * 100

            # Missing
            col['missing_count'] = na[col_name]
            col['p_missing'] = na[col_name] / rows_count * 100

            # Categorical column
            if column_type == "string":
                col['frequency'] = collect_to_dict(df.select(F.col(col_name).alias("value")).groupBy("value").count()
                                                   .orderBy('count', ascending=False).limit(10)
                                                   .withColumn('percentage',
                                                               F.col('count') / rows_count * 100).collect())

            # Numeric Column
            elif column_type == "integer" or column_type == "float":
                # Quantile statistics
                min_value = df.cols().min(col_name)
                max_value = df.cols().max(col_name)
                col['min'] = min_value
                col['max'] = max_value
                col['quantile'] = df.cols().percentile(col_name, [0.05, 0.25, 0.5, 0.75, 0.95])
                col['range'] = max_value - min_value
                col['median'] = col['quantile'][0.5]
                col['interquartile_range'] = col['quantile'][0.75] - col['quantile'][0.25]

                # Descriptive statistic
                col['stdev'] = df.cols().std(col_name)
                # Coef of variation
                col['kurt'] = df.cols().kurt(col_name)
                col['mean'] = df.cols().mean(col_name)
                col['mad'] = df.cols().mad(col_name)
                col['skewness'] = df.cols().skewness(col_name)
                col['sum'] = df.cols().sum(col_name)
                col['variance'] = df.cols().variance(col_name)

                # Zeros
                col['zeros'] = df.cols().count_zeros(col_name)
                col['p_zeros'] = col['zeros'] / rows_count

                col['hist'] = df.hist(col_name)

            elif column_type == "boolean":
                pass

            # Buckets: values, count, %

            column_info['columns'][col_name] = col


        path = Path.cwd() / "data.json"
        write_json(column_info, path=path)
        return column_info

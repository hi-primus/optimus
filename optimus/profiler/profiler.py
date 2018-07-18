# Reference http://nbviewer.jupyter.org/github/julioasotodv/spark-df-profiling/blob/master/examples/Demo.ipynb

import pyspark.sql.functions as F
from optimus.helpers.functions import *
from optimus.profiler.functions import *


class Profiler:
    def __init__(self, df):
        self._df = df

    def dataset_info(self):
        df = self._df
        columns = parse_columns(df, df.columns)

        cols_count = len(self._df.columns)
        rows_count = df.count()
        missing = df.cols().count_na(columns)

        return (
            {'cols_count': cols_count,
             'rows_count': rows_count,
             'missing': missing}
        )

    def var_type_with_max_count(self, columns):
        """
        Return the count of columns by type
        :return:
        """
        columns = parse_columns(self._df, columns)

        result = self.count_data_types(columns)

        col_type = {}
        for key, value in result.items():
            max_key = max(value, key=value.get)
            col_type[key] = ({max_key: value[max_key]})

        return col_type

    def columns_by_types(self, columns):
        """

        :param columns:
        :return:
        """
        columns = parse_columns(self._df, columns)

        result = self.count_data_types(columns)

        col_type = {}
        # Get higher column count per var type
        for key, value in result.items():
            max_key = max(value, key=value.get)
            col_type[key] = max_key

        # Count columns per higher max type
        result = {}
        for key, value in col_type.items():
            result[value] = result[value] + 1 if value in result else 1

        return fill_missing_var_types(result)

    def count_data_types(self, columns):
        """
        Count the number of int, float, strings and bool
        :param columns:
        :return:
        """

        def _count_data_types(col_name):
            """
            Function for determine if register value is float or int or string.
            :param col_name:
            :return:
            """
            types = self._df.cols().apply(col_name, check_data_type, "udf") \
                .groupBy(col_name) \
                .count()

            # Convert the collect result to a list
            # FIX: check if collect_to_dict function can be used here

            results = {}
            for row in types.collect():
                results[row[0]] = row[1]

            # Fill missing data types with 0
            results = fill_missing_var_types(results)

            # Subtract white spaces to the total string count
            count_empty_strings = self._df.where(F.col(col_name) == '').count()
            results['string'] = results['string'] - count_empty_strings

            # List of returning values:
            return {"null": results['null'],
                    "missing": count_empty_strings,
                    "string": results['string'],
                    "boolean": results['boolean'],
                    "integer": results['integer'],
                    "float": results['float']
                    }

        columns = parse_columns(self._df, columns)

        return {c: _count_data_types(c) for c in columns}

    def columns(self, columns):
        df = self._df

        columns = parse_columns(df, columns)
        # Distinct, % , missing , %
        column_info = {}
        column_info['columns'] = {}

        # Total
        rows_count = df.count()
        column_info['rows_count'] = rows_count

        uniques = df.cols().count_uniques(columns)

        na = df.cols().count_na(columns)

        for col_name in columns:
            col = {}
            # Check if column is numeric or categorical

            # Uniques
            col['uniques_count'] = uniques[col_name]
            col['p_uniques'] = uniques[col_name] / rows_count * 100

            # Missing
            col['missing_count'] = na[col_name]
            col['p_missing'] = na[col_name] / rows_count * 100

            # Buckets: values, count, %

            # col['f'] = collect_to_dict(df.groupBy(col_name).count().orderBy('count', ascending=False).limit(10) \
            #                           .withColumn('%', F.col('count') / rows_count * 100).collect())

            col['frequency'] = collect_to_dict(df.select(F.col("num").alias("value")).groupBy("value").count()
                                       .orderBy('count', ascending=False).limit(10)
                                       .withColumn('percentage', F.col('count') / rows_count * 100).collect())

            # print(col)
            # print(column_info)
            # print(col_name)

            column_info['columns'][col_name] = col

        return column_info
        # print(df.columns)

        # print(df.cols().count_zeros(columns))

        # print(df.dtypes)

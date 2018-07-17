# Reference http://nbviewer.jupyter.org/github/julioasotodv/spark-df-profiling/blob/master/examples/Demo.ipynb

import pyspark.sql.functions as F
from optimus.helpers.functions import *
from optimus.helpers.constants import *


class Profiler:
    def __init__(self, df):
        self._df = df

    def column(self, columns):
        df = self._df

        # columns = parse_columns(df, columns)

        print(df.columns)

        print(df.cols().count_zeros(columns))

        print(df.cols().count_uniques(columns))

        print(df.dtypes)

        return

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
            result[value] = result[value] + 1 if value in result else 0

        return result

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
            results = {}
            for row in types.collect():
                results[row[0]] = row[1]

            # Fill the not present data types with 0
            for label in TYPES_PROFILER:
                if label not in results:
                    results[label] = 0

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

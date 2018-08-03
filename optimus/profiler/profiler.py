from pathlib import Path

import pyspark.sql.functions as F
from optimus.helpers.functions import parse_columns, collect_to_dict
from optimus.functions import filter_row_by_data_type as fbdt
from optimus.profiler.functions import human_readable_bytes, fill_missing_var_types, fill_missing_col_types, write_json


class Profiler:

    @staticmethod
    def dataset_info(df):
        """
        Return info about cols and row counts
        :param df:
        :return:
        """

        columns = parse_columns(df, df.columns)

        cols_count = len(df.columns)
        rows_count = df.count()
        missing_count = sum(df.cols().count_na(columns).values())

        return (
            {'cols_count': cols_count,
             'rows_count': rows_count,
             'missing_count': missing_count,
             'size': df.size()}
        )

    @staticmethod
    def count_column_by_type(df, columns):
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

    # TODO: This should check only the StringType Columns. The datatype from others columns can be taken from schema().
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
            temp = col_name + "_type"
            types = df.withColumn(temp, fbdt(col_name, get_type=True)).groupBy(temp).count()

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
                                "bool": count_by_data_type['bool'],
                                "int": count_by_data_type['int'],
                                "float": count_by_data_type['float'],
                                "date": count_by_data_type['date']
                                }

            null_missed_count = {"null": count_by_data_type['null'],
                                 "missing": count_empty_strings,
                                 }

            # Get the greatest count by column data type
            greatest_data_type_count = max(data_types_count, key=data_types_count.get)

            if greatest_data_type_count is "string":
                cat = "categorical"
            elif greatest_data_type_count is "int" or greatest_data_type_count is "float":
                cat = "numeric"
            elif greatest_data_type_count is "date":
                cat = "date"
            else:
                cat = "null"

            col = {}
            col['type'] = cat
            col['details'] = {**data_types_count, **null_missed_count}

            return col

        columns = parse_columns(df, columns)

        type_details = {c: _count_data_types(c) for c in columns}

        count_types = {}
        # Count the categorical, numerical and date columns
        for k, v in type_details.items():
            name = v["type"]
            if name in count_types:
                count_types[name] += 1
            else:
                count_types[name] = 1

        count_types = fill_missing_col_types(count_types)

        results = {}
        results["count_types"] = count_types
        results["columns"] = type_details
        return results

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
            if column_type == "categorical":
                col['frequency'] = collect_to_dict(df.select(F.col(col_name).alias("value")).groupBy("value").count()
                                                   .orderBy('count', ascending=False).limit(10)
                                                   .withColumn('percentage',
                                                               F.col('count') / rows_count * 100).collect())

            # Numeric Column
            elif column_type == "numerical":
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

                col['hist'] = df.cols().hist(col_name)

            elif column_type == "boolean":
                pass

            # Buckets: values, count, %

            column_info['columns'][col_name] = col

        path = Path.cwd() / "data.json"
        write_json(column_info, path=path)
        return column_info

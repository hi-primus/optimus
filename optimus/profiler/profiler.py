import os
from pathlib import Path

import jinja2
import pyspark.sql.functions as F
from IPython.core.display import display, HTML

from optimus.functions import filter_row_by_data_type as fbdt
from optimus.helpers.functions import parse_columns, collect_to_dict
from optimus.profiler.functions import human_readable_bytes, fill_missing_var_types, fill_missing_col_types, \
    write_json, sample_size


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
        missing_count = sum(df.cols.count_na(columns).values())

        return (
            {'cols_count': cols_count,
             'rows_count': rows_count,
             'missing_count': str(missing_count / rows_count) + "%",
             'size': human_readable_bytes(df.size())}
        )

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

            types = df.withColumn(temp, fbdt(col_name, get_type=True)).groupBy(temp).count().collect()

            # Convert the collect result to a list
            # TODO: check if collect_to_dict function can be used here

            count_by_data_type = {}
            print(types)
            for row in types:
                count_by_data_type[row[0]] = row[1]

            # Fill missing data types with 0
            count_by_data_type = fill_missing_var_types(count_by_data_type)

            # Subtract white spaces to the total string count
            count_empty_strings = df.where(F.col(col_name) == '').count()
            count_by_data_type['string'] = count_by_data_type['string'] - count_empty_strings

            # if the data type is string we try to infer
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
            elif greatest_data_type_count is "bool":
                cat = "bool"
            else:
                cat = "null"

            col = {}
            col['type'] = cat
            col['details'] = {**data_types_count, **null_missed_count}

            return col

        columns = parse_columns(df, columns)

        type_details = {c: _count_data_types(c) for c in columns}

        results = {}
        count_types = {}

        # Count the categorical, numerical and date columns
        for v in type_details.values():
            name = v["type"]
            if name in count_types:
                count_types[name] += 1
            else:
                count_types[name] = 1

        count_types = fill_missing_col_types(count_types)
        results["count_types"] = count_types

        results["columns"] = type_details
        return results

    @staticmethod
    def columns_new(df, columns):
        """
        Get statistical information in json format
        count_data_type()
        :param df: Dataframe to be processed
        :param columns: Columns that you want to profile
        :return: json object with the
        """

        columns = parse_columns(df, columns)
        # Initialize Objects
        column_info = {}

        column_info['columns'] = {}

        rows_count = df.count()
        column_info['rows_count'] = rows_count

        # count_dtype = Profiler.count_data_types(df, columns)
        # column_info["count_types"] = count_dtype["count_types"]

        column_info['size'] = human_readable_bytes(df.size())

        # column_type = count_dtype["columns"][col_name]['type']
        # dtypes_stats = count_dtype["columns"][col_name]['details']

        na = df.cols.count_na(columns)

        # Missing
        # col_info['missing_count'] = round(na[col_name], 2)
        # col_info['p_missing'] = round(na[col_name] / rows_count * 100, 2)

        # Categorical column
        # col_info['column_type'] = column_type

        uniques = df.cols.count_uniques(columns)

        r = df.cols._exprs([F.min, F.max, F.stddev, F.kurtosis, F.mean, F.skewness, F.sum, F.variance], columns)

        return na

        for col_name in columns:
            col_info = {}

            # Get if a column is numerical or categorical

            if column_type == "categorical" or column_type == "numeric" or column_type == "date" or column_type == "bool":
                uniques_df = df.select(F.col(col_name).alias("value")).groupBy("value").count() \
                    .orderBy('count', ascending=False)

                col_info['frequency'] = collect_to_dict(uniques_df.limit(10)
                                                        .withColumn('percentage',
                                                                    F.round(F.col('count') / rows_count * 100,
                                                                            2))

                                                        .collect())
                # col_info['other_values'] = uniques_df.cols.sum(col_name)
                uniques = uniques_df.count()
                col_info['uniques_count'] = uniques
                col_info['p_uniques'] = round(uniques / rows_count * 100, 2)

            # Numeric Column
            if column_type == "numeric" or column_type == "date":
                # Quantile statistics
                min_value = df.cols.min(col_name)
                max_value = df.cols.max(col_name)
                col_info['min'] = min_value
                col_info['max'] = max_value

            if column_type == "numeric":

                r = df.cols._agg([F.min, F.max, F.stddev, F.kurtosis, F.mean, F.skewness, F.sum, F.variance],
                                 "m2_superficie_construida")

                col_info['quantile'] = df.cols.percentile(col_name, [0.05, 0.25, 0.5, 0.75, 0.95])
                col_info['range'] = max_value - min_value
                col_info['median'] = col_info['quantile'][0.5]
                col_info['interquartile_range'] = col_info['quantile'][0.75] - col_info['quantile'][0.25]

                # Descriptive statistic
                col_info['stdev'] = round(df.cols.std(col_name), 5)
                col_info['kurt'] = round(df.cols.kurt(col_name), 5)
                col_info['mean'] = round(df.cols.mean(col_name), 5)
                col_info['mad'] = round(df.cols.mad(col_name), 5)
                col_info['skewness'] = round(df.cols.skewness(col_name), 5)
                col_info['sum'] = round(df.cols.sum(col_name), 2)
                col_info['variance'] = round(df.cols.variance(col_name), 0)
                col_info['coef_variation'] = round((col_info['stdev'] / col_info['mean']), 5)

                # Zeros
                col_info['zeros'] = df.cols.count_zeros(col_name)
                col_info['p_zeros'] = round(col_info['zeros'] / rows_count, 2)

                col_info['hist'] = df.cols.hist(col_name)

            elif column_type == "date":
                pass
            elif column_type == "boolean":
                pass

            # Buckets: values, count, %
            column_info['columns'][col_name] = col_info
            column_info['columns'][col_name]["dtypes_stats"] = dtypes_stats

        return column_info

    @staticmethod
    def columns(df, columns):
        """
        Get statistical information in json format
        count_data_type()
        :param df: Dataframe to be processed
        :param columns: Columns that you want to profile
        :return: json object with the
        """

        columns = parse_columns(df, columns)
        # Initialize Objects
        column_info = {}

        column_info['columns'] = {}

        rows_count = df.count()

        column_info['rows_count'] = rows_count
        count_dtype = Profiler.count_data_types(df, columns)

        column_info["count_types"] = count_dtype["count_types"]

        column_info['size'] = human_readable_bytes(df.size())

        for col_name in columns:
            col_info = {}
            col_info["stats"] = {}

            # Get if a column is numerical or categorical
            column_type = count_dtype["columns"][col_name]['type']
            dtypes_stats = count_dtype["columns"][col_name]['details']

            na = df.cols.count_na(col_name)

            # Get uniques
            # uniques = df.cols.count_uniques(col_name)
            # Uniques
            # if column_type == "categorical":
            #    col['uniques_count'] = uniques[col_name]
            #    col['p_uniques'] = uniques[col_name] / rows_count * 100

            # Missing
            col_info['stats']['missing_count'] = round(na, 2)
            col_info['stats']['p_missing'] = round(na / rows_count * 100, 2)

            # Categorical column
            col_info['column_type'] = column_type
            col_info['name'] = col_name

            if column_type == "categorical" or column_type == "numeric" or column_type == "date" or column_type == "bool":
                uniques_df = df.select(F.col(col_name).alias("value")).groupBy("value").count() \
                    .orderBy('count', ascending=False)

                col_info['frequency'] = collect_to_dict(uniques_df.limit(10)
                                                        .withColumn('percentage',
                                                                    F.round(F.col('count') / rows_count * 100,
                                                                            2))

                                                        .collect())
                # col_info['other_values'] = uniques_df.cols.sum(col_name)
                uniques = uniques_df.count()
                col_info['stats']['uniques_count'] = uniques
                col_info['stats']['p_uniques'] = round(uniques / rows_count * 100, 2)

            # Numeric Column
            if column_type == "numeric" or column_type == "date":
                # Quantile statistics
                min_value = df.cols.min(col_name)
                max_value = df.cols.max(col_name)
                col_info['stats']['min'] = min_value
                col_info['stats']['max'] = max_value

            if column_type == "numeric":

                col_info['stats']['quantile'] = df.cols.percentile(col_name, [0.05, 0.25, 0.5, 0.75, 0.95])
                col_info['stats']['range'] = max_value - min_value
                col_info['stats']['median'] = col_info['stats']['quantile'][0.5]
                col_info['stats']['interquartile_range'] = col_info['stats']['quantile'][0.75] - \
                                                           col_info['stats']['quantile'][0.25]

                # Descriptive statistic
                col_info['stats']['stdev'] = round(df.cols.std(col_name), 5)
                col_info['stats']['kurt'] = round(df.cols.kurt(col_name), 5)
                col_info['stats']['mean'] = round(df.cols.mean(col_name), 5)
                col_info['stats']['mad'] = round(df.cols.mad(col_name), 5)
                col_info['stats']['skewness'] = round(df.cols.skewness(col_name), 5)
                col_info['stats']['sum'] = round(df.cols.sum(col_name), 2)
                col_info['stats']['variance'] = round(df.cols.variance(col_name), 0)
                col_info['stats']['coef_variation'] = round((col_info['stats']['stdev'] / col_info['stats']['mean']), 5)

                # Zeros
                col_info['stats']['zeros'] = df.cols.count_zeros(col_name)
                col_info['stats']['p_zeros'] = round(col_info['stats']['zeros'] / rows_count, 2)

                col_info['hist'] = df.cols.hist(col_name)

            elif column_type == "date":
                pass
            elif column_type == "boolean":
                pass

            # Buckets: values, count, %
            column_info['columns'][col_name] = col_info
            column_info['columns'][col_name]["dtypes_stats"] = dtypes_stats

        return column_info

    @staticmethod
    def run(df, columns):
        """
        Get statistical information in HTML Format
        :param columns:
        :return:
        """

        path = os.path.dirname(os.path.abspath(__file__))
        templateLoader = jinja2.FileSystemLoader(searchpath=path + "//templates")
        templateEnv = jinja2.Environment(loader=templateLoader)

        template = templateEnv.get_template("column_stats.html")

        rows_count = df.count()
        # Get just a sample to infer the column data type
        sample_size_number = sample_size(rows_count, 95.0, 2.0)
        fraction = sample_size_number / rows_count
        sample = df.sample(False, fraction, seed=1)

        dataset = Profiler.dataset_info(df)

        summary = Profiler.columns(df, columns)
        summary["summary"] = dataset

        # Write json
        path = Path.cwd() / "data.json"
        write_json(summary, path=path)

        # Render template
        output = template.render(data=summary)
        display(HTML(output))

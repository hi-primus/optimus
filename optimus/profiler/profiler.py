import logging
import os
from fastnumbers import fast_float

import jinja2
import pyspark.sql.functions as F
from IPython.core.display import display, HTML

from optimus.functions import filter_row_by_data_type as fbdt, plot_hist, plot_freq
from optimus.helpers.functions import parse_columns
from optimus.profiler.functions import human_readable_bytes, fill_missing_var_types, fill_missing_col_types, \
    write_json


class Profiler:

    def __init__(self, output_path=None):
        self.path = output_path

    @staticmethod
    def dataset_info(df):
        """
        Return info about cols and row counts
        :param df: Dataframe to be processed
        :return:
        """

        columns = parse_columns(df, df.columns)

        cols_count = len(df.columns)
        rows_count = df.count()
        missing_count = round(sum(df.cols.count_na(columns).values()), 2)

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
        Count the number of int, float, string and bool in a  in json format
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
            col['dtype'] = greatest_data_type_count
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
    def columns(df, columns, buckets=10):
        """
        Return statistical information about a specific column in json format
        count_data_type()
        :param df: Dataframe to be processed
        :param columns: Columns that you want to profile
        :param buckets:
        :return: json object with the
        """

        columns = parse_columns(df, columns)

        # Get just a sample to infer the column data type
        # sample_size_number = sample_size(rows_count, 95.0, 2.0)
        # fraction = sample_size_number / rows_count
        # sample = df.sample(False, fraction, seed=1)

        # Initialize Objects
        column_info = {}
        column_info['columns'] = {}

        rows_count = df.count()
        column_info['rows_count'] = rows_count

        count_dtypes = Profiler.count_data_types(df, columns)

        column_info["count_types"] = count_dtypes["count_types"]
        column_info['size'] = human_readable_bytes(df.size())

        def na(col_name):
            return F.count(F.when(F.isnan(col_name) | F.col(col_name).isNull(), col_name))

        def zeros(col_name):
            return F.count(F.when(F.col(col_name) == 0, col_name))

        # Cast every column to a specific type to ensure the correct profiling
        # For example if we calculate the min or max of a string column with numeric values the result will be incorrect
        for col_name in columns:
            dtype = count_dtypes["columns"][col_name]['dtype']
            # Not force date type conversion, we can not trust that is going to be representative
            if dtype in ["string", "float", "int", "bool"]:
                df = df.cols.cast(col_name, dtype)

        stats = df.cols._exprs(
            [F.min, F.max, F.stddev, F.kurtosis, F.mean, F.skewness, F.sum, F.variance, F.approx_count_distinct, na,
             zeros],
            columns)

        for col_name in columns:
            logging.info("Processing column '" + col_name + "'...")

            col_info = {}
            col_info["stats"] = {}
            column_info['columns'][col_name] = {}

            column_type = count_dtypes["columns"][col_name]['type']
            col_info['column_dtype'] = count_dtypes["columns"][col_name]['dtype']

            na = stats[col_name]["na"]
            max_value = stats[col_name]["max"]
            min_value = stats[col_name]["min"]

            col_info['name'] = col_name
            col_info['column_type'] = column_type

            # Numeric Column
            if column_type == "numeric" or column_type == "date":
                # Merge
                col_info["stats"] = stats[col_name]

            # Missing
            col_info['stats']['missing_count'] = round(na, 2)
            col_info['stats']['p_missing'] = round(na / rows_count * 100, 2)
            col_info["dtypes_stats"] = count_dtypes["columns"][col_name]['details']

            if column_type == "categorical" or column_type == "numeric" or column_type == "date" or column_type == "bool":
                # Frequency

                col_info['frequency'] = (df.groupBy(col_name)
                                         .count()
                                         .rows.sort([("count", "desc"), (col_name, "desc")])
                                         .limit(10)
                                         .withColumn("percentage",
                                                     F.round((F.col("count") / rows_count) * 100,
                                                             3))
                                         .cols.rename(col_name, "value").to_json())

                # Uniques
                uniques = stats[col_name].pop("approx_count_distinct")
                col_info['stats']["uniques_count"] = uniques
                col_info['stats']["p_uniques"] = round(uniques / rows_count * 100, 3)

            if column_type == "numeric":
                # Additional Stats
                # Percentile can not be used a normal sql.functions. approxQuantile in this case need and extra pass
                # https: // stackoverflow.com / questions / 45287832 / pyspark - approxquantile - function
                max_value = fast_float(max_value)
                min_value = fast_float(min_value)
                col_info['stats']['quantile'] = df.cols.percentile(col_name, [0.05, 0.25, 0.5, 0.75, 0.95])
                col_info['stats']['range'] = max_value - min_value
                col_info['stats']['median'] = col_info['stats']['quantile'][0.5]
                col_info['stats']['interquartile_range'] = col_info['stats']['quantile'][0.75] - \
                                                           col_info['stats']['quantile'][0.25]
                col_info['stats']['coef_variation'] = round((col_info['stats']['stddev'] / col_info['stats']['mean']),
                                                            5)
                col_info['stats']['mad'] = round(df.cols.mad(col_name), 5)

                col_info["hist"] = df.cols.hist(col_name, min_value, max_value, buckets)

            column_info['columns'][col_name] = col_info

        return column_info

    def run(self, df, columns, buckets=20):
        """
        Return statistical information in HTML Format
        :param df:
        :param columns:
        :param buckets:
        :return:
        """

        columns = parse_columns(df, columns)
        output = Profiler.to_json(df, columns, buckets)

        # Load jinja
        path = os.path.dirname(os.path.abspath(__file__))
        template_loader = jinja2.FileSystemLoader(searchpath=path + "//templates")
        template_env = jinja2.Environment(loader=template_loader, autoescape=True)

        # Render template
        # Create the profiler info header
        html = ""
        general_template = template_env.get_template("general_info.html")
        html = html + general_template.render(data=output)

        template = template_env.get_template("one_column.html")

        # Create every column stats
        for col_name in columns:
            if "hist" in output["columns"][col_name]:
                hist_pic = plot_hist({col_name: output["columns"][col_name]["hist"]}, output="base64")
            else:
                hist_pic = None
            if "frequency" in output["columns"][col_name]:
                freq_pic = plot_freq({col_name: output["columns"][col_name]["frequency"]}, output="base64")
            else:
                freq_pic = None

            html = html + template.render(data=output["columns"][col_name], hist_pic=hist_pic, freq_pic=freq_pic)

        html = html + df.table_html(10)
        # df.plots.correlation(columns)

        # Display HTML
        display(HTML(html))

        # Save to file
        write_json(output, self.path)

    @staticmethod
    def to_json(df, columns, buckets=20):
        """
        Return the profiling data in json format
        :param df: Dataframe to be processed
        :param columns: column to calculate the histogram
        :param buckets: buckets on the histogram
        :param path: Path where the json is going to be saved
        :return: json file
        """

        output = Profiler.columns(df, columns, buckets)
        dataset = Profiler.dataset_info(df)
        output["summary"] = dataset

        data = []
        # Get a sample of the data and transform it to friendly json format
        for l in df.sample_n(10).to_json():
            data.append([v for k, v in l.items()])
        output["sample"] = [{"columns": df.columns}, {"data": data}]

        return output

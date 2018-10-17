import configparser
import json
import logging
import os
from collections import defaultdict

import dateutil
import humanize
import jinja2
import pika
import pyspark.sql.functions as F
from IPython.core.display import display, HTML
from pyspark.sql.types import ArrayType, LongType

from optimus.functions import filter_row_by_data_type as fbdt, plot_hist, plot_freq
from optimus.helpers.decorators import time_it
from optimus.helpers.functions import parse_columns
from optimus.profiler.functions import fill_missing_var_types, fill_missing_col_types, \
    write_json


class Profiler:

    def __init__(self, output_path=None, queue_url=None, queue_exchange=None, queue_routing_key=None):
        """

        :param output_path:
        :param queue_url:
        :param queue_exchange:
        :param queue_routing_key:
        """

        config = configparser.ConfigParser()
        # If not path defined. Try to load from the config.ini file
        if output_path is None:
            try:
                # try to load the config file
                config.read("config.ini")
                output_path = config["PROFILER"]["Output"]
            except (IOError, KeyError):
                logging.info("Config.ini not found")
                output_path = "data.json"
                pass

        self.path = output_path
        self.queue_url = queue_url
        self.queue_exchange = queue_exchange
        self.queue_routing_key = queue_routing_key

    @staticmethod
    @time_it
    def dataset_info(df):
        """
        Return info about cols, row counts, total missing and disk size
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
             'missing_count': str(round(missing_count / rows_count, 2)) + "%",
             'size': humanize.naturalsize(df.size())}
        )

    # TODO: This should check only the StringType Columns. The datatype from others columns can be taken from schema().
    @staticmethod
    @time_it
    def count_data_types(df, columns, infer=False):
        """
        Count the number of int, float, string, date and booleans and output the count in json format
        :param df: Dataframe to be processed
        :param columns: Columns to be processed
        :return: json
        """

        @time_it
        def _count_data_types(col_name):
            """
            Function for determine if register value is float or int or string.
            :param col_name:
            :return:
            """
            logging.info("Processing column '" + col_name + "'...")
            # If String, process the data to try to infer which data type is inside. This a kind of optimization.
            # We do not need to analyze the data if the column data type is integer or boolean.etc

            temp = col_name + "_type"
            col_data_type = df.cols.dtypes(col_name)

            count_by_data_type = {}
            count_empty_strings = 0

            if infer is True and col_data_type == "string":

                types = (df
                         .h_repartition(col_name=col_name)
                         .withColumn(temp, fbdt(col_name, get_type=True))
                         .groupBy(temp).count()
                         .to_json())

                for row in types:
                    count_by_data_type[row[temp]] = row["count"]

                count_empty_strings = df.where(F.col(col_name) == '').count()

            else:
                nulls = df.cols.count_na(col_name)
                count_by_data_type[col_data_type] = int(df.count()) - nulls
                count_by_data_type["null"] = nulls

            count_by_data_type = fill_missing_var_types(count_by_data_type)

            # Subtract white spaces to the total string count
            data_types_count = {"string": count_by_data_type['string'],
                                "bool": count_by_data_type['bool'],
                                "int": count_by_data_type['int'],
                                "float": count_by_data_type['float'],
                                "double": count_by_data_type['double'],
                                "date": count_by_data_type['date'],
                                "array": count_by_data_type['array']
                                }

            null_missed_count = {"null": count_by_data_type['null'],
                                 "missing": count_empty_strings,
                                 }
            # Get the greatest count by column data type
            greatest_data_type_count = max(data_types_count, key=data_types_count.get)

            if greatest_data_type_count is "string":
                cat = "categorical"
            elif greatest_data_type_count is "int" or greatest_data_type_count is "float" or greatest_data_type_count is "double":
                cat = "numeric"
            elif greatest_data_type_count is "date":
                cat = "date"
            elif greatest_data_type_count is "bool":
                cat = "bool"
            elif greatest_data_type_count is "array":
                cat = "array"
            else:
                cat = "null"

            col = {}
            col['dtype'] = greatest_data_type_count
            col['type'] = cat
            col['details'] = {**data_types_count, **null_missed_count}

            return col

        columns = parse_columns(df, columns)

        # Info from all the columns
        type_details = {c: _count_data_types(c) for c in columns}

        results = {}
        count_types = {}

        # Count the categorical, numerical, boolean and date columns
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

    @time_it
    def run(self, df, columns, buckets=40, infer=False, relative_error=1):
        """
        Return dataframe statistical information in HTML Format

        :param df: Dataframe to be analyzed
        :param columns: Columns to be analized
        :param buckets: Number of buckets calculated to print the histogram
        :param relative_error: Relative Error for quantile discretizer calculation
        :return:
        """

        columns = parse_columns(df, columns)
        output = Profiler.to_json(df, columns, buckets, infer, relative_error)

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
            hist_pic = None
            col = output["columns"][col_name]
            if "hist" in col:
                if col["column_dtype"] == "date":
                    hist_year = plot_hist({col_name: col["hist"]["years"]}, "base64", "years")
                    hist_month = plot_hist({col_name: col["hist"]["months"]}, "base64", "months")
                    hist_weekday = plot_hist({col_name: col["hist"]["weekdays"]}, "base64", "weekdays")
                    hist_hour = plot_hist({col_name: col["hist"]["hours"]}, "base64", "hours")
                    hist_minute = plot_hist({col_name: col["hist"]["minutes"]}, "base64", "minutes")
                    hist_pic = {"hist_years": hist_year, "hist_months": hist_month, "hist_weekdays": hist_weekday,
                                "hist_hours": hist_hour, "hist_minutes": hist_minute}
                else:

                    hist = plot_hist({col_name: col["hist"]}, output="base64")
                    hist_pic = {"hist_pic": hist}

            if "frequency" in col:
                freq_pic = plot_freq({col_name: col["frequency"]}, output="base64")
            else:
                freq_pic = None

            html = html + template.render(data=col, freq_pic=freq_pic, **hist_pic)

        html = html + df.table_html(10)

        # Display HTML
        display(HTML(html))

        # send to queue

        if self.queue_url is not None:
            self.to_queue(output)

        # Save to file
        write_json(output, self.path)

    def to_queue(self, message):
        """
        Send the profiler information to a queue. By default it use a public encryted queue.
        :return:
        """

        # Access the CLODUAMQP_URL environment variable and parse it (fallback to localhost)
        url = os.environ.get('CLOUDAMQP_URL', self.queue_url)
        params = pika.URLParameters(url)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()  # start a channel
        channel.queue_declare(queue='optimus')  # Declare a queue

        channel.basic_publish(exchange=self.queue_exchange,
                              routing_key=self.queue_routing_key,
                              body=json.dumps(message))
        channel.close()

    @staticmethod
    def to_json(df, columns, buckets=40, infer=False, relative_error=1):
        """
        Return the profiling data in json format
        :param df: Dataframe to be processed
        :param columns: column to calculate the histogram
        :param buckets: buckets on the histogram
        :return: json file
        """

        # Get the stats for all the columns
        output = Profiler.columns(df, columns, buckets, infer, relative_error)

        # Add the data summary to the output
        output["summary"] = Profiler.dataset_info(df)

        # Get a data sample and transform it to friendly json format
        data = []
        for l in df.sample_n(10).to_json():
            data.append([v for k, v in l.items()])
        output["sample"] = {"columns": df.columns, "data": data}

        return output

    @staticmethod
    def columns(df, columns, buckets=40, infer=False, relative_error=1):
        """
        Return statistical information about a specific column in json format
        :param df: Dataframe to be processed
        :param columns: Columns that you want to profile
        :param buckets: Create buckets divided by range. Each bin is equal.
        :param relative_error: relative error when the percentile is calculated. 0 is more exact as slow 1 more error and faster
        :return: json object with the
        """

        columns = parse_columns(df, columns)

        # Get just a sample to infer the column data type
        # sample_size_number = sample_size(rows_count, 95.0, 2.0)
        # fraction = sample_size_number / rows_count
        # sample = df.sample(False, fraction, seed=1)

        # Initialize Objects
        columns_info = {}
        columns_info['columns'] = {}

        rows_count = df.count()
        columns_info['rows_count'] = humanize.intword(rows_count)
        count_dtypes = Profiler.count_data_types(df, columns, infer)

        columns_info["count_types"] = count_dtypes["count_types"]
        columns_info['size'] = humanize.naturalsize(df.size())

        # Cast columns to the data type infer by count_data_types()
        df = Profiler.cast_columns(df, columns, count_dtypes).cache()

        # Calculate stats
        stats = Profiler.general_stats(df, columns)

        for col_name in columns:
            col_info = {}
            logging.info("------------------------------")
            logging.info("Processing column '" + col_name + "'...")
            columns_info['columns'][col_name] = {}

            col_info["stats"] = stats[col_name]
            col_info.update(Profiler.frequency(df, col_name, buckets))
            col_info.update(Profiler.stats_by_column(col_name, stats, count_dtypes, rows_count))

            col_info['column_dtype'] = count_dtypes["columns"][col_name]['dtype']
            col_info["dtypes_stats"] = count_dtypes["columns"][col_name]['details']

            column_type = count_dtypes["columns"][col_name]['type']

            if column_type == "numeric":
                col_info["stats"].update(Profiler.extra_numeric_stats(df, col_name, stats, relative_error))
                col_info["hist"] = df.cols.hist(col_name, stats[col_name]["min"], stats[col_name]["max"], buckets)

            if column_type == "categorical" or column_type == "array":
                col_info["hist"] = Profiler.hist_string(df, col_name, buckets)

            if column_type == "date":
                col_info["hist"] = Profiler.hist_date(df, col_name)

            columns_info['columns'][col_name] = col_info

        return columns_info

    @staticmethod
    @time_it
    def frequency(df, col_name, buckets):
        """
        Calculate the item frequency by column
        :param df:
        :param col_name:
        :param buckets:
        :return:
        """
        rows_count = df.count()
        col_info = {}
        # Frequency
        freq = (df
                .h_repartition(col_name=col_name)
                .groupBy(col_name)
                .count()
                .rows.sort([("count", "desc"), (col_name, "desc")])
                .limit(buckets)
                .withColumn("percentage",
                            F.round((F.col("count") / rows_count) * 100,
                                    3))
                .cols.rename(col_name, "value").to_json())

        # Get only ten items to print the table
        col_info['frequency'] = freq
        return col_info

    @staticmethod
    @time_it
    def general_stats(df, columns):
        """
        Return General stats for a column
        :param df:
        :param columns:
        :return:
        """

        def na(col_name):
            return F.count(F.when(F.isnan(col_name) | F.col(col_name).isNull(), col_name))

        def zeros(col_name):
            return F.count(F.when(F.col(col_name) == 0, col_name))

        stats = df.cols._exprs(
            [F.min, F.max, F.stddev, F.kurtosis, F.mean, F.skewness, F.sum, F.variance, F.approx_count_distinct, na,
             zeros],
            columns)
        return stats

    @staticmethod
    @time_it
    def extra_numeric_stats(df, col_name, stats, relative_error):
        """
        Specific Stats for numeric columns
        :param df:
        :param col_name:
        :param stats:
        :param relative_error:
        :return:
        """

        col_info = defaultdict()
        col_info['stats'] = {}
        # Percentile can not be used a normal sql.functions. approxQuantile in this case need and extra pass
        # https://stackoverflow.com/questions/45287832/pyspark-approxquantile-function
        quantile = df.cols.percentile(col_name, [0.05, 0.25, 0.5, 0.75, 0.95],
                                      relative_error)

        max_value = stats[col_name]["max"]
        min_value = stats[col_name]["min"]
        stddev = stats[col_name]['stddev']
        mean = stats[col_name]['mean']

        col_info['range'] = max_value - min_value
        col_info['median'] = quantile[0.5]
        col_info['interquartile_range'] = quantile[0.75] - quantile[0.25]

        col_info['coef_variation'] = round((stddev / mean), 5)
        col_info['mad'] = round(df.cols.mad(col_name), 5)
        col_info['quantile'] = quantile

        return col_info

    @staticmethod
    @time_it
    def cast_columns(df, columns, count_dtypes):
        """
        Cast column depending of inferred data type.
        :param df: Dataframe to be analyzed
        :param columns: Dataframe columns to be analyzed
        :param count_dtypes: String with columns and data types
        :return: Dataframe with casted columns
        """
        # Cast every column to a specific type to ensure the correct profiling
        # For example if we calculate the min or max of a string column with numeric values the result will be incorrect
        for col_name in columns:
            dtype = count_dtypes["columns"][col_name]['dtype']
            # Not force date type conversion, we can not trust that is going to be representative
            if dtype in ["string", "float", "int", "bool"]:
                df = df.cols.cast(col_name, dtype)
        return df

    @staticmethod
    @time_it
    def stats_by_column(col_name, stats, count_dtypes, rows_count):
        """
        :param df: Dataframe to be analyzed
        :param col_name: Dataframe column to be analyzed
        :param count_dtypes:
        :return:
        """

        col_info = {}
        col_info["stats"] = {}

        column_type = count_dtypes["columns"][col_name]['type']
        col_info['column_dtype'] = count_dtypes["columns"][col_name]['dtype']

        na = stats[col_name]["na"]

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

        # Uniques
        uniques = stats[col_name].pop("approx_count_distinct")
        col_info['stats']["uniques_count"] = uniques
        col_info['stats']["p_uniques"] = round((uniques / rows_count) * 100, 3)
        return col_info

    @staticmethod
    @time_it
    def hist_date(df, col_name):
        """
        Create a histogram for a date type column
        :param df: Dataframe to be analyzed
        :param col_name: Dataframe column to be analyzed
        :return:
        """
        col_info = {}

        # Create year/month/week day/hour/minute

        def func_infer_date(value, args):
            if value is None:
                result = [None]
            else:
                date = dateutil.parser.parse(value)
                result = [date.year, date.month, date.weekday(), date.hour, date.minute]
            return result

        df = (df
              .cols.select(col_name)
              .cols.apply(col_name, func_infer_date, ArrayType(LongType()))
              .cols.unnest(col_name).h_repartition().cache()
              )

        for i in range(5):
            key_name = ""
            temp_col = col_name + "_" + str(i)
            # Years
            if i == 0:
                buckets_date = 100
                key_name = "years"

                min_value = df.cols.min(temp_col)
                max_value = df.cols.max(temp_col)

            # Months
            elif i == 1:
                buckets_date = 12
                min_value = 0
                max_value = 12
                key_name = "months"

            # Weekdays
            elif i == 2:
                buckets_date = 7
                min_value = 0
                max_value = 7
                key_name = "weekdays"

            # Hours
            elif i == 3:
                buckets_date = 24
                min_value = 0
                max_value = 24
                key_name = "hours"

            # Minutes
            elif i == 4:
                buckets_date = 60
                min_value = 0
                max_value = 60
                key_name = "minutes"

            col_info[key_name] = df.cols.hist(temp_col, min_value, max_value, buckets_date)

        return col_info

    @staticmethod
    @time_it
    def hist_string(df, col_name, buckets):
        """
        Create a string for a date type column
        :param df: Dataframe to be analyzed
        :param col_name: Dataframe column to be analyzed
        :param buckets:
        :return:
        """

        col_name_len = col_name + "_len"
        df = df.cols.apply_expr(col_name_len, F.length(F.col(col_name)))
        min_value = df.cols.min(col_name_len)
        max_value = df.cols.max(col_name_len)

        # Max value can be considered as the number of buckets
        buckets_for_string = buckets
        if max_value <= 50:
            buckets_for_string = max_value

        result = df.cols.hist(col_name_len, min_value, max_value, buckets_for_string)

        return result

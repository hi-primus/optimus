import ntpath
import os
import tempfile
from urllib.request import Request, urlopen

import pandas as pd
from packaging import version

from optimus.helpers.check import is_url
from optimus.helpers.columns import replace_columns_special_characters
from optimus.helpers.logger import logger
from optimus.spark import Spark


class Load:

    @staticmethod
    def json(path, multiline=False, *args, **kwargs):
        """
        Return a dataframe from a json file.
        :param path: path or location of the file.
        :param multiline:
        :return:
        """
        file, file_name = prepare_path(path, "json")

        try:
            # TODO: Check a better way to handle this Spark.instance.spark. Very verbose.
            df = Spark.instance.spark.read \
                .option("multiLine", multiline) \
                .option("mode", "PERMISSIVE") \
                .json(file, *args, **kwargs)

            df.set_meta("file_name", file_name)

        except IOError as error:
            logger.print(error)
            raise
        return replace_columns_special_characters(df)

    @staticmethod
    def tsv(path, header='true', infer_schema='true', *args, **kwargs):
        """
        Return a dataframe from a tsv file.
        :param path: path or location of the file.
        :param header: tell the function whether dataset has a header row. 'true' default.
        :param infer_schema: infers the input schema automatically from data.
        It requires one extra pass over the data. 'true' default.

        :return:
        """

        return Load.csv(path, sep='\t', header=header, infer_schema=infer_schema, *args, **kwargs)

    @staticmethod
    def csv(path, sep=',', header='true', infer_schema='true', *args, **kwargs):
        """
        Return a dataframe from a csv file. It is the same read.csv Spark function with some predefined
        params

        :param path: path or location of the file.
        :param sep: usually delimiter mark are ',' or ';'.
        :param header: tell the function whether dataset has a header row. 'true' default.
        :param infer_schema: infers the input schema automatically from data.
        It requires one extra pass over the data. 'true' default.

        :return dataFrame
        """
        file, file_name = prepare_path(path, "csv")

        try:
            df = (Spark.instance.spark.read
                  .options(header=header)
                  .options(mode="DROPMALFORMED")
                  .options(delimiter=sep)
                  .options(inferSchema=infer_schema)
                  .csv(file, *args, **kwargs))

            df.set_meta("file_name", file_name)
        except IOError as error:
            logger.print(error)
            raise
        return replace_columns_special_characters(df)

    @staticmethod
    def parquet(path, *args, **kwargs):
        """
        Return a dataframe from a parquet file.
        :param path: path or location of the file. Must be string dataType
        :param args: custom argument to be passed to the spark parquet function
        :param kwargs: custom keyword arguments to be passed to the spark parquet function
        :return: Spark Dataframe
        """

        file, file_name = prepare_path(path, "parquet")

        try:
            df = Spark.instance.spark.read.parquet(file, *args, **kwargs)
            df.set_meta("file_name", file_name)

        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def avro(path, *args, **kwargs):
        """
        Return a dataframe from a avro file.
        :param path: path or location of the file. Must be string dataType
        :param args: custom argument to be passed to the spark avro function
        :param kwargs: custom keyword arguments to be passed to the spark avro function
        :return: Spark Dataframe
        """
        file, file_name = prepare_path(path, "avro")

        try:
            if version.parse(Spark.instance.spark.version) < version.parse("2.4"):
                avro_version = "com.databricks.spark.avro"
            else:
                avro_version = "avro "
            df = Spark.instance.spark.read.format(avro_version).load(file, *args, **kwargs)

            df.set_meta("file_name", file_name)
        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def excel(path, sheet_name=0, *args, **kwargs):
        """
        Return a dataframe from a excel file.
        :param path: Path or location of the file. Must be string dataType
        :param sheet_name: excel sheet name
        :param args: custom argument to be passed to the excel function
        :param kwargs: custom keyword arguments to be passed to the excel function
        :return: Spark Dataframe
        """
        file, file_name = prepare_path(path, "xls")

        try:
            pdf = pd.read_excel(file, sheet_name=sheet_name, *args, **kwargs)

            # Parse object column data type to string to ensure that Spark can handle it. With this we try to reduce
            # exception when Spark try to infer the column data type
            col_names = list(pdf.select_dtypes(include=['object']))

            column_dtype = {}
            for col in col_names:
                column_dtype[col] = str

            # Convert object columns to string
            pdf = pdf.astype(column_dtype)

            # Create spark data frame
            df = Spark.instance.spark.createDataFrame(pdf)
            df.set_meta("file_name", ntpath.basename(file_name))
        except IOError as error:
            logger.print(error)
            raise

        return replace_columns_special_characters(df)


def prepare_path(path, file_format):
    """
    Helper to return the file to be loaded and the file name
    :param path: Path to the file to be loaded
    :param file_format: format file
    :return:
    """
    file_name = ntpath.basename(path)
    if is_url(path):
        file = downloader(path, file_format)
    else:
        file = path
    return file, file_name


def downloader(url, file_format):
    """
    Send the request to download a file
    """

    def write_file(response, file, chunk_size=8192):
        """
        Load the data from the http request and save it to disk
        :param response: data returned from the server
        :param file:
        :param chunk_size: size chunk size of the data
        :return:
        """
        total_size = response.headers['Content-Length'].strip() if 'Content-Length' in response.headers else 100
        total_size = int(total_size)
        bytes_so_far = 0

        while 1:
            chunk = response.read(chunk_size)
            bytes_so_far += len(chunk)
            if not chunk:
                break
            file.write(chunk)
            total_size = bytes_so_far if bytes_so_far > total_size else total_size

        return bytes_so_far

    # try to infer the file format using the file extension
    if file_format is None:
        filename, file_format = os.path.splitext(url)
        file_format = file_format.replace('.', '')

    i = url.rfind('/')
    data_name = url[(i + 1):]

    headers = {"User-Agent": "Optimus Data Downloader/1.0"}

    req = Request(url, None, headers)

    logger.print("Downloading %s from %s", data_name, url)

    # It seems that avro need a .avro extension file
    with tempfile.NamedTemporaryFile(suffix="." + file_format, delete=False) as f:
        bytes_downloaded = write_file(urlopen(req), f)
        path = f.name

    if bytes_downloaded > 0:
        logger.print("Downloaded %s bytes", bytes_downloaded)

    logger.print("Creating DataFrame for %s. Please wait...", data_name)

    return path

import os
import tempfile
from urllib.request import Request, urlopen

import pandas as pd
from packaging import version

from optimus.helpers.functions import replace_columns_special_characters
from optimus.helpers.logger import logger
from optimus.helpers.raiseit import RaiseIt
from optimus.spark import Spark


class Load:

    def url(self, url=None, file_format=None):
        """
        Entry point for loading data from a URL. Check that the url is well format
        :param url: string for URL to read
        :param file_format: type of the URL backend (can be csv or json)
        :return: pyspark dataframe from URL.
        """

        if "https://" in str(url) or "http://" in str(url) or "file://" in str(url):
            return self._data_loader(str(url), file_format)
        else:
            RaiseIt.type_error(file_format, ["https://", "http://", "file://"])

    def _data_loader(self, url, file_format=None):
        """
        Select the correct method to download the file depending of the format
        :param url: string url
        :param file_format: format data type
        :return:
        """

        # try to infer the file format using the file extension
        if file_format is None:
            filename, file_format = os.path.splitext(url)
            file_format = file_format.replace('.', '')

        data_loader = None
        if file_format == "csv":
            data_loader = self.csv
        elif file_format == "json":
            data_loader = self.json
        elif file_format == "parquet":
            data_loader = self.parquet
        elif file_format == "avro":
            data_loader = self.avro
        elif file_format == "excel":
            data_loader = self.avro
        else:
            RaiseIt.type_error(file_format, ["csv", "json", "parquet", "avro", "excel"])

        i = url.rfind('/')
        data_name = url[(i + 1):]
        data_def = {
            "displayName": data_name,
            "url": url
        }

        return Downloader(data_def).download(data_loader, file_format)

    @staticmethod
    def json(path, multiline=False, *args, **kwargs):
        """
        Return a dataframe from a json file.
        :param path: path or location of the file.
        :param multiline:
        :return:
        """
        try:
            # TODO: Check a better way to handle this Spark.instance.spark. Very verbose.
            df = Spark.instance.spark.read \
                .option("multiLine", multiline) \
                .option("mode", "PERMISSIVE") \
                .json(path, *args, **kwargs)

        except IOError as error:
            logger.print(error)
            raise
        return replace_columns_special_characters(df)

    @staticmethod
    def csv(path, sep=',', header='true', infer_schema='true', *args, **kwargs):
        """
        Return a dataframe from a csv file.. It is the same read.csv Spark function with some predefined
        params

        :param path: path or location of the file.
        :param sep: usually delimiter mark are ',' or ';'.
        :param header: tell the function whether dataset has a header row. 'true' default.
        :param infer_schema: infers the input schema automatically from data.
        It requires one extra pass over the data. 'true' default.

        :return dataFrame
        """
        try:
            df = (Spark.instance.spark.read
                  .options(header=header)
                  .options(mode="DROPMALFORMED")
                  .options(delimiter=sep)
                  .options(inferSchema=infer_schema)
                  .csv(path, *args, **kwargs))
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

        try:
            df = Spark.instance.spark.read.parquet(path, *args, **kwargs)
        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def avro(path, *args, **kwargs):
        """
        Return a dataframe from a avro file.
        :param path: path or location of the file. Must be string dataType
        :param args: custom argument to be passed to the spark parquet function
        :param kwargs: custom keyword arguments to be passed to the spark parquet function
        :return: Spark Dataframe
        """
        try:
            if version.parse(Spark.instance.spark.version) < version.parse("2.4"):
                avro_version = "com.databricks.spark.avro"
            else:
                avro_version = "avro "
            df = Spark.instance.spark.read.format(avro_version).load(path, *args, **kwargs)

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
        :param args: custom argument to be passed to the spark parquet function
        :param kwargs: custom keyword arguments to be passed to the spark parquet function
        :return: Spark Dataframe
        """
        try:
            pdf = pd.read_excel(path, sheet_name=sheet_name, *args, **kwargs)

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
        except IOError as error:
            logger.print(error)
            raise

        return replace_columns_special_characters(df)


class Downloader(object):
    """
    Send the request to download a file
    """

    def __init__(self, data_def):
        self.data_def = data_def
        self.headers = {"User-Agent": "Optimus Data Downloader/1.0"}

    def download(self, data_loader, file_format):
        display_name = self.data_def["displayName"]
        bytes_downloaded = 0
        if "path" in self.data_def:
            path = self.data_def["path"]
        else:
            url = self.data_def["url"]
            req = Request(url, None, self.headers)

            logger.print("Downloading %s from %s", display_name, url)

            # It seems that avro need a .avro extension file
            with tempfile.NamedTemporaryFile(suffix="." + file_format, delete=False) as f:
                bytes_downloaded = self.write(urlopen(req), f)
                path = f.name
                self.data_def["path"] = path = f.name
        if path:
            try:
                if bytes_downloaded > 0:
                    logger.print("Downloaded %s bytes", bytes_downloaded)
                logger.print("Creating DataFrame for %s. Please wait...", display_name)
                return data_loader(path)
            finally:
                logger.print("Successfully created DataFrame for '%s'", display_name)

    @staticmethod
    def write(response, file, chunk_size=8192):
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

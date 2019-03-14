import logging
import tempfile
from urllib.request import Request, urlopen

from optimus.helpers.raiseit import RaiseIt
from optimus.spark import Spark
from packaging import version
from optimus.helpers.logger import logger


class Load:

    def url(self, path=None, type_of="csv"):
        """
        Entry point for loading data from a URL. Check that the url is well format
        :param path: string for URL to read
        :param type_of: type of the URL backend (can be csv or json)
        :return: pyspark dataframe from URL.
        """

        if "https://" in str(path) or "http://" in str(path) or "file://" in str(path):
            return self._data_loader(str(path), type_of)
        else:
            RaiseIt.type_error(type_of, ["https://", "http://", "file://"])

    def _data_loader(self, url, type_of):
        """
        Select the correct method to download the file depending of the format
        :param url: string url
        :param type_of: format data type
        :return:
        """

        file_format = None
        if type_of == "csv":
            file_format = self.csv
        elif type_of == "json":
            file_format = self.json
        elif type_of == "parquet":
            file_format = self.parquet
        elif type_of == "avro":
            file_format = self.avro
        else:
            RaiseIt.type_error(file_format, ["csv", "json", "parquet", "avro", ])

        i = url.rfind('/')
        data_name = url[(i + 1):]
        data_def = {
            "displayName": data_name,
            "url": url
        }
        return Downloader(data_def).download(file_format, type_of)

    @staticmethod
    def json(path, *args, **kwargs):
        """
        Return a dataframe from a json file.
        :param path:
        :return:
        """
        try:
            # TODO: Check a better way to handle this Spark.instance.spark. Very verbose.
            df = Spark.instance.spark.read.json(path, *args, **kwargs)
        except IOError as error:
            logging.error(error)
            raise
        return df

    @staticmethod
    def csv(path, sep=',', header='true', infer_schema='true', *args, **kwargs):
        """
        Return a dataframe from a csv file.. It is the same read.csv Spark function with some predefined
        params

        :param path: Path or location of the file.
        :param sep: Usually delimiter mark are ',' or ';'.
        :param  header: Tell the function whether dataset has a header row. 'true' default.
        :param infer_schema: Infers the input schema automatically from data.
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
            logging.error(error)
            raise
        return df

    @staticmethod
    def parquet(path, *args, **kwargs):
        """
        Return a dataframe from a parquet file.
        :param path: Path or location of the file. Must be string dataType.
        :param args: custom argument to be passed to the spark parquet function
        :param kwargs: custom keyword arguments to be passed to the spark parquet function
        :return dataFrame
        """

        try:
            df = Spark.instance.spark.read.parquet(path, *args, **kwargs)
        except IOError as error:
            logging.error(error)
            raise

        return df

    @staticmethod
    def avro(path, *args, **kwargs):
        """
        Return a dataframe from a avro file.
        :param path: Path or location of the file. Must be string dataType.
        :param args: custom argument to be passed to the spark parquet function
        :param kwargs: custom keyword arguments to be passed to the spark parquet function
        :return:
        """
        try:
            if version.parse(Spark.instance.spark.version) < version.parse("2.4"):
                avro_version = "com.databricks.spark.avro"
            else:
                avro_version = "avro "
            df = Spark.instance.spark.read.format(avro_version).load(path, *args, **kwargs)

        except IOError as error:
            logging.error(error)
            raise

        return df


class Downloader(object):
    """
    Send the request to download a file
    """

    def __init__(self, data_def):
        self.data_def = data_def
        self.headers = {"User-Agent": "Optimus Data Downloader/1.0"}

    def download(self, data_loader, ext):
        display_name = self.data_def["displayName"]
        bytes_downloaded = 0
        if "path" in self.data_def:
            path = self.data_def["path"]
        else:
            url = self.data_def["url"]
            req = Request(url, None, self.headers)

            logger.print("Downloading %s from %s", display_name, url)

            # It seems that avro need a .avro extension file
            with tempfile.NamedTemporaryFile(suffix="." + ext, delete=False) as f:
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
        :param response: data retruned
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

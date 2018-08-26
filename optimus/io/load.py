# URL reading
import logging
import tempfile
from io import BytesIO
from urllib.request import Request, urlopen

import fastavro

from optimus.spark import Spark


class Load:

    def url(self, path=None, type_of="csv"):
        """
        Reads a dataset from URL.
        :param path: string for URL to read
        :param type_of: type of the URL backend (can be csv or json)
        :return: pyspark dataframe from URL.
        """

        if "https://" in str(path) or "http://" in str(path) or "file://" in str(path):
            return self.data_loader(str(path), type_of)
        else:
            print("Unknown sample data identifier. Please choose an id from the list below")

    def data_loader(self, url, type_of):
        """
        Load data in csv or json format from a url
        :param url:
        :param type_of:
        :return:
        """
        i = url.rfind('/')
        data_name = url[(i + 1):]
        data_def = {
            "displayName": data_name,
            "url": url
        }
        if type_of == "csv":
            data_loader = self.csv
        elif type_of == "json":
            data_loader = self.json
        elif type_of == "parquet":
            data_loader = self.parquet
        elif type_of == "avro":
            data_loader = self.avro
        elif type_of == "geojson":
            data_loader = self.geojson

        return Downloader(data_def).download(data_loader)

    @staticmethod
    def json(path):
        """
        This function read data from a json file.
        :param path:
        :return:
        """
        try:
            df = Spark.instance.spark.read.json(path)
        except IOError as error:
            logging.error(error)
            raise
        return df

    @staticmethod
    def csv(path, sep=',', header='true', infer_schema='true', *args, **kargs):
        """
        This function read data from a csv file. It is the same read.csv Spark funciont with some predefined
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
                  .csv(path, *args, **kargs))
        except IOError as error:
            logging.error(error)
            raise
        return df

    def parquet(self, path):
        """

        :param path: Path or location of the file. Must be string dataType.
        :return dataFrame
        """

        try:
            df = Spark.instance.spark.read.parquet(path)
        except IOError as error:
            logging.error(error)
            raise

        return df

    @staticmethod
    def avro(path):
        """

        :param  path: Path or location of the file. Must be string dataType.

        :return dataFrame
        """
        try:
            df = Spark.instance.spark.binaryFiles(path) \
                .flatMap(lambda args: fastavro.reader(BytesIO(args[1]))).toDF()
        except IOError as error:
            logging.error(error)
            raise

        return df

    # reference https://medium.com/@sabman/loading-geojson-data-in-apache-spark-f7a52390cdc9
    def geojson(self, path):
        """

        :param path:
        :return:
        """

        return path


class Downloader(object):
    def __init__(self, data_def):
        self.data_def = data_def
        self.headers = {"User-Agent": "Optimus Data Downloader/1.0"}

    def download(self, data_loader):
        display_name = self.data_def["displayName"]
        bytes_downloaded = 0
        if "path" in self.data_def:
            path = self.data_def["path"]
        else:
            url = self.data_def["url"]
            req = Request(url, None, self.headers)
            logging.info("Downloading %s from %s", display_name, url)
            with tempfile.NamedTemporaryFile(delete=False) as f:
                bytes_downloaded = self.write_file(urlopen(req), f)
                path = f.name
                self.data_def["path"] = path = f.name
        if path:
            try:
                if bytes_downloaded > 0:
                    logging.info("Downloaded %s bytes", bytes_downloaded)
                logging.info("Creating DataFrame for %s. Please wait...", display_name)
                return data_loader(path)
            finally:
                logging.info("Successfully created DataFrame for '%s'", display_name)

    @staticmethod
    def write_file(response, file, chunk_size=8192):
        """
        Write file from http request to Disk
        :param response:
        :param file:
        :param chunk_size:
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
            file.write_file(chunk)
            total_size = bytes_so_far if bytes_so_far > total_size else total_size

        return bytes_so_far

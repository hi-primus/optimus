# URL reading
import tempfile
import logging

from urllib.request import Request, urlopen
from optimus.spark import Spark

# TODO: Append to write dataframe class
class Load:

    def url(self, path=None, type_of="csv"):
        """
        Reads dataset from URL.
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
            data_loader = self.csv_data_loader
        else:
            data_loader = self.json_data_loader

        return Downloader(data_def).download(data_loader)

    @staticmethod
    def csv_data_loader(path):
        """

        :param path:
        :return:
        """

        logging.info("Loading file using SparkSession")
        csvload = Spark.instance.spark() \
            .read \
            .format("csv") \
            .options(header=True) \
            .options(mode="DROPMALFORMED")

        return csvload.option("inferSchema", "true").load(path)

    @staticmethod
    def json_data_loader(path):
        """

        :param path:
        :return:
        """
        res = open(path, 'r').read()
        print("Loading file using a pyspark.read.json")
        data_rdd = Spark.instance.sc().parallelize([res])
        return Spark.instance.spark().read.json(data_rdd)


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
                bytes_downloaded = self.write(urlopen(req), f)
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
    def write(response, file, chunk_size=8192):
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

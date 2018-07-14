# Importing module to work with urls
import urllib.request

# URL reading
import tempfile
from urllib.request import Request, urlopen
from optimus.spark import Spark


class Load:

    def data_loader(self, url, type_of):
        """

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
            print("csv")
            data_loader = self.csv_data_loader
        else:
            data_loader = self.json_data_loader

        return Downloader(data_def).download(data_loader)

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

    def csv_data_loader(self, path):
        """

        :param path:
        :return:
        """

        print("path")
        print("Loading file using 'SparkSession'")
        csvload = Spark.instance.get_ss() \
            .read \
            .format("csv") \
            .options(header=True) \
            .options(mode="DROPMALFORMED")

        return csvload.option("inferSchema", "true").load(path)

    def json_data_loader(self, path):
        """

        :param path:
        :return:
        """
        res = open(path, 'r').read()
        print("Loading file using a pyspark dataframe for spark")
        data_rdd = Spark.instace.get_sc().parallelize([res])
        return Spark.instance.get_ss().read.json(data_rdd)

        print(SUCCESS)
class Downloader(object):
    def __init__(self, data_def):
        self.data_def = data_def
        self.headers = {"User-Agent": "PixieDust Sample Data Downloader/1.0"}

    def download(self, data_loader):
        display_name = self.data_def["displayName"]
        bytes_downloaded = 0
        if "path" in self.data_def:
            path = self.data_def["path"]
        else:
            url = self.data_def["url"]
            req = Request(url, None, self.headers)
            print("Downloading '{0}' from {1}".format(display_name, url))
            with tempfile.NamedTemporaryFile(delete=False) as f:
                bytes_downloaded = self.write(urlopen(req), f)
                path = f.name
                self.data_def["path"] = path = f.name
        if path:
            try:
                if bytes_downloaded > 0:
                    print("Downloaded {} bytes".format(bytes_downloaded))
                print("Creating {1} DataFrame for '{0}'. Please wait...".format(display_name, 'pySpark'))
                return data_loader(path)
            finally:
                print("Successfully created {1} DataFrame for '{0}'".format(display_name, 'pySpark'))

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

def load_spark_data_frame_from_url(self, data_url):
    """

    :param data_url:
    :return:
    """
    i = data_url.rfind('/')
    data_name = data_url[(i + 1):]
    data_def = {
        "displayName": data_name,
        "url": data_url
    }

    return Downloader(data_def).download(self.data_loader)


def json_load_spark_data_frame_from_url(self, data_url):
    """

    :param data_url:
    :return:
    """
    i = data_url.rfind('/')
    data_name = data_url[(i + 1):]
    data_def = {
        "displayName": data_name,
        "url": data_url
    }

    return Downloader(data_def).download(self.json_data_loader)


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

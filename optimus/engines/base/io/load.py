import csv
import os

import magic

from optimus.helpers.functions import prepare_path
from optimus.helpers.raiseit import RaiseIt

XML_THRESHOLD = 10
JSON_THRESHOLD = 20
BYTES_SIZE = 16384


class BaseLoad:
    def csv(self, full_path, *args, **kwargs):
        pass

    def xml(self, full_path, *args, **kwargs):
        pass

    def json(self, full_path, *args, **kwargs):
        pass

    def excel(self, full_path, *args, **kwargs):
        pass

    def file(self, path, *args, **kwargs):
        """
        Try to  infer the file data format and encoding
        :param path: Path to the file we want to load.
        :param args:
        :param kwargs:
        :return:
        """

        full_path, file_name = prepare_path(path)[0]
        file_ext = os.path.splitext(file_name)[1].replace(".", "")

        mime, encoding = magic.Magic(mime=True, mime_encoding=True).from_file(full_path).split(";")
        mime_info = {"mime": mime, "encoding": encoding.strip().split("=")[1], "file_ext": file_ext}

        # CSV
        if mime == "text/plain" or mime == "application/csv":
            # In some case magic get a "unknown-8bit" which can not be use to decode the file use latin-1 instead
            if mime_info["encoding"] == "unknown-8bit":
                mime_info["encoding"] = "latin-1"
            try:
                file = open(full_path, encoding=mime_info["encoding"]).read(BYTES_SIZE)
                dialect = csv.Sniffer().sniff(file)
                mime_info["file_type"] = "csv"

                r = {"properties": {"delimiter": dialect.delimiter,
                                    "doublequote": dialect.doublequote,
                                    "escapechar": dialect.escapechar,
                                    "lineterminator": dialect.lineterminator,
                                    "quotechar": dialect.quotechar,
                                    "quoting": dialect.quoting,
                                    "skipinitialspace": dialect.skipinitialspace}}

                mime_info.update(r)
                df = self.csv(path, encoding=mime_info["encoding"], dtype=str, **mime_info["properties"],
                              **kwargs, engine="python", na_values='nan')
            except Exception as err:
                raise err
                pass

        # JSON
        elif mime == "application/json":
            mime_info["file_type"] = "json"
            df = self.json(full_path, *args, **kwargs)

        # XML
        elif mime == "text/xml":
            mime_info["file_type"] = "xml"
            df = self.xml(full_path, **kwargs)

        elif mime == "application/vnd.ms-excel":
            mime_info["file_type"] = "excel"
            df = self.excel(full_path, **kwargs)

        else:
            RaiseIt.value_error(mime_info["file_ext"], ["csv", "json", "xml", "xls", "xlsx"])

        # print(os.path.abspath(__file__))
        df.meta.update("mime_info", value=mime_info)
        return df

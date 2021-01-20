import csv
import os
from abc import abstractmethod

import magic

from optimus.helpers.functions import prepare_path
from optimus.helpers.raiseit import RaiseIt

XML_THRESHOLD = 10
JSON_THRESHOLD = 20
BYTES_SIZE = 16384


class BaseLoad:

    @staticmethod
    @abstractmethod
    def csv(filepath_or_buffer, *args, **kwargs):
        pass

    def xml(self, full_path, *args, **kwargs):
        pass

    def json(self, full_path, *args, **kwargs):
        pass

    def excel(self, full_path, *args, **kwargs):
        pass

    @staticmethod
    @abstractmethod
    def avro(full_path, *args, **kwargs):
        pass

    @staticmethod
    @abstractmethod
    def parquet(full_path, *args, **kwargs):
        pass

    @staticmethod
    @abstractmethod
    def orc(full_path, columns=None, *args, **kwargs):
        pass

    def file(self, path, *args, **kwargs):
        """
        Try to  infer the file data format and encoding
        :param path: Path to the file we want to load.
        :param args:
        :param kwargs:
        :return:
        """

        try:
            full_path, file_name = prepare_path(path)[0]
            file_ext = os.path.splitext(file_name)[1].replace(".", "")
            mime, encoding = magic.Magic(mime=True, mime_encoding=True).from_file(full_path).split(";")
            mime_info = {"mime": mime, "encoding": encoding.strip().split("=")[1], "file_ext": file_ext}
        except Exception as e:
            print(getattr(e, 'message', repr(e)))
            full_path = path
            file_name = path.split('/')[-1]
            file_ext = file_name.split('.')[-1]
            mime = False
            mime_info = { "file_type": file_ext, "encoding": False }

        file_type = file_ext

        if mime:
            if mime in ["text/plain", "application/csv"]:
                file_type = "csv"
            elif mime == "application/json":
                file_type = "json"
            elif mime == "text/xml":
                file_type = "xml"
            elif mime in ["application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"]:
                file_type = "excel"
            else:
                RaiseIt.value_error(mime, ["csv", "json", "xml", "xls", "xlsx"])
        
        if file_type == "csv":
            # In some case magic get a "unknown-8bit" which can not be use to decode the file use latin-1 instead
            if mime_info.get("encoding", None) == "unknown-8bit":
                mime_info["encoding"] = "latin-1"
            
            if mime:
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
                    df = self.csv(path, encoding=mime_info.get("encoding", None), dtype=str, **mime_info.get("properties", {}), **kwargs, engine="python", na_values='nan')
                except Exception as err:
                    raise err
                    pass
            else:
                df = self.csv(path, dtype=str, **kwargs, engine="python", na_values='nan')
                
        elif file_type == "json":
            mime_info["file_type"] = "json"
            df = self.json(full_path, *args, **kwargs)

        elif file_type == "xml":
            mime_info["file_type"] = "xml"
            df = self.xml(full_path, **kwargs)

        elif file_type == "excel":
            mime_info["file_type"] = "excel"
            df = self.excel(full_path, **kwargs)

        else:
            RaiseIt.value_error(file_type, ["csv", "json", "xml", "xls", "xlsx"])

        # print(os.path.abspath(__file__))
        # df.meta.update("mime_info", value=mime_info)

        return df

    # def to_optimus_pandas(self, df):
    #     return PandasDataFrame(df.to_pandas())

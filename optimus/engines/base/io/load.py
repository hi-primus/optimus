import os
from abc import abstractmethod

from optimus.engines.base.basedataframe import BaseDataFrame
from optimus.helpers.functions import prepare_path
from optimus.helpers.raiseit import RaiseIt

XML_THRESHOLD = 10
JSON_THRESHOLD = 20
BYTES_SIZE = 327680


class BaseLoad:

    @staticmethod
    @abstractmethod
    def csv(filepath_or_buffer, *args, **kwargs) -> BaseDataFrame:
        pass

    @staticmethod
    @abstractmethod
    def xml(self, full_path, *args, **kwargs) -> BaseDataFrame:
        pass

    @staticmethod
    @abstractmethod
    def json(self, full_path, *args, **kwargs) -> BaseDataFrame:
        pass

    @staticmethod
    @abstractmethod
    def excel(self, full_path, *args, **kwargs) -> BaseDataFrame:
        pass

    @staticmethod
    @abstractmethod
    def avro(full_path, *args, **kwargs) -> BaseDataFrame:
        pass

    @staticmethod
    @abstractmethod
    def parquet(full_path, *args, **kwargs) -> BaseDataFrame:
        pass

    @staticmethod
    @abstractmethod
    def orc(full_path, columns=None, *args, **kwargs) -> BaseDataFrame:
        pass

    @staticmethod
    @abstractmethod
    def zip(zip_path, filename, dest=None, columns=None, storage_options=None, conn=None, *args, **kwargs) -> BaseDataFrame:
        pass

    @staticmethod
    @abstractmethod
    def hdf5(full_path, columns=None, *args, **kwargs) -> BaseDataFrame:
        pass

    def file(self, path, *args, **kwargs) -> BaseDataFrame:
        """
        Try to  infer the file data format and encoding
        :param path: Path to the file we want to load.
        :param args:
        :param kwargs:
        :return:
        """
        conn = kwargs.get("conn")

        if conn:
            import boto3
            remote_obj = boto3.resource(
                conn.type, **conn.boto).Object(conn.options.get("bucket"), path)
            body = remote_obj.get()['Body']
            buffer = body.read(amt=BYTES_SIZE)
            full_path = conn.path(path)
            file_name = os.path.basename(path)

        else:

            full_path, file_name = prepare_path(path)[0]
            file = open(full_path, "rb")
            buffer = file.read(BYTES_SIZE)

        # Detect the file type
        try:
            file_ext = os.path.splitext(file_name)[1].replace(".", "")
            import magic
            mime, encoding = magic.Magic(
                mime=True, mime_encoding=True).from_buffer(buffer).split(";")
            mime_info = {"mime": mime, "encoding": encoding.strip().split("=")[
                1], "file_ext": file_ext}

        except Exception as e:
            print(getattr(e, 'message', repr(e)))
            full_path = path
            file_name = path.split('/')[-1]
            file_ext = file_name.split('.')[-1]
            mime = False
            mime_info = {"file_type": file_ext, "encoding": False}

        file_type = file_ext

        if mime:
            if mime in ["text/plain", "application/csv"]:
                file_type = "csv"
            elif mime == "application/json":
                file_type = "json"
            elif mime == "text/xml":
                file_type = "xml"
            elif mime in ["application/vnd.ms-excel",
                          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"]:
                file_type = "excel"
            else:
                RaiseIt.value_error(
                    mime, ["csv", "json", "xml", "xls", "xlsx"])

        # Detect the file encoding
        if file_type == "csv":
            # In some case magic get a "unknown-8bit" which can not be use to decode the file use latin-1 instead
            if mime_info.get("encoding", None) == "unknown-8bit":
                mime_info["encoding"] = "latin-1"

            if mime:
                try:
                    import csv
                    dialect = csv.Sniffer().sniff(str(buffer))
                    mime_info["file_type"] = "csv"

                    r = {"properties": {"delimiter": dialect.delimiter,
                                        "doublequote": dialect.doublequote,
                                        "escapechar": dialect.escapechar,
                                        "lineterminator": dialect.lineterminator,
                                        "quotechar": dialect.quotechar,
                                        "quoting": dialect.quoting,
                                        "skipinitialspace": dialect.skipinitialspace}}

                    mime_info.update(r)
                    kwargs.update({
                        "encoding": mime_info.get("encoding", None),
                        **mime_info.get("properties", {})
                    })
                    df = self.csv(path, **kwargs)
                except Exception as err:
                    raise err
                    pass
            else:
                df = self.csv(path, **kwargs)

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
            RaiseIt.value_error(
                file_type, ["csv", "json", "xml", "xls", "xlsx"])

        return df

    @staticmethod
    def model(path):
        import joblib
        return joblib.load(path)

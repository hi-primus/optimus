import os
from abc import abstractmethod

from optimus.engines.base.basedataframe import BaseDataFrame
from optimus.helpers.functions import prepare_path
from optimus.helpers.raiseit import RaiseIt
from optimus.helpers.types import DataFrameType

XML_THRESHOLD = 10
JSON_THRESHOLD = 20
BYTES_SIZE = 327680


class BaseLoad:

    def __init__(self, op):
        self.op = op

    def csv(self, filepath_or_buffer, sep=",", header=True, infer_schema=True, encoding="UTF-8", n_rows=None,
            null_value="None", quoting=3, lineterminator='\r\n', error_bad_lines=False, cache=False, na_filter=False,
            storage_options=None, conn=None, n_partitions=None, *args, **kwargs) -> 'DataFrameType':
        """
        Loads a dataframe from a csv file. It is the same read.csv Spark function with some predefined
        params


        :param encoding:
        :param storage_options:
        :param quoting:
        :param filepath_or_buffer: path or location of the file.
        :param sep: usually delimiter mark are ',' or ';'.
        :param header: tell the function whether dataset has a header row. True default.
        :param infer_schema: infers the input schema automatically from data.
        :param n_rows:
        :param null_value:
        :param cache:
        :param na_filter:
        :param lineterminator:
        :param error_bad_lines:
        :param conn:
        It requires one extra pass over the data. True default.

        :return dataFrame
        """
        raise NotImplementedError('Not implemented yet')

    def xml(self, path, n_partitions=None, *args, **kwargs) -> 'DataFrameType':
        raise NotImplementedError('Not implemented yet')

    def json(self, path, n_partitions=None, *args, **kwargs) -> 'DataFrameType':
        raise NotImplementedError('Not implemented yet')

    def excel(self, path, n_partitions=None, *args, **kwargs) -> 'DataFrameType':
        """
        Loads a dataframe from a excel file.
        :param path: Path or location of the file. Must be string dataType
        :param sheet_name: excel sheet name
        :param args: custom argument to be passed to the excel function
        :param kwargs: custom keyword arguments to be passed to the excel function
        """
        raise NotImplementedError('Not implemented yet')

    def avro(self, path, sheet_name=0, storage_options=None, conn=None, n_partitions=None, *args, **kwargs) -> 'DataFrameType':
        """
        Loads a dataframe from a avro file.
        :param path: path or location of the file. Must be string dataType
        :param sheet_name: A specific sheet name in the excel file to be loaded.
        :param storage_options:
        :param conn:
        :param args: custom argument to be passed to the spark avro function
        :param kwargs: custom keyword arguments to be passed to the spark avro function
        """
        raise NotImplementedError('Not implemented yet')

    def parquet(self, path, columns=None, storage_options=None, conn=None, n_partitions=None, *args, **kwargs) -> 'DataFrameType':
        """
        Loads a dataframe from a parquet file.
        :param path: path or location of the file. Must be string dataType
        :param columns: select the columns that will be loaded. In this way you do not need to load all the dataframe
        :param storage_options:
        :param conn:
        :param args: custom argument to be passed to the spark parquet function
        :param kwargs: custom keyword arguments to be passed to the spark parquet function
        """
        raise NotImplementedError('Not implemented yet')

    def orc(self, path, columns, storage_options=None, conn=None, n_partitions=None, *args, **kwargs) -> 'DataFrameType':
        """
        Loads a dataframe from a OCR file.
        :param path: path or location of the file. Must be string dataType
        :param columns: Specific column names to be loaded from the file.
        :param storage_options:
        :param conn:
        :param args: custom argument to be passed to the spark avro function
        :param kwargs: custom keyword arguments to be passed to the spark avro function
        """
        raise NotImplementedError('Not implemented yet')

    def zip(self, path, filename, dest=None, columns=None, storage_options=None, conn=None, n_partitions=None, *args,
            **kwargs) -> 'DataFrameType':
        pass

    def hdf5(self, path, columns=None, n_partitions=None, *args, **kwargs) -> 'DataFrameType':
        raise NotImplementedError('Not implemented yet')

    def file(self, path, *args, **kwargs) -> 'DataFrameType':
        """
        Try to  infer the file data format and encoding
        :param path: Path to the file you want to load.
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
                if mime_info["file_ext"] == "json":
                    file_type = "json"
                else:
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

                    r = {"properties": {"sep": dialect.delimiter,
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
                    df = self.csv(filepath_or_buffer=path, *args, **kwargs)
                except Exception as err:
                    raise err
                    pass
            else:
                df = self.csv(filepath_or_buffer=path, *args, **kwargs)

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
        """
        Load a machine learning model from a file
        :param path: Path to the file we want to load.
        :return:
        """
        import joblib
        return joblib.load(path)

import glob
import ntpath
import os
from abc import abstractmethod

import psutil

from optimus.engines.base.meta import Meta
from optimus.helpers.core import val_to_list
from optimus.helpers.functions import prepare_path, unquote_path
from optimus.helpers.logger import logger
from optimus.helpers.raiseit import RaiseIt
from optimus.helpers.types import DataFrameType, InternalDataFrameType
from optimus.infer import is_empty_function, is_list, is_str, is_url

XML_THRESHOLD = 10
JSON_THRESHOLD = 20
BYTES_SIZE = 327680


class BaseLoad:

    def __init__(self, op):
        self.op = op

    @abstractmethod
    def df(self, *args, **kwargs) -> 'DataFrameType':
        pass

    def _csv(self, *args, **kwargs) -> 'InternalDataFrameType':
        pass

    def _json(self, *args, **kwargs) -> 'InternalDataFrameType':
        pass

    def _excel(self, *args, **kwargs) -> 'InternalDataFrameType':
        pass

    def _avro(self, *args, **kwargs) -> 'InternalDataFrameType':
        pass

    def _xml(self, *args, **kwargs) -> 'InternalDataFrameType':
        pass

    def _parquet(self, *args, **kwargs) -> 'InternalDataFrameType':
        pass

    def csv(self, filepath_or_buffer, sep=",", header=True, infer_schema=True, encoding="UTF-8", n_rows=None,
            null_value="None", quoting=3, lineterminator='\r\n', on_bad_lines='warn', cache=False, na_filter=False,
            storage_options=None, conn=None, *args, **kwargs) -> 'DataFrameType':
        """
        Loads a dataframe from a csv file. It is the same read.csv Spark function with some predefined
        params.

        :param filepath_or_buffer: Path or location of the file.
        :param sep: Usually delimiter mark are ',' or ';'.
        :param encoding: Encoding to use for UTF when reading/writing.
        :param header: Tell the function whether dataset has a header row. True default.
        :param infer_schema: Infers the input schema automatically from data.
        :param n_rows:
        :param null_value: Whether or not to include the default NaN values when parsing the data
        :param quoting: Control field quoting behavior
        :param cache:
        :param na_filter: Detect missing value markers (empty strings and the value of na_values).
        :param lineterminator: Character to break file into lines.
        :param on_bad_lines: Specifies what to do upon encountering a bad line (a line with too many fields).
            Allowed values are:
            ‘error’, raise an Exception when a bad line is encountered.
            ‘warn’, raise a warning when a bad line is encountered and skip that line.
            ‘skip’, skip bad lines without raising or warning when they are encountered
        :param storage_options: A dict with the connection params.
        :param conn: A connection object.
        It requires one extra pass over the data. True default.

        :return dataFrame
        """

        if is_empty_function(self._csv):
            raise NotImplementedError(f"'load.csv' is not implemented on '{self.op.engine_label}'")

        unquoted_path = None

        if not is_url(filepath_or_buffer):
            unquoted_path = glob.glob(unquote_path(filepath_or_buffer))

        if unquoted_path and len(unquoted_path):
            meta = {"file_name": unquoted_path[0], "name": ntpath.basename(unquoted_path[0])}
        else:
            meta = {"file_name": filepath_or_buffer, "name": ntpath.basename(filepath_or_buffer)}

        try:

            # Pandas do not support \r\n terminator.
            if lineterminator and lineterminator.encode(encoding='UTF-8', errors='strict') == b'\r\n':
                lineterminator = None

            if conn is not None:
                filepath_or_buffer = [conn.path(fb) for fb in val_to_list(filepath_or_buffer)]
                storage_options = conn.storage_options

            if kwargs.get("chunk_size") == "auto":
                # Chunk size is going to be 75% of the memory available
                kwargs.pop("chunk_size")
                kwargs["chunksize"] = psutil.virtual_memory().free * 0.75

            na_filter = na_filter if null_value else False

            if not is_str(on_bad_lines):
                on_bad_lines = 'error' if on_bad_lines else 'skip'

            def _read(_filepath_or_buffer):
                return self._csv(_filepath_or_buffer, sep=sep, header=0 if header else None, encoding=encoding,
                                 nrows=n_rows, quoting=quoting, lineterminator=lineterminator,
                                 on_bad_lines=on_bad_lines, na_filter=na_filter,
                                 na_values=val_to_list(null_value), index_col=False,
                                 storage_options=storage_options, *args, **kwargs)

            if is_list(filepath_or_buffer):
                df = self.op.F.new_df()
                for f in filepath_or_buffer:
                    df = df.append(_read(f))
            else:
                df = _read(filepath_or_buffer)

            df = self.df(df, op=self.op)

            df.meta = Meta.set(df.meta, value=meta)

        except IOError as error:
            logger.print(error)
            raise

        return df

    def xml(self, path, n_rows=None, storage_options=None, conn=None, *args, **kwargs) -> 'DataFrameType':
        """
        Loads a dataframe from a XML file.

        :param path: Any valid XML string or path is acceptable. The string could be a URL.
        :param n_rows:
        :param storage_options: A dict with the connection params.
        :param conn: A connection object.
        :param args:
        :param kwargs:
        :return:
        """

        if is_empty_function(self._xml):
            raise NotImplementedError(f"'load.xml' is not implemented on '{self.op.engine_label}'")

        path = unquote_path(path)

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        file, file_name = prepare_path(path, "xml")[0]

        try:
            df = self._xml(file, n_rows, storage_options=storage_options, *args, **kwargs)
            df = self.df(df, op=self.op)
            df.meta = Meta.set(df.meta, "file_name", ntpath.basename(file_name))
        except IOError as error:
            logger.print(error)
            raise

        return df

    def json(self, filepath_or_buffer, multiline=False, n_rows=False, storage_options=None,
             conn=None, *args, **kwargs) -> 'DataFrameType':
        """
        Loads a dataframe from a json file.

        :param filepath_or_buffer: path or location of the file.
        :param multiline:
        :param n_rows:
        :param storage_options: A dict with the connection params.
        :param conn: A connection object.
        :param args:
        :param kwargs:
        :return:
        """

        if is_empty_function(self._json):
            raise NotImplementedError(f"'load.json' is not implemented on '{self.op.engine_label}'")

        if conn is not None:
            filepath_or_buffer = conn.path(filepath_or_buffer)
            storage_options = conn.storage_options

        if n_rows:
            kwargs["nrows"] = n_rows
            multiline = True

        if is_str(filepath_or_buffer):
            try:
                filepath_or_buffer = unquote_path(filepath_or_buffer)
                local_file_names = prepare_path(filepath_or_buffer, "json")
                df_list = []

                for file_name, j in local_file_names:
                    df = self._json(file_name, lines=multiline, *args, **kwargs)
                    df_list.append(df)
                df = self.op.F.df_concat(df_list)
                df = self.df(df, op=self.op)
                df.meta = Meta.set(df.meta, "file_name", local_file_names[0])
            except IOError as error:
                logger.print(error)
                raise

        else:
            df = self._json(filepath_or_buffer, lines=multiline, storage_options=storage_options, *args, **kwargs)
            df = self.df(df, op=self.op)

        return df

    def excel(self, filepath_or_buffer, header=0, sheet_name=0, merge_sheets=False, skip_rows=0, n_rows=None,
              storage_options=None, conn=None, *args, **kwargs) -> 'DataFrameType':
        """
        Loads a dataframe from a excel file.
        
        :param filepath_or_buffer: Path or location of the file. Must be string dataType
        :param header: 
        :param sheet_name: excel sheet name
        :param merge_sheets: 
        :param skip_rows: 
        :param n_rows: 
        :param storage_options: A dict with the connection params.
        :param conn: A connection object.
        :param args: custom argument to be passed to the excel function
        :param kwargs: custom keyword arguments to be passed to the excel function
        :return: 
        """

        if is_empty_function(self._excel):
            raise NotImplementedError(f"'load.excel' is not implemented on '{self.op.engine_label}'")

        filepath_or_buffer = unquote_path(filepath_or_buffer)

        if conn is not None:
            filepath_or_buffer = conn.path(filepath_or_buffer)
            storage_options = conn.storage_options

        file, file_name = prepare_path(filepath_or_buffer, "xls")[0]

        if merge_sheets is True:
            skip_rows = -1

        df, sheet_names = self._excel(file, sheet_name=sheet_name, skiprows=skip_rows, header=header, nrows=n_rows,
                                      storage_options=storage_options, *args, **kwargs)

        df = self.df(df, op=self.op)

        df.meta = Meta.set(df.meta, "file_name", ntpath.basename(file_name))
        df.meta = Meta.set(df.meta, "sheet_names", sheet_names)

        return df

    def avro(self, filepath_or_buffer, n_rows=None, storage_options=None, conn=None,
             *args, **kwargs) -> 'DataFrameType':
        """
        Loads a dataframe from a avro file.

        :param filepath_or_buffer: path or location of the file. Must be string dataType
        :param n_rows:
        :param storage_options: A dict with the connection params.
        :param conn: A connection object.
        :param args: custom argument to be passed to the spark avro function
        :param kwargs: custom keyword arguments to be passed to the spark avro function
        """

        if is_empty_function(self._avro):
            raise NotImplementedError(f"'load.avro' is not implemented on '{self.op.engine_label}'")

        filepath_or_buffer = unquote_path(filepath_or_buffer)

        if conn is not None:
            logger.warn("'load.avro' does not support connection options ('conn')")

        if storage_options is not None:
            logger.warn("'load.avro' does not support 'storage_options'")

        file, file_name = prepare_path(filepath_or_buffer, "avro")[0]

        try:
            df = self._avro(filepath_or_buffer, nrows=n_rows, *args, **kwargs)
            df = self.df(df, op=self.op)
            df.meta = Meta.set(df.meta, value={"file_name": file_name, "name": ntpath.basename(filepath_or_buffer)})

        except IOError as error:
            logger.print(error)
            raise

        return df

    def parquet(self, filepath_or_buffer, columns=None, n_rows=None, storage_options=None, conn=None,
                *args, **kwargs) -> 'DataFrameType':
        """
        Loads a dataframe from a parquet file.

        :param filepath_or_buffer: path or location of the file. Must be string dataType
        :param columns: select the columns that will be loaded. In this way you do not need to load all the dataframe
        :param storage_options: A dict with the connection params.
        :param conn: A connection object.
        :param args: custom argument to be passed to the spark parquet function
        :param kwargs: custom keyword arguments to be passed to the spark parquet function
        """

        if is_empty_function(self._parquet):
            raise NotImplementedError(f"'load.parquet' is not implemented on '{self.op.engine_label}'")

        filepath_or_buffer = unquote_path(filepath_or_buffer)

        if conn is not None:
            filepath_or_buffer = conn.path(filepath_or_buffer)
            storage_options = conn.storage_options

        try:
            dfd = self._parquet(filepath_or_buffer, columns=columns, nrows=n_rows,
                                storage_options=storage_options, *args, **kwargs)
            df = self.df(dfd, op=self.op)
            df.meta = Meta.set(df.meta,
                               value={"file_name": filepath_or_buffer, "name": ntpath.basename(filepath_or_buffer)})

        except IOError as error:
            logger.print(error)
            raise

        return df

    def orc(self, path, columns, storage_options=None, conn=None, n_partitions=None, *args,
            **kwargs) -> 'DataFrameType':
        """
        Loads a dataframe from a OCR file.

        :param path: path or location of the file. Must be string dataType.
        :param columns: Specific column names to be loaded from the file.
        :param storage_options: A dict with the connection params.
        :param conn: A connection object.
        :param args: custom argument to be passed to the spark avro function.
        :param kwargs: custom keyword arguments to be passed to the spark avro function.
        """
        raise NotImplementedError('Not implemented yet')

    def zip(self, path, filename, dest=None, columns=None, storage_options=None, conn=None, n_partitions=None,
            *args, **kwargs) -> 'DataFrameType':
        pass

    def hdf5(self, path, columns=None, n_partitions=None, *args, **kwargs) -> 'DataFrameType':
        """
        Loads a dataframe from a HDF5 file.

        :param path: path or location of the file. Must be string dataType.
        :param columns: Specific column names to be loaded from the file.
        :param n_partitions:
        :param args: custom argument to be passed to the spark avro function.
        :param kwargs: custom keyword arguments to be passed to the spark avro function.
        :return:
        """
        raise NotImplementedError('Not implemented yet')

    def tsv(self, filepath_or_buffer, header=True, infer_schema=True, *args, **kwargs):
        """
        Loads a dataframe from a tsv(Tabular separated values) file.

        :param filepath_or_buffer: Path or location of the file. Must be string dataType
        :param header:
        :param infer_schema:
        :param args: custom argument to be passed to the spark avro function.
        :param kwargs: custom keyword arguments to be passed to the spark avro function.
        :return:
        """
        return self.csv(filepath_or_buffer, sep='\t', header=header, infer_schema=infer_schema, *args, **kwargs)

    def file(self, path, *args, **kwargs) -> 'DataFrameType':
        """
        Try to infer the file data format and encoding and load the data into a dataframe.

        :param path: Path to the file you want to load.
        :param args: custom argument to be passed to the spark avro function.
        :param kwargs: custom keyword arguments to be passed to the spark avro function.
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

        ext_to_type = {
            "xls": "excel",
            "xlsx": "excel"
        }

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
            mime_info = {"file_type": ext_to_type.get(file_ext, file_ext), "encoding": False}

        file_type = ext_to_type.get(file_ext, file_ext)

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
            elif not file_type:
                RaiseIt.value_error(
                    mime, ["csv", "json", "xml", "xls", "xlsx"])

        # Detect the file encoding
        if file_type == "csv":
            # In some case magic get a "unknown-8bit" which can not be use to decode the file use latin-1 instead
            if mime_info.get("encoding", None) == "unknown-8bit":
                mime_info["encoding"] = "latin-1"

            if mime:
                import csv
                dialect = csv.Sniffer().sniff(str(buffer))
                mime_info["file_type"] = "csv"

                properties = {"sep": dialect.delimiter,
                              "doublequote": dialect.doublequote,
                              "escapechar": dialect.escapechar,
                              "lineterminator": dialect.lineterminator,
                              "quotechar": dialect.quotechar,
                              "quoting": dialect.quoting,
                              "skipinitialspace": dialect.skipinitialspace}

                for k in properties:
                    if k not in kwargs:
                        kwargs.update({k: properties[k]})
                
                mime_info.update({"properties": properties})
                kwargs.update({"encoding": mime_info.get("encoding", None)})

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
                file_type, ["csv", "json", "xml", "excel"])

        return df

    @staticmethod
    def model(path):
        """
        Load a machine learning model from a file.

        :param path: Path to the file we want to load.
        :return:
        """
        import joblib
        return joblib.load(path)

import glob
import ntpath
import uuid
import zipfile
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import pandavro as pdx
import psutil

from optimus.engines.base.basedataframe import BaseDataFrame
from optimus.engines.base.io.load import BaseLoad
from optimus.engines.base.meta import Meta
from optimus.engines.pandas.dataframe import PandasDataFrame
from optimus.helpers.functions import prepare_path, unquote_path
from optimus.helpers.logger import logger
from optimus.helpers.core import val_to_list
from optimus.infer import is_str, is_list, is_url


class Load(BaseLoad):

    def json(self, path, multiline=False, n_rows=False, n_partitions=1, *args, **kwargs):
        """
        Loads a dataframe from a json file.
        :param path: path or location of the file.
        :param multiline:

        :return:
        """
        if n_rows:
            kwargs["n_rows"] = n_rows
            kwargs["lines"] = True

        if is_str(path):
            try:
                path = unquote_path(path)
                local_file_names = prepare_path(path, "json")
                df_list = []

                for file_name, j in local_file_names:
                    df = pd.read_json(file_name, lines=multiline, *args, **kwargs)
                    df_list.append(df)
                df = pd.concat(df_list, axis=0, ignore_index=True)
                df = PandasDataFrame(df, op=self.op)
                df.meta = Meta.set(df.meta, "file_name", local_file_names[0])
            except IOError as error:
                logger.print(error)
                raise

        else:
            df = pd.read_json(path, lines=multiline, *args, **kwargs)
            df = PandasDataFrame(df, op=self.op)

        return df

    def tsv(self, filepath_or_buffer, header=True, infer_schema=True, n_partitions=1, *args, **kwargs):
        return self.csv(filepath_or_buffer, sep='\t', header=header, infer_schema=infer_schema, *args, **kwargs)

    def csv(self, filepath_or_buffer, sep=",", header=True, infer_schema=True, encoding="UTF-8", n_rows=None,
            null_value="None", quoting=3, lineterminator='\r\n', error_bad_lines=False, cache=False, na_filter=False,
            storage_options=None, conn=None, n_partitions=1, *args, **kwargs):

        if not is_url(filepath_or_buffer):
            filepath_or_buffer = glob.glob(unquote_path(filepath_or_buffer))
            meta = {"file_name": filepath_or_buffer, "name": ntpath.basename(filepath_or_buffer[0])}
        else:
            meta = {"file_name": filepath_or_buffer, "name": ntpath.basename(filepath_or_buffer)}

        try:

            # Pandas do not support \r\n terminator.
            if lineterminator and lineterminator.encode(encoding='UTF-8', errors='strict') == b'\r\n':
                lineterminator = None

            if conn is not None:
                filepath_or_buffer = conn.path(filepath_or_buffer)
                storage_options = conn.storage_options
            else:
                storage_options = None

            if kwargs.get("chunk_size") == "auto":
                ## Chunk size is going to be 75% of the memory available
                kwargs.pop("chunk_size")
                kwargs["chunksize"] = psutil.virtual_memory().free * 0.75

            na_filter = na_filter if null_value else False

            def _read(_filepath_or_buffer):
                return pd.read_csv(_filepath_or_buffer, sep=sep, header=0 if header else None, encoding=encoding,
                                   nrows=n_rows,
                                   quoting=quoting, lineterminator=lineterminator, error_bad_lines=error_bad_lines,
                                   na_filter=na_filter, na_values=val_to_list(null_value), index_col=False, storage_options=storage_options, *args,
                                   **kwargs)

            if is_list(filepath_or_buffer):
                df = pd.DataFrame()
                for f in filepath_or_buffer:
                    df = df.append(_read(f))
            else:
                df = _read(filepath_or_buffer)

            if isinstance(df, pd.io.parsers.TextFileReader):
                df = df.get_chunk()

            df = PandasDataFrame(df, op=self.op)

            df.meta = Meta.set(df.meta, value=meta)

        except IOError as error:
            print(error)
            logger.print(error)
            raise

        return df

    def parquet(self, path, columns=None, storage_options=None, conn=None, n_partitions=1, *args, **kwargs):

        path = unquote_path(path)

        # file, file_name = prepare_path(path, "parquet")[0]

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        try:
            df = pd.read_parquet(path, columns=columns, engine='pyarrow', storage_options=storage_options, **kwargs)
            df = PandasDataFrame(df, op=self.op)
            df.meta = Meta.set(df.meta, value={"file_name": path, "name": ntpath.basename(path)})

        except IOError as error:
            logger.print(error)
            raise

        return df

    def avro(self, path, storage_options=None, conn=None, n_partitions=1, *args, **kwargs):

        path = unquote_path(path)

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        file, file_name = prepare_path(path, "avro")[0]

        try:
            df = pdx.read_avro(file_name, storage_options=storage_options, n_partitions=1, *args, **kwargs)
            df = PandasDataFrame(df, op=self.op)
            df.meta = Meta.set(df.meta, value={"file_name": path, "name": ntpath.basename(path)})

        except IOError as error:
            logger.print(error)
            raise

        return df

    def excel(self, path, sheet_name=0, storage_options=None, conn=None, n_partitions=1, *args, **kwargs):

        path = unquote_path(path)

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        file, file_name = prepare_path(path, "xls")[0]

        try:
            df = pd.read_excel(file, sheet_name=sheet_name, storage_options=storage_options, *args, **kwargs)

            # Parse object column data type to string to ensure that Spark can handle it. With this we try to reduce
            # exception when Spark try to infer the column data type
            col_names = list(df.select_dtypes(include=['object']))

            column_dtype = {}
            for col in col_names:
                column_dtype[col] = str

            # Convert object columns to string
            df = df.astype(column_dtype)

            # Create spark data frame
            df = PandasDataFrame(df, op=self.op)
            df.meta = Meta.set(df.meta, "file_name", ntpath.basename(file_name))
        except IOError as error:
            logger.print(error)
            raise

        return df

    def orc(self, path, columns, storage_options=None, conn=None, n_partitions=1, *args, **kwargs):

        path = unquote_path(path)

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        file, file_name = prepare_path(path, "orc")[0]

        try:
            df = pdx.read_orc(file_name, columns, storage_options=storage_options)
            df = PandasDataFrame(df, op=self.op)
            df.meta = Meta.set(df.meta, "file_name", file_name)

        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def zip(zip_path, filename, dest=None, merge=False, storage_options=None, conn=None, n_partitions=1, *args, **kwargs):
        if dest is None:
            dest = str(uuid.uuid4()) + "/"

        zip_path = glob.glob(zip_path)

        dest = Path(dest).expanduser()

        # if csv concat all files
        # if json multilie concat files

        for filename in zip_path:
            # print(filename)
            with zipfile.ZipFile(filename) as zf:
                zf.infolist()
                for member in zf.infolist():
                    # print(member.filename)
                    try:
                        zf.extract(member, dest)
                    except zipfile.error as e:
                        pass

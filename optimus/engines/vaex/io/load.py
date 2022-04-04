import ntpath

import pandas as pd
import vaex

from optimus.engines.base.io.load import BaseLoad
from optimus.engines.base.meta import Meta
from optimus.engines.vaex.dataframe import VaexDataFrame
from optimus.helpers.functions import prepare_path, unquote_path
from optimus.helpers.logger import logger


class Load(BaseLoad):

    @staticmethod
    def df(*args, **kwargs):
        return VaexDataFrame(*args, **kwargs)

    def hdf5(self, path, columns=None, *args, **kwargs):
        path = unquote_path(path)
        dfd = vaex.open(path)
        df = VaexDataFrame(dfd, op=self.op)
        df.meta = Meta.set(df.meta, value={"file_name": path, "name": ntpath.basename(path)})
        return df

    def json(self, path, multiline=False, *args, **kwargs):
        """
        Return a dask dataframe from a json file.
        :param path: path or location of the file.
        :param multiline:

        :return:
        """
        file, file_name = prepare_path(path, "json")

        try:

            df = vaex.read_json(path, lines=multiline, *args, **kwargs)
            df = VaexDataFrame(df, op=self.op)
            df.meta = Meta.set(df.meta, "file_name", file_name)

        except IOError as error:
            logger.print(error)
            raise
        return df

    def _csv(self, filepath_or_buffer, sep=',', header=True, infer_schema=True, na_values=None, encoding="utf-8",
             n_rows=-1, cache=False, quoting=0, lineterminator=None, on_bad_lines='warn', engine="c",
             keep_default_na=False, na_filter=False, null_value=None, storage_options=None, conn=None,
             n_partitions=1, *args, **kwargs):

        filepath_or_buffer = unquote_path(filepath_or_buffer)

        if cache is False:
            prepare_path.cache_clear()

        if conn is not None:
            filepath_or_buffer = conn.path(filepath_or_buffer)
            storage_options = conn.storage_options

        remove_param = "chunk_size"
        if kwargs.get(remove_param):
            # This is handle in this way to preserve compatibility with others dataframe technologies.
            logger.print(f"{remove_param} is not supported. Used to preserve compatibility with Optimus Pandas")
            kwargs.pop(remove_param)

        try:
            print("header", header)
            # From the panda docs using na_filter
            # Detect missing value markers (empty strings and the value of na_values). In data without any NAs,
            # passing na_filter=False can improve the performance of reading a large file.
            df = vaex.read_csv(filepath_or_buffer, sep=sep, header=header, encoding=encoding,
                                quoting=quoting, lineterminator=lineterminator, on_bad_lines=on_bad_lines,
                                keep_default_na=True, na_values=None, engine=engine, na_filter=na_filter,
                                storage_options=storage_options, *args, **kwargs)

            if n_rows > -1:
                df = vaex.from_pandas(df.head(n=n_rows), npartitions=1).reset_index(drop=True)

        except IOError as error:
            logger.print(error)
            raise
        # print(df)
        return df

    @staticmethod
    def parquet(path, columns=None, *args, **kwargs):

        file, file_name = prepare_path(path, "parquet")

        try:
            df = vaex.open(path, columns=columns, engine='pyarrow', *args, **kwargs)
            df.meta = Meta.set(df.meta, "file_name", file_name)

        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def avro(path, *args, **kwargs):

        file, file_name = prepare_path(path, "avro")

        try:
            df = vaex.open(path, *args, **kwargs).to_dataframe()
            df.meta = Meta.set(df.meta, "file_name", file_name)

        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def excel(path, sheet_name=0, *args, **kwargs):

        file, file_name = prepare_path(path, "xls")

        try:
            pdf = pd.read_excel(file, sheet_name=sheet_name, *args, **kwargs)

            # Parse object column data type to string to ensure that Spark can handle it. With this we try to reduce
            col_names = list(pdf.select_dtypes(include=['object']))

            column_dtype = {}
            for col in col_names:
                column_dtype[col] = str

            # Convert object columns to string
            pdf = pdf.astype(column_dtype)

            df = vaex.open(pdf)
            df.meta = Meta.set(df.meta, "file_name", ntpath.basename(file_name))
        except IOError as error:
            logger.print(error)
            raise

        return df

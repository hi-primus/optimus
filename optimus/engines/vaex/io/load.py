import ntpath
from io import StringIO

import pandas as pd
import requests
import vaex

from optimus.engines.base.io.load import BaseLoad
from optimus.engines.base.meta import Meta
from optimus.engines.vaex.dataframe import VaexDataFrame
from optimus.helpers.functions import prepare_path, unquote_path
from optimus.helpers.logger import logger
from optimus.infer import is_url


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

    @staticmethod
    def _json(filepath_or_buffer, *args, **kwargs):
        kwargs.pop("n_partitions", None)

        if is_url(filepath_or_buffer):
            filepath_or_buffer = StringIO(requests.get(filepath_or_buffer).text)

        df = vaex.read_json(filepath_or_buffer, lines=multiline, *args, **kwargs)

        return df

    @staticmethod
    def _csv(filepath_or_buffer, *args, **kwargs):

        kwargs.pop("n_partitions", None)
        if is_url(filepath_or_buffer):
            filepath_or_buffer = StringIO(requests.get(filepath_or_buffer).text)
        df = vaex.read_csv(filepath_or_buffer, *args, **kwargs)

        # if isinstance(df, pd.io.parsers.TextFileReader):
        #     df = df.get_chunk()
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

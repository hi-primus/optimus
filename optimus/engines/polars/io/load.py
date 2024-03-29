import glob
import uuid
import zipfile
from io import StringIO
from pathlib import Path
from typing import Tuple

import pandavro as pdx
import polars as pl
import requests
import pandas as pd
from optimus.engines.base.io.load import BaseLoad
from optimus.engines.base.meta import Meta
from optimus.engines.polars.dataframe import PolarsDataFrame
from optimus.helpers.core import val_to_list
from optimus.helpers.functions import prepare_path, unquote_path
from optimus.helpers.logger import logger
from optimus.infer import is_url
from optimus.optimus import EnginePretty


class Load(BaseLoad):

    @staticmethod
    def df(*args, **kwargs):
        print("vvvv",args)
        return PolarsDataFrame(*args, **kwargs)

    @staticmethod
    def _csv(filepath_or_buffer, *args, **kwargs):
        kwargs.pop("n_partitions", None)

        if is_url(filepath_or_buffer):
            try:
                resp = requests.get(filepath_or_buffer)
                df = pl.scan_csv(StringIO(resp.text), *args, **kwargs)
                resp.raise_for_status()
            except requests.exceptions.HTTPError as err:
                print(err)

        else:
            kwargs.pop("encoding",None)
            df = pl.scan_csv(filepath_or_buffer, *args, **kwargs)

        # if isinstance(df, pd.io.parsers.TextFileReader):
        #     df = df.get_chunk()

        return df

    @staticmethod
    def _json(filepath_or_buffer, *args, **kwargs):

        def _safe_json(filepath_or_buffer, *args, **kwargs) -> Tuple[pd.DataFrame, bool]:

            try:
                df = pl.scan_json(filepath_or_buffer, *args, **kwargs)
                return df, True
            except ValueError:
                kwargs.pop("nrows", None)
                kwargs.pop("lines", None)
                df = pl.scan_json(filepath_or_buffer, *args, **kwargs)
                return df, False

        kwargs.pop("n_partitions", None)

        if is_url(filepath_or_buffer):
            s = requests.get(filepath_or_buffer).text
            df, truncated = _safe_json(StringIO(s), *args, **kwargs)
        else:
            df, truncated = _safe_json(filepath_or_buffer, *args, **kwargs)

        if not truncated:
            nrows = kwargs.get("nrows", None)
            if nrows:
                logger.warn(
                    f"'load.json' on {EnginePretty.POLARS.value} loads the whole dataset and then truncates it if file is not multiline.")
                df = df[:nrows]

        return df

    @staticmethod
    def _avro(filepath_or_buffer, nrows=None, *args, **kwargs):
        kwargs.pop("n_partitions", None)

        if is_url(filepath_or_buffer):
            s = requests.get(filepath_or_buffer).text
            df = pdx.read_avro(StringIO(s), *args, **kwargs)
        else:
            df = pdx.read_avro(filepath_or_buffer, *args, **kwargs)

        if nrows:
            logger.warn(f"'load.avro' on {EnginePretty.POLARS.value} loads the whole dataset and then truncates it")
            df = df[:nrows]

        return df

    @staticmethod
    def _parquet(filepath_or_buffer, nrows=None, engine="pyarrow", *args, **kwargs):
        kwargs.pop("n_partitions", None)

        if is_url(filepath_or_buffer):
            s = requests.get(filepath_or_buffer).text
            df = pl.scan_parquet(StringIO(s), engine=engine, *args, **kwargs)
        else:
            df = pl.scan_parquet(filepath_or_buffer, engine=engine, *args, **kwargs)

        if nrows:
            logger.warn(f"'load.parquet' on {EnginePretty.POLARS.value} loads the whole dataset and then truncates it")
            df = df[:nrows]

        return df

    @staticmethod
    def _xml(filepath_or_buffer, nrows=None, *args, **kwargs):
        kwargs.pop("n_partitions", None)

        if is_url(filepath_or_buffer):
            s = requests.get(filepath_or_buffer).text
            df = pl.read_xml(StringIO(s), *args, **kwargs)
        else:
            df = pl.read_xml(filepath_or_buffer, *args, **kwargs)

        if nrows:
            logger.warn(f"'load.xml' on {EnginePretty.POLARS.value} loads the whole dataset and then truncates it")
            df = df[:nrows]

        return df

    @staticmethod
    def _excel(filepath_or_buffer, nrows=None, storage_options=None, *args, **kwargs):
        kwargs.pop("n_partitions", None)

        if is_url(filepath_or_buffer):
            s = requests.get(filepath_or_buffer).text
            filepath_or_buffer = StringIO(s)

        dfs = pd.read_excel(filepath_or_buffer, nrows=nrows, storage_options=storage_options, *args, **kwargs)
        sheet_names = list(pd.read_excel(filepath_or_buffer, None, storage_options=storage_options).keys())
        df = pd.concat(val_to_list(dfs), axis=0).reset_index(drop=True)

        return df, sheet_names

    def orc(self, path, columns, storage_options=None, conn=None, n_partitions=1, *args, **kwargs):

        path = unquote_path(path)

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        file, file_name = prepare_path(path, "orc")[0]

        try:
            df = pdx.read_orc(file_name, columns, storage_options=storage_options)
            df = PolarsDataFrame(df, op=self.op)
            df.meta = Meta.set(df.meta, "file_name", file_name)

        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def zip(zip_path, filename, dest=None, merge=False, storage_options=None, conn=None, n_partitions=1, *args,
            **kwargs):
        if dest is None:
            dest = str(uuid.uuid4()) + "/"

        zip_path = glob.glob(zip_path)

        dest = Path(dest).expanduser()

        # if csv concat all files
        # if json, multiple concat files

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

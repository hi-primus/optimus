import glob
import uuid
import zipfile
from pathlib import Path

import pandas as pd
import pandavro as pdx

from optimus.optimus import EnginePretty
from optimus.engines.base.io.load import BaseLoad
from optimus.engines.base.meta import Meta
from optimus.engines.pandas.dataframe import PandasDataFrame
from optimus.helpers.functions import prepare_path, unquote_path
from optimus.helpers.logger import logger
from optimus.helpers.core import val_to_list
from optimus.infer import is_str, is_list, is_url


class Load(BaseLoad):

    @staticmethod
    def df(*args, **kwargs):
        return PandasDataFrame(*args, **kwargs)

    @staticmethod
    def _csv(filepath_or_buffer, *args, **kwargs):
        kwargs.pop("n_partitions", None)
        df = pd.read_csv(filepath_or_buffer, *args, **kwargs)
        if isinstance(df, pd.io.parsers.TextFileReader):
            df = df.get_chunk()
        return df

    @staticmethod
    def _json(filepath_or_buffer, *args, **kwargs):
        kwargs.pop("n_partitions", None)
        return pd.read_json(filepath_or_buffer, *args, **kwargs)

    @staticmethod
    def _avro(filepath_or_buffer, nrows=None, *args, **kwargs):
        kwargs.pop("n_partitions", None)
        df = pdx.read_avro(filepath_or_buffer, *args, **kwargs)
        if nrows:
            logger.warn(f"'load.avro' on {EnginePretty.PANDAS.value} loads the whole dataset and then truncates it")
            df = df[:nrows]
        return df

    @staticmethod
    def _parquet(filepath_or_buffer, nrows=None, engine="pyarrow", *args, **kwargs):
        kwargs.pop("n_partitions", None)        
        df = pd.read_parquet(filepath_or_buffer, engine=engine, *args, **kwargs)
        if nrows:
            logger.warn(f"'load.parquet' on {EnginePretty.PANDAS.value} loads the whole dataset and then truncates it")
            df = df[:nrows]
        
        return df

    @staticmethod
    def _xml(filepath_or_buffer, nrows=None, *args, **kwargs):
        kwargs.pop("n_partitions", None)
        df = pd.read_xml(filepath_or_buffer, *args, **kwargs)
        if nrows:
            logger.warn(f"'load.xml' on {EnginePretty.PANDAS.value} loads the whole dataset and then truncates it")
            df = df[:nrows]

        return df

    @staticmethod
    def _excel(path, nrows=None, storage_options=None, *args, **kwargs):
        kwargs.pop("n_partitions", None)
        dfs = pd.read_excel(path, nrows=nrows, storage_options=storage_options, *args, **kwargs)
        sheet_names = list(pd.read_excel(path, None, storage_options=storage_options).keys())
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

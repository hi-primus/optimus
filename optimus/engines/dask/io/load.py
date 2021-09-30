import re

import ntpath
from optimus.helpers.raiseit import RaiseIt

import dask.bag as dask_bag
import pandas as pd
from dask import dataframe as dd

import optimus.helpers.functions_spark
from optimus.optimus import EnginePretty
from optimus.engines.base.io.load import BaseLoad
from optimus.engines.base.meta import Meta
from optimus.engines.dask.dataframe import DaskDataFrame
from optimus.helpers.core import val_to_list
from optimus.helpers.functions import prepare_path, unquote_path
from optimus.helpers.logger import logger


class Load(BaseLoad):

    @staticmethod
    def df(*args, **kwargs):
        return DaskDataFrame(*args, **kwargs)

    @staticmethod
    def _csv(filepath_or_buffer, n_partitions=None, nrows=None, engine="c", na_filter=True,
             na_values=None, index_col=None, on_bad_lines='warn', *args, **kwargs):
        
        na_filter = na_filter if na_values else False

        new_dtype = None
        tries = 0

        while tries < 3:

            tries += 1

            if new_dtype is not None:
                logger.warn(f"Load failed, retrying using dtype={new_dtype}")
                if "dtype" in kwargs:
                    kwargs["dtype"].update(new_dtype)
                else:
                    kwargs.update({"dtype": new_dtype})

            if engine == "python":
                on_bad_lines = 'warn'
                df = dd.read_csv(filepath_or_buffer, keep_default_na=True,
                                 na_values=None, engine=engine, on_bad_lines=on_bad_lines, *args, **kwargs)

            elif engine == "c":
                df = dd.read_csv(filepath_or_buffer, keep_default_na=True,
                                 engine=engine, na_filter=na_filter, na_values=val_to_list(na_values),
                                 low_memory=False, on_bad_lines=on_bad_lines, *args, **kwargs)
            
            else:
                RaiseIt.value_error(engine, ["python", "c"])

            try:
                if index_col:
                    df = df.set_index(index_col)

                if nrows:
                    logger.warn(f"'load.csv' on {EnginePretty.DASK.value} loads the whole dataset and then truncates it")
                    df = df.head(n=nrows, compute=False)

                if n_partitions is not None:
                    df = df.repartition(npartitions=n_partitions)

                df = df.persist()

                # TODO use 'check' parameter
                dtypes = df.compute().dtypes
            except ValueError as e:

                if tries >= 3:
                    raise e

                # TODO retry without parsing error message
                e_str = " ".join(str(e).split("\n"))
                found = re.findall(r"dtype=({.*})", e_str)

                if len(found) > 0:
                    new_dtype = eval(found[0])
                else:
                    raise e
            else:
                break

        return df


    @staticmethod
    def _json(filepath_or_buffer, n_partitions=None, *args, **kwargs):
        df = dd.read_json(filepath_or_buffer, *args, **kwargs)

        if n_partitions is not None:
            df = df.repartition(npartitions=n_partitions)
        
        return df

    @staticmethod
    def _avro(filepath_or_buffer, n_partitions=None, nrows=None, *args, **kwargs):
        df = dask_bag.read_avro(filepath_or_buffer, *args, **kwargs).to_dataframe()
        if nrows:
            logger.warn(f"'load.avro' on {EnginePretty.DASK.value} loads the whole dataset and then truncates it")
            df = df.head(n=nrows, compute=False)
        
        if n_partitions is not None:
            df = df.repartition(npartitions=n_partitions)
        
        return df

    @staticmethod
    def _parquet(filepath_or_buffer, n_partitions=None, nrows=None, engine="pyarrow", *args, **kwargs):
        
        df = dd.read_parquet(filepath_or_buffer, engine=engine, *args, **kwargs)

        if nrows:
            logger.warn(f"'load.parquet' on {EnginePretty.DASK.value} loads the whole dataset and then truncates it")
            df = df.head(n=nrows, compute=False)

        if n_partitions is not None:
            df = df.repartition(npartitions=n_partitions)
        
        return df

    def zip(self, path, sep=',', header=True, infer_schema=True, charset="UTF-8", null_value="None", n_rows=-1,
            storage_options=None, conn=None, n_partitions=None, *args, **kwargs):

        path = unquote_path(path)

        file, file_name = prepare_path(path, "zip")

        from zipfile import ZipFile
        import dask.dataframe as dd
        import os

        wd = '/path/to/zip/files'
        file_list = os.listdir(wd)
        destdir = '/extracted/destination/'

        dfd = dd.from_pandas(pd.DataFrame())

        for f in file_list:
            with ZipFile(wd + f, "r") as zip:
                zip.extractall(destdir, None, None)
                df = dd.read_csv(zip.namelist(), usecols=['Enter', 'Columns', 'Here'], parse_dates=['Date'])
                dfd = optimus.helpers.functions_spark.append(df)

        dfd.compute()

        try:
            df = dd.read_csv(file, sep=sep, header=0 if header else None, encoding=charset, na_values=null_value,
                             compression="gzip", *args, **kwargs)

            if n_rows > -1:
                df = df.rows.limit(n_rows)

            df = DaskDataFrame(df, op=self.op)

            df.meta = Meta.set(df.meta, "file_name", file_name)
        except IOError as error:
            logger.print(error)
            raise
        return df

    def orc(self, path, columns=None, cache=None, storage_options=None, conn=None, n_partitions=None, *args, **kwargs):

        path = unquote_path(path)

        if cache is False:
            prepare_path.cache_clear()

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        try:
            # From the panda docs using na_filter
            # Detect missing value markers (empty strings and the value of na_values). In data without any NAs,
            # passing na_filter=False can improve the performance of reading a large file.
            dfd = dd.read_orc(path, columns, storage_options=storage_options, *args, **kwargs)

            df = DaskDataFrame(dfd, op=self.op)
            df.meta = Meta.set(df.meta, value={"file_name": path, "name": ntpath.basename(path)})
        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def _xml(filepath_or_buffer, nrows=None, n_partitions=1, *args, **kwargs):
        pdf = pd.read_xml(filepath_or_buffer, *args, **kwargs)
        if nrows:
            logger.warn(f"'load.xml' on {EnginePretty.PANDAS.value} loads the whole dataset and then truncates it")
            pdf = pdf[:nrows]

        df = dd.from_pandas(pdf, npartitions=n_partitions)
        return df


    @staticmethod
    def _excel(path, nrows, storage_options=None, n_partitions=1, *args, **kwargs):
        pdfs = pd.read_excel(path, nrows=nrows, storage_options=storage_options, *args, **kwargs)
        sheet_names = list(pd.read_excel(path, None, storage_options=storage_options).keys())
        pdf = pd.concat(val_to_list(pdfs), axis=0).reset_index(drop=True)
        df = dd.from_pandas(pdf, npartitions=n_partitions)

        return df, sheet_names

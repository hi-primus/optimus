import ntpath

import dask.bag as dask_bag
import pandas as pd
from dask import dataframe as dd

import optimus.helpers.functions_spark
from optimus.engines.base.basedataframe import BaseDataFrame
from optimus.engines.base.io.load import BaseLoad
from optimus.engines.base.meta import Meta
from optimus.engines.dask.dataframe import DaskDataFrame
from optimus.helpers.core import val_to_list
from optimus.helpers.functions import prepare_path, unquote_path
from optimus.helpers.logger import logger


class Load(BaseLoad):

    def json(self, path, multiline=False, lines=True, conn=None, storage_options=None, n_partitions=None, *args, **kwargs):

        path = unquote_path(path)

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        try:
            # TODO: Check a better way to handle this Spark.instance.spark. Very verbose.
            dfd = dd.read_json(path, lines=lines, storage_options=storage_options, *args, **kwargs)
            df = DaskDataFrame(dfd, op=self.op)
            df.meta = Meta.set(df.meta, value={"file_name": path, "name": ntpath.basename(path)})

        except IOError as error:
            logger.print(error)
            raise
        return df

    def tsv(self, path, header1=True, infer_schema=True, *args):
        return self.csv(path, sep='\t', header=header1, infer_schema=infer_schema, *args)

    def csv(self, filepath_or_buffer, sep=',', header=True, infer_schema=True, na_values=None, encoding="utf-8", n_rows=-1, cache=False,
            quoting=0, lineterminator=None, error_bad_lines=False, engine="c", keep_default_na=False,
            na_filter=True, null_value=None, storage_options=None, conn=None, n_partitions=None, *args, **kwargs):

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
            # From the panda docs using na_filter
            # Detect missing value markers (empty strings and the value of na_values). In data without any NAs,
            # passing na_filter=False can improve the performance of reading a large file.

            na_filter = na_filter if null_value else False

            if engine == "python":
                # na_filter=na_filter, error_bad_lines and low_memory are not support by pandas engine
                dfd = dd.read_csv(filepath_or_buffer, sep=sep, header=0 if header else None, encoding=encoding,
                                  quoting=quoting, lineterminator=lineterminator, keep_default_na=True,
                                  na_values=None, engine=engine, storage_options=storage_options,
                                  error_bad_lines=False, *args, **kwargs)

            elif engine == "c":
                dfd = dd.read_csv(filepath_or_buffer, sep=sep, header=0 if header else None, encoding=encoding,
                                  quoting=quoting, lineterminator=lineterminator,
                                  error_bad_lines=error_bad_lines, keep_default_na=True, 
                                  engine=engine, na_filter=na_filter, na_values=val_to_list(null_value),
                                  storage_options=storage_options, low_memory=False, *args, **kwargs)

            # error = True

            # if error:
            #     for col in dfd.columns:
            #         dfd[col] = dd.from_array(dfd[col].values)

            if n_rows > -1:
                dfd = dfd.head(n=n_rows, compute=False)

            if n_partitions is not None:
                dfd = dfd.repartition(npartitions=n_partitions)

            dfd = dfd.persist()

            df = DaskDataFrame(dfd, op=self.op)
            df.meta = Meta.set(df.meta, value={"file_name": filepath_or_buffer, "name": ntpath.basename(filepath_or_buffer)})

            try:
                max_cell_length = df.cols.len("*").cols.max()
                df.meta = Meta.set(df.meta, value={"max_cell_length": max_cell_length})
            except Exception as error:
                pass
                                                
        except IOError as error:
            logger.print(error)
            raise

        return df

    def parquet(self, path, columns=None, engine="pyarrow", storage_options=None, conn=None, n_partitions=None, *args, **kwargs):

        path = unquote_path(path)

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        try:
            dfd = dd.read_parquet(path, columns=columns, engine=engine, storage_options=storage_options, *args,
                                  **kwargs)
            df = DaskDataFrame(dfd, op=self.op)
            df.meta = Meta.set(df.meta, "file_name", path)

        except IOError as error:
            logger.print(error)
            raise

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

    def avro(self, path, storage_options=None, conn=None, n_partitions=None, *args, **kwargs):
        path = unquote_path(path)

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        file, file_name = prepare_path(path, "avro")

        try:
            df = dask_bag.read_avro(path, storage_options=storage_options, *args, **kwargs).to_dataframe()
            df = DaskDataFrame(df, op=self.op)
            df.meta = Meta.set(df.meta, "file_name", file_name)

        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def excel(path, sheet_name=0, merge_sheets=False, skiprows=1, n_rows=-1, storage_options=None,
              conn=None, n_partitions=None, *args, **kwargs):

        path = unquote_path(path)

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        file, file_name = prepare_path(path)
        header = None
        if merge_sheets is True:
            skiprows = -1
        else:
            header = 0
            skiprows = 0

        if n_rows == -1:
            n_rows = None

        pdfs = pd.read_excel(file, sheet_name=sheet_name, header=header, skiprows=skiprows, nrows=n_rows,
                             storage_options=storage_options, n_partitions=None, *args, **kwargs)
        sheet_names = list(pd.read_excel(file, None, storage_options=storage_options).keys())

        pdf = pd.concat(val_to_list(pdfs), axis=0).reset_index(drop=True)

        df = dd.from_pandas(pdf, npartitions=1)
        df.meta = Meta.set(df.meta, "file_name", ntpath.basename(file_name))
        df.meta = Meta.set(df.meta, "sheet_names", sheet_names)

        return df

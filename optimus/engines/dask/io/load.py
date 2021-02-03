import ntpath

import dask.bag as dask_bag
import pandas as pd
from dask import dataframe as dd

import optimus.helpers.functions_spark
from optimus.engines.base.io.load import BaseLoad
from optimus.engines.base.meta import Meta
from optimus.engines.dask.dataframe import DaskDataFrame
from optimus.helpers.core import val_to_list
from optimus.helpers.functions import prepare_path
from optimus.helpers.logger import logger


class Load(BaseLoad):

    @staticmethod
    def json(path, multiline=False, *args, **kwargs):
        """
        Return a dask dataframe from a json file.
        :param path: path or location of the file.
        :param multiline:

        :return:
        """
        file, file_name = prepare_path(path, "json")[0]

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        try:
            # TODO: Check a better way to handle this Spark.instance.spark. Very verbose.
            df = dd.read_json(path, lines=multiline, storage_options=storage_options, *args, **kwargs)
            df.meta = Meta.set(df.meta, "file_name", file_name)

        except IOError as error:
            logger.print(error)
            raise
        return df

    @staticmethod
    def tsv(path, header=True, infer_schema=True, *args, **kwargs):
        """
        Return a dataframe from a tsv file.
        :param path: path or location of the file.
        :param header: tell the function whether dataset has a header row. True default.
        :param infer_schema: infers the input schema automatically from data.
        It requires one extra pass over the data. True default.

        :return:
        """

        return Load.csv(path, sep='\t', header=header, infer_schema=infer_schema, *args, **kwargs)

    @staticmethod
    def csv(path, sep=',', header=True, infer_schema=True, na_values=None, encoding="utf-8", n_rows=-1, cache=False,
            quoting=0, lineterminator=None, error_bad_lines=False, engine="c", keep_default_na=False,
            na_filter=False, null_value=None, storage_options=None, conn=None, *args, **kwargs):

        """
        Return a dataframe from a csv file. It is the same read.csv Spark function with some predefined
        params

        :param na_filter:
        :param path: path or location of the file.
        :param sep: usually delimiter mark are ',' or ';'.
        :param header: tell the function whether dataset has a header row. True default.
        :param infer_schema: infers the input schema automatically from data.
        :param encoding:
        :param na_values:
        :param n_rows:
        :param quoting:
        :param engine: 'python' or 'c'. 'python' slower but support better error handling
        :param lineterminator:
        :param error_bad_lines:
        :param keep_default_na:
        :param cache: If calling from a url we cache save the path to the temp file so we do not need to download the file again

        """

        if cache is False:
            prepare_path.cache_clear()

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        try:
            # From the panda docs using na_filter
            # Detect missing value markers (empty strings and the value of na_values). In data without any NAs,
            # passing na_filter=False can improve the performance of reading a large file.
            ddf = dd.read_csv(path, sep=sep, header=0 if header else None, encoding=encoding,
                              quoting=quoting, lineterminator=lineterminator, error_bad_lines=error_bad_lines,
                              keep_default_na=True, na_values=None, engine=engine, na_filter=na_filter,
                              storage_options=storage_options,*args, **kwargs)

            if n_rows > -1:
                ddf = dd.from_pandas(ddf.head(n=n_rows), npartitions=1).reset_index()

            df = DaskDataFrame(ddf)
            df.meta = Meta.set(df.meta, value={"file_name": path, "name": ntpath.basename(path)})
        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def parquet(path, columns=None, engine="pyarrow", storage_options=None, conn=None, *args, **kwargs):
        """
        Return a dataframe from a parquet file.
        :param path: path or location of the file. Must be string dataType
        :param columns: select the columns that will be loaded. In this way you do not need to load all the dataframe
        :param engine:
        :param args: custom argument to be passed to the spark parquet function
        :param kwargs: custom keyword arguments to be passed to the spark parquet function
        :return: Spark Dataframe
        """

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        try:
            df = dd.read_parquet(path, columns=columns, engine=engine, storage_options=storage_options, *args, **kwargs)
            df.meta = Meta.set(df.meta, "file_name", path)

        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def zip(path, sep=',', header=True, infer_schema=True, charset="UTF-8", null_value="None", n_rows=-1,
            storage_options=None, conn=None, *args, **kwargs):
        file, file_name = prepare_path(path, "zip")

        from zipfile import ZipFile
        import dask.dataframe as dd
        import os

        wd = '/path/to/zip/files'
        file_list = os.listdir(wd)
        destdir = '/extracted/destination/'

        ddf = dd.from_pandas(pd.DataFrame())

        for f in file_list:
            with ZipFile(wd + f, "r") as zip:
                zip.extractall(destdir, None, None)
                df = dd.read_csv(zip.namelist(), usecols=['Enter', 'Columns', 'Here'], parse_dates=['Date'])
                ddf = optimus.helpers.functions_spark.append(df)

        ddf.compute()

        try:
            df = dd.read_csv(file, sep=sep, header=0 if header else None, encoding=charset, na_values=null_value,
                             compression="gzip", *args, **kwargs)

            if n_rows > -1:
                df = df.head(n_rows)

            df.meta = Meta.set(df.meta, "file_name", file_name)
        except IOError as error:
            logger.print(error)
            raise
        return df

    @staticmethod
    def orc(path, columns=None, cache=None, storage_options=None, conn=None, *args, **kwargs):

        """
        Optimized Row Columnar
        Return a dataframe from a .orc file.
        params


        """

        if cache is False:
            prepare_path.cache_clear()

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        try:
            # From the panda docs using na_filter
            # Detect missing value markers (empty strings and the value of na_values). In data without any NAs,
            # passing na_filter=False can improve the performance of reading a large file.
            ddf = dd.read_orc(path, columns, storage_options=storage_options, *args, **kwargs)

            df = DaskDataFrame(ddf)
            df.meta = Meta.set(df.meta, value={"file_name": path, "name": ntpath.basename(path)})
        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def avro(path, storage_options=None, conn=None, *args, **kwargs):
        """
        Return a dataframe from a avro file.
        :param path: path or location of the file. Must be string dataType
        :param storage_options:
        :param args: custom argument to be passed to the avro function
        :param kwargs: custom keyword arguments to be passed to the avro function
        :return: Spark Dataframe
        """
        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        file, file_name = prepare_path(path, "avro")

        try:
            df = dask_bag.read_avro(path, storage_options=storage_options, *args, **kwargs).to_dataframe()
            df.meta = Meta.set(df.meta, "file_name", file_name)

        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def excel(path, sheet_name=0, merge_sheets=False, skiprows=1, n_rows=-1, n_partitions=1, storage_options=None,
            conn=None, *args, **kwargs):
        """
        Return a dataframe from a excel file.
        :param path: Path or location of the file. Must be string dataType
        :param sheet_name: excel sheet name
        :param merge_sheets:
        :param args: custom argument to be passed to the excel function
        :param kwargs: custom keyword arguments to be passed to the excel function

        """
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
                            storage_options=storage_options, *args, **kwargs)
        sheet_names = list(pd.read_excel(file, None, storage_options=storage_options).keys())

        pdf = pd.concat(val_to_list(pdfs), axis=0).reset_index(drop=True)

        df = dd.from_pandas(pdf, npartitions=n_partitions)
        df.meta = Meta.set(df.meta, "file_name", ntpath.basename(file_name))
        df.meta = Meta.set(df.meta, "sheet_names", sheet_names)

        return df

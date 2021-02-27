import ntpath

import dask.bag as db
import pandas as pd
from dask import dataframe as dd

from optimus.engines.base.io.load import BaseLoad
from optimus.engines.base.meta import Meta
from optimus.engines.dask_cudf.dataframe import DaskCUDFDataFrame
from optimus.helpers.functions import prepare_path, unquote_path
from optimus.helpers.logger import logger


class Load(BaseLoad):
    
    def __init__(self, op):
        self.op = op

    @staticmethod
    def json(path, multiline=False, storage_options=None, conn=None, *args, **kwargs):
        """
        Return a dask dataframe from a json file.
        :param path: path or location of the file.
        :param multiline:

        :return:
        """
        
        path = unquote_path(path)
        
        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options
        
        file, file_name = prepare_path(path, "json")[0]

        try:
            import dask_cudf
            df = dask_cudf.read_json(path, lines=multiline, storage_options=storage_options, *args, **kwargs)
            df = DaskCUDFDataFrame(df)
            df.meta = Meta.set(df.meta, "file_name", file_name)

        except IOError as error:
            logger.print(error)
            raise
        return df

    @staticmethod
    def tsv(path, header=0, infer_schema='true', *args, **kwargs):
        """
        Return a spark from a tsv file.
        :param path: path or location of the file.
        :param header: tell the function whether dataset has a header row. 'true' default.
        :param infer_schema: infers the input schema automatically from data.
        It requires one extra pass over the data. 'true' default.

        :return:
        """

        return Load.csv(path, sep='\t', header=header, infer_schema=infer_schema, *args, **kwargs)

    @staticmethod
    def csv(path, sep=',', header=True, infer_schema=True, encoding="utf-8", null_value="None", n_rows=-1, cache=False,
            quoting=0, lineterminator=None, error_bad_lines=False, keep_default_na=True, na_filter=True,
            storage_options=None, conn=None, *args,**kwargs):
        """
        Return a dataframe from a csv file. It is the same read.csv Spark function with some predefined
        params

        :param path: path or location of the file.
        :param sep: usually delimiter mark are ',' or ';'.
        :param keep_default_na:
        :param error_bad_lines:
        :param lineterminator:
        :param header: tell the function whether dataset has a header row. 'true' default.
        :param infer_schema: infers the input schema automatically from data.
        :param quoting:
        :param null_value:
        :param na_filter:
        :param encoding:
        It requires one extra pass over the data. 'true' default.

        :return dataFrame
        """

        path = unquote_path(path)

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        try:
            import dask_cudf
            dcdf = dask_cudf.read_csv(path, sep=sep, header=0 if header else None, encoding=encoding,
                                      quoting=quoting, error_bad_lines=error_bad_lines,
                                      keep_default_na=keep_default_na, na_values=null_value, na_filter=na_filter,
                                      storage_options=storage_options)
            
            if n_rows > -1:
                dcdf = dask_cudf.from_cudf(dcdf.head(n=n_rows), npartitions=1).reset_index(drop=True)

            df = DaskCUDFDataFrame(dcdf)
            df.meta = Meta.set(df.meta, "file_name", path)
            df.meta = Meta.set(df.meta, "name", ntpath.basename(path))
        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def parquet(path, columns=None, storage_options=None, conn=None, *args, **kwargs):
        """
        Return a spark from a parquet file.
        :param path: path or location of the file. Must be string dataType
        :param columns: select the columns that will be loaded. In this way you do not need to load all the dataframe
        :param args: custom argument to be passed to the spark parquet function
        :param kwargs: custom keyword arguments to be passed to the spark parquet function
        :return: Spark Dataframe
        """
        
        path = unquote_path(path)
        
        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        file, file_name = prepare_path(path, "parquet")

        try:
            import dask_cudf
            df = dask_cudf.read_parquet(path, columns=columns, storage_options=storage_options, *args, **kwargs)

            df.meta = Meta.set(df.meta, "file_name", file_name)

        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def avro(path, storage_options=None, conn=None, *args, **kwargs):
        """
        Return a spark from a avro file.
        :param path: path or location of the file. Must be string dataType
        :param args: custom argument to be passed to the spark avro function
        :param kwargs: custom keyword arguments to be passed to the spark avro function
        :return: Spark Dataframe
        """
        
        path = unquote_path(path)
        
        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        file, file_name = prepare_path(path, "avro")

        try:
            df = db.read_avro(path, storage_options=storage_options, *args, **kwargs).to_dataframe()
            df.meta = Meta.set(df.meta, "file_name", file_name)

        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def excel(path, sheet_name=0, storage_options=None, conn=None, *args, **kwargs):
        """
        Return a spark from a excel file.
        :param path: Path or location of the file. Must be string dataType
        :param sheet_name: excel sheet name
        :param args: custom argument to be passed to the excel function
        :param kwargs: custom keyword arguments to be passed to the excel function
        :return: Spark Dataframe
        """
        
        path = unquote_path(path)
        
        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        file, file_name = prepare_path(path, "xls")

        try:
            pdf = pd.read_excel(file, sheet_name=sheet_name, storage_options=storage_options, *args, **kwargs)

            # Parse object column data type to string to ensure that Spark can handle it. With this we try to reduce
            # exception when Spark try to infer the column data type
            col_names = list(pdf.select_dtypes(include=['object']))

            column_dtype = {}
            for col in col_names:
                column_dtype[col] = str

            # Convert object columns to string
            pdf = pdf.astype(column_dtype)

            # Create spark data frame
            df = dd.from_pandas(pdf, npartitions=3)
            df.meta = Meta.set(df.meta, "file_name", ntpath.basename(file_name))
        except IOError as error:
            logger.print(error)
            raise

        return df

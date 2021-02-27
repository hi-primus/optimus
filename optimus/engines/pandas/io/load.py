import ntpath

import pandas as pd
import pandavro as pdx

from optimus.engines.base.io.load import BaseLoad
from optimus.engines.base.meta import Meta
from optimus.engines.pandas.dataframe import PandasDataFrame
from optimus.helpers.functions import prepare_path, unquote_path
from optimus.helpers.logger import logger
from optimus.infer import is_str


class Load(BaseLoad):
    
    def __init__(self, op):
        self.op = op

    @staticmethod
    def json(path, multiline=False, *args, **kwargs):
        """
        Return a dataframe from a json file.
        :param path: path or location of the file.
        :param multiline:

        :return:
        """
        
        path = unquote_path(path)
        
        local_file_names = prepare_path(path, "json")
        try:
            df_list = []

            for file_name, j in local_file_names:
                df = pd.read_json(file_name, lines=multiline, *args, **kwargs)
                df_list.append(df)

            df = pd.concat(df_list, axis=0, ignore_index=True)
            df = PandasDataFrame(df)
            df.meta = Meta.set(df.meta, "file_name", local_file_names[0])

        except IOError as error:
            logger.print(error)
            raise
        return df

    @staticmethod
    def tsv(path, header=True, infer_schema=True, *args, **kwargs):
        """
        Return a spark from a tsv file.
        :param path: path or location of the file.
        :param header: tell the function whether dataset has a header row. True default.
        :param infer_schema: infers the input schema automatically from data.
        It requires one extra pass over the data. True default.

        :return:
        """

        return Load.csv(path, sep='\t', header=header, infer_schema=infer_schema, *args, **kwargs)

    @staticmethod
    def csv(filepath_or_buffer, sep=",", header=True, infer_schema=True, encoding="UTF-8", n_rows=None,
            null_value="None", quoting=3,
            lineterminator="\n", error_bad_lines=False, cache=False, na_filter=False, storage_options=None, conn=None,
            *args, **kwargs):
        """
        Return a dataframe from a csv file. It is the same read.csv Spark function with some predefined
        params


        :param filepath_or_buffer: path or location of the file.
        :param sep: usually delimiter mark are ',' or ';'.
        :param header: tell the function whether dataset has a header row. True default.
        :param infer_schema: infers the input schema automatically from data.
        :param n_rows:
        :param null_value:
        :param charset:
        :param na_filter:
        :param lineterminator:
        :param error_bad_lines:
        :param conn:
        It requires one extra pass over the data. True default.

        :return dataFrame
        """
        
        path = unquote_path(path)
        
        if is_str(filepath_or_buffer):
            _meta = {"file_name": filepath_or_buffer, "name": ntpath.basename(filepath_or_buffer)}

        try:

            # Pandas do not support \r\n terminator.
            if lineterminator.encode(encoding='UTF-8', errors='strict') == b'\r\n':
                lineterminator = None
            if conn is not None:
                filepath_or_buffer = conn.path(filepath_or_buffer)
                storage_options = conn.storage_options
            else:
                storage_options = None

            df = pd.read_csv(filepath_or_buffer, sep=sep, header=0 if header else -1, encoding=encoding, nrows=n_rows,
                             quoting=quoting, lineterminator=lineterminator, error_bad_lines=error_bad_lines,
                             na_filter=na_filter, index_col=False, storage_options=storage_options, *args, **kwargs)

            df = PandasDataFrame(df)

            df.meta = Meta.set(df.meta, value=_meta)

        except IOError as error:
            print(error)
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
        """
        
        path = unquote_path(path)
        
        # file, file_name = prepare_path(path, "parquet")[0]

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        try:
            df = pd.read_parquet(path, columns=columns, engine='pyarrow', storage_options=storage_options, **kwargs)
            df = PandasDataFrame(df)
            df.meta = Meta.set(df.meta, value={"file_name": path, "name": ntpath.basename(path)})

        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def avro(path, storage_options=None, conn=None, *args, **kwargs):
        """
        Return a spark from a avro file.
        :param storage_options:
        :param path: path or location of the file. Must be string dataType
        :param args: custom argument to be passed to the spark avro function
        :param kwargs: custom keyword arguments to be passed to the spark avro function
        """
        
        path = unquote_path(path)
        
        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        file, file_name = prepare_path(path, "avro")[0]

        try:
            df = pdx.read_avro(file_name, storage_options=storage_options, *args, **kwargs)
            df = PandasDataFrame(df)
            df.meta = Meta.set(df.meta, value={"file_name": path, "name": ntpath.basename(path)})

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
        """
        
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
            # df = pd.from_pandas(pdf, npartitions=3)
            df = PandasDataFrame(df)
            df.meta = Meta.set(df.meta, "file_name", ntpath.basename(file_name))
        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def orc(path, columns, storage_options=None, conn=None, *args, **kwargs):
        """
        Return a dataframe from a avro file.
        :param path: path or location of the file. Must be string dataType
        :param args: custom argument to be passed to the spark avro function
        :param kwargs: custom keyword arguments to be passed to the spark avro function
        """
        
        path = unquote_path(path)
        
        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options
        
        file, file_name = prepare_path(path, "orc")[0]

        try:
            df = pdx.read_orc(file_name, columns, storage_options=storage_options)
            df = PandasDataFrame(df)
            df.meta = Meta.set(df.meta, "file_name", file_name)

        except IOError as error:
            logger.print(error)
            raise

        return df

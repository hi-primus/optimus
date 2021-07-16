import ntpath

import cudf

from optimus.engines.base.io.load import BaseLoad
from optimus.engines.base.meta import Meta
from optimus.engines.cudf.dataframe import CUDFDataFrame
from optimus.helpers.functions import prepare_path, unquote_path
from optimus.helpers.logger import logger


class Load(BaseLoad):

    def __init__(self, op):
        self.op = op

    @staticmethod
    def json(path, multiline=False, n_rows=-1, *args, **kwargs):
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
                df = cudf.read_json(file_name, lines=multiline, nrows=n_rows, *args, **kwargs)
                df_list.append(df)

            df = cudf.concat(df_list, axis=0, ignore_index=True)
            df = CUDFDataFrame(df)
            df.meta = Meta.set(df.meta, "file_name", local_file_names[0])

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
    def csv(path, sep=',', header=True, infer_schema=True, encoding="utf-8", null_value="None", n_rows=-1, cache=False,
            quoting=0, lineterminator=None, error_bad_lines=False, keep_default_na=False, na_filter=True, data_type=None,
            *args, **kwargs):

        """
        Return a dataframe from a csv file.
        params

        :param data_type:
        :param cache:
        :param na_filter:
        :param path: path or location of the file.
        :param sep: usually delimiter mark are ',' or ';'.
        :param keep_default_na:
        :param error_bad_lines:
        :param quoting:
        :param lineterminator:
        :param header: tell the function whether dataset has a header row. True default.
        :param infer_schema: infers the input schema automatically from data.
        :param null_value:
        :param n_rows:
        :param encoding:
        It requires one extra pass over the data. True default.

        :return dataFrame
        """

        path = unquote_path(path)

        # file, file_name = prepare_path(path, "csv")[0]

        try:
            # TODO:  lineterminator=lineterminator seems to be broken
            if header is True:
                header = 0
            elif header is False:
                header = None
            # The str to ["str] is due to a bug in cudf https://github.com/rapidsai/cudf/issues/6606
            if data_type == str or data_type is None:
                data_type = ["str"]

            cdf = cudf.read_csv(path, sep=sep, header=header, encoding=encoding,
                                quoting=quoting, error_bad_lines=error_bad_lines,
                                keep_default_na=keep_default_na, na_values=null_value, nrows=n_rows,
                                na_filter=na_filter, dtype=data_type, *args, **kwargs)
            df = CUDFDataFrame(cdf)
            df.meta = Meta.set(df.meta, None,
                               {"file_name": path, "max_cell_length": df.cols.len("*").cols.max()})


        except IOError as error:
            logger.print(error)
            raise
        return df

    @staticmethod
    def parquet(path, columns=None, *args, **kwargs):
        """
        Return a dataframe from a parquet file.
        :param path: path or location of the file. Must be string dataType
        :param columns: select the columns that will be loaded. In this way you do not need to load all the dataframe
        :param args: custom argument to be passed to the parquet function
        :param kwargs: custom keyword arguments to be passed to the parquet function
        """

        path = unquote_path(path)

        try:
            df = cudf.read_parquet(path, columns=columns, engine='pyarrow', *args, **kwargs)
            df = CUDFDataFrame(df)
            df.meta = Meta.set(df.meta, "file_name", path)

        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def avro(path, storage_options=None, conn=None, *args, **kwargs):
        """
        :param path: path or location of the file. Must be string dataType
        :param storage_options:
        :param args: custom argument to be passed to the avro function
        :param kwargs: custom keyword arguments to be passed to the avro function
        """

        path = unquote_path(path)

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        file, file_name = prepare_path(path, "avro")[0]

        try:
            df = cudf.read_avro(path, storage_options=storage_options, *args, **kwargs)
            df = CUDFDataFrame(df)
            df.meta = Meta.set(df.meta, "file_name", file_name)

        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def orc(path, columns=None, storage_options=None, conn=None, *args, **kwargs):
        """
        Return a dataframe from a avro file.
        :param path: path or location of the file. Must be string dataType
        :param columns: Subset of columns to be loaded
        :param args: custom argument to be passed to the avro function
        :param kwargs: custom keyword arguments to be passed to the avro function
        """

        path = unquote_path(path)

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        file, file_name = prepare_path(path, "orc")[0]

        try:
            df = cudf.read_orc(path, columns, storage_options=storage_options, *args, **kwargs)
            df = CUDFDataFrame(df)
            df.meta = Meta.set(df.meta, "file_name", file_name)

        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def excel(path, sheet_name=0, storage_options=None, conn=None, *args, **kwargs):
        """
        Return a dataframe from a excel file.
        :param path: Path or location of the file. Must be string dataType
        :param sheet_name: excel sheet name
        :param args: custom argument to be passed to the excel function
        :param kwargs: custom keyword arguments to be passed to the excel function
        """

        path = unquote_path(path)

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        file, file_name = prepare_path(path, "xls")

        try:
            pdf = cudf.read_excel(file, sheet_name=sheet_name, storage_options=storage_options, *args, **kwargs)

            # exception when Spark try to infer the column data type
            # Parse object column data type to string to ensure that Spark can handle it. With this we try to reduce
            col_names = list(pdf.select_dtypes(include=['object']))

            column_dtype = {}
            for col in col_names:
                column_dtype[col] = str

            # Convert object columns to string
            pdf = pdf.astype(column_dtype)

            df = cudf.from_pandas(pdf, npartitions=3)
            df.meta = Meta.set(df.meta, "file_name", ntpath.basename(file_name))
        except IOError as error:
            logger.print(error)
            raise

        return df

import ntpath

import cudf

from optimus.engines.base.io.load import BaseLoad
from optimus.engines.base.meta import Meta
from optimus.engines.cudf.dataframe import CUDFDataFrame
from optimus.helpers.functions import prepare_path, unquote_path
from optimus.helpers.logger import logger


class Load(BaseLoad):

    def json(self, path, multiline=False, n_rows=-1, *args, **kwargs):

        path = unquote_path(path)

        local_file_names = prepare_path(path, "json")
        try:
            df_list = []

            for file_name, j in local_file_names:
                df = cudf.read_json(file_name, lines=multiline, nrows=n_rows, *args, **kwargs)
                df_list.append(df)

            df = cudf.concat(df_list, axis=0, ignore_index=True)
            df = CUDFDataFrame(df, op=self.op)
            df.meta = Meta.set(df.meta, "file_name", local_file_names[0])

        except IOError as error:
            logger.print(error)
            raise
        return df

    def csv(self, filepath_or_buffer, sep=',', header=True, infer_schema=True, encoding="utf-8", null_value="None",
            n_rows=-1, cache=False,
            quoting=0, lineterminator=None, on_bad_lines='warn', keep_default_na=False, na_filter=True, data_type=None,
            *args, **kwargs):

        filepath_or_buffer = unquote_path(filepath_or_buffer)

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

            cdf = cudf.read_csv(filepath_or_buffer, sep=sep, header=header, encoding=encoding,
                                quoting=quoting, on_bad_lines=on_bad_lines,
                                keep_default_na=keep_default_na, na_values=null_value, nrows=n_rows,
                                na_filter=na_filter, dtype=data_type, *args, **kwargs)
            df = CUDFDataFrame(cdf, op=self.op)
            df.meta = Meta.set(df.meta, None,
                               {"file_name": filepath_or_buffer, "max_cell_length": df.cols.len("*").cols.max()})


        except IOError as error:
            logger.print(error)
            raise
        return df

    def parquet(self, path, columns=None, *args, **kwargs):

        path = unquote_path(path)

        try:
            df = cudf.read_parquet(path, columns=columns, engine='pyarrow', *args, **kwargs)
            df = CUDFDataFrame(df, op=self.op)
            df.meta = Meta.set(df.meta, "file_name", path)

        except IOError as error:
            logger.print(error)
            raise

        return df

    def avro(self, path, storage_options=None, conn=None, *args, **kwargs):

        path = unquote_path(path)

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        file, file_name = prepare_path(path, "avro")[0]

        try:
            df = cudf.read_avro(path, storage_options=storage_options, *args, **kwargs)
            df = CUDFDataFrame(df, op=self.op)
            df.meta = Meta.set(df.meta, "file_name", file_name)

        except IOError as error:
            logger.print(error)
            raise

        return df

    def orc(self, path, columns=None, storage_options=None, conn=None, *args, **kwargs):

        path = unquote_path(path)

        if conn is not None:
            path = conn.path(path)
            storage_options = conn.storage_options

        file, file_name = prepare_path(path, "orc")[0]

        try:
            df = cudf.read_orc(path, columns, storage_options=storage_options, *args, **kwargs)
            df = CUDFDataFrame(df, op=self.op)
            df.meta = Meta.set(df.meta, "file_name", file_name)

        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def excel(path, sheet_name=0, storage_options=None, conn=None, *args, **kwargs):

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

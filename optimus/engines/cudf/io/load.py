import ntpath

import cudf

from optimus.engines.base.io.load import BaseLoad
from optimus.helpers.functions import prepare_path
from optimus.helpers.logger import logger


class Load(BaseLoad):

    @staticmethod
    def json(path, multiline=False, *args, **kwargs):
        """
        Return a dataframe from a json file.
        :param path: path or location of the file.
        :param multiline:

        :return:
        """

        local_file_names = prepare_path(path, "json")
        try:
            df_list = []

            for file_name, j in local_file_names:
                df = cudf.read_json(file_name, lines=multiline, *args, **kwargs)
                df_list.append(df)

            df = cudf.concat(df_list, axis=0, ignore_index=True)
            df.meta.set("file_name", local_file_names[0])

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
    def csv(path, sep=',', header=True, infer_schema=True, encoding="utf-8", null_value="None", n_rows=-1, cache=False,
            quoting=0, lineterminator=None, error_bad_lines=False, keep_default_na=False, *args, **kwargs):

        """
        Return a dataframe from a csv file. It is the same read.csv Spark function with some predefined
        params

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
        # file, file_name = prepare_path(path, "csv")[0]

        try:
            # TODO:  lineterminator=lineterminator seems to be broken
            if header is True:
                header = 0
            elif header is False:
                header = None
            # else is_float(header):
            #     header = header

            df = cudf.read_csv(path, sep=sep, header=header, encoding=encoding,
                               quoting=quoting, error_bad_lines=error_bad_lines,
                               keep_default_na=keep_default_na, na_values=null_value, nrows=n_rows)
            df.ext.reset()
            df.meta.set("file_name", path)
        except IOError as error:
            logger.print(error)
            raise
        return df

    @staticmethod
    def parquet(path, columns=None, *args, **kwargs):
        """
        Return a spark from a parquet file.
        :param path: path or location of the file. Must be string dataType
        :param columns: select the columns that will be loaded. In this way you do not need to load all the dataframe
        :param args: custom argument to be passed to the spark parquet function
        :param kwargs: custom keyword arguments to be passed to the spark parquet function
        :return: Spark Dataframe
        """

        try:
            df = cudf.read_parquet(path, columns=columns, engine='pyarrow', *args, **kwargs)

            df.meta.set("file_name", path)

        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def avro(path, *args, **kwargs):
        """
        Return a spark from a avro file.
        :param path: path or location of the file. Must be string dataType
        :param args: custom argument to be passed to the spark avro function
        :param kwargs: custom keyword arguments to be passed to the spark avro function
        :return: Spark Dataframe
        """
        file, file_name = prepare_path(path, "avro")

        try:
            df = cudf.read_avro(path, *args, **kwargs).to_dataframe()
            df.meta.set("file_name", file_name)

        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def excel(path, sheet_name=0, *args, **kwargs):
        """
        Return a spark from a excel file.
        :param path: Path or location of the file. Must be string dataType
        :param sheet_name: excel sheet name
        :param args: custom argument to be passed to the excel function
        :param kwargs: custom keyword arguments to be passed to the excel function
        :return: Spark Dataframe
        """
        file, file_name = prepare_path(path, "xls")

        try:
            pdf = cudf.read_excel(file, sheet_name=sheet_name, *args, **kwargs)

            # Parse object column data type to string to ensure that Spark can handle it. With this we try to reduce
            # exception when Spark try to infer the column data type
            col_names = list(pdf.select_dtypes(include=['object']))

            column_dtype = {}
            for col in col_names:
                column_dtype[col] = str

            # Convert object columns to string
            pdf = pdf.astype(column_dtype)

            # Create spark data frame
            df = cudf.from_pandas(pdf, npartitions=3)
            df.meta.set("file_name", ntpath.basename(file_name))
        except IOError as error:
            logger.print(error)
            raise

        return df

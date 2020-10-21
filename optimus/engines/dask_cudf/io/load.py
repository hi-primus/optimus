import csv
import ntpath
import magic
import os
import dask.bag as db
import dask_cudf
import pandas as pd
from dask import dataframe as dd

from optimus.helpers.functions import prepare_path
from optimus.helpers.logger import logger
from optimus.helpers.raiseit import RaiseIt

XML_THRESHOLD = 10
JSON_THRESHOLD = 20
BYTES_SIZE = 16384


class Load:

    @staticmethod
    def json(path, multiline=False, *args, **kwargs):
        """
        Return a dask dataframe from a json file.
        :param path: path or location of the file.
        :param multiline:

        :return:
        """
        file, file_name = prepare_path(path, "json")[0]

        try:
            # TODO: Check a better way to handle this Spark.instance.spark. Very verbose.
            df = dd.read_json(path, lines=multiline, *args, **kwargs)
            df.meta.set("file_name", file_name)

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
    def csv(path, sep=',', header=True, infer_schema='true', encoding="UTF-8", null_value="None", *args, **kwargs):
        """
        Return a dataframe from a csv file. It is the same read.csv Spark function with some predefined
        params

        :param path: path or location of the file.
        :param sep: usually delimiter mark are ',' or ';'.
        :param header: tell the function whether dataset has a header row. 'true' default.
        :param infer_schema: infers the input schema automatically from data.
        :param null_value:
        :param charset:
        It requires one extra pass over the data. 'true' default.

        :return dataFrame
        """

        file, file_name = prepare_path(path, "csv")[0]

        try:
            # print(charset)
            df = dask_cudf.read_csv(path, sep=sep, header=0 if header else None, encoding=encoding, na_values=null_value,
                                    *args, **kwargs)
            df.meta.set("file_name", file_name)
        except IOError as error:
            logger.print(error)
            raise
        df.ext.reset()
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

        file, file_name = prepare_path(path, "parquet")

        try:
            df = dask_cudf.read_parquet(path, columns=columns, *args, **kwargs)
            df.meta.set("file_name", file_name)

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
            df = db.read_avro(path, *args, **kwargs).to_dataframe()
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
            pdf = pd.read_excel(file, sheet_name=sheet_name, *args, **kwargs)

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
            df.meta.set("file_name", ntpath.basename(file_name))
        except IOError as error:
            logger.print(error)
            raise

        return df

    @staticmethod
    def file(path, *args, **kwargs):

        full_path, file_name = prepare_path(path)[0]

        file_ext = os.path.splitext(file_name)[1].replace(".", "")

        mime, encoding = magic.Magic(mime=True, mime_encoding=True).from_file(full_path).split(";")
        mime_info = {"mime": mime, "encoding": encoding.strip().split("=")[1], "file_ext": file_ext}
        print(mime)
        if mime == "text/plain" or "application/csv":

            # In some case magic get a "unknown-8bit" which can not be use to decode the file use latin-1 instead
            if mime_info["encoding"] == "unknown-8bit":
                mime_info["encoding"] = "latin-1"

            file = open(full_path, encoding=mime_info["encoding"]).read(BYTES_SIZE)
            # JSON
            # Try to infer if is a valid json

            if sum([file.count(i) for i in ['{', '}', '[', ']']]) > JSON_THRESHOLD and (
                    file[0] == "{" or file[0] == "["):
                mime_info["file_type"] = "json"
                df = Load.json(full_path, *args, **kwargs)

            # XML
            elif sum([file.count(i) for i in ['<', '/>']]) > XML_THRESHOLD:
                mime_info["file_type"] = "xml"

            # CSV
            else:
                try:
                    dialect = csv.Sniffer().sniff(file)
                    mime_info["file_type"] = "csv"

                    r = {"properties": {"delimiter": dialect.delimiter,
                                        "doublequote": dialect.doublequote,
                                        "escapechar": dialect.escapechar,
                                        "lineterminator": dialect.lineterminator,
                                        "quotechar": dialect.quotechar,
                                        "quoting": dialect.quoting,
                                        "skipinitialspace": dialect.skipinitialspace}}

                    mime_info.update(r)
                    df = Load.csv(path, encoding=mime_info["encoding"], dtype=object, **mime_info["properties"],
                                  **kwargs, engine="python")
                except Exception as err:
                    raise err
                    pass

        elif mime_info["file_ext"] == "xls" or mime_info["file_ext"] == "xlsx":
            mime_info["file_type"] = "excel"
            df = Load.excel(full_path, **kwargs)

        else:
            RaiseIt.value_error(mime_info["file_ext"], ["csv", "json", "xml", "xls", "xlsx"])

        # print(os.path.abspath(__file__))
        df.meta.update("mime_info", value=mime_info)
        return df
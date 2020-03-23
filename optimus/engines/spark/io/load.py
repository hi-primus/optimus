import io
import ntpath
import zipfile

import pandas as pd
from packaging import version

from optimus.engines.spark.spark import Spark
from optimus.helpers.columns import replace_columns_special_characters
from optimus.helpers.functions import prepare_path
from optimus.helpers.logger import logger


class Load:

    @staticmethod
    def json(path, multiline=False, *args, **kwargs):
        """
        Return a spark from a json file.
        :param path: path or location of the file.
        :param multiline:
        :return:
        """
        file, file_name = prepare_path(path, "json")

        try:
            df = Spark.instance.spark.read \
                .option("multiLine", multiline) \
                .option("mode", "PERMISSIVE") \
                .json(file, *args, **kwargs)

            df.meta.set("file_name", file_name)

        except IOError as error:
            print(error)
            raise
        df = replace_columns_special_characters(df)

        df = df.meta.add_action("columns", df.cols.names())
        df.ext.reset()
        return df

    @staticmethod
    def tsv(path, header=True, infer_schema=True, charset="UTF-8", *args, **kwargs):
        """
        Return a spark from a tsv file.
        :param path: path or location of the file.
        :param header: tell the function whether dataset has a header row. True default.
        :param infer_schema: infers the input schema automatically from data.
        :param charset: Charset file encoding
        It requires one extra pass over the data. True default.

        :return:
        """
        df = Load.csv(path, sep='\t', header='true' if header else 'false',
                      infer_schema='true' if infer_schema else 'false', charset=charset, *args, **kwargs)
        df.ext.reset()
        return df

    @staticmethod
    def csv(path, sep=',', header=True, infer_schema=True, charset="UTF-8", null_value="None", n_rows=-1,
            error_bad_lines=False, *args, **kwargs):
        """
        Return a dataframe from a csv file. It is the same read.csv Spark function with some predefined
        params

        :param path: path or location of the file.
        :param sep: usually delimiter mark are ',' or ';'.
        :param header: tell the function whether dataset has a header row. True default.
        :param infer_schema: infers the input schema automatically from data.
        :param charset: Charset file encoding
        :param error_bad_lines:
        :param null_value: value to convert the string to a None value
        :param n_rows:
        It requires one extra pass over the data. True default.

        :return dataFrame
        """
        file, file_name = prepare_path(path, "csv")

        try:
            read = (Spark.instance.spark.read
                    .options(header='true' if header else 'false')
                    .options(delimiter=sep)
                    .options(inferSchema='true' if infer_schema else 'false')
                    .options(nullValue=null_value)
                    .option("charset", charset))

            if error_bad_lines is True:
                read.options(mode="FAILFAST")
            else:
                read.options(mode="DROPMALFORMED")

            df = read.csv(file, *args, **kwargs)

            if n_rows > -1:
                df = df.limit(n_rows)

            df.meta.set("file_name", file_name)
        except IOError as error:
            print(error)
            raise
        df = replace_columns_special_characters(df)
        df.ext.reset()
        return df

    @staticmethod
    def parquet(path, *args, **kwargs):
        """
        Return a spark from a parquet file.
        :param path: path or location of the file. Must be string dataType
        :param args: custom argument to be passed to the spark parquet function
        :param kwargs: custom keyword arguments to be passed to the spark parquet function
        :return: Spark Dataframe
        """

        file, file_name = prepare_path(path, "parquet")

        try:
            df = Spark.instance.spark.read.parquet(file, *args, **kwargs)
            df.meta.set("file_name", file_name)

        except IOError as error:
            print(error)
            raise
        df.ext.reset()
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
            if version.parse(Spark.instance.spark.version) < version.parse("2.4"):
                avro_version = "com.databricks.spark.avro"
            else:
                avro_version = "avro "
            df = Spark.instance.spark.read.format(avro_version).load(file, *args, **kwargs)

            df.meta.set("file_name", file_name)
        except IOError as error:
            print(error)
            raise
        df.ext.reset()
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
            df = Spark.instance.spark.createDataFrame(pdf)
            df.meta.set("file_name", ntpath.basename(file_name))
        except IOError as error:
            print(error)
            raise

        df = replace_columns_special_characters(df)
        df.ext.reset()
        return df

    @staticmethod
    def zip(path, file_name=None):
        """
        Return a Dataframe from a file inside a zip
        :param path:
        :param file:
        :return:
        """

        zip_file, zip_filename = prepare_path(path, "zip")

        def zip_extract(x):
            in_memory_data = io.BytesIO(x[1])
            file_obj = zipfile.ZipFile(in_memory_data, "r")
            files = [i for i in file_obj.namelist()]
            return dict(zip(files, [file_obj.open(file).read() for file in files]))

        zips = Spark.instance.sc.binaryFiles(zip_file)

        files_data = zips.map(zip_extract).collect()
        if file_name is None:
            result = files_data
        else:
            result = files_data[file_name]
        df.ext.reset()
        return result


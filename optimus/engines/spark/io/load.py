import glob
import io
import ntpath
import zipfile

import databricks.koalas as ks
import psutil
from packaging import version

from optimus.engines.base.io.load import BaseLoad
from optimus.engines.base.meta import Meta
from optimus.engines.spark.dataframe import SparkDataFrame
from optimus.engines.spark.spark import Spark
from optimus.helpers.columns import replace_columns_special_characters
from optimus.helpers.core import val_to_list
from optimus.helpers.functions import prepare_path, unquote_path
from optimus.helpers.logger import logger
from optimus.infer import is_list, is_url


class Load(BaseLoad):

    @staticmethod
    def df(*args, **kwargs):
        return SparkDataFrame(*args, **kwargs)

    @staticmethod
    def xml(path, *args, **kwargs) -> 'DataFrameType':
        pass

    @staticmethod
    def orc(path, columns, storage_options=None, conn=None, *args, **kwargs) -> 'DataFrameType':
        pass

    @staticmethod
    def hdf5(path, columns=None, *args, **kwargs) -> 'DataFrameType':
        pass

    @staticmethod
    def json(path, multiline=False, *args, **kwargs):
        file, file_name = prepare_path(path, "json")

        try:
            df = Spark.instance.spark.read \
                .option("multiLine", multiline) \
                .option("mode", "PERMISSIVE") \
                .json(file, *args, **kwargs)

            df.meta = Meta.set(df.meta, "file_name", file_name)

        except IOError as error:
            print(error)
            raise
        df = replace_columns_special_characters(df)

        df.meta = Meta.set(df.meta, value=df.meta.add_action("columns", df.cols.names()).get())
        return df

    @staticmethod
    def _csv(filepath_or_buffer, *args, **kwargs):

        # TODO support arguments
        
        kwargs.pop("n_partitions", None)
        kwargs.pop("encoding", None)
        kwargs.pop("quoting", None)
        kwargs.pop("on_bad_lines", None)
        kwargs.pop("na_filter", None)
        kwargs.pop("na_values", None)
        kwargs.pop("storage_options", None)

        kwargs["index_col"] = kwargs.get("index_col", None)
        if kwargs["index_col"] is False:
            kwargs["index_col"] = None

        df = ks.read_csv(filepath_or_buffer, *args, **kwargs)
        return df

    @staticmethod
    def parquet(path, *args, **kwargs):

        file, file_name = prepare_path(path, "parquet")

        try:
            df = Spark.instance.spark.read.parquet(file, *args, **kwargs)
            df.meta = Meta.set(df.meta, "file_name", file_name)

        except IOError as error:
            print(error)
            raise
        return df

    @staticmethod
    def avro(path, *args, **kwargs):

        file, file_name = prepare_path(path, "avro")

        try:
            if version.parse(Spark.instance.spark.version) < version.parse("2.4"):
                avro_version = "com.databricks.spark.avro"
            else:
                avro_version = "avro "
            df = Spark.instance.spark.read.format(avro_version).load(file, *args, **kwargs)

            df.meta = Meta.set(df.meta, "file_name", file_name)
        except IOError as error:
            print(error)
            raise
        return df

    @staticmethod
    def excel(path, sheet_name=0, *args, **kwargs):

        file, file_name = prepare_path(path, "xls")

        try:
            pdf = ks.read_excel(file, sheet_name=sheet_name, *args, **kwargs)

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
            df.meta = Meta.set(df.meta, "file_name", ntpath.basename(file_name))
        except IOError as error:
            print(error)
            raise

        df = replace_columns_special_characters(df)
        return df

    @staticmethod
    def zip(path, file_name=None):

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
        return result

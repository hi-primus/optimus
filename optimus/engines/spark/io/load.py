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

    def tsv(self, path, header=True, infer_schema=True, charset="UTF-8", *args, **kwargs):
        return self.csv(path, sep='\t', header=header, infer_schema=infer_schema, charset=charset, *args, **kwargs)

    def csv(self, filepath_or_buffer, sep=",", header=True, infer_schema=True, encoding="UTF-8", n_rows=None,
            null_value="None", quoting=3, lineterminator='\r\n', error_bad_lines=False, cache=False, na_filter=False,
            storage_options=None, conn=None, n_partitions=1, *args, **kwargs):

        if not is_url(filepath_or_buffer):
            filepath_or_buffer = glob.glob(unquote_path(filepath_or_buffer))
            meta = {"file_name": filepath_or_buffer, "name": ntpath.basename(filepath_or_buffer[0])}
        else:
            meta = {"file_name": filepath_or_buffer, "name": ntpath.basename(filepath_or_buffer)}

        try:

            # Pandas do not support \r\n terminator.
            if lineterminator and lineterminator.encode(encoding='UTF-8', errors='strict') == b'\r\n':
                lineterminator = None

            if conn is not None:
                filepath_or_buffer = conn.path(filepath_or_buffer)
                storage_options = conn.storage_options
            else:
                storage_options = None

            if kwargs.get("chunk_size") == "auto":
                ## Chunk size is going to be 75% of the memory available
                kwargs.pop("chunk_size")
                kwargs["chunksize"] = psutil.virtual_memory().free * 0.75

            na_filter = na_filter if null_value else False

            def _read(_filepath_or_buffer):
                return ks.read_csv(_filepath_or_buffer)

            print(filepath_or_buffer[0])
            if len(filepath_or_buffer) > 1:
                df = ks.DataFrame()
                for f in filepath_or_buffer:
                    df = df.append(_read(f))
            else:
                df = _read(filepath_or_buffer[0])

            # if isinstance(df, ks.io.parsers.TextFileReader):
            #     df = df.get_chunk()

            df = SparkDataFrame(df, op=self.op)

            df.meta = Meta.set(df.meta, value=meta)

        except IOError as error:
            print(error)
            logger.print(error)
            raise

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

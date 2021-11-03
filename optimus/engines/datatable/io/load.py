from datatable import dt

from optimus.engines.base.io.load import BaseLoad
from optimus.engines.datatable.dataframe import DatatableDataFrame


class Load(BaseLoad):

    @staticmethod
    def df(*args, **kwargs):
        return DatatableDataFrame(*args, **kwargs)

    @staticmethod
    def _csv(filepath_or_buffer, *args, **kwargs):
        return dt.fread(filepath_or_buffer)

    @staticmethod
    def _json(filepath_or_buffer, *args, **kwargs):
        kwargs.pop("n_partitions", None)

        return dt.fread(filepath_or_buffer)

    @staticmethod
    def _avro(filepath_or_buffer, nrows=None, *args, **kwargs):
        kwargs.pop("n_partitions", None)
        return dt.fread(filepath_or_buffer)

    @staticmethod
    def _parquet(filepath_or_buffer, nrows=None, engine="pyarrow", *args, **kwargs):
        kwargs.pop("n_partitions", None)
        return dt.fread(filepath_or_buffer)

    @staticmethod
    def _xml(filepath_or_buffer, nrows=None, *args, **kwargs):
        kwargs.pop("n_partitions", None)
        return dt.fread(filepath_or_buffer)

    @staticmethod
    def _excel(filepath_or_buffer, nrows=None, storage_options=None, *args, **kwargs):
        kwargs.pop("n_partitions", None)
        return dt.fread(filepath_or_buffer)

    def orc(self, path, columns, storage_options=None, conn=None, n_partitions=1, *args, **kwargs):
        raise NotImplementedError('Not implemented yet')

    @staticmethod
    def zip(zip_path, filename, dest=None, merge=False, storage_options=None, conn=None, n_partitions=1, *args,
            **kwargs):
        raise NotImplementedError('Not implemented yet')

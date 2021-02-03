from optimus.engines.dask.dataframe import DaskDataFrame
from optimus.engines.dask_cudf.io.save import Save


class DaskCUDFDataFrame(DaskDataFrame):

    def __init__(self, data):
        super().__init__(self, data)

    @property
    def rows(self):
        from optimus.engines.dask_cudf.rows import Rows
        return Rows(self)

    @property
    def cols(self):
        from optimus.engines.dask_cudf.columns import Cols
        return Cols(self)

    @property
    def functions(self):
        from optimus.engines.dask_cudf.functions import DaskCUDFFunctions
        return DaskCUDFFunctions()

    @property
    def save(self):
        return Save(self)

    @staticmethod
    def pivot(index, column, values):
        pass

    @staticmethod
    def melt(id_vars, value_vars, var_name="variable", value_name="value", data_type="str"):
        pass

    @staticmethod
    def query(sql_expression):
        pass

    @staticmethod
    def debug():
        pass

    @staticmethod
    def create_id(column="id"):
        pass

    def to_pandas(self):
        return self.data.compute().to_pandas()


    def to_dict(self, orient="records", limit=None):
        """
        Create a dict
        :param orient:
        :param limit:
        :return:
        """
        series = self.root
        return series.compute().to_pandas().to_dict(orient)
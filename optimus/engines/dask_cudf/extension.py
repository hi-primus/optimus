import dask
import imgkit

from optimus.engines.base.commons.functions import to_datetime
from optimus.engines.base.extension import BaseExt
from optimus.engines.cudf.functions import CUDFFunctions
from optimus.helpers.functions import random_int, absolute_path
from optimus.helpers.output import print_html
from optimus.helpers.raiseit import RaiseIt


class Ext(BaseExt):

    # _name = None
    def __init__(self, df):
        super().__init__(df)
        self.df = df

    @staticmethod
    def delayed(func):
        def wrapper(*args, **kwargs):
            return dask.delayed(func)(*args, **kwargs)

        return wrapper

    def cache(self):
        df = self.parent.data
        return self.parent.new(df.persist(), meta=self.parent)

    def compute(self):
        df = self.df
        return df.compute()

    @staticmethod
    def sample(n=10, random=False):
        """
        Return a n number of sample from a dataFrame
        :param n: Number of samples
        :param random: if true get a semi random sample
        :return:
        """
        if random is True:
            seed = random_int()
        elif random is False:
            seed = 0
        else:
            RaiseIt.value_error(random, ["True", "False"])

        rows_count = self.rows.count()
        if n < rows_count:
            # n/rows_count can return a number that represent less the total number we expect. multiply by 1.1
            fraction = (n / rows_count) * 1.1
        else:
            fraction = 1.0
        return self.sample(frac=fraction, random_state=seed)

    @staticmethod
    def stratified_sample(col_name, seed: int = 1):
        """
        Stratified Sampling
        :param col_name:
        :param seed:
        :return:
        """
        df = self
        n = min(5, df[col_name].value_counts().min())
        df = df.groupby(col_name).apply(lambda x: x.sample(2))
        # df_.index = df_.index.droplevel(0)
        return df

    @staticmethod
    def pivot(index, column, values):
        """
        Return reshaped DataFrame organized by given index / column values.
        :param index: Column to use to make new frame's index.
        :param column: Column to use to make new frame's columns.
        :param values: Column(s) to use for populating new frame's values.
        :return:
        """
        raise NotImplementedError

    @staticmethod
    def melt(id_vars, value_vars, var_name="variable", value_name="value", data_type="str"):
        """
        Convert DataFrame from wide to long format.
        :param id_vars: column with unique values
        :param value_vars: Column names that are going to be converted to columns values
        :param var_name: Column name for vars
        :param value_name: Column name for values
        :param data_type: All columns must have the same type. It will transform all columns to this data type.
        :return:
        """

        raise NotImplementedError

    def run(self):
        """
        This method is a very useful function to break lineage of transformations. By default Spark uses the lazy
        evaluation approach in processing data: transformation functions are not computed into an action is called.
        Sometimes when transformations are numerous, the computations are very extensive because the high number of
        operations that spark needs to run in order to get the results.

        Other important thing is that Apache Spark save task but not result of dataFrame, so tasks are
        accumulated and the same situation happens.

        :return:
        """

        return self.parent.cache().count()

    @staticmethod
    def query(sql_expression):
        raise NotImplementedError

    def partitions(self):
        return self.parent.data.npartitions

    @staticmethod
    def partitioner():
        raise NotImplementedError

    #
    # @staticmethod
    # def repartition(n=0, col_name=None):
    #     return df.repartition(n)

    @staticmethod
    def table_image(path, limit=10):
        """
        Output table as image
        :param limit:
        :param path:
        :return:
        """

        css = absolute_path("/css/styles.css")

        imgkit.from_string(Ext.table_html(limit=limit, full=True), path, css=css)
        print_html("<img src='" + path + "'>")

    def show(self):
        """
        Print df lineage
        :return:
        """
        return self.parent.compute()

    @staticmethod
    def debug():
        """
        Print df lineage
        :return:
        """
        raise NotImplementedError

    @staticmethod
    def create_id(column="id"):
        """
        Create a unique id for every row.
        :param column: Columns to be processed
        :return:
        """

        raise NotImplementedError

    @staticmethod
    def is_cached(df):
        """

        :return:
        """
        return False if df.meta.get("profile") is None else True

    @staticmethod
    def delayed(func):
        def wrapper(*args, **kwargs):
            return dask.delayed(func)(*args, **kwargs)

        return wrapper

    def to_dict(self, orient="records", limit=None):
        """
        Create a dict
        :param index: Return the series index
        :return:
        """
        series = self.parent.data
        # if index is True:
        return series.compute().to_pandas().to_dict(orient)
        # else:
        #     return series.compute().to_arrow().to_pylist()

    def to_pandas(self):
        return self.parent.data.compute().to_pandas()

    def to_float(self):
        series = self.series
        return series.map_partitions(CUDFFunctions.to_float())

    def to_integer(self):
        series = self.series
        return series.map_partitions(CUDFFunctions.to_integer)

    def to_datetime(self, format):
        series = self.series
        return to_datetime(series, format)

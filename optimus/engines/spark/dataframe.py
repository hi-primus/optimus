class SparkDataFrame:
    def __init__(self, df):
        super().__init__(df)

    @property
    def rows(self):
        from optimus.engines.spark.rows import Rows
        return Rows(self)

    @property
    def cols(self):
        from optimus.engines.spark.columns import Cols
        return Cols(self)

    @property
    def constants(self):
        from optimus.engines.spark.constants import Constants
        return Constants()

    @property
    def functions(self):
        from optimus.engines.spark.functions import SparkFunctions
        return SparkFunctions()

    @property
    def meta(self):
        return Meta(self)


from optimus.engines.base.dataframe.extension import DataFrameBaseExt


# import ibis

class Ext(DataFrameBaseExt):

    def __init__(self, df):
        super().__init__(df)

    def compile(self):
        # return str(ibis.impala.compiler(self.parent.data))
        return str(self.parent.data.compile())

    def to_pandas(self):
        return self.parent.data.execute()

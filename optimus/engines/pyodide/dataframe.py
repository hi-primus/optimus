from optimus.engines.pandas.dataframe import PandasDataFrame


class PyodideDataFrame(PandasDataFrame):

    def print(self, limit=10, cols=None):
        console.log(self.ascii(limit, cols))

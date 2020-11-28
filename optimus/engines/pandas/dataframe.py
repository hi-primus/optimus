from optimus.engines.base.dataframe.extension import Ext as PandasExtension


class PandasDataFrame(PandasExtension):
    def __init__(self, data):
        super().__init__(self, data)

    @property
    def rows(self):
        from optimus.engines.pandas.rows import Rows
        return Rows(self)

    @property
    def cols(self):
        from optimus.engines.pandas.columns import Cols
        return Cols(self)

    @property
    def functions(self):
        from optimus.engines.pandas.functions import PandasFunctions
        return PandasFunctions(self)

    @property
    def constants(self):
        from optimus.engines.pandas.constants import constants
        return constants(self)
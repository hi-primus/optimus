from optimus.engines.base.dataframe.extension import Ext as BaseExt


class Ext(BaseExt):

    def __init__(self, root, data):
        super().__init__(root, data)

    def to_pandas(self):
        return self.data.to_pandas()

    def to_dict(self, orient="records", index=True):
        """
        Create a dict
        :param orient:
        :param index: Return the series index
        :return:
        """

        series = self.data
        if index is True:
            return series.to_pandas().to_dict(orient)
        else:
            return series.to_pandas().to_list()

    def repartition(self, n=None, *args, **kwargs):
        return self.root

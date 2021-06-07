from abc import abstractmethod

class CUDFBaseColumns():

    def _series_to_dict_delayed(self, series):
        return series.to_pandas().to_dict()
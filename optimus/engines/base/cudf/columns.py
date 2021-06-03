from abc import abstractmethod

class CUDFBaseColumns():

    def _series_to_dict_delayed(self, series):
        return series.to_pandas().to_dict()

    @staticmethod
    def exec_agg(exprs, compute):
        """
        Execute and aggregation
        :param exprs:
        :return:
        """
        # print("exprs",type(exprs[0]),exprs[0])
        try:
            return exprs[0].to_pandas().to_dict()
        except TypeError:
            return exprs
        except AttributeError:
            return exprs
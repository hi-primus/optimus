from abc import abstractmethod

class CUDFBaseColumns():
    pass

    @staticmethod
    def exec_agg(exprs, compute):
        """
        Execute and aggregation
        :param exprs:
        :return:
        """
        # print("exprs",type(exprs[0]),exprs[0])
        return exprs[0].to_pandas().to_dict()
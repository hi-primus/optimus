import pandas as pd

from optimus.engines.vaex.dataframe import VaexDataFrame
from optimus.infer import is_dict


class Create:
    def __init__(self, root):
        self.root = root

    def dataframe(self, dict=None, cols=None, rows=None, pdf=None, n_partitions=1, *args, **kwargs):
        """
        Helper to create dataframe:
        :param cols: List of Tuple with name, data type and a flag to accept null
        :param rows: List of Tuples with the same number and types that cols
        :param pdf: a pandas dataframe
        :param n_partitions:
        :return: Dataframe
        """

        if is_dict(dict):
            pdf = pd.DataFrame(dict)
        elif not pdf:
            pdf = pd.DataFrame(kwargs)

        df = VaexDataFrame(pdf)
        return df

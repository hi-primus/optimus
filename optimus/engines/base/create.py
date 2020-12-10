import pandas as pd

from optimus.engines.base.meta import Meta


class Create:
    def __init__(self, root):
        self.root = root

    def dataframe(self, dict, cols=None, rows=None, pdf=None, n_partitions=1, *args, **kwargs):
        """
        Helper to create dataframe:
        :param cols: List of Tuple with name, data type and a flag to accept null
        :param rows: List of Tuples with the same number and types that cols
        :param pdf: a pandas dataframe
        :param n_partitions:
        :return: Dataframe
        """
        if dict:
            pdf = pd.DataFrame(dict)

        df = self.root.dataframe(pdf, n_partitions)
        return df


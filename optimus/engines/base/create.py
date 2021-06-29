from optimus.helpers.types import DataFrameType
import pandas as pd


class Create:
    def __init__(self, root):
        self.root = root

    def dataframe(self, dict: dict=None, cols=None, rows=None, pdf: pd.DataFrame=None, n_partitions: int=1, *args, **kwargs) -> DataFrameType:
        """
        Helper to create dataframe:
        :param dict:
        :param cols: List of Tuple with name, data type and a flag to accept null
        :param rows: List of Tuples with the same number and types that cols
        :param pdf: a pandas dataframe
        :param n_partitions:
        :return: Dataframe
        """
        
        if dict:
            pdf = pd.DataFrame(dict)
        elif pdf is None:
            pdf = pd.DataFrame(kwargs)

        df = self.root.dataframe(pdf, n_partitions)
        return df


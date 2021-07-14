from abc import abstractmethod
from optimus.engines.base.meta import Meta
from optimus.infer import is_tuple
from optimus.helpers.types import DataFrameType, InternalDataFrameType
import pandas as pd


class BaseCreate:
    def __init__(self, root):
        self.root = root

    def _dictionary(self, dict):

        new_dict = {}
        
        for key, values in dict.items():
            if is_tuple(key):
                if len(key) == 3:
                    name, dtype, nulls = key
                elif len(key) == 2:
                    name, dtype = key
                    nulls = False
                dtype = self.root.constants.DTYPES_ALIAS.get(dtype, dtype)
            else:
                name = key
                dtype = None
                nulls = False

            new_dict[(name, dtype, nulls)] = values

        return new_dict

    
    def _dfd_from_dict(self, dict):
        return pd.DataFrame({ name: pd.Series(values, dtype=dtype) for (name, dtype, nulls), values in dict.items() })

    @abstractmethod
    def _df_from_dfd(self, dfd, *args, **kwargs):
        pass


    def dataframe(self, dict: dict=None, dfd: InternalDataFrameType=None, n_partitions: int=1, *args, **kwargs) -> DataFrameType:
        """
        Creates a dictionary using the form 
        {"Column name": ["value 1", "value 2"], ...} or {("Column name", "str", True): ["value 1", "value 2"]}
        Where the tuple keys uses the form (str, str, boolean) for (column name, data type, allow nulls)
        :param dict: A dictionary to construct the dataframe for
        :param dfd: A pandas dataframe, ignores dict when passed
        :return: BaseDataFrame
        """

        if dfd is None:
            if dict is None:
                dict = kwargs
                kwargs = {}
            dict = self._dictionary(dict)
            dfd = self._dfd_from_dict(dict)

        df = self._df_from_dfd(dfd, n_partitions=n_partitions, *args, **kwargs)
        df.meta = Meta.set(df.meta, value={"max_cell_length": df.cols.len("*").cols.max()})
        return df


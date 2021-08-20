from abc import abstractmethod
import warnings
from optimus.engines.base.meta import Meta
from optimus.infer import is_tuple
from optimus.helpers.types import *
import pandas as pd


class BaseCreate:
    def __init__(self, op: 'DataFrameType'):
        self.op = op

    def _dictionary(self, dict, force_dtypes=False):

        new_dict = {}
        
        for key, values in dict.items():
            if is_tuple(key):
                dtype = None
                force_dtype = force_dtypes
                nulls = False
                if len(key) == 4:
                    name, dtype, nulls, force_dtype = key
                if len(key) == 3:
                    name, dtype, nulls = key
                elif len(key) == 2:
                    name, dtype = key
                if force_dtype:
                    dtype = self.op.constants.OPTIMUS_TO_INTERNAL.get(dtype, dtype)
            else:
                name = key
                dtype = None
                nulls = False
                force_dtype = force_dtypes

            new_dict[(name, dtype, nulls, force_dtype)] = values

        return new_dict

    @property
    def _pd(self):
        return pd
    
    def _dfd_from_dict(self, dict) -> 'InternalDataFrameType':
        pd_dict = {}
        for (name, dtype, nulls, force_dtype), values in dict.items():
            dtype = self.op.constants.COMPATIBLE_DTYPES.get(dtype, dtype) if force_dtype else None
            pd_series = self._pd.Series(values, dtype=dtype)
            pd_dict.update({name: pd_series})
        return self._pd.DataFrame(pd_dict)

    @abstractmethod
    def _df_from_dfd(self, dfd, *args, **kwargs) -> 'DataFrameType':
        pass

    def dataframe(self, dict: dict = None, dfd: 'InternalDataFrameType' = None, force_data_types=False, n_partitions: int = 1, *args, **kwargs) -> 'DataFrameType':
        """
        Creates a dictionary using the form 
        {"Column name": ["value 1", "value 2"], ...} or {("Column name", "str", True, True): ["value 1", "value 2"]}
        Where the tuple uses the form (str, str, boolean, boolean) for (name, data type, allow nulls, force data type in creation)
        You can also pass 2-length and 3-length tuples.
        :param dict: A dictionary to construct the dataframe for
        :param dfd: A pandas dataframe, ignores dict when passed
        :return: BaseDataFrame
        """

        if dfd is None:
            if dict is None:
                dict = kwargs
                kwargs = {}
            dict = self._dictionary(dict, force_dtypes=force_data_types)
            dfd = self._dfd_from_dict(dict)

        df = self._df_from_dfd(dfd, n_partitions=n_partitions, *args, **kwargs)
        
        try:
            df.meta = Meta.set(df.meta, value={"max_cell_length": df.cols.len("*").cols.max()})
        except:
            warnings.warn("Could not set max_cell_length")
        
        if dict is not None:
            for (name, dtype, nulls, force_dtype) in dict:
                if dtype and not force_dtype:
                    df = df.cols.set_data_type(name, dtype)

        return df


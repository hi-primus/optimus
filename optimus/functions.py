from optimus.engines.base.basedataframe import BaseDataFrame
import numpy as np

class Functions(type):
    """
    Handle the method name and execute the function corresponding to the dataframe class
    """
    def __getattr__(self, func):
        """

        :param func: Method name
        :return:
        """

        def _func(df_or_value, *args):
            # Get the internal series in the optimus dataframe
            # print("df_or_value",type(df_or_value),df_or_value)
            if isinstance(df_or_value, BaseDataFrame):
                _df = df_or_value.data[df_or_value.cols.names(0)[0]]
                # Convert the frame to a dataframe and to a optimus Dataframe
                return df_or_value.__class__(getattr(df_or_value.functions, func)(_df, *args).to_frame())
            else:
                return getattr(np, func)(df_or_value, *args)

        return _func


class F(metaclass=Functions):
    """
    Dummy class to handle method names
    For example: In F.sqrt, sqrt is captured and mapped a matched to the function dataframe
    """
    pass

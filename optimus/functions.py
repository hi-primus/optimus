from optimus.engines.base.basedataframe import BaseDataFrame
import numpy as np

class Functions(type):
    """
    Handle the method name and execute the function corresponding to the dataframe class
    """
    def __getattr__(self, func) -> callable:
        """

        :param func: Method name
        :return:
        """

        def _func(df_or_value, *args):
            # Get the internal series in the optimus dataframe
            if isinstance(df_or_value, BaseDataFrame):
                _df = df_or_value.data[df_or_value.cols.names(0)[0]]
                # Convert the frame to a dataframe and to a optimus Dataframe
                series = getattr(df_or_value.functions, func)(_df, *args)
                series.name = df_or_value.cols.names(0)[0]
                r = df_or_value.__class__(series.to_frame(), df_or_value.op)
            else:
                r = getattr(np, func)(df_or_value, *args)

            if isinstance(r, np.generic):
                return np.asscalar(r)
            return r

        return _func


class F(metaclass=Functions):
    """
    Dummy class to handle method names
    For example: In F.sqrt, sqrt is captured and mapped a matched to the function dataframe
    """
    pass

class Functions(type):
    """
    Handle the method name and execute the function corresponding to the dataframe class
    """
    def __getattr__(self, func):
        """

        :param func: Method name
        :return:
        """

        def _func(df, *args):
            # Get the internal series in the optimus dataframe
            _df = df.data[df.cols.names(0)[0]]
            # Convert the frame to a dataframe and to a optimus Dataframe
            return df.__class__(getattr(df.functions, func)(_df, *args).to_frame())

        return _func


class F(metaclass=Functions):
    """
    Dummy class to handle method names
    For example: In F.sqrt, sqrt is captured and mapped a matched to the function dataframe
    """
    pass

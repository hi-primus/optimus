import cudf


class Create:
    def __init__(self, root):
        self.root = root

    def dataframe(self, dict=None, cols=None, rows=None, pdf=None, n_partitions=1, *args, **kwargs):
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
            cdf = cudf.DataFrame(dict)
        elif pdf:
            cdf = cudf.from_pandas(pdf)
        else:
            cdf = cudf.DataFrame(kwargs)

        df = self.root.dataframe(cdf, n_partitions)
        return df


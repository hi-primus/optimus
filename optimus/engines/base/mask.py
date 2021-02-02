from optimus.helpers.core import val_to_list
from optimus.infer import Infer


class Mask:
    def __init__(self, root):
        self.root = root

    def greater_than(self, col_name, value):
        dfd = self.root.data
        return self.root.new((dfd[col_name] > value).to_frame())

    def greater_than_equal(self, col_name, value):
        dfd = self.root.data
        return self.root.new((dfd[col_name] >= value).to_frame())

    def less_than(self, col_name, value):
        dfd = self.root.data
        return self.root.new((dfd[col_name] < value).to_frame())

    def less_than_equal(self, col_name, value):
        dfd = self.root.data
        return self.root.new((dfd[col_name] <= value).to_frame())

    def equal(self, col_name, value):
        dfd = self.root.data
        return self.root.new((dfd[col_name] == value).to_frame())

    def not_equal(self, col_name, value):
        dfd = self.root.data
        return self.root.new((dfd[col_name] != value).to_frame())

    def missing(self, col_name):
        """
        Return missing values
        :param col_name:
        :return:
        """
        mask = self.root.data[col_name].isnull()
        return self.root.new(mask.to_frame())

    def mismatch(self, col_name, dtype):
        """
        Return missing values
        :param col_name:
        :param dtype:
        :return:
        """
        dfd = self.root.data
        mask_mismatch = ~dfd[col_name].astype("str").str.match(Infer.ProfilerDataTypesFunctions[dtype])
        mask_null = dfd[col_name].isnull()
        return self.root.new((mask_mismatch | mask_null).to_frame())

    def match(self, col_name, dtype):
        """
        Return Match values
        :param col_name:
        :param dtype:
        :return:
        """
        mask = self.root.data[col_name].astype("str").str.match(Infer.ProfilerDataTypesFunctions[dtype])
        return self.root.new(mask.to_frame())

    def values_in(self):
        pass

    def pattern(self):
        pass

    def starts_with(self, col_name, value):
        mask = self.root.data[col_name].str.startswith(value, na=False)
        return self.root.new(mask.to_frame())

    def ends_with(self, col_name, value):
        mask = self.root.data[col_name].str.endswith(value, na=False)
        return self.root.new(mask.to_frame())

    def find(self, value):
        """

        :param value: Regex or string
        :return:
        """

        pass

    def nulls(self, col_name, how="any"):
        """
        Find the rows that have null values
        :param how:
        :param col_name:
        :return:
        """

        dfd = self.root.data

        if col_name is not None:
            columns = val_to_list(col_name)
            subset_df = dfd[columns]
        else:
            subset_df = dfd

        if how == "all":
            mask = subset_df.isnull().all(axis=1)
        else:
            mask = subset_df.isnull()

        return self.root.new(mask)

    def duplicated(self, keep="first", columns=None):
        """
        Find the rows that have duplicated values

        :param keep:
        :param columns:
        :return:
        """

        dfd = self.root.data
        if columns is not None:
            subset = val_to_list(columns)
            subset_df = dfd[subset]
        else:
            subset_df = dfd

        mask = subset_df.duplicated(keep=keep, subset=columns)

        return self.root.new(mask)

    def empty(self, col_name):
        """
        Find the rows that do not have any info

        :param col_name:
        :return:
        """
        mask = self.root.data[col_name] = ""
        return self.root.new(mask)

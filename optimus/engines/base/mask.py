from optimus.infer import Infer


class Mask:
    def __init__(self, root):
        self.root = root

    def greater_than(self, col_name, value):
        dfd = self.root.data
        return self.root.new(dfd[col_name] > value)

    def greater_than_equal(self, col_name, value):
        dfd = self.root.data
        return self.root.new(dfd[col_name] >= value)

    def less_than(self, col_name, value):
        dfd = self.root.data
        return self.root.new(dfd[col_name] < value)

    def less_than_equal(self, col_name, value):
        dfd = self.root.data
        return self.root.new(dfd[col_name] <= value)

    def equal(self, col_name, value):
        dfd = self.root.data
        return self.root.new((dfd[col_name] == value).to_frame())

    def not_equal(self, col_name, value):
        dfd = self.root.data
        return self.root.new(dfd[col_name] != value)

    def missing(self, col_name):
        """
        Return missing values
        :param col_name:
        :return:
        """
        dfd = self.root.data
        return self.root.new(dfd[col_name].isnull())

    def mismatch(self, col_name, dtype):
        """
        Return missing values
        :param col_name:
        :param dtype:
        :return:
        """
        df = self.root
        mask = df[col_name].astype("str").str.match(Infer.ProfilerDataTypesFunctions[dtype])
        mask_null = df[col_name].isnull()
        return self.root.new(~mask & ~mask_null)

    def match(self, col_name, dtype):
        """
        Return Match values
        :param col_name:
        :param dtype:
        :return:
        """
        return self.root.new(self.root.cols.select(col_name).cols.to_string().data[col_name].str.match(
            Infer.ProfilerDataTypesFunctions[dtype]))

    def values_in(self):
        pass

    def pattern(self):
        pass

    def starts_with(self, col_name, value):
        return self.root.data[col_name].str.startswith(value, na=False)

    def ends_with(self, col_name, value):
        return self.root.data[col_name].str.endswith(value, na=False)

    def find(self, value):
        """

        :param value: Regex or string
        :return:
        """

        pass

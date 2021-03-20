from abc import abstractmethod, ABC

from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import ProfilerDataTypes
from optimus.helpers.core import val_to_list
from optimus.infer import Infer, is_str, regex_http_code, regex_social_security_number, regex_phone_number, \
    regex_credit_card_number, regex_zip_code, regex_boolean, regex_gender, regex_url, regex_ip, regex_email


class Mask(ABC):
    def __init__(self, root):
        self.root = root

    def greater_than(self, col_name, value):
        df = self.root
        return df[col_name] > value

    def greater_than_equal(self, col_name, value):
        df = self.root
        return df[col_name] >= value

    def less_than(self, col_name, value):
        df = self.root
        return df[col_name] < value

    def less_than_equal(self, col_name, value):
        df = self.root
        return df[col_name] <= value

    def equal(self, col_name, value):
        df = self.root
        return df[col_name] == value

    def not_equal(self, col_name, value):
        df = self.root
        return df[col_name] != value

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
        mask_mismatch = ~dfd[col_name].astype("str").str.match(Infer.ProfilerDataTypesRegex[dtype])
        mask_null = dfd[col_name].isnull()
        return self.root.new((mask_mismatch | mask_null).to_frame())

    def match(self, col_name, dtype):
        """
        Return Match values
        :param col_name:
        :param dtype:
        :return:
        """
        mask = self.root.data[col_name].astype("str").str.match(Infer.ProfilerDataTypesRegex[dtype])
        return self.root.new(mask.to_frame())

    def values_in(self, col_name, values):
        values = val_to_list(values)
        mask = self.root.data[col_name].isin(values)
        return self.root.new(mask.to_frame())

    def pattern(self):
        pass

    def starts_with(self, col_name, value):
        mask = self.root.data[col_name].str.startswith(value, na=False)
        return self.root.new(mask.to_frame())

    def ends_with(self, col_name, value):
        mask = self.root.data[col_name].str.endswith(value, na=False)
        return self.root.new(mask.to_frame())

    def contains(self, col_name, value):
        mask = self.root.data[col_name].str.contains(value, na=False)
        return self.root.new(mask.to_frame())

    def find(self, col_name, value):
        dfd = self.root.data
        if is_str(value):
            mask = self.root.data[col_name].astype(str).str.match(value, na=False)
        else:
            mask = dfd[col_name] == value
        return self.root.new(mask.to_frame())

    @abstractmethod
    def integer(self, col_name):
        pass

    @abstractmethod
    def float(self, col_name):
        pass

    @abstractmethod
    def numeric(self, col_name):
        pass

    def nulls(self, columns, how="any"):
        """
        Find the rows that have null values
        :param how:
        :param columns:
        :return:
        """
        df = self.root
        dfd = self.root.data

        if columns is not None:
            subset = parse_columns(df, columns)
            subset_df = dfd[subset]
        else:
            subset_df = dfd

        if how == "all":
            mask = subset_df.isnull().all(axis=1)
        else:
            mask = subset_df.isnull()

        return self.root.new(mask)

    def duplicated(self, columns, keep="first"):
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
        mask = self.root.data[col_name] == ""
        return self.root.new(mask)

    def string(self, col_name="*"):
        return ~self.root.mask.nulls(col_name)

    def email(self, col_name="*"):
        return self.root[col_name].cols.to_string().cols.match(col_name, regex_email)

    def ip(self, col_name="*"):
        return self.root[col_name].cols.to_string().cols.match(col_name, regex_ip)

    def url(self, col_name="*"):
        return self.root[col_name].cols.to_string().cols.match(col_name, regex_url)

    def gender(self, col_name="*"):
        return self.root[col_name].cols.to_string().cols.match(col_name, regex_gender)

    def boolean(self, col_name="*"):
        return self.root[col_name].cols.to_string().cols.match(col_name, regex_boolean)

    def zip_code(self, col_name="*"):
        return self.root[col_name].cols.to_string().cols.match(col_name, regex_zip_code)

    def credit_card_number(self, col_name="*"):
        return self.root[col_name].cols.to_string().cols.match(col_name, regex_credit_card_number)

    def datetime(self, col_name="*"):
        df = self.root
        if df[col_name].cols.dtype  == df.constants.
        return self.root[col_name].cols.to_string().cols.match(col_name, Infer.ProfilerDataTypesRegex[
            ProfilerDataTypes.DATE.value])

    def object(self, col_name="*"):
        return self.root[col_name].cols.to_string().cols.match(col_name, Infer.ProfilerDataTypesRegex[
            ProfilerDataTypes.OBJECT.value])

    def array(self, col_name="*"):
        return self.root[col_name].cols.to_string().cols.match(col_name, regex_array)

    def phone_number(self, col_name="*"):
        return self.root[col_name].cols.to_string().cols.match(col_name, regex_phone_number)

    def social_security_number(self, col_name="*"):
        return self.root[col_name].cols.to_string().cols.match(col_name, regex_social_security_number)

    def http_code(self, col_name="*"):
        return self.root[col_name].cols.to_string().cols.match(col_name, regex_http_code)
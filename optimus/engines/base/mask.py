from abc import abstractmethod, ABC
from optimus.helpers.types import MaskDataFrameType

from optimus.helpers.columns import parse_columns
from optimus.helpers.constants import ProfilerDataTypes
from optimus.helpers.core import val_to_list, one_list_to_val
from optimus.helpers.raiseit import RaiseIt
from optimus.infer import is_str, regex_http_code, regex_social_security_number, regex_phone_number, \
    regex_credit_card_number, regex_zip_code, regex_gender, regex_ip, regex_email, \
    is_datetime, is_list, is_bool, is_object, regex_full_url


class Mask(ABC):
    def __init__(self, root):
        self.root = root

    @abstractmethod
    def str(self, cols="*") -> MaskDataFrameType:
        pass

    @abstractmethod
    def int(self, cols) -> MaskDataFrameType:
        pass

    @abstractmethod
    def float(self, cols) -> MaskDataFrameType:
        pass

    @abstractmethod
    def numeric(self, cols) -> MaskDataFrameType:
        pass

    def greater_than(self, cols, value) -> MaskDataFrameType:
        df = self.root
        return df[cols] > value

    def greater_than_equal(self, cols, value) -> MaskDataFrameType:
        df = self.root
        return df[cols] >= value

    def less_than(self, cols, value) -> MaskDataFrameType:
        df = self.root
        return df[cols] < value

    def less_than_equal(self, cols, value) -> MaskDataFrameType:
        df = self.root
        return df[cols] <= value

    def equal(self, cols, value) -> MaskDataFrameType:
        df = self.root
        return df[cols] == value

    def not_equal(self, cols, value) -> MaskDataFrameType:
        df = self.root
        return df[cols] != value

    def missing(self, cols) -> MaskDataFrameType:
        """
        Return missing values
        :param cols:
        :return:
        """
        df = self.root

        return df.mask.nulls(cols)

    def mismatch(self, cols="*", dtype=None) -> MaskDataFrameType:
        """
        Return missing values
        :param cols:
        :param dtype:
        :return:
        """

        df = self.root
        cols = one_list_to_val(parse_columns(df, cols))

        mask_match = df[cols].mask.match(cols, dtype)
        mask_null = df[cols].mask.nulls(cols)
        return ~(mask_match | mask_null)

    def match(self, cols="*", dtype=None) -> MaskDataFrameType:
        """
        Return values that match with a datatype
        :param cols:
        :param dtype:
        :return:
        """
        df = self.root
        cols = one_list_to_val(parse_columns(df, cols))

        if dtype is None:
            profiled_dtype = df.profile.dtypes(cols)
            if profiled_dtype is None:
                RaiseIt.value_error(dtype, ProfilerDataTypes.list(),
                                    "dtype not found in cache. Need to passed as dtype param")
            else:
                dtype = profiled_dtype

        if is_list(dtype):
            mask_match = None
            for i, j in zip(cols, dtype):
                if mask_match is None:
                    mask_match = getattr(df[i].mask, j)(i)
                else:
                    mask_match[i] = getattr(df[i].mask, j)(i)
        else:
            mask_match = getattr(df[cols].mask, dtype)(cols)

        return mask_match

    def values_in(self, cols, values) -> MaskDataFrameType:
        values = val_to_list(values)
        mask = self.root.data[cols].isin(values)
        return self.root.new(mask.to_frame())

    def pattern(self) -> MaskDataFrameType:
        pass

    def starts_with(self, cols, value) -> MaskDataFrameType:
        mask = self.root.data[cols].str.startswith(value, na=False)
        return self.root.new(mask.to_frame())

    def ends_with(self, cols, value) -> MaskDataFrameType:
        mask = self.root.data[cols].str.endswith(value, na=False)
        return self.root.new(mask.to_frame())

    def contains(self, cols, value, case=True, flags=0, na=False, regex=False) -> MaskDataFrameType:
        if is_list(value):
            value = "|".join(value)
            regex = True
        mask = self.root.data[cols].str.contains(value, case=case, flags=flags, na=na, regex=regex)
        return self.root.new(mask.to_frame())

    def find(self, input_col="*", value=None) -> MaskDataFrameType:
        dfd = self.root.data
        if is_str(value):
            mask = self.root.data[input_col].astype(str).str.match(value, na=False)
        else:
            mask = dfd[input_col] == value
        return self.root.new(mask.to_frame())

    def nulls(self, cols="*", how="any") -> MaskDataFrameType:
        """
        Find the rows that have null values
        :param how:
        :param cols:
        :return:
        """
        df = self.root
        dfd = self.root.data

        if cols is not None:
            subset = parse_columns(df, cols)
            subset_df = dfd[subset]
        else:
            subset_df = dfd

        if how == "all":
            mask = subset_df.isnull().all(axis=1)
        else:
            mask = subset_df.isnull()

        return self.root.new(mask)

    def duplicated(self, cols="*", keep="first") -> MaskDataFrameType:
        """
        Find the rows that have duplicated values

        :param keep:
        :param cols:
        :return:
        """
        df = self.root
        dfd = self.root.data

        if cols is not None:
            subset = parse_columns(df, cols)
            subset_df = dfd[subset]
        else:
            subset_df = dfd

        col_name = cols[0] if is_list(cols) else cols or "duplicated_rows"

        mask = subset_df.duplicated(keep=keep).rename(col_name).to_frame()

        return self.root.new(mask)

    def empty(self, cols) -> MaskDataFrameType:
        """
        Find the rows that do not have any info

        :param cols:
        :return:
        """
        mask = self.root.data[cols] == ""
        return self.root.new(mask)

    def email(self, cols="*") -> MaskDataFrameType:
        return self.root[cols].cols.to_string().cols.match(cols, regex_email)

    def ip(self, cols="*") -> MaskDataFrameType:
        return self.root[cols].cols.to_string().cols.match(cols, regex_ip)

    def url(self, cols="*") -> MaskDataFrameType:
        return self.root[cols].cols.to_string().cols.match(cols, regex_full_url)

    def gender(self, cols="*") -> MaskDataFrameType:
        return self.root[cols].cols.to_string().cols.match(cols, regex_gender)

    def boolean(self, cols="*") -> MaskDataFrameType:
        return self.root.cols.apply(cols, is_bool)
        # return self.root[cols].cols.to_string().cols.match(cols, regex_boolean)

    def zip_code(self, cols="*") -> MaskDataFrameType:
        return self.root[cols].cols.to_string().cols.match(cols, regex_zip_code)

    def credit_card_number(self, cols="*") -> MaskDataFrameType:
        return self.root[cols].cols.to_string().cols.match(cols, regex_credit_card_number)

    def datetime(self, cols="*") -> MaskDataFrameType:
        # df = self.root
        # if df[cols].cols.dtype  == df.constants.
        return self.root.cols.apply(cols, is_datetime)
        # return self.root[cols].cols.to_string().cols.match(cols, regex_date)

    def object(self, cols="*") -> MaskDataFrameType:
        return self.root[cols].cols.apply(cols, is_object)
        # return self.root[cols].cols.to_string().cols.match(cols, is_object)

    def array(self, cols="*") -> MaskDataFrameType:
        return self.root[cols].cols.apply(cols, is_list)
        # return self.root[cols].cols.to_string().cols.match(cols, is_list_value)

    def phone_number(self, cols="*") -> MaskDataFrameType:
        return self.root[cols].cols.to_string().cols.match(cols, regex_phone_number)

    def social_security_number(self, cols="*") -> MaskDataFrameType:
        return self.root[cols].cols.to_string().cols.match(cols, regex_social_security_number)

    def http_code(self, cols="*") -> MaskDataFrameType:
        return self.root[cols].cols.to_string().cols.match(cols, regex_http_code)

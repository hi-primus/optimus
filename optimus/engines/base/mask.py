
import numpy as np

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

    def _to_frame(self, series):
        if callable(getattr(series, "to_frame", False)):
            return series.to_frame()
        return series

    @abstractmethod
    def str(self, cols="*") -> MaskDataFrameType:
        pass

    @abstractmethod
    def int(self, cols="*") -> MaskDataFrameType:
        pass

    @abstractmethod
    def float(self, cols="*") -> MaskDataFrameType:
        pass

    @abstractmethod
    def numeric(self, cols="*") -> MaskDataFrameType:
        pass

    def greater_than(self, cols="*", value=None) -> MaskDataFrameType:
        df = self.root
        return df[cols] > value

    def greater_than_equal(self, cols="*", value=None) -> MaskDataFrameType:
        df = self.root
        return df[cols] >= value

    def less_than(self, cols="*", value=None) -> MaskDataFrameType:
        df = self.root
        return df[cols] < value

    def less_than_equal(self, cols="*", value=None) -> MaskDataFrameType:
        df = self.root
        return df[cols] <= value

    def equal(self, cols="*", value=None) -> MaskDataFrameType:
        df = self.root
        return df[cols] == value

    def not_equal(self, cols="*", value=None) -> MaskDataFrameType:
        df = self.root
        return df[cols] != value

    def missing(self, cols="*") -> MaskDataFrameType:
        """
        Return missing values
        :param cols:
        :return:
        """
        return self.null(cols) | self.empty(cols)

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
        mask_null = df[cols].mask.null(cols)
        return ~(mask_match | mask_null)

    def match(self, cols="*", regex=None, dtype=None) -> MaskDataFrameType:
        if dtype is None:
            return self.match_regex(cols=cols, regex=regex)
        else:
            return self.match_dtype(cols=cols, dtype=dtype)

    def match_regex(self, cols="*", regex="") -> MaskDataFrameType:
        return self.root[cols].cols.apply(cols, self.root.cols.F.match, args=(regex,))

    def match_dtype(self, cols="*", dtype=None) -> MaskDataFrameType:
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

    def value_in(self, cols="*", values=None) -> MaskDataFrameType:
        
        df = self.root
        cols = parse_columns(df, cols)

        values = val_to_list(values)

        mask = df.data[cols].isin(values)
        return df.new(self._to_frame(mask))

    def pattern(self, cols="*", pattern=None) -> MaskDataFrameType:
        
        df = self.root
        cols = parse_columns(df, cols)
        
        return df[cols].cols.pattern() == pattern

    def starts_with(self, cols="*", value=None) -> MaskDataFrameType:
                
        df = self.root
        cols = val_to_list(parse_columns(df, cols))

        mask = None

        for col in cols:
            series = df.data[col].str.startswith(value, na=False)
            if mask is None:
                mask = self._to_frame(series)
            else:
                mask[col] = series


        return df.new(self._to_frame(mask))

    def ends_with(self, cols="*", value=None) -> MaskDataFrameType:
                
        df = self.root
        cols = val_to_list(parse_columns(df, cols))

        mask = None

        for col in cols:
            series = df.data[col].str.endswith(value, na=False)
            if mask is None:
                mask = self._to_frame(series)
            else:
                mask[col] = series

        return df.new(self._to_frame(mask))

    def contains(self, cols="*", value=None, case=True, flags=0, na=False, regex=False) -> MaskDataFrameType:
                
        df = self.root
        cols = val_to_list(parse_columns(df, cols))

        mask = None

        for col in cols:
            series = df.data[col].str.contains(value, case=case, flags=flags, na=na, regex=regex)
            if mask is None:
                mask = self._to_frame(series)
            else:
                mask[col] = series
        
        if is_list(value):
            value = "|".join(value)
            regex = True
        
        return df.new(self._to_frame(mask))

    def find(self, cols="*", value=None) -> MaskDataFrameType:
        
        df = self.root
        cols = val_to_list(parse_columns(df, cols))

        
        if is_str(value):

            mask = None
            for col in cols:
                series = self.root.data[col].astype(str).str.match(value, na=False)
                if mask is None:
                    mask = self._to_frame(series)
                else:
                    mask[col] = series
                    
            return self.root.new(self._to_frame(mask))

        else:
            return df[cols] == value



    def null(self, cols="*", how="any") -> MaskDataFrameType:
        """
        Find the rows that have null values
        :param how:
        :param cols:
        :return:
        """
        df = self.root
        dfd = self.root.data
        cols = val_to_list(parse_columns(df, cols))
        subset_df = dfd[cols]

        if how == "all":
            col_name = cols[0] if len(cols)==1 else "__null__"
            mask = subset_df.isnull().all(axis=1).rename(col_name)
        else:
            mask = subset_df.isnull()

        return self.root.new(self._to_frame(mask))

    
    def none(self, cols="*") -> MaskDataFrameType:
        """
        Find the rows that have None values

        :param cols:
        :return:
        """
        return ~self.numeric(cols) & self.null(cols)

    def nan(self, cols="*") -> MaskDataFrameType:
        """
        Find the rows that have np.nan values

        :param cols:
        :return:
        """
        return self.numeric(cols) & self.null(cols)

    def duplicated(self, cols, keep="first") -> MaskDataFrameType:
        """
        Find the rows that have duplicated values

        :param keep:
        :param cols:
        :return:
        """
        df = self.root
        dfd = self.root.data
        cols = val_to_list(parse_columns(df, cols))

        if cols is not None:
            subset = cols
            subset_df = dfd[subset]
        else:
            subset_df = dfd

        col_name = cols[0] if len(cols)==1 else "__duplicated__"

        mask = subset_df.duplicated(keep=keep, subset=cols).rename(col_name)

        return self.root.new(self._to_frame(mask))

    def empty(self, cols="*") -> MaskDataFrameType:
        """
        Find the rows that do not have any info

        :param cols:
        :return:
        """
        return self.root[cols] == ""

    def email(self, cols="*") -> MaskDataFrameType:
        return self.match_regex(cols, regex_email)

    def ip(self, cols="*") -> MaskDataFrameType:
        return self.match_regex(cols, regex_ip)

    def url(self, cols="*") -> MaskDataFrameType:
        return self.match_regex(cols, regex_full_url)

    def gender(self, cols="*") -> MaskDataFrameType:
        return self.match_regex(cols, regex_gender)

    def boolean(self, cols="*") -> MaskDataFrameType:
        return self.root[cols].cols.apply(cols, is_bool)

    def zip_code(self, cols="*") -> MaskDataFrameType:
        return self.match_regex(cols, regex_zip_code)

    def credit_card_number(self, cols="*") -> MaskDataFrameType:
        return self.match_regex(cols, regex_credit_card_number)

    def datetime(self, cols="*") -> MaskDataFrameType:
        # df = self.root
        # if df[cols].cols.dtype  == df.constants.
        return self.root[cols].cols.apply(cols, is_datetime)
        # return self.match_regex(cols, regex_date)

    def object(self, cols="*") -> MaskDataFrameType:
        return self.root[cols].cols.apply(cols, is_object)
        # return self.match_regex(cols, is_object)

    def array(self, cols="*") -> MaskDataFrameType:
        return self.root[cols].cols.apply(cols, is_list)
        # return self.match_regex(cols, is_list_value)

    def phone_number(self, cols="*") -> MaskDataFrameType:
        return self.match_regex(cols, regex_phone_number)

    def social_security_number(self, cols="*") -> MaskDataFrameType:
        return self.match_regex(cols, regex_social_security_number)

    def http_code(self, cols="*") -> MaskDataFrameType:
        return self.match_regex(cols, regex_http_code)

    #

    def all(self, cols="*") -> MaskDataFrameType:
        
        df = self.root

        mask = None

        for col in df.cols.names(cols):
            _mask = df[col]
            if mask is None:
                mask = _mask
            else:
                mask = mask & _mask

        return mask

    def any(self, cols="*") -> MaskDataFrameType:
        
        df = self.root

        mask = None

        for col in df.cols.names(cols):
            _mask = df[col]
            if mask is None:
                mask = _mask
            else:
                mask = mask | _mask

        return mask

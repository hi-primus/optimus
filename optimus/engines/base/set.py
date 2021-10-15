from typing import Callable

from optimus.helpers.types import *


class BaseSet():
    """Base class for all set implementations"""

    def __init__(self, root: 'DataFrameType'):
        self.root = root

    def __call__(self, cols=None, value=None, default=None, args=[], where=None):
        return self.root.cols.set(cols=cols, value_func=value, args=args, where=where, default=default)

    def all(self, cols, value=None) -> 'DataFrameType':
        return self.__call__(cols, value)

    def _mask(self, cols, true_value=None, default_value=None, args=[], func: Callable = None, **kwargs) -> 'DataFrameType':

        df = self.root

        if cols in df.cols.names():
            input = df[cols]
            input_cols = cols
        elif isinstance(true_value, (self.root.__class__, )):
            input = true_value
            input_cols = input.cols.names()[0]
        else:
            df[cols] = default_value or true_value
            input = df[cols]
            input_cols = cols

        # uses name instead of the function to use it correctly with 'input' instead of with the whole dataframe
        mask = getattr(input.mask, func.__name__)(input_cols, **kwargs)
        return self.__call__(cols, value=true_value, default=default_value, args=args, where=mask)

    # Types

    def str(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.str)

    def int(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.int)

    def float(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.float)

    def numeric(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.numeric)

    def email(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.email)

    def ip(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.ip)

    def url(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.url)

    def gender(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.gender)

    def boolean(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.boolean)

    def zip_code(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.zip_code)

    def credit_card_number(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.credit_card_number)

    def datetime(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.datetime)

    def object(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.object)

    def array(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.array)

    def phone_number(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.phone_number)

    def social_security_number(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.social_security_number)

    def http_code(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.http_code)

    # Other

    def null(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.null)

    def none(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.none)

    def nan(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.nan)

    def empty(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.empty)

    def greater_than(self, cols, true_value=None, default_value=None, args=[], value=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, func=self.root.mask.greater_than, value=value)

    def greater_than_equal(self, cols, true_value=None, default_value=None, args=[], value=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, func=self.root.mask.greater_than_equal, value=value)

    def less_than(self, cols, true_value=None, default_value=None, args=[], value=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, func=self.root.mask.less_than, value=value)

    def less_than_equal(self, cols, true_value=None, default_value=None, args=[], value=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, func=self.root.mask.less_than_equal, value=value)

    def between(self, cols, true_value=None, default_value=None, args=[], lower_bound=None, upper_bound=None, equal=True, bounds=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, func=self.root.mask.between, lower_bound=lower_bound, upper_bound=upper_bound, equal=equal, bounds=bounds)

    def equal(self, cols, true_value=None, default_value=None, args=[], value=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, func=self.root.mask.equal, value=value)

    def not_equal(self, cols, true_value=None, default_value=None, args=[], value=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, func=self.root.mask.not_equal, value=value)

    def missing(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.missing)

    def mismatch(self, cols, value=None, default_value=None, args=[], data_type=None) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.mismatch, data_type=data_type)

    def duplicated(self, cols, value=None, default_value=None, args=[], keep="first") -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.duplicated, keep=keep)

    def unique(self, cols, value=None, default_value=None, args=[], keep="first") -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.unique, keep=keep)

    def match(self, cols, value=None, default_value=None, args=[], regex=None, data_type=None) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.match_data_type, regex=regex, data_type=data_type)

    def match_regex(self, cols, value=None, default_value=None, args=[], regex=None) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.match_regex, regex=regex)

    def match_data_type(self, cols, value=None, default_value=None, args=[], data_type=None) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.match_data_type, data_type=data_type)

    def match_pattern(self, cols, value=None, default_value=None, args=[], pattern=None) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, func=self.root.mask.match_pattern, pattern=pattern)

    def value_in(self, cols, true_value=None, default_value=None, args=[], values=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, func=self.root.mask.value_in, values=values)

    def starts_with(self, cols, true_value=None, default_value=None, args=[], value=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, func=self.root.mask.starts_with, value=value)

    def ends_with(self, cols, true_value=None, default_value=None, args=[], value=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, func=self.root.mask.ends_with, value=value)

    def contains(self, cols, true_value=None, default_value=None, args=[], value=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, func=self.root.mask.contains, value=value)

    def expression(self, cols, true_value=None, default_value=None, args=[], where=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, func=self.root.mask.expression, where=where)

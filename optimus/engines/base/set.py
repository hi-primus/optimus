from optimus.helpers.types import *


class BaseSet():
    """Base class for all set implementations"""

    def __init__(self, root: 'DataFrameType'):
        self.root = root

    def __call__(self, cols=None, value=None, default=None, args=[], where=None):
        return self.root.cols.set(cols=cols, value_func=value, args=args, where=where, default=default)


    def all(self, cols, value=None) -> 'DataFrameType':
        return self.__call__(cols, value)


    def _mask(self, cols, true_value=None, default_value=None, args=[], method: str=None, **kwargs) -> 'DataFrameType':
        
        df = self.root
        
        if cols in df.cols.names():
            input = df[cols]
            input_cols = cols
        elif isinstance(true_value, (self.root.__class__,)):
            input = true_value
            input_cols = input.cols.names()[0]
        else:
            df[cols] = default_value or true_value
            input = df[cols]
            input_cols = cols

        mask = getattr(input.mask, method)(input_cols, **kwargs)
        return self.__call__(cols, value=true_value, default=default_value, args=args, where=mask)

    # Types

    def str(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="str")

    def int(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="int")
    
    def float(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="float")
    
    def numeric(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="numeric")
    
    def email(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="email")
    
    def ip(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="ip")
    
    def url(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="url")
    
    def gender(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="gender")
    
    def boolean(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="boolean")
    
    def zip_code(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="zip_code")
    
    def credit_card_number(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="credit_card_number")
    
    def datetime(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="datetime")
    
    def object(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="object")
    
    def array(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="array")
    
    def phone_number(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="phone_number")
    
    def social_security_number(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="social_security_number")
    
    def http_code(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="http_code")

    # Other    

    def null(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="null")

    def none(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="none")
    
    def nan(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="nan")
    
    def empty(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="empty")

    def greater_than(self, cols, true_value=None, default_value=None, args=[], value=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, method="greater_than", value=value)

    def greater_than_equal(self, cols, true_value=None, default_value=None, args=[], value=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, method="greater_than_equal", value=value)

    def less_than(self, cols, true_value=None, default_value=None, args=[], value=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, method="less_than", value=value)

    def less_than_equal(self, cols, true_value=None, default_value=None, args=[], value=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, method="less_than_equal", value=value)

    def between(self, cols, true_value=None, default_value=None, args=[], lower_bound=None, upper_bound=None, equal=True, bounds=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, method="between", lower_bound=lower_bound, upper_bound=upper_bound, equal=equal, bounds=bounds)

    def equal(self, cols, true_value=None, default_value=None, args=[], value=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, method="equal", value=value)

    def not_equal(self, cols, true_value=None, default_value=None, args=[], value=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, method="not_equal", value=value)

    def missing(self, cols, value=None, default_value=None, args=[]) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="missing")

    def mismatch(self, cols, value=None, default_value=None, args=[], data_type=None) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="mismatch", data_type=data_type)

    def duplicated(self, cols, value=None, default_value=None, args=[], keep="first") -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="duplicated", keep=keep)

    def unique(self, cols, value=None, default_value=None, args=[], keep="first") -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="unique", keep=keep)

    def match(self, cols, value=None, default_value=None, args=[], regex=None, data_type=None) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="match_data_type", regex=regex, data_type=data_type)

    def match_regex(self, cols, value=None, default_value=None, args=[], regex=None) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="match_regex", regex=regex)

    def match_data_type(self, cols, value=None, default_value=None, args=[], data_type=None) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="match_data_type", data_type=data_type)

    def match_pattern(self, cols, value=None, default_value=None, args=[], pattern=None) -> 'DataFrameType':
        return self._mask(cols, true_value=value, default_value=default_value, args=args, method="match_pattern", pattern=pattern)

    def value_in(self, cols, true_value=None, default_value=None, args=[], values=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, method="value_in", values=values)
        
    def starts_with(self, cols, true_value=None, default_value=None, args=[], value=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, method="starts_with", value=value)

    def ends_with(self, cols, true_value=None, default_value=None, args=[], value=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, method="ends_with", value=value)

    def contains(self, cols, true_value=None, default_value=None, args=[], value=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, method="contains", value=value)
    
    def expression(self, cols, true_value=None, default_value=None, args=[], where=None) -> 'DataFrameType':
        return self._mask(cols, true_value=true_value, default_value=default_value, args=args, method="expression", where=where)


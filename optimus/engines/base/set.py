from optimus.helpers.types import DataFrameType


class BaseSet():
    """Base class for all set implementations"""

    def __init__(self, root):
        self.root = root

    def __call__(self, col_name=None, value=None, default=None, where=None):
        return self.root.cols.set(col_name=col_name, value=value, where=where, default=default)


    def all(self, col_name, value=None) -> DataFrameType:
        return self.__call__(col_name, value)


    def _mask(self, col_name, true_value=None, default_value=None, method: str=None, *args, **kwargs) -> DataFrameType:
        
        df = self.root
        
        if col_name in df.cols.names():
            input = df[col_name]
            input_col_name = col_name
        elif isinstance(true_value, (self.root.__class__,)):
            input = true_value
            input_col_name = input.cols.names()[0]
        else:
            df[col_name] = default_value or true_value
            input = df[col_name]
            input_col_name = col_name

        mask = getattr(input.mask, method)(input_col_name, *args, **kwargs)
        return self.__call__(col_name, value=true_value, default=default_value, where=mask)

    # Types

    def str(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="str")

    def int(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="int")
    
    def float(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="float")
    
    def numeric(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="numeric")
    
    def email(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="email")
    
    def ip(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="ip")
    
    def url(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="url")
    
    def gender(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="gender")
    
    def boolean(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="boolean")
    
    def zip_code(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="zip_code")
    
    def credit_card_number(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="credit_card_number")
    
    def datetime(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="datetime")
    
    def object(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="object")
    
    def array(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="array")
    
    def phone_number(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="phone_number")
    
    def social_security_number(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="social_security_number")
    
    def http_code(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="http_code")

    # Other    

    def null(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="null")

    def none(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="none")
    
    def nan(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="nan")
    
    def empty(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="empty")

    def greater_than(self, col_name, true_value=None, default_value=None, value=None) -> DataFrameType:
        return self._mask(col_name, true_value=true_value, default_value=default_value, method="greater_than", value=value)

    def greater_than_equal(self, col_name, true_value=None, default_value=None, value=None) -> DataFrameType:
        return self._mask(col_name, true_value=true_value, default_value=default_value, method="greater_than_equal", value=value)

    def less_than(self, col_name, true_value=None, default_value=None, value=None) -> DataFrameType:
        return self._mask(col_name, true_value=true_value, default_value=default_value, method="less_than", value=value)

    def less_than_equal(self, col_name, true_value=None, default_value=None, value=None) -> DataFrameType:
        return self._mask(col_name, true_value=true_value, default_value=default_value, method="less_than_equal", value=value)

    def equal(self, col_name, true_value=None, default_value=None, value=None) -> DataFrameType:
        return self._mask(col_name, true_value=true_value, default_value=default_value, method="equal", value=value)

    def not_equal(self, col_name, true_value=None, default_value=None, value=None) -> DataFrameType:
        return self._mask(col_name, true_value=true_value, default_value=default_value, method="not_equal", value=value)

    def missing(self, col_name, value=None, default_value=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="missing")

    def mismatch(self, col_name, value=None, default_value=None, dtype=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="mismatch", dtype=dtype)

    def duplicated(self, col_name, value=None, default_value=None, keep="first") -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="duplicated", keep=keep)

    def unique(self, col_name, value=None, default_value=None, keep="first") -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="unique", keep=keep)

    def match(self, col_name, value=None, default_value=None, regex=None, dtype=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="match_dtype", regex=regex, dtype=dtype)

    def match_regex(self, col_name, value=None, default_value=None, regex=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="match_regex", regex=regex)

    def match_dtype(self, col_name, value=None, default_value=None, dtype=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="match_dtype", dtype=dtype)

    def match_pattern(self, col_name, value=None, default_value=None, pattern=None) -> DataFrameType:
        return self._mask(col_name, true_value=value, default_value=default_value, method="match_pattern", pattern=pattern)

    def value_in(self, col_name, true_value=None, default_value=None, values=None) -> DataFrameType:
        return self._mask(col_name, true_value=true_value, default_value=default_value, method="value_in", values=values)
        
    def starts_with(self, col_name, true_value=None, default_value=None, value=None) -> DataFrameType:
        return self._mask(col_name, true_value=true_value, default_value=default_value, method="starts_with", value=value)

    def ends_with(self, col_name, true_value=None, default_value=None, value=None) -> DataFrameType:
        return self._mask(col_name, true_value=true_value, default_value=default_value, method="ends_with", value=value)

    def contains(self, col_name, true_value=None, default_value=None, value=None) -> DataFrameType:
        return self._mask(col_name, true_value=true_value, default_value=default_value, method="contains", value=value)
    
    def expression(self, col_name, true_value=None, default_value=None, where=None) -> DataFrameType:
        return self._mask(col_name, true_value=true_value, default_value=default_value, method="expression", where=where)


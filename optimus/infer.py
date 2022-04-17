# This file need to be send to the cluster via .addPyFile to handle the pickle problem
# This is outside the optimus folder on purpose because it cause problem importing optimus when using de udf.
# This can not import any optimus file unless it's imported via addPyFile in Spark
import datetime
import math
import os
import re
from ast import literal_eval

import fastnumbers
import hidateinfer
import pandas as pd

# This function return True or False if a string can be converted to any datatype.
from optimus.helpers.constants import CURRENCIES, US_STATES_NAMES


class Trie:
    """Regex::Trie in Python. Creates a Trie out of a list of words. The trie can be exported to a Regex pattern.
    The corresponding Regex should match much faster than a simple Regex union."""

    def __init__(self):
        self.data = {}

    def add(self, word):
        ref = self.data
        for char in word:
            ref[char] = char in ref and ref[char] or {}
            ref = ref[char]
        ref[''] = 1

    def dump(self):
        return self.data

    def quote(self, char):
        return re.escape(char)

    def _pattern(self, pData):
        data = pData
        if "" in data and len(data.keys()) == 1:
            return None

        alt = []
        cc = []
        q = 0
        for char in sorted(data.keys()):
            if isinstance(data[char], dict):
                try:
                    recurse = self._pattern(data[char])
                    alt.append(self.quote(char) + recurse)
                except:
                    cc.append(self.quote(char))
            else:
                q = 1
        cconly = not len(alt) > 0

        if len(cc) > 0:
            if len(cc) == 1:
                alt.append(cc[0])
            else:
                alt.append('[' + ''.join(cc) + ']')

        if len(alt) == 1:
            result = alt[0]
        else:
            result = "(?:" + "|".join(alt) + ")"

        if q:
            if cconly:
                result += "?"
            else:
                result = "(?:%s)?" % result
        return result

    def pattern(self):
        return self._pattern(self.dump())

    def regex(self, words):

        for word in words:
            self.add(word)
        return re.compile(r"\b" + self.pattern() + r"\b", re.IGNORECASE)


def is_datetime_str(value: str):
    try:
        pdi = hidateinfer.infer([value])
        code_count = pdi.count('%')
        value_code_count = value.count('%')
        return code_count >= 2 and value_code_count < code_count and code_count >= len(value) / 7 and any(
            [c in pdi for c in ['/', '-', ':', '%b', '%B']])
    except Exception:
        return False


def str_to_date_format(value, date_format):
    # Check this https://stackoverflow.com/questions/17134716/convert-dataframe-column-type-from-string-to-datetime-dd-mm-yyyy-format
    try:
        pd.to_datetime(value).dt.strftime(date_format)
        return True
    except ValueError:
        return False


def str_to_null(value):
    value = value.lower()
    if value == "null":
        return True
    else:
        return False


def is_null(value):
    return pd.isnull(value)


regex_us_state_compiled = Trie().regex(US_STATES_NAMES)


def str_to_data_type(_value, _data_types):
    """
    Check if value can be parsed to a tuple or and list.
    Because Spark can handle tuples we will try to transform tuples to arrays
    :param _value:
    :param _data_types:
    :return:
    """
    # return True if isinstance(_value, str) else False
    try:
        if isinstance(literal_eval((_value.encode('ascii', 'ignore')).decode("utf-8")), _data_types):
            return True
    except (ValueError, SyntaxError, AttributeError):
        return False


def is_list_value(value):
    """
    Check if an object is a list
    :param value:
    :return:
    """
    return isinstance(value, list)


def is_list_str(value):
    # try:
    #     json.loads(value)
    #     return True
    # except JSONDecodeError:
    #     return False
    # ^ \[. *\]$
    return str_to_data_type(value, (list, tuple))


def is_object_str(value):
    # return False
    return str_to_data_type(value, (dict, set))


regex_str = r"."

regex_int = r"(^\d+\.[0]+$)|(^\d+$)"  # For cudf 0.14 regex_int = r"^\d+$"
regex_int_compiled = re.compile(regex_int)

regex_decimal = r"(^\d+\.\d+$)|(^\d+$)"
regex_decimal_compiled = re.compile(regex_decimal)

regex_non_int_decimal = r"^(\d+\.\d+)$"

regex_boolean = r"\bTrue\b|\bFalse\b|\btrue\b|\bfalse\b|\b0\b|\b1\b"
regex_boolean_compiled = re.compile(regex_boolean)

regex_list = r"^\[.*\]$"
regex_list_compiled = re.compile(regex_list)

regex_dict = r"^\{.*\}$"
regex_dict_compiled = re.compile(regex_dict)

regex_tuple = r"^\(.*\)$"
regex_tuple_compiled = re.compile(regex_tuple)


def is_bool_str(value, compile=False):
    return str_to(str(value), regex_boolean, regex_boolean_compiled, compile)


regex_gender = r"\bmale\b|\bfemale\b"
regex_gender_compiled = re.compile(regex_gender)


def is_gender(value, compile=False):
    return str_to(value, regex_gender, regex_gender_compiled, compile)


regex_full_url = (r'^(?:http|ftp)s?://'  # http:// or https://
                  r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
                  r'localhost|'  # localhost...
                  r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
                  r'(?::\d+)?'  # optional port
                  r'(?:/?|[/?]\S+)$')

regex_full_url_compiled = re.compile(regex_full_url, re.IGNORECASE)


def str_to_full_url(value, compile=False):
    return str_to(value, regex_url, regex_url_compiled, compile)


regex_url = r"([a-zA-Z]+\.)?([a-zA-Z0-9-]+\.)+([a-zA-Z]+)\/?"

regex_url_compiled = re.compile(regex_url, re.IGNORECASE)


def str_to_url(value, compile=False):
    return str_to(value, regex_url, regex_url_compiled, compile)


regex_ipv4_address = r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}"
regex_ipv4_address_compiled = re.compile(regex_ipv4_address, re.IGNORECASE)

# regex_ipv6_address = r"(?i)(?:[\da-f]{0,4}:){1,7}(?:(?<ipv4>(?:(?:25[0-5]|2[0-4]\d|1?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|1?\d\d?))|[\da-f]{0,4})"
IPV4SEG = r'(?:25[0-5]|(?:2[0-4]|1{0,1}[0-9]){0,1}[0-9])'
IPV4ADDR = r'(?:(?:' + IPV4SEG + r'\.){3,3}' + IPV4SEG + r')'
IPV6SEG = r'(?:(?:[0-9a-fA-F]){1,4})'
IPV6GROUPS = (
    r'(?:' + IPV6SEG + r':){7,7}' + IPV6SEG,  # 1:2:3:4:5:6:7:8
    r'(?:' + IPV6SEG + r':){1,7}:',  # 1::                                 1:2:3:4:5:6:7::
    r'(?:' + IPV6SEG + r':){1,6}:' + IPV6SEG,  # 1::8               1:2:3:4:5:6::8   1:2:3:4:5:6::8
    r'(?:' + IPV6SEG + r':){1,5}(?::' + IPV6SEG + r'){1,2}',  # 1::7:8             1:2:3:4:5::7:8   1:2:3:4:5::8
    r'(?:' + IPV6SEG + r':){1,4}(?::' + IPV6SEG + r'){1,3}',  # 1::6:7:8           1:2:3:4::6:7:8   1:2:3:4::8
    r'(?:' + IPV6SEG + r':){1,3}(?::' + IPV6SEG + r'){1,4}',  # 1::5:6:7:8         1:2:3::5:6:7:8   1:2:3::8
    r'(?:' + IPV6SEG + r':){1,2}(?::' + IPV6SEG + r'){1,5}',  # 1::4:5:6:7:8       1:2::4:5:6:7:8   1:2::8
    IPV6SEG + r':(?:(?::' + IPV6SEG + r'){1,6})',  # 1::3:4:5:6:7:8     1::3:4:5:6:7:8   1::8
    r':(?:(?::' + IPV6SEG + r'){1,7}|:)',  # ::2:3:4:5:6:7:8    ::2:3:4:5:6:7:8  ::8       ::
    r'fe80:(?::' + IPV6SEG + r'){0,4}%[0-9a-zA-Z]{1,}',
    # fe80::7:8%eth0     fe80::7:8%1  (link-local IPv6 addresses with zone index)
    r'::(?:ffff(?::0{1,4}){0,1}:){0,1}[^\s:]' + IPV4ADDR,
    # ::255.255.255.255  ::ffff:255.255.255.255  ::ffff:0:255.255.255.255 (IPv4-mapped IPv6 addresses and IPv4-translated addresses)
    r'(?:' + IPV6SEG + r':){1,4}:[^\s:]' + IPV4ADDR,
    # 2001:db8:3:4::192.0.2.33  64:ff9b::192.0.2.33 (IPv4-Embedded IPv6 Address)
)
regex_ipv6_address = '|'.join(['(?:{})'.format(g) for g in IPV6GROUPS[::-1]])  # Reverse rows for greedy match

regex_ipv6_address_compiled = re.compile(regex_ipv6_address, re.IGNORECASE)


def str_to_ip(value, compile=False):
    return str_to(value, regex_ipv4_address, regex_ipv4_address_compiled, compile)


# regex_email = r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)" # This do not work in CUDF/RE2
regex_email = r"^[^@]+@[^@]+\.[a-zA-Z]{2,}$"
regex_email_compiled = re.compile(regex_email, re.IGNORECASE)


def is_email(value, compile=False):
    return str_to(value, regex_email, regex_email_compiled, compile)


# Reference https://www.regular-expressions.info/creditcard.html
# https://codereview.stackexchange.com/questions/74797/credit-card-checking
regex_credit_card_number = (r'(4(?:\d{12}|\d{15})'  # Visa
                            r'|5[1-5]\d{14}'  # Mastercard
                            r'|6011\d{12}'  # Discover (incomplete?)
                            r'|7\d{15}'  # What's this?
                            r'|3[47]\d{13}'  # American Express
                            r')$')

regex_credit_card_compiled = re.compile(regex_credit_card_number)


def is_credit_card_number(value, compile=False):
    return str_to(value, regex_credit_card_number, regex_credit_card_compiled, compile)


regex_zip_code = r"^(\d{4,5}(?:[- ]\d{4})?)$"
regex_zip_code_compiled = re.compile(regex_zip_code, re.IGNORECASE)


def is_zip_code(value, compile=False):
    return str_to(value, regex_zip_code, regex_zip_code_compiled, compile)


def is_missing(value):
    return value == ""


regex_social_security_number = "^([1-9])(?!\1{2}-\1{2}-\1{4})[1-9]{2}-[1-9]{2}-[1-9]{4}"
regex_social_security_number_compiled = re.compile(regex_social_security_number, re.IGNORECASE)


def is_social_security_number(value, compile=False):
    return str_to(value, regex_social_security_number, regex_social_security_number_compiled, compile)


regex_http_code = "^[1-5][0-9][0-9]$"
regex_http_code_compiled = re.compile(regex_http_code)


def is_http_code(value, compile=False):
    return str_to(value, regex_http_code, regex_http_code_compiled, compile)


# Reference https://stackoverflow.com/questions/8634139/phone-validation-regex
regex_phone_number = r"^(\+\d{1,2}\s?)?1?\-?\.?\s?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$"
regex_phone_number_compiled = re.compile(regex_phone_number, re.IGNORECASE)


def is_phone_number(value, compile=False):
    # print("value",value, str_to(value, regex_phone_number, regex_phone_number_compiled, compile))
    return str_to(value, regex_phone_number, regex_phone_number_compiled, compile)


regex_BAN = r"^^[0-9]{7,14}$"
regex_BAN_compiled = re.compile(regex_BAN, re.IGNORECASE)

regex_uuid = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
regex_uuid_compiled = re.compile(regex_uuid, re.IGNORECASE)

regex_mac_address = "^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$"
regex_mac_address_compiled = re.compile(regex_mac_address, re.IGNORECASE)

regex_driver_license = r"\b[A-Z](?:\d[- ]*){14}\b"
regex_driver_license_compiled = re.compile(regex_driver_license, re.IGNORECASE)

regex_address = r"\d+(\s+\w+){1,}\s+(?:st(?:\.|reet)?|dr(?:\.|ive)?|pl(?:\.|ace)?|ave(?:\.|nue)?|rd|road|lane|drive|way|court|plaza|square|run|parkway|point|pike|square|driveway|trace|park|terrace|blvd)"
regex_address_compiled = re.compile(regex_address, re.IGNORECASE)


def str_to(value, regex, compiled_regex, compile=False):
    value = str(value)
    if value is None:
        result = False
    else:
        if compile is True:
            regex = compiled_regex
        else:
            regex = regex

        result = bool(re.match(regex, value))
    return result


def str_to_int(value):
    return True if fastnumbers.isint(value) else False


def str_to_decimal(value):
    return True if fastnumbers.isfloat(value) else False


def str_to_str(value):
    return True if isinstance(value, str) else False


regex_currencies = "|".join(list(CURRENCIES.keys()))
regex_currencies_compiled = re.compile(regex_currencies)


def str_to_currency(value, compile=False):
    return str_to(value, regex_boolean, regex_boolean_compiled, compile)


def is_nan(value):
    """
    Check if a value is nan
    :param value:
    :return:
    """
    result = False
    if is_str(value):
        if value.lower() == "nan":
            result = True
    elif math.isnan(value):
        result = True
    return result


def is_none(value):
    """
    Check if a value is none
    :param value:
    :return:
    """
    result = False
    if is_str(value):
        if value.lower() == "none":
            result = True
    elif value is None:
        result = True
    return result


def is_same_class(class1, class2):
    """
    Check if 2 class are the same
    :param class1:
    :param class2:
    :return:
    """
    return class1 == class2


def is_(value, type_):
    """
    Check if a value is instance of a class
    :param value:
    :param type_:
    :return:
    """

    return isinstance(value, type_)


def is_type(type1, type2):
    """
    Check if a value is a specific class
    :param type1:
    :param type2:
    :return:
    """
    return type1 == type2


def is_function(value):
    """
    Check if a param is a function
    :param value: object to check for
    :return:
    """
    return hasattr(value, '__call__')


def is_empty_function(value):
    """
    Returns True if f is an empty function.
    """

    if not is_function(value):
        return False

    def empty_func():
        pass

    def empty_func_with_docstring():
        """
        Empty function with docstring.
        """
        pass

    empty_lambda = lambda: None

    empty_lambda_with_docstring = lambda: None
    empty_lambda_with_docstring.__doc__ = """Empty function with docstring."""

    def constants(f):
        """
        Return a tuple containing all the constants of a function without a docstring
        """
        return tuple(
            x
            for x in f.__code__.co_consts
            if x != f.__doc__
        )

    return (
                   value.__code__.co_code == empty_func.__code__.co_code and
                   constants(value) == constants(empty_func)
           ) or (
                   value.__code__.co_code == empty_func_with_docstring.__code__.co_code and
                   constants(value) == constants(empty_func_with_docstring)
           ) or (
                   value.__code__.co_code == empty_lambda.__code__.co_code and
                   constants(value) == constants(empty_lambda)
           ) or (
                   value.__code__.co_code == empty_lambda_with_docstring.__code__.co_code and
                   constants(value) == constants(empty_lambda_with_docstring)
           )


def is_list(value, mode=None):
    """
    Check if a string or any not string value is a python list
    :param value:
    :return:
    """
    if mode == "string":
        result = is_list_str(value)
    else:
        result = is_list_value(value)

    return result


def is_object(value):
    """
    Check if a string or any not string value is a python list
    :param value:
    :return:
    """
    return True if is_object_value(value) or is_object_str(value) else False


def is_bool(value):
    if is_bool_value(value):
        return True
    elif is_bool_value(value):
        return True
    return False


def is_list_empty(value):
    """
    Check is a list is empty
    :param value:
    :return:
    """
    return len(value) == 0


def is_dict(value):
    """
    Check if an object is a list
    :param value:
    :return:
    """
    return isinstance(value, dict)


def is_tuple(value):
    """
    Check if an object is a tuple
    :param value:
    :return:
    """
    return isinstance(value, tuple)


def is_list_or_tuple(value):
    """
    Check if an object is a list or a tuple
    :param value:
    :return:
    """
    return isinstance(value, list)


def is_list_of_int(value):
    """
    Check if an object is a list of integers
    :param value:
    :return:
    """
    return isinstance(value, list) and all(isinstance(elem, int) for elem in value)


def is_list_of_dicts(value):
    """
    Check if an object is a list of dictionaries
    :param value:
    :return:
    """
    return isinstance(value, list) and all(isinstance(elem, dict) for elem in value)


def is_list_with_dicts(value):
    """
    Check if an object is a list with some dictionaries
    :param value:
    :return:
    """
    return isinstance(value, list) and any(isinstance(elem, dict) for elem in value)


def is_list_of_float(value):
    """
    Check if an object is a list of floats
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, float) for elem in value)


def is_list_of_str_or_int(value):
    """
    Check if an object is a string or an integer
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, (int, str)) for elem in value)


def is_list_of_str_or_num(value):
    """
    Check if an object is string, integer or float
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, (str, int, float)) for elem in value)


def is_list_of_dask_dataframes(value):
    """
    Check if an object is a Spark DataFrame
    :param value:
    :return:
    """
    from dask.dataframe.core import DataFrame as DaskDataFrame
    return isinstance(value, list) and all(isinstance(elem, DaskDataFrame) for elem in value)


def is_filepath(file_path):
    """
    Check if a value ia a valid file path
    :param file_path:
    :return:
    """
    # the file is there
    if os.path.exists(file_path):
        return True
    # the file does not exists but write privileges are given
    elif os.access(os.path.dirname(file_path), os.W_OK):
        return True
    # can not write there
    else:
        return False


def is_ipv4(value):
    """
    Check if a value is valid ip
    :param value:
    :return:
    """
    return re.match(regex_ipv4_address_compiled, value)


def is_list_of_str(value):
    """
    Check if all elements in a list are strings
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, str) for elem in value)


def is_list_of_numeric(value):
    """
    Check if all elements in a list are int or float
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, (int, float)) for elem in value)


def is_list_of_list(value):
    """
    Check if all elements in a list are tuples
    :param value:
    :return:
    """

    return bool(value) and isinstance(value, list) and all(isinstance(elem, list) for elem in value)


def is_list_of_tuples(value):
    """
    Check if all elements in a list are tuples
    :param value:
    :return:
    """

    return bool(value) and isinstance(value, list) and all(isinstance(elem, tuple) for elem in value)


def is_list_of_one_element(value):
    """
    Check if a var is a single element
    :param value:
    :return:
    """
    if is_list_value(value):
        return len(value) == 1


def is_dict_of_one_element(value):
    """
    Check if a var is a single element
    :param value:
    :return:
    """
    if is_dict(value):
        return len(value) == 1


def is_one_element(value):
    """
    Check if a var is a single element
    :param value:
    :return:
    """
    return isinstance(value, (str, int, float, bool))


def is_num_or_str(value):
    """
    Check if a var is numeric(int, float) or string
    :param value:
    :return:
    """
    return isinstance(value, (int, float, str))


def is_str_or_int(value):
    """
    Check if a var is a single element
    :param value:
    :return:
    """

    return isinstance(value, (str, int))


def is_numeric(value):
    """
    Check if a var is a single element
    :param value:
    :return:
    """
    return isinstance(value, (int, float))


def is_numeric_like(value):
    """
    Check if a var is a single element
    :param value:
    :return:
    """
    return fastnumbers.isfloat(value) or fastnumbers.isintlike(value)


def is_str(value):
    """
    Check if an object is a string
    :param value:
    :return:
    """
    # Seems 20% faster than
    return isinstance(value, str)
    # return True if type("str") == "str" else False


def is_object_value(value):
    """
    Check if an object is a string
    :param value:
    :return:
    """
    return isinstance(value, object)


def is_list_of_dask_futures(value):
    """
    Check if an object is a list of strings
    :param value:
    :return:
    """
    from dask import distributed
    return bool(value) and isinstance(value, list) and all(
        isinstance(elem, distributed.client.Future) for elem in value)


def is_dask_future(value):
    """
    Check if an object is a list of strings
    :param value:
    :return:
    """
    from dask import distributed
    return isinstance(value, distributed.client.Future)


def is_float(value):
    return fastnumbers.isfloat(value, allow_nan=True)


def is_int(value):
    """
    Check if an object is an integer
    :param value:
    :return:
    """
    return isinstance(value, int) and not isinstance(value, bool)


def is_int_like(value):
    """
    Check if an object is an integer
    :param value:
    :return:
    """
    return fastnumbers.isintlike(value)


def is_float_like(value):
    """
    Check if a var is a float
    :param value:
    :return:
    """
    return fastnumbers.isfloat(value)


def is_url(value):
    regex = re.compile(
        r'^(?:http|ftp|hdfs)s?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)

    return re.match(regex, value)


def is_float(value):
    """
    Check if an object is an integer
    :param value:
    :return:
    """
    return isinstance(value, float)


def is_bool_value(value):
    return isinstance(value, bool)


def is_datetime(value):
    """
    Check if an object is a datetime
    :param value:
    :return:
    """
    result = False
    if isinstance(value, datetime.datetime):
        result = True
    # else:
    #     result = is_datetime_str(str(value))

    return result


def is_valid_datetime_format(value):
    try:
        now = datetime.datetime.strftime(datetime.datetime.now(tz=datetime.timezone.utc), value)
        datetime.datetime.strptime(now, value)
    except ValueError:
        return False
    except re.error:
        return False

    return True


def is_binary(value):
    """
    Check if an object is a bytearray
    :param value:
    :return:
    """
    return isinstance(value, bytearray)


def is_date(value):
    """
    Check if an object is a date
    :param value:
    :return:
    """

    return isinstance(value, datetime.date)

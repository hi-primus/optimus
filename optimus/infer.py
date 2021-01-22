# This file need to be send to the cluster via .addPyFile to handle the pickle problem
# This is outside the optimus folder on purpose because it cause problem importing optimus when using de udf.
# This can not import any optimus file unless it's imported via addPyFile
import datetime
import math
import os
import re
from ast import literal_eval

import fastnumbers
import pandas as pd
import pendulum
from dask import distributed
from dask.dataframe.core import DataFrame as DaskDataFrame

# This function return True or False if a string can be converted to any datatype.
from optimus.helpers.constants import ProfilerDataTypes
from optimus.helpers.raiseit import RaiseIt


def str_to_date(_value, date_format=None):
    try:
        # date_format = "DD/MM/YYYY"
        pendulum.parse(_value, strict=False)
        return True
    except ValueError:
        return False


def str_to_date_format(_value, date_format):
    # Check this https://stackoverflow.com/questions/17134716/convert-dataframe-column-type-from-string-to-datetime-dd-mm-yyyy-format
    try:
        pendulum.from_format(_value, date_format)
        return True
    except ValueError:
        return False


def str_to_null(_value):
    _value = _value.lower()
    if _value == "null":
        return True
    else:
        return False


def is_null(_value):
    if pd.isnull(_value):
        return True
    else:
        return False


def str_to_data_type(_value, _dtypes):
    """
    Check if value can be parsed to a tuple or and list.
    Because Spark can handle tuples we will try to transform tuples to arrays
    :param _value:
    :param _dtypes:
    :return:
    """
    # return True if isinstance(_value, str) else False
    try:
        if isinstance(literal_eval((_value.encode('ascii', 'ignore')).decode("utf-8")), _dtypes):
            return True
    except (ValueError, SyntaxError, AttributeError):
        return False


def str_to_array(_value):
    return False
    # return str_to_data_type(_value, (list, tuple))


def str_to_object(_value):
    return False
    # return str_to_data_type(_value, (dict, set))


regex_int = r"^\d+$"  # For cudf 0.14 regex_int = r"^\d+$" # For cudf 0.14
regex_decimal = r"^\d+\.\d$"

regex_boolean = r"\btrue\b|\bfalse\b"
regex_boolean_compiled = re.compile(regex_boolean)


def str_to_boolean(value, compile=False):
    return str_to(value, regex_boolean, regex_boolean_compiled, compile)


regex_gender = r"\bmale\b|\bfemale\b"
regex_gender_compiled = re.compile(regex_gender)


def str_to_gender(value, compile=False):
    return str_to(value, regex_gender, regex_gender_compiled, compile)


regex_full_url = r"^((.*):)?(//)((\w+)\.)?([A-Za-z0-9\-\.]+):?(\d+)?(/|$)?(\w+\.?\w+)?\??(\w.+)?$"

regex_url = r"(http|https|ftp|s3):\/\/.?[a-zA-Z]*.\w*.[a-zA-Z0-9]*\/?[a-zA-z_-]*.?[a-zA-Z]*\/?"

regex_url_compiled = re.compile(regex_url, re.IGNORECASE)


def str_to_url(value, compile=False):
    return str_to(value, regex_url, regex_url_compiled, compile)


regex_ip = r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}"
regex_ip_compiled = re.compile(regex_ip, re.IGNORECASE)


def str_to_ip(value, compile=False):
    return str_to(value, regex_ip, regex_ip_compiled, compile)


# regex_email = r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)" # This do not work in CUDF/RE2
regex_email = r"^[^@]+@[^@]+\.[a-zA-Z]{2,}$"
regex_email_compiled = re.compile(regex_email, re.IGNORECASE)


def str_to_email(value, compile=False):
    return str_to(value, regex_email, regex_email_compiled, compile)


# Reference https://www.regular-expressions.info/creditcard.html
# https://codereview.stackexchange.com/questions/74797/credit-card-checking
regex_credit_card = (r'(4(?:\d{12}|\d{15})'  # Visa
                     r'|5[1-5]\d{14}'  # Mastercard
                     r'|6011\d{12}'  # Discover (incomplete?)
                     r'|7\d{15}'  # What's this?
                     r'|3[47]\d{13}'  # American Express
                     r')$')

regex_credit_card_compiled = re.compile(regex_credit_card)


def str_to_credit_card(value, compile=False):
    return str_to(value, regex_credit_card, regex_credit_card_compiled, compile)


regex_zip_code = r"^(\d{5})([- ])?(\d{4})?$"
regex_zip_code_compiled = re.compile(regex_zip_code, re.IGNORECASE)


def str_to_zip_code(value, compile=False):
    return str_to(value, regex_zip_code, regex_zip_code_compiled, compile)


regex_missing = r" "
regex_missing_compiled = re.compile(regex_missing, re.IGNORECASE)


def str_to_missing(value, compile=False):
    return str_to(value, regex_missing, regex_missing_compiled, compile)


regex_social_security_number = "^([1-9])(?!\1{2}-\1{2}-\1{4})[1-9]{2}-[1-9]{2}-[1-9]{4}"
regex_social_security_number_compiled = re.compile(regex_social_security_number, re.IGNORECASE)


def str_to_social_security_number(value, compile=False):
    return str_to(value, regex_social_security_number, regex_social_security_number_compiled, compile)


regex_http_code = "/^[1-5][0-9][0-9]$/"
regex_http_code_compiled = re.compile(regex_http_code, re.IGNORECASE)


def str_to_http_code(value, compile=False):
    return str_to(value, regex_http_code, regex_http_code_compiled, compile)


# Reference https://stackoverflow.com/questions/8634139/phone-validation-regex
regex_phone_number = r"/\(?([0-9]{3})\)?([ .-]?)([0-9]{3})\2([0-9]{4})/"
regex_phone_number_compiled = re.compile(regex_phone_number, re.IGNORECASE)


def str_to_phone_number(value, compile=False):
    return str_to(value, regex_phone_number, regex_phone_number_compiled, compile)


# States
US_STATES_NAMES = ["alabama",
                   "alaska",
                   "american samoa",
                   "arizona",
                   "arkansas",
                   "california",
                   "colorado",
                   "connecticut",
                   "delaware",
                   "district of columbia",
                   "federated states of micronesia",
                   "florida",
                   "georgia",
                   "guam",
                   "hawaii",
                   "idaho",
                   "illinois",
                   "indiana",
                   "iowa",
                   "kansas",
                   "kentucky",
                   "louisiana",
                   "maine",
                   "marshall islands",
                   "maryland",
                   "massachusetts",
                   "michigan",
                   "minnesota",
                   "mississippi",
                   "missouri",
                   "montana",
                   "nebraska",
                   "nevada",
                   "new hampshire",
                   "new jersey",
                   "new mexico",
                   "new york",
                   "north carolina",
                   "north dakota",
                   "northern mariana islands",
                   "ohio",
                   "oklahoma",
                   "oregon",
                   "palau",
                   "pennsylvania",
                   "puerto rico",
                   "rhode island",
                   "south carolina",
                   "south dakota",
                   "tennessee",
                   "texas",
                   "utah",
                   "vermont",
                   "virgin islands",
                   "virginia",
                   "washington",
                   "west virginia",
                   "wisconsin",
                   "wyoming"
                   ]

US_STATES_CODE = [
    "al",
    "ak",
    "as",
    "az",
    "ar",
    "ca",
    "co",
    "ct",
    "de",
    "dc",
    "fm",
    "fl",
    "ga",
    "gu",
    "hi",
    "id",
    "il",
    "in",
    "ia",
    "ks",
    "ky",
    "la",
    "me",
    "mh",
    "md",
    "ma",
    "mi",
    "mn",
    "ms",
    "mo",
    "mt",
    "ne",
    "nv",
    "nh",
    "nj",
    "nm",
    "ny",
    "nc",
    "nd",
    "mp",
    "oh",
    "ok",
    "or",
    "pw",
    "pa",
    "pr",
    "ri",
    "sc",
    "sd",
    "tn",
    "tx",
    "ut",
    "vt",
    "vi",
    "va",
    "wa",
    "wv",
    "wi",
    "wy"
]


def str_to(value, regex, compiled_regex, compile=False):
    if value is None:
        result = False
    else:
        if compile is True:
            regex = compiled_regex
        else:
            regex = regex
        result = bool(re.match(regex, value))
    return result


def str_to_int(_value):
    return True if fastnumbers.isint(_value) else False


def str_to_decimal(_value):
    return True if fastnumbers.isfloat(_value) else False


def str_to_str(_value):
    return True if isinstance(_value, str) else False


currencies = {"$": "dollar",
              "¢": "cent",
              "£": "point",
              "€": "euro",
              "¥": "yen",
              "₹": "indian rupee",
              "₽": "ruble",
              "元": "yuan",
              "¤": "currency",
              "₠": "euro-currency",
              "₡": "colon",
              "₢": "cruzeiro",
              "₣": "french franc",
              "₤": "lira",
              "₥": "mill",
              "₦": "naira",
              "₧": "peseta",
              "₨": "rupee",
              "₩": "won",
              "₪": "new shequel",
              "₫": "dong",
              "₭": "kip",
              "₮": "tugrik",
              "₯": "drachma",
              "₰": "german penny",
              "₱": "peso",
              "₲": "guarani",
              "₳": "austral",
              "₴": "hryvnia",
              "₵": "cedi",
              "₶": "livre tournois",
              "₸": "tenge",
              "₺": "turkish lira",
              "₼": "manat",
              "৲": "bengali rupee mark",
              "৳": "bengali rupee sign",
              "૱": "gujarati rupee sign",
              "௹": "tamil rupee sign",
              "฿": "thai currency bath",
              "៛": "khmer currency reil",
              "㍐": "square yuan",
              "円": "yen character",
              "圆": "yen/yuan character variant one",
              "圎": "yen/yuan character variant two",
              "圓": "yuan character, in hong kong and taiwan",
              "圜": "yen/yuan character variant three",
              "원": "won character",
              "﷼": "rial sign",
              "＄": "fullwidth dollar sign",
              "￠": "fullwidth cent sign",
              "￡": "fullwidth pound sign",
              "￥": "fullwidth yen sign",
              "￦": "fullwidth won sign"}

regex_currencies = "|".join(list(currencies.keys()))
regex_currencies_compiled = re.compile(regex_currencies)


def str_to_currency(value, compile=False):
    return str_to(value, regex_boolean, regex_boolean_compiled, compile)


class Infer(object):
    """
    This functions return True or False if match and specific dataType
    """
    ProfilerDataTypesFunctions = {ProfilerDataTypes.INT.value: regex_int,
                                  ProfilerDataTypes.DECIMAL.value: regex_decimal,
                                  ProfilerDataTypes.STRING.value: r".",
                                  ProfilerDataTypes.EMAIL.value: regex_email,
                                  ProfilerDataTypes.IP.value: regex_ip,
                                  ProfilerDataTypes.URL.value: regex_url,
                                  ProfilerDataTypes.GENDER.value: regex_gender,
                                  ProfilerDataTypes.BOOLEAN.value: regex_boolean,
                                  ProfilerDataTypes.ZIP_CODE.value: regex_zip_code,
                                  ProfilerDataTypes.CREDIT_CARD_NUMBER.value: regex_credit_card,
                                  ProfilerDataTypes.DATE.value: r".",
                                  ProfilerDataTypes.OBJECT.value: r"",
                                  ProfilerDataTypes.ARRAY.value: r"",
                                  ProfilerDataTypes.PHONE_NUMBER.value: regex_phone_number,
                                  ProfilerDataTypes.SOCIAL_SECURITY_NUMBER.value: regex_social_security_number,
                                  ProfilerDataTypes.HTTP_CODE.value: regex_http_code,
                                  # ProfilerDataTypes.US_STATE.value: US_STATES
                                  }

    DTYPE_FUNC = {"string": str_to_str, "boolean": str_to_boolean, "date": str_to_date,
                  "array": str_to_array, "object": str_to_object, "ip": str_to_ip,
                  "url": str_to_url, "email": str_to_email, "gender": str_to_gender,
                  "credit_card_number": str_to_credit_card, "zip_code": str_to_zip_code, "int": str_to_int,
                  "decimal": str_to_decimal,
                  ProfilerDataTypes.PHONE_NUMBER.value: str_to_phone_number,
                  ProfilerDataTypes.SOCIAL_SECURITY_NUMBER.value: str_to_social_security_number,
                  ProfilerDataTypes.HTTP_CODE.value: str_to_http_code,
                  }

    # @staticmethod
    # def mismatch(value: tuple, dtypes: dict):
    #     """
    #     Count the dataType that match, do not match, nulls and missing.
    #     For example if we have an string column we also need to pass the column type we want to match.
    #     Like credit card or postal code.
    #     :param value: tuple(Column/Row, value)
    #     :param dtypes: dict {col_name:(dataType, mismatch)}
    #
    #     :return:
    #     """
    #     col_name, value = value
    #
    #     _data_type = ""
    #     dtype = dtypes[col_name]
    #
    #     if Infer.DTYPE_FUNC[dtype](value) is True:
    #         _data_type = dtype
    #     else:
    #         if is_null(value) is True:
    #             _data_type = "null"
    #         elif str_to_missing(value) is True:
    #             _data_type = "missing"
    #         else:
    #             _data_type = "mismatch"
    #
    #     result = (col_name, _data_type), 1
    #     return result



    @staticmethod
    def parse(col_and_value, infer: bool = False, dtypes=None, str_funcs=None, int_funcs=None, full=True):
        """

        :param col_and_value: Column and value tuple
        :param infer: If 'True' try to infer in all the dataTypes available. See int_func and str_funcs
        :param dtypes:
        :param str_funcs: Custom string function to infer.
        :param int_funcs: Custom numeric functions to infer.
        {col_name: regular_expression}
        :param full: True return a tuple with (col_name, dtype), count or False return dtype
        :return:
        """
        col_name, value = col_and_value

        # Try to order the functions from less to more computational expensive
        if int_funcs is None:
            int_funcs = [(str_to_credit_card, "credit_card_number"), (str_to_zip_code, "zip_code")]

        if str_funcs is None:
            str_funcs = [
                (str_to_missing, "missing"), (str_to_boolean, "boolean"), (str_to_date, "date"),
                (str_to_array, "array"), (str_to_object, "object"), (str_to_ip, "ip"),
                (str_to_url, "url"),
                (str_to_email, "email"), (str_to_gender, "gender"), (str_to_null, "null")
            ]
        # Check 'string' for Spark, 'object' for Dask
        if (dtypes[col_name] == "object" or dtypes[col_name] == "string") and infer is True:

            if isinstance(value, bool):
                _data_type = "boolean"
            elif fastnumbers.isint(value):  # Check if value is integer
                _data_type = "int"
                for func in int_funcs:
                    if func[0](value) is True:
                        _data_type = func[1]
                        break

            elif value != value:
                _data_type = "null"

            elif fastnumbers.isfloat(value):
                _data_type = "decimal"

            elif isinstance(value, str):
                _data_type = "string"
                for func in str_funcs:
                    if func[0](value) is True:
                        _data_type = func[1]
                        break

        else:
            _data_type = dtypes[col_name]
            if is_null(value) is True:
                _data_type = "null"
            elif str_to_missing(value) is True:
                _data_type = "missing"
            else:
                if dtypes[col_name].startswith("array"):
                    _data_type = "array"
                else:
                    _data_type = dtypes[col_name]
                    # print(_data_type)

        result = (col_name, _data_type), 1

        if full:
            return result
        else:
            return _data_type

    @staticmethod
    def parse_pandas(value, date_format="DD/MM/YYYY"):
        #
        int_funcs = [(str_to_credit_card, "credit_card_number"), (str_to_zip_code, "zip_code")]
        str_funcs = [
            (str_to_missing, "missing"), (str_to_boolean, "boolean"),
            (str_to_array, "array"), (str_to_object, "object"), (str_to_ip, "ip"),
            (str_to_url, "url"),
            (str_to_email, "email"), (str_to_gender, "gender"), (str_to_null, "null")]
        if isinstance(value, list):
            _data_type = "array"
        elif pd.isnull(value):
            _data_type = "null"
        elif isinstance(value, bool):
            _data_type = "boolean"
        elif profiler_dtype_func("int", True)(
                value):  # We first check if a number can be parsed as a credit card or zip code
            _data_type = "int"
            for func in int_funcs:
                if func[0](str(value)) is True:
                    _data_type = func[1]

        # Seems like float can be parsed as dates
        elif profiler_dtype_func("decimal", True)(value):
            _data_type = "decimal"

        elif str_to_date(value):
            _data_type = "date"

        else:
            _data_type = "string"
            for func in str_funcs:
                if func[0](str(value)) is True:
                    _data_type = func[1]

        return _data_type


def profiler_dtype_func(dtype, null=False):
    """
    Return a function that check if a value match a datatype
    :param dtype:
    :param null:
    :return:
    """

    def _float(value):
        if null is True:
            return fastnumbers.isfloat(value, allow_nan=True) is True and fastnumbers.isint(value) is False
        else:
            return fastnumbers.isfloat(value) is True and fastnumbers.isint(value) is False or value != value

    def _int(value):
        if null is True:
            return fastnumbers.isint(value)
        else:
            return fastnumbers.isint(value) or value != value

    if dtype == ProfilerDataTypes.INT.value:
        return _int

    elif dtype == ProfilerDataTypes.DECIMAL.value:
        return _float

    elif dtype == ProfilerDataTypes.STRING.value:
        return is_str

    elif dtype == ProfilerDataTypes.BOOLEAN.value:
        return str_to_boolean

    elif dtype == ProfilerDataTypes.DATE.value:
        return str_to_object

    elif dtype == ProfilerDataTypes.ARRAY.value:
        return is_str

    elif dtype == ProfilerDataTypes.OBJECT.value:
        return str_to_object

    elif dtype == ProfilerDataTypes.GENDER.value:
        return str_to_gender

    elif dtype == ProfilerDataTypes.IP.value:
        return str_to_ip

    elif dtype == ProfilerDataTypes.URL.value:
        return str_to_url

    elif dtype == ProfilerDataTypes.EMAIL.value:
        return str_to_email

    elif dtype == ProfilerDataTypes.CREDIT_CARD_NUMBER.value:
        return str_to_credit_card

    elif dtype == ProfilerDataTypes.ZIP_CODE.value:
        return str_to_zip_code

    elif dtype == ProfilerDataTypes.MISSING.value:
        return is_str

    else:
        RaiseIt.value_error(dtype, ProfilerDataTypes.list())


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
    elif is_numeric(value):
        if math.isnan(value):
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


def is_list(value):
    """
    Check if an object is a list
    :param value:
    :return:
    """
    return isinstance(value, list)


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


def is_list_of_int(value):
    """
    Check if an object is a list of integers
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(isinstance(elem, int) for elem in value)


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


def is_ip(value):
    """
    Check if a value is valid ip
    :param value:
    :return:
    """
    parts = value.split(".")
    if len(parts) != 4:
        return False
    for item in parts:
        if not 0 <= int(item) <= 255:
            return False
    return True


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
    if is_list(value):
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


def is_str(value):
    """
    Check if an object is a string
    :param value:
    :return:
    """
    # Seems 20% faster than
    return isinstance(value, str)
    # return True if type("str") == "str" else False


def is_object(value):
    """
    Check if an object is a string
    :param value:
    :return:
    """
    return isinstance(value, str)


def is_list_of_futures(value):
    """
    Check if an object is a list of strings
    :param value:
    :return:
    """
    return bool(value) and isinstance(value, list) and all(
        isinstance(elem, distributed.client.Future) for elem in value)


def is_future(value):
    """
    Check if an object is a list of strings
    :param value:
    :return:
    """
    return isinstance(value, distributed.client.Future)


def is_int(value):
    """
    Check if an object is an integer
    :param value:
    :return:
    """
    return isinstance(value, int)


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


def is_bool(value):
    return isinstance(value, bool)


def is_datetime(value):
    """
    Check if an object is a datetime
    :param value:
    :return:
    """

    return isinstance(value, datetime.datetime)


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


PYTHON_SHORT_TYPES = {"string": "string",
                      "str": "string",
                      "integer": "int",
                      "int": "int",
                      "float": "float",
                      "double": "double",
                      "bool": "boolean",
                      "boolean": "boolean",
                      "array": "array",
                      "null": "null"
                      }
PYTHON_TYPES = {"string": str, "int": int, "float": float, "boolean": bool}
PROFILER_COLUMN_TYPES = {"categorical", "numeric", "date", "null", "array", "binary"}

PYTHON_TO_PROFILER = {"string": "categorical", "boolean": "categorical", "int": "numeric", "float": "numeric",
                      "decimal": "numeric", "date": "date", "array": "array", "binaty": "binary", "null": "null"}


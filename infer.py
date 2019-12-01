# This file need to be send to the cluster via .addPyFile to handle the pickle problem
# This is outside the optimus folder on purpose because it cause problem importing optimus when using de udf.

import re
from ast import literal_eval

import fastnumbers
from dateutil.parser import parse as dparse


class Infer(object):
    """
    This functions return True or False if match and specific dataType
    """

    @staticmethod
    def str_to_boolean(_value):
        _value = _value.lower()
        if _value == "true" or _value == "false":
            return True

    @staticmethod
    def str_to_date(_value):
        try:
            dparse(_value)
            return True
        except (ValueError, OverflowError):
            pass

    @staticmethod
    def str_to_null(_value):
        _value = _value.lower()
        if _value == "null":
            return True

    @staticmethod
    def is_null(_value):
        if _value is None:
            return True

    @staticmethod
    def str_to_gender(_value):
        _value = _value.lower()
        if _value == "male" or _value == "female":
            return True

    @staticmethod
    def str_to_data_type(_value, _dtypes):
        """
        Check if value can be parsed to a tuple or and list.
        Because Spark can handle tuples we will try to transform tuples to arrays
        :param _value:
        :return:
        """
        try:

            if isinstance(literal_eval((_value.encode('ascii', 'ignore')).decode("utf-8")), _dtypes):
                return True
        except (ValueError, SyntaxError):
            pass

    @staticmethod
    def str_to_array(_value):
        return Infer.str_to_data_type(_value, (list, tuple))

    @staticmethod
    def str_to_object(_value):
        return Infer.str_to_data_type(_value, (dict, set))

    @staticmethod
    def str_to_url(_value):
        regex = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        if regex.match(_value):
            return True

    @staticmethod
    def str_to_ip(_value):
        regex = re.compile('''\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}''')
        if regex.match(_value):
            return True

    @staticmethod
    def str_to_email(_value):
        regex = re.compile(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)")
        if regex.match(_value):
            return True

    @staticmethod
    def str_to_credit_card(_value):
        # Reference https://www.regular-expressions.info/creditcard.html
        # https://codereview.stackexchange.com/questions/74797/credit-card-checking
        regex = re.compile(r'(4(?:\d{12}|\d{15})'  # Visa
                           r'|5[1-5]\d{14}'  # Mastercard
                           r'|6011\d{12}'  # Discover (incomplete?)
                           r'|7\d{15}'  # What's this?
                           r'|3[47]\d{13}'  # American Express
                           r')$')
        return bool(regex.match(_value))

    @staticmethod
    def str_to_zip_code(_value):
        regex = re.compile(r'^(\d{5})([- ])?(\d{4})?$')
        if regex.match(_value):
            return True
        return False

    @staticmethod
    def str_to_missing(_value):
        if _value == "":
            return True

    @staticmethod
    def parse(value, infer: bool, dtypes, str_funcs, int_funcs, mismatch):
        """

        :param value:
        :param infer:
        :param dtypes:
        :param str_funcs:
        :param int_funcs:
        :param mismatch:
        :return:
        """
        col_name, value = value

        # Try to order the functions from less to more computational expensive
        if int_funcs is None:
            int_funcs = [(Infer.str_to_credit_card, "credit_card_number"), (Infer.str_to_zip_code, "zip_code")]

        if str_funcs is None:
            str_funcs = [
                (Infer.str_to_missing, "missing"), (Infer.str_to_boolean, "boolean"), (Infer.str_to_date, "date"),
                (Infer.str_to_array, "array"), (Infer.str_to_object, "object"), (Infer.str_to_ip, "ip"),
                (Infer.str_to_url, "url"),
                (Infer.str_to_email, "email"), (Infer.str_to_gender, "gender"), (Infer.str_to_null, "null")
            ]

        mismatch_count = 0
        if dtypes[col_name] == "string" and mismatch is not None:
            # Here we can create a list of predefined functions
            regex_list = {"dd/mm/yyyy": r'^([0-2][0-9]|(3)[0-1])(\/)(((0)[0-9])|((1)[0-2]))(\/)\d{4}$',
                          "yyyy-mm-dd": '([12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]))'
                          }

            if col_name in mismatch:
                predefined = mismatch[col_name]
                if predefined in regex_list:
                    expr = regex_list[predefined]
                else:
                    expr = mismatch[col_name]
                regex = re.compile(expr)
                if regex.match(value):
                    mismatch_count = 0
                else:
                    mismatch_count = 1

        if dtypes[col_name] == "string" and infer is True:

            if isinstance(value, bool):
                _data_type = "boolean"

            elif fastnumbers.isint(value):  # Check if value is integer
                _data_type = "int"
                for func in int_funcs:
                    if func[0](value) is True:
                        _data_type = func[1]
                        break

            elif fastnumbers.isfloat(value):
                _data_type = "decimal"

            elif isinstance(value, str):
                _data_type = "string"
                for func in str_funcs:
                    if func[0](value) is True:
                        _data_type = func[1]
                        break
            else:
                _data_type = "null"

        else:
            _data_type = dtypes[col_name]
            if Infer.is_null(value) is True:
                _data_type = "null"
            elif Infer.str_to_missing(value) is True:
                _data_type = "missing"
            else:
                if dtypes[col_name].startswith("array"):
                    _data_type = "array"
                else:
                    _data_type = dtypes[col_name]

        if mismatch:
            result = (col_name, _data_type), (1, mismatch_count)
        else:
            result = (col_name, _data_type), 1

        return result

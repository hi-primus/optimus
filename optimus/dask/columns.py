import re
from ast import literal_eval

import dask
import dask.dataframe as dd
import numpy as np
import pandas
from dask.dataframe.core import DataFrame
from dateutil.parser import parse as dparse
from fastnumbers import isint, isfloat
from multipledispatch import dispatch

from optimus.helpers.check import is_list_of_tuples, is_int, is_function, is_str, is_dict
from optimus.helpers.columns import parse_columns, validate_columns_names
from optimus.profiler.functions import fill_missing_var_types, parse_profiler_dtypes


def cols(self):
    class Cols:

        # TODO: Check if we must use * to select all the columns
        @staticmethod
        @dispatch(object, object)
        def rename(columns_old_new=None, func=None):
            """"
            Changes the name of a column(s) dataFrame.
            :param columns_old_new: List of tuples. Each tuple has de following form: (oldColumnName, newColumnName).
            :param func: can be lower, upper or any string transformation function
            """

            df = self

            # Apply a transformation function
            if is_list_of_tuples(columns_old_new):
                validate_columns_names(self, columns_old_new)
                for col_name in columns_old_new:

                    old_col_name = col_name[0]
                    if is_int(old_col_name):
                        old_col_name = self.schema.names[old_col_name]
                    if func:
                        old_col_name = func(old_col_name)

                    # Cols.set_meta(col_name, "optimus.transformations", "rename", append=True)
                    # TODO: this seems to the only change in this function compare to pandas. Maybe this can be moved to a base class
                    print(old_col_name, col_name[1])
                    if old_col_name != col_name:
                        df = df.rename({old_col_name: col_name[1]})

            df.ext.meta = self.ext.meta

            return df

        @staticmethod
        @dispatch(list)
        def rename(columns_old_new=None):
            return Cols.rename(columns_old_new, None)

        @staticmethod
        @dispatch(object)
        def rename(func=None):
            return Cols.rename(None, func)

        @staticmethod
        @dispatch(str, str, object)
        def rename(old_column, new_column, func=None):
            return Cols.rename([(old_column, new_column)], func)

        @staticmethod
        @dispatch(str, str)
        def rename(old_column, new_column):
            return Cols.rename([(old_column, new_column)], None)

        @staticmethod
        def date_transform():
            raise NotImplementedError('Look at me I am dask now')

        @staticmethod
        def names():
            return list(self.columns)

        @staticmethod
        def count_by_dtypes(columns, infer=False, str_funcs=None, int_funcs=None):
            def parse(value, col_name, _infer, _dtypes, _str_funcs, _int_funcs):

                def str_to_boolean(_value):
                    _value = _value.lower()
                    if _value == "true" or _value == "false":
                        return True

                def str_to_date(_value):
                    try:
                        dparse(_value)
                        return True
                    except (ValueError, OverflowError):
                        pass

                def str_to_null(_value):
                    _value = _value.lower()
                    if _value == "null":
                        return True

                def is_null(_value):
                    if _value is None:
                        return True

                def str_to_gender(_value):
                    _value = _value.lower()
                    if _value == "male" or _value == "female":
                        return True

                def str_to_array(_value):
                    return str_to_data_type(_value, (list, tuple))

                def str_to_object(_value):
                    return str_to_data_type(_value, (dict, set))

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

                def str_to_ip(_value):
                    regex = re.compile('''\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}''')
                    if regex.match(_value):
                        return True

                def str_to_email(_value):
                    regex = re.compile(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)")
                    if regex.match(_value):
                        return True

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

                def str_to_zip_code(_value):
                    regex = re.compile(r'^(\d{5})([- ])?(\d{4})?$')
                    if regex.match(_value):
                        return True
                    return False

                def str_to_missing(_value):
                    # print(_value)
                    if _value == "":
                        return True

                # Try to order the functions from less to more computational expensive
                if _int_funcs is None:
                    _int_funcs = [(str_to_credit_card, "credit_card_number"), (str_to_zip_code, "zip_code")]

                if _str_funcs is None:
                    _str_funcs = [
                        (str_to_missing, "missing"), (str_to_boolean, "boolean"), (str_to_date, "date"),
                        (str_to_array, "array"), (str_to_object, "object"), (str_to_ip, "ip"), (str_to_url, "url"),
                        (str_to_email, "email"), (str_to_gender, "gender"), (str_to_null, "null")
                    ]
                # print(_dtypes)
                if _dtypes[col_name] == "string" and infer is True:

                    if isinstance(value, bool):
                        _data_type = "boolean"

                    elif isint(value):  # Check if value is integer
                        _data_type = "int"
                        for func in _int_funcs:
                            if func[0](value) is True:
                                _data_type = func[1]
                                break

                    elif isfloat(value):
                        _data_type = "decimal"

                    elif isinstance(value, str):
                        _data_type = "string"
                        for func in _str_funcs:
                            if func[0](value) is True:
                                _data_type = func[1]
                                break
                    else:
                        _data_type = "null"
                else:
                    _data_type = _dtypes[col_name]
                    if is_null(value) is True:
                        _data_type = "null"
                    elif str_to_missing(value) is True:
                        _data_type = "missing"
                    else:
                        if _dtypes[col_name].startswith("array"):
                            _data_type = "array"
                        else:
                            _data_type = _dtypes[col_name]

                return _data_type

            columns = parse_columns(self, columns)
            df = self
            dtypes = df.cols.dtypes()

            result = {}
            for col_name in columns:
                df_result = df[col_name].apply(parse, args=(col_name, infer, dtypes, str_funcs, int_funcs))
                result[col_name] = dict(df_result.value_counts())

            if infer is True:
                for k in result.keys():
                    result[k] = fill_missing_var_types(result[k])
            else:
                result = parse_profiler_dtypes(result)
            return result

        @staticmethod
        def is_numeric(col_name):
            # TODO: Check if this is the best way to check the data type
            if np.dtype(self[col_name]).type in [np.int64, np.int32, np.float64]:
                return True

        @staticmethod
        def exec_agg(exprs):
            """
            Execute and aggregation
            :param exprs:
            :return:
            """
            agg = dd.compute(exprs)[0]
            print("agg", agg)

            if len(agg) > 0:
                result = {}
                for a in agg:
                    agg_name = a[0]
                    col_value = a[1]
                    result[agg_name] = {}
                    if isinstance(col_value, pandas.core.series.Series):
                        for col, value in col_value.items():
                            result[agg_name][col] = value
                    elif isinstance(col_value, dask.dataframe.core.Series):
                        for col, value in col_value.items():
                            result[agg_name][col] = value
                    elif is_dict(col_value):
                        result[agg_name] = col_value
            else:
                result = None
            return result

        @staticmethod
        def create_exprs(columns, funcs, *args):

            exprs = {}

            # for col_name in columns:
            #     for func in funcs:
            # print(col_name)
            # print(func)
            # print(args)
            # # print(list(func.values())[0])
            # # exprs[col_name] = list(func.values())
            # # print(func==self.functions.count_uniques_agg)
            # # if func == self.functions.count_uniques_agg:
            # #     print('111')
            #
            # # if is_dict(func):
            # #     print(col_name)
            # #     print(list(func.keys())[0])

            # exprs[func] = getattr(self[col_name].functions, name)()
            # print(func)
            # for f in func.items():
            #     print(f)
            #     print(func)
            #     if func == "percentile_agg":
            #         print("aaa",func)

            # if is_function(func):
            #     name = func.__name__
            #     exprs[func] = getattr(self[col_name].functions, name)()
            #
            # elif is_str(func):
            #     exprs[func] = getattr(self[col_name], func)()
            # else:
            #     exprs[list(func.keys())[0]] = list(func.values())[0]

            # Some operations support rows
            for col_name in columns:
                for func in funcs:
                    # print("func", func)
                    if is_function(func):
                        # print(self)
                        if col_name in exprs:
                            exprs[col_name].update(func(col_name, args)(self))
                        else:
                            exprs[col_name] = func(col_name, args)(self)
                        # name = func.__name__
                        # exprs[col_name] = getattr(self[columns].functions, name)()

                    elif is_str(func):
                        print(2)
                        exprs[func] = getattr(self[columns], func)()
                    else:
                        print(3)
                        exprs[list(func.keys())[0]] = list(func.values())[0]

            print(exprs)
            result = {}
            agg_funcs = ["min", "max", "std", "mean"]

            for k, v in exprs.items():
                if k in agg_funcs:
                    for k1, v1 in v.items():
                        if k1 in result:
                            if k1 not in k:
                                result[k1][k] = {}
                                result[k1][k] = v1
                        else:

                            result[k1] = {}
                            result[k1][k] = v1
                else:
                    if k in result:
                        result[k].update(v)
                    else:
                        result[k] = {}
                        result[k] = v

            # Convert to list
            result = [r for r in result.items()]

            # Some operations only support series
            # for col_name in columns:
            #     for func in funcs:
            #         if is_function(func):
            #             name = func.__name__
            #             exprs[func] = getattr(self[col_name].functions, name)()
            #
            #         elif is_str(func):
            #             exprs[func] = getattr(self[col_name], func)()
            #         else:
            #             exprs[list(func.keys())[0]] = list(func.values())[0]

            return result

        @staticmethod
        def dtypes(columns="*"):
            """
            Return the column(s) data type as string
            :param columns: Columns to be processed
            :return:
            """

            columns = parse_columns(self, columns)
            data_types = ({k: str(v) for k, v in dict(self.dtypes).items()})
            return {col_name: data_types[col_name] for col_name in columns}

    return Cols()


DataFrame.cols = property(cols)

import re
from ast import literal_eval

import dask.dataframe as dd
import numpy as np
from dask.dataframe.core import DataFrame
from dateutil.parser import parse as dparse
from fastnumbers import isint, isfloat
from multipledispatch import dispatch

from optimus.dask.dask import Dask
from optimus.helpers.check import is_list_of_tuples, is_int, is_list_of_futures
from optimus.helpers.columns import parse_columns, validate_columns_names, check_column_numbers
from optimus.helpers.converter import format_dict, val_to_list
from optimus.profiler.functions import fill_missing_var_types, RELATIVE_ERROR


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
        def count():
            return len(self)

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
                df_result = df[col_name].apply(parse, args=(col_name, infer, dtypes, str_funcs, int_funcs),
                                               meta=str).compute()

                result[col_name] = dict(df_result.value_counts())

            if infer is True:
                for k in result.keys():
                    result[k] = fill_missing_var_types(result[k])
            else:
                result = Cols.parse_profiler_dtypes(result)
            return result

        @staticmethod
        def parse_profiler_dtypes(col_data_type):
            """
               Parse a spark data type to a profiler data type
               :return:
               """

            columns = {}
            for k, v in col_data_type.items():
                result_default = {data_type: 0 for data_type in self.constants.DTYPES_TO_PROFILER.keys()}
                for k1, v1 in v.items():
                    for k2, v2 in self.constants.DTYPES_TO_PROFILER.items():
                        if k1 in self.constants.DTYPES_TO_PROFILER[k2]:
                            result_default[k2] = result_default[k2] + v1
                columns[k] = result_default
            return columns

        @staticmethod
        def is_numeric(col_name):
            """
            Check if a column is numeric
            :param col_name:
            :return:
            """
            # TODO: Check if this is the best way to check the data type
            if np.dtype(self[col_name]).type in [np.int64, np.int32, np.float64]:
                result = True
            else:
                result = False
            return result

        @staticmethod
        def frequency(columns, n=10, percentage=False, total_rows=None):
            columns = parse_columns(self, columns)
            df = self
            q = []
            for col_name in columns:
                q.append({col_name: [{"value": k, "count": v} for k, v in
                                     self[col_name].value_counts().nlargest(n).iteritems()]})

            result = dd.compute(*q)[0]

            if percentage is True:
                if total_rows is None:
                    total_rows = df.rows.count()
                    for c in result:
                        c["percentage"] = round((c["count"] * 100 / total_rows), 2)

            return result

        @staticmethod
        def percentile(columns, values=None, relative_error=RELATIVE_ERROR):
            """
            Return the percentile of a spark
            :param columns:  '*', list of columns names or a single column name.
            :param values: list of percentiles to be calculated
            :param relative_error:  If set to zero, the exact percentiles are computed, which could be very expensive.
            :return: percentiles per columns
            """
            return Cols.agg_exprs(columns, self.functions.percentile_agg, self, values, relative_error)

        @staticmethod
        def median(columns, relative_error=RELATIVE_ERROR):
            """
            Return the median of a column spark
            :param columns: '*', list of columns names or a single column name.
            :param relative_error: If set to zero, the exact median is computed, which could be very expensive. 0 to 1 accepted
            :return:
            """

            return format_dict(Cols.percentile(columns, [0.5], relative_error))

        @staticmethod
        def agg_exprs(columns, funcs, *args):
            """
            Create and run aggregation
            :param columns:
            :param funcs:
            :param args:
            :return:
            """
            # print(args)
            return Cols.exec_agg(Cols.create_exprs(columns, funcs, *args))

        @staticmethod
        def mad(columns, relative_error=RELATIVE_ERROR, more=None):
            """
            Return the Median Absolute Deviation
            :param columns: Column to be processed
            :param more: Return some extra computed values (Median).
            :param relative_error: Relative error calculating the media
            :return:
            """

            columns = parse_columns(self, columns, filter_by_column_dtypes=self.constants.NUMERIC_TYPES)
            check_column_numbers(columns, "*")

            df = self

            result = {}
            for col_name in columns:
                funcs = [df.functions.mad_agg]

                result[col_name] = Cols.agg_exprs(columns, funcs, more)

            return format_dict(result)

        @staticmethod
        def exec_agg(exprs):
            """
            Execute and aggregation
            :param exprs:
            :return:
            """
            agg_list = Dask.instance.compute(exprs)

            if len(agg_list) > 0:
                agg_result = []
                # Distributed mode return a list of Futures objects, Single mode not.
                if is_list_of_futures(agg_list):
                    for agg_element in agg_list:
                        agg_result.append(agg_element.result())
                else:
                    agg_result = agg_list[0]

                result = {}

                for agg_element in agg_result:
                    agg_col_name, agg_element_result = agg_element
                    if agg_col_name not in result:
                        result[agg_col_name] = {}

                    result[agg_col_name].update(agg_element_result)
            else:
                result = None

            return result

        @staticmethod
        def create_exprs(columns, funcs, *args):

            # Std, kurtosis, mean, skewness and other agg functions can not process date columns.
            filters = {"object": [self.functions.min],
                       }

            def _filter(_col_name, _func):
                for data_type, func_filter in filters.items():
                    for f in func_filter:
                        if (func.__code__.co_code == f.__code__.co_code) and self.cols.dtypes(col_name)[col_name] == data_type:
                            return True
                return False

            columns = parse_columns(self, columns)
            funcs = val_to_list(funcs)
            exprs = {}

            for col_name in columns:
                for func in funcs:
                    # If the key exist update it
                    if not _filter(col_name, func):
                        if col_name in exprs:
                            exprs[col_name].update(func(col_name, args)(self))
                        else:
                            exprs[col_name] = func(col_name, args)(self)

            result = {}

            for k, v in exprs.items():
                if k in result:
                    result[k].update(v)
                else:
                    result[k] = {}
                    result[k] = v

            # Convert to list
            result = [r for r in result.items()]
            print(result)
            return result

        @staticmethod
        def schema_dtype(columns="*"):
            """
            Return the column(s) data type as Type
            :param columns: Columns to be processed
            :return:
            """

            # if np.dtype(self[col_name]).type in [np.int64, np.int32, np.float64]:
            #     result = True
            #
            columns = parse_columns(self, columns)
            return format_dict([np.dtype(self[col_name]).type for col_name in columns])

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

        @staticmethod
        def select(columns="*", regex=None, data_type=None, invert=False):
            """
            Select columns using index, column name, regex to data type
            :param columns:
            :param regex: Regular expression to filter the columns
            :param data_type: Data type to be filtered for
            :param invert: Invert the selection
            :return:
            """
            df = self
            columns = parse_columns(df, columns, is_regex=regex, filter_by_column_dtypes=data_type, invert=invert)
            if columns is not None:
                df = df[columns]
                # Metadata get lost when using select(). So we copy here again.
                # df.ext.meta = self.ext.meta
                result = df
            else:
                result = None

            return result

        @staticmethod
        def min(columns):
            """
            Return the min value from a column spark
            :param columns: '*', list of columns names or a single column name.
            :return:
            """
            return Cols.agg_exprs(columns, self.functions.min)

    return Cols()


DataFrame.cols = property(cols)

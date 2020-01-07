import re
from ast import literal_eval

import dask.dataframe as dd
import fastnumbers
import numpy as np
from dask.dataframe.core import DataFrame
from dask.distributed import as_completed
from dateutil.parser import parse as dparse
from multipledispatch import dispatch

from optimus.dask.dask import Dask
from optimus.helpers.check import equal_function, is_column_a
from optimus.helpers.constants import RELATIVE_ERROR
from optimus.infer import is_list_of_tuples, is_int, is_list_of_futures, is_list, \
    is_one_element, PYTHON_TYPES
from optimus.helpers.columns import parse_columns, validate_columns_names, check_column_numbers, get_output_cols
from optimus.helpers.converter import format_dict, val_to_list
from optimus.helpers.raiseit import RaiseIt
from optimus.profiler.functions import fill_missing_var_types

# Some expression accepts multiple columns at the same time.
python_set = set


def cols(self: DataFrame):
    class Cols:
        @staticmethod
        def exec_agg(exprs):
            """
            Execute and aggregation
            :param exprs:
            :return:
            """
            agg_list = Dask.instance.compute(exprs)

            if len(agg_list) > 0:
                agg_results = []
                # Distributed mode return a list of Futures objects, Single mode not.
                if is_list_of_futures(agg_list):
                    for future in as_completed(agg_list):
                        agg_results.append(future.result())
                else:
                    agg_results = agg_list[0]

                result = {}
                # print("AGG_RESULT", agg_result)
                for agg_element in agg_results:
                    agg_col_name, agg_element_result = agg_element
                    if agg_col_name not in result:
                        result[agg_col_name] = {}

                    result[agg_col_name].update(agg_element_result)

                # Parsing results
                def parse_percentile(value):
                    _result = {}

                    for (p_value, p_result) in value.iteritems():
                        _result.setdefault(p_value, p_result)
                    return _result

                def parse_hist(value):
                    x = value["count"]
                    y = value["bins"]
                    _result = []
                    for idx, v in enumerate(y):
                        if idx < len(y) - 1:
                            _result.append({"count": x[idx], "lower": y[idx], "upper": y[idx + 1]})
                    return _result

                for columns in result.values():
                    for agg_name, agg_results in columns.items():
                        if agg_name == "percentile":
                            agg_parsed = parse_percentile(agg_results)
                        elif agg_name == "hist":
                            agg_parsed = parse_hist(agg_results)
                        # elif agg_name in ["min", "max", "stddev", "mean", "variance"]:
                        #     agg_parsed = parse_single(agg_results)
                        else:
                            agg_parsed = agg_results
                        columns[agg_name] = agg_parsed

            else:
                result = None

            return result

        @staticmethod
        def create_exprs(columns, funcs, *args):
            df = self
            # Std, kurtosis, mean, skewness and other agg functions can not process date columns.
            filters = {"object": [self.functions.min],
                       }

            def _filter(_col_name, _func):
                for data_type, func_filter in filters.items():
                    for f in func_filter:
                        if equal_function(func, f) and \
                                self.cols.dtypes(col_name)[col_name] == data_type:
                            return True
                return False

            columns = parse_columns(df, columns)
            funcs = val_to_list(funcs)
            exprs = {}

            multi = [self.functions.min, self.functions.max, self.functions.stddev,
                     self.functions.mean, self.functions.variance, self.functions.percentile_agg]

            for func in funcs:
                # Create expression for functions that accepts multiple columns
                if equal_function(func, multi):
                    exprs.update(func(columns, args)(df))
                # If not process by column
                else:
                    for col_name in columns:
                        # If the key exist update it
                        if not _filter(col_name, func):
                            if col_name in exprs:
                                exprs[col_name].update(func(col_name, args)(df))
                            else:
                                exprs[col_name] = func(col_name, args)(df)

            result = {}

            for k, v in exprs.items():
                if k in result:
                    result[k].update(v)
                else:
                    result[k] = {}
                    result[k] = v

            # Convert to list
            result = [r for r in result.items()]

            return result

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
        def count_by_dtypes(columns, infer=False, str_funcs=None, int_funcs=None, mismatch=None):
            def parse(value, col_name, _infer, _dtypes, _str_funcs, _int_funcs):
                # print("PARSE", (value, _infer, _dtypes, _str_funcs, _int_funcs))

                # col_name, value = value

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
                    return str_to_data_type(_value, (dict, python_set))

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
                    if value == "":
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

                mismatch_count = 0
                if _dtypes[col_name] == "string" and mismatch is not None:
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

                if _dtypes[col_name] == "string" and infer is True:

                    if isinstance(value, bool):
                        _data_type = "boolean"

                    elif fastnumbers.isint(value):  # Check if value is integer
                        _data_type = "int"
                        for func in _int_funcs:
                            if func[0](value) is True:
                                _data_type = func[1]
                                break

                    elif fastnumbers.isfloat(value):
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

                if mismatch:
                    result = (col_name, _data_type), (1, mismatch_count)
                else:
                    result = (col_name, _data_type), 1

                return result

            columns = parse_columns(self, columns)
            df = self
            dtypes = df.cols.dtypes()

            result = {}
            for col_name in columns:
                # print((col_name, infer, dtypes, str_funcs, int_funcs))

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
        def lower(input_cols, output_cols=None):

            def _lower(col_name, args):
                return col_name[args].str.lower()

            return Cols.apply(input_cols, _lower, func_return_type=str, filter_col_by_dtypes=["string", "object"],
                              output_cols=output_cols)

        @staticmethod
        def upper(input_cols, output_cols=None):

            def _upper(col_name, args):
                return col_name[args].str.upper()

            return Cols.apply(input_cols, _upper, func_return_type=str, filter_col_by_dtypes=["string", "object"],
                              output_cols=output_cols)

        @staticmethod
        def trim(input_cols, output_cols=None):

            def _strip(col_name, args):
                return col_name[args].str.strip()

            return Cols.apply(input_cols, _strip, func_return_type=str, filter_col_by_dtypes=["string", "object"],
                              output_cols=output_cols)

        @staticmethod
        def apply(input_cols, func=None, func_return_type=None, args=None, func_type=None, when=None,
                  filter_col_by_dtypes=None, output_cols=None, skip_output_cols_processing=False, meta="apply"):

            input_cols = parse_columns(self, input_cols, filter_by_column_dtypes=filter_col_by_dtypes,
                                       accepts_missing_cols=True)
            check_column_numbers(input_cols, "*")

            if skip_output_cols_processing:
                output_cols = val_to_list(output_cols)
            else:
                output_cols = get_output_cols(input_cols, output_cols)

            if output_cols is None:
                output_cols = input_cols

            df = self

            for input_col, output_col in zip(input_cols, output_cols):
                # df = df.withColumn(output_col, expr(when))
                # print(input_col, output_col, args, func_return_type)
                kwargs = {output_col: self[[input_col]].map_partitions(func, args=(input_col),
                                                                       meta=(input_col, func_return_type))}
                df = df.assign(**kwargs)
            return df

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
        def cast(input_cols=None, dtype=None, output_cols=None, columns=None):
            """
            Cast a column or a list of columns to a specific data type
            :param input_cols: Columns names to be casted
            :param output_cols:
            :param dtype: final data type
            :param columns: List of tuples of column names and types to be casted. This variable should have the
                    following structure:
                    colsAndTypes = [('columnName1', 'integer'), ('columnName2', 'float'), ('columnName3', 'string')]
                    The first parameter in each tuple is the column name, the second is the final datatype of column after
                    the transformation is made.
            :return: Dask DataFrame
            """

            df = self
            _dtypes = []

            def _cast_int(value):
                try:
                    return int(value)
                except ValueError:
                    return None

            def _cast_float(value):
                try:
                    return float(value)
                except ValueError:
                    return None

            def _cast_bool(value):
                return bool(value)
            
            def _cast_str(value):
                return value.astype(str)

            # Parse params
            if columns is None:
                input_cols = parse_columns(df, input_cols)
                if is_list(input_cols) or is_one_element(input_cols):
                    output_cols = get_output_cols(input_cols, output_cols)
                    for _ in builtins.range(0, len(input_cols)):
                        _dtypes.append(dtype)
            else:
                input_cols = list([c[0] for c in columns])
                if len(columns[0]) == 2:
                    output_cols = get_output_cols(input_cols, output_cols)
                    _dtypes = list([c[1] for c in columns])
                elif len(columns[0]) == 3:
                    output_cols = list([c[1] for c in columns])
                    _dtypes = list([c[2] for c in columns])

                output_cols = get_output_cols(input_cols, output_cols)

            for input_col, output_col, dtype in zip(input_cols, output_cols, _dtypes):
                if dtype=='int':
                    df[output_col] = df[input_col].apply(func=_cast_int, meta=df[input_col])
                elif dtype=='float':
                    df[output_col] = df[input_col].apply(func=_cast_float, meta=df[input_col])
                elif dtype=='bool':
                    df[output_col] = df[input_col].apply(func=_cast_bool, meta=df[input_col])
                else:
                    df[output_col] = df[input_col].apply(func=_cast_str, meta=df[input_col])
                df[output_col].odtype = dtype

            return df
            
        @staticmethod
        def cast_type(input_cols=None, dtype=None, output_cols=None, columns=None):
            """
            Cast a column or a list of columns to a specific data type
            :param input_cols: Columns names to be casted
            :param output_cols:
            :param dtype: final data type
            :param columns: List of tuples of column names and types to be casted. This variable should have the
                    following structure:
                    colsAndTypes = [('columnName1', 'int64'), ('columnName2', 'float'), ('columnName3', 'int32')]
                    The first parameter in each tuple is the column name, the second is the final datatype of column after
                    the transformation is made.
            :return: Dask DataFrame
            """

            df = self
            _dtypes = []

            # Parse params
            if columns is None:
                input_cols = parse_columns(df, input_cols)
                if is_list(input_cols) or is_one_element(input_cols):
                    output_cols = get_output_cols(input_cols, output_cols)
                    for _ in builtins.range(0, len(input_cols)):
                        _dtypes.append(dtype)
            else:
                input_cols = list([c[0] for c in columns])
                if len(columns[0]) == 2:
                    output_cols = get_output_cols(input_cols, output_cols)
                    _dtypes = list([c[1] for c in columns])
                elif len(columns[0]) == 3:
                    output_cols = list([c[1] for c in columns])
                    _dtypes = list([c[2] for c in columns])

                output_cols = get_output_cols(input_cols, output_cols)

            for input_col, output_col, dtype in zip(input_cols, output_cols, _dtypes):
                df[output_col] = df[input_col].astype(dtype=dtype)

            return df
            

        @staticmethod
        def nest(input_cols, shape="string", separator="", output_col=None):
            """
            Concat multiple columns to one with the format specified
            :param input_cols: columns to be nested
            :param separator: char to be used as separator at the concat time
            :param shape: final data type, 'array', 'string' or 'vector'
            :param output_col:
            :return: Dask DataFrame
            """

            df = self
            input_cols = parse_columns(df, input_cols)
            output_col = parse_columns(df, output_col, accepts_missing_cols=True)
            check_column_numbers(output_col, 1)

            def _nest_string(value):
                v = value[input_cols[0]].astype(str)
                for i in builtins.range(1, len(input_cols)):
                    v = v + separator +  value[input_cols[i]].astype(str)
                return v
            
            def _nest_array(value):
                v = value[input_cols[0]].astype(str)
                for i in builtins.range(1, len(input_cols)):
                    v += ", " +  value[input_cols[i]].astype(str)
                return "["+v+"]"

            if shape=="string":
                df = df.assign(**{output_col[0]: _nest_string})
            else:
                df = df.assign(**{output_col[0]: _nest_array})

            return df

        @staticmethod
        def replace(input_cols, search=None, replace_by=None, search_by="chars", output_cols=None):
            """
            Replace a value, list of values by a specified string
            :param input_cols: '*', list of columns names or a single column name.
            :param output_cols:
            :param search: Values to look at to be replaced
            :param replace_by: New value to replace the old one
            :param search_by: Match substring or words. Can be 'chars' or 'words'
            :return:
            """

            # TODO check if .contains can be used instead of regexp
            def func_chars(_df, _input_col, _output_col, _search, _replace_by):
                # Reference https://www.oreilly.com/library/view/python-cookbook/0596001673/ch03s15.html

                # Create as dict
                if is_list(search):
                    _search_and_replace_by = {s: _replace_by for s in search}
                elif is_one_element(search):
                    _search_and_replace_by = {search: _replace_by}

                _search_and_replace_by = {str(k): str(v) for k, v in _search_and_replace_by.items()}

                def multiple_replace(_value, __search_and_replace_by):
                    # Create a regular expression from all of the dictionary keys
                    if _value is not None:

                        _regex = re.compile("|".join(map(re.escape, __search_and_replace_by.keys())))
                        result = _regex.sub(lambda match: __search_and_replace_by[match.group(0)], str(_value))
                    else:
                        result = None
                    return result

                return _df.cols.apply(_input_col, multiple_replace, "string", _search_and_replace_by,
                                      output_cols=_output_col)

            def func_words(_df, _input_col, _output_col, _search, _replace_by):
                _search = val_to_list(search)
                # Convert the value to column data type
                data_type = self.cols.dtypes(_input_col)
                _search = [PYTHON_TYPES[data_type](s) for s in _search]

                if _input_col != output_col:
                    _df = _df.cols.copy(_input_col, _output_col)

                return _df.replace(_search, _replace_by, _input_col)

            if search_by is "words":
                func = func_words
            elif search_by is "chars":
                func = func_chars
            else:
                RaiseIt.value_error(search_by, ["words", "chars"])

            input_cols = parse_columns(self, input_cols,
                                       filter_by_column_dtypes=[
                                           self.constants.STRING_TYPES + self.constants.NUMERIC_TYPES])

            check_column_numbers(input_cols, "*")
            output_cols = get_output_cols(input_cols, output_cols)

            df = self
            for input_col, output_col in zip(input_cols, output_cols):
                if is_column_a(df, input_col, "int"):
                    df = df.cols.cast(input_col, "str", output_col)

                df = func(df, input_col, output_col, search, replace_by)

            return df

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
        def hist(columns, buckets=20):
            result = Cols.agg_exprs(columns, self.functions.hist_agg, self, buckets, None)
            return result

        @staticmethod
        def frequency(columns, n=10, percentage=False, total_rows=None):
            columns = parse_columns(self, columns)
            df = self
            q = []
            for col_name in columns:
                q.append({col_name: [{"value": k, "count": v} for k, v in
                                     self[col_name].value_counts().nlargest(n).iteritems()]})

            result = dd.compute(*q)
            # From list of tuples to dict
            final_result = {}
            for i in result:
                for x, y in i.items():
                    final_result[x] = y

            print(result)
            if percentage is True:
                if total_rows is None:
                    total_rows = df.rows.count()
                    for c in final_result:
                        c["percentage"] = round((c["count"] * 100 / total_rows), 2)

            return result

        @staticmethod
        def test_agg(columns):
            def chunk(s):
                # for the comments, assume only a single grouping column, the
                # implementation can handle multiple group columns.
                #
                # s is a grouped series. value_counts creates a multi-series like
                # (group, value): count
                return s.value_counts()

            def agg(s):
                # s is a grouped multi-index series. In .apply the full sub-df will passed
                # multi-index and all. Group on the value level and sum the counts. The
                # result of the lambda function is a series. Therefore, the result of the
                # apply is a multi-index series like (group, value): count
                return s.apply(lambda s: s.groupby(level=-1).sum())

                # faster version using pandas internals
                s = s._selected_obj
                return s.groupby(level=list(range(s.index.nlevels))).sum()

            def finalize(s):
                # s is a multi-index series of the form (group, value): count. First
                # manually group on the group part of the index. The lambda will receive a
                # sub-series with multi index. Next, drop the group part from the index.
                # Finally, determine the index with the maximum value, i.e., the mode.
                level = list(range(s.index.nlevels - 1))
                return (
                    s.groupby(level=level)
                        .apply(lambda s: s.reset_index(level=level, drop=True).argmax())
                )

            mode = dd.Aggregation('mode', chunk, agg, finalize)
            res = ddf.groupby(['g0', 'g1']).agg({'col': mode}).compute()

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

        ####################################################
        ####################################################
        ####################################################
        ####################################################
        ####################################################

        # TODO: This functions are the same that spark/columns.py
        #  but I have not figure out the best way to abstract them. Some Work in commit abstracting
        #  this functions in 95fdbeb128e6d29676f5ed65e0bfd1d8d64d805c

        @staticmethod
        def min(columns):
            """
            Return the min value from a Dask dataframe column
            :param columns: '*', list of columns names or a single column name.
            :return:
            """
            df = self
            return Cols.agg_exprs(columns, df.functions.min)

        @staticmethod
        def max(columns):
            """
            Return the max value from a Dask dataframe column
            :param columns: '*', list of columns names or a single column name.
            :return:
            """
            df = self
            return Cols.agg_exprs(columns, df.functions.max)

        @staticmethod
        def range(columns):
            """
            Return the range form the min to the max value
            :param columns: '*', list of columns names or a single column name.
            :return:
            """
            df = self
            return Cols.agg_exprs(columns, df.functions.range_agg)

        @staticmethod
        def percentile(columns, values=None, relative_error=RELATIVE_ERROR):
            """
            Return the percentile of a spark
            :param columns:  '*', list of columns names or a single column name.
            :param values: list of percentiles to be calculated
            :param relative_error:  If set to zero, the exact percentiles are computed, which could be very expensive.
            :return: percentiles per columns
            """
            df = self
            # values = [str(v) for v in values]
            if values is None:
                values = [0.5]
            return Cols.agg_exprs(columns, df.functions.percentile_agg, df, values, relative_error)

        # Descriptive Analytics
        # TODO: implement double MAD http://eurekastatistics.com/using-the-median-absolute-deviation-to-find-outliers/

        @staticmethod
        def std(columns):
            """
            Return the standard deviation of a column spark
            :param columns: '*', list of columns names or a single column name.
            :return:
            """
            df = self
            columns = parse_columns(self, columns, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
            check_column_numbers(columns, "*")
            return format_dict(Cols.agg_exprs(columns, df.functions.stddev))

        @staticmethod
        def kurt(columns):
            """
            Return the kurtosis of a column spark
            :param columns: '*', list of columns names or a single column name.
            :return:
            """
            df = self
            columns = parse_columns(self, columns, filter_by_column_dtypes=self.constants.NUMERIC_TYPES)
            check_column_numbers(columns, "*")

            return format_dict(Cols.agg_exprs(columns, df.functions.kurtosis))

        @staticmethod
        def mean(columns):
            """
            Return the mean of a column spark
            :param columns: '*', list of columns names or a single column name.
            :return:
            """
            df = self
            columns = parse_columns(self, columns, filter_by_column_dtypes=self.constants.NUMERIC_TYPES)
            check_column_numbers(columns, "*")

            return format_dict(Cols.agg_exprs(columns, df.functions.mean))

        @staticmethod
        def skewness(columns):
            """
            Return the skewness of a column spark
            :param columns: '*', list of columns names or a single column name.
            :return:
            """
            df = self
            columns = parse_columns(self, columns, filter_by_column_dtypes=self.constants.NUMERIC_TYPES)
            check_column_numbers(columns, "*")

            return format_dict(Cols.agg_exprs(columns, df.functions.skewness))

        @staticmethod
        def sum(columns):
            """
            Return the sum of a column spark
            :param columns: '*', list of columns names or a single column name.
            :return:
            """
            df = self
            columns = parse_columns(self, columns, filter_by_column_dtypes=self.constants.NUMERIC_TYPES)
            check_column_numbers(columns, "*")

            return format_dict(Cols.agg_exprs(columns, df.functions.sum))

        @staticmethod
        def variance(columns):
            """
            Return the column variance
            :param columns: '*', list of columns names or a single column name.
            :return:
            """
            df = self
            columns = parse_columns(self, columns, filter_by_column_dtypes=self.constants.NUMERIC_TYPES)
            check_column_numbers(columns, "*")

            return format_dict(Cols.agg_exprs(columns, df.functions.variance))

        @staticmethod
        def abs(input_cols, output_cols=None):
            """
            Apply abs to the values in a column
            :param input_cols:
            :param output_cols:
            :return:
            """
            df = self
            input_cols = parse_columns(df, input_cols, filter_by_column_dtypes=df.constants.NUMERIC_TYPES)
            output_cols = get_output_cols(input_cols, output_cols)

            check_column_numbers(output_cols, "*")
            # Abs not accepts column's string names. Convert to Spark Column

            # TODO: make this in one pass.

            for col_name in output_cols:
                df = df.withColumn(col_name, F.abs(F.col(col_name)))
            return df

        @staticmethod
        def mode(columns):
            """
            Return the column mode
            :param columns: '*', list of columns names or a single column name.
            :return:
            """

            columns = parse_columns(self, columns)
            mode_result = []

            for col_name in columns:
                count = self.groupBy(col_name).count()
                mode_df = count.join(
                    count.agg(F.max("count").alias("max_")), F.col("count") == F.col("max_")
                )
                if SparkEngine.cache:
                    mode_df = mode_df.cache()
                # if none of the values are repeated we not have mode
                mode_list = (mode_df
                             .rows.select(mode_df["count"] > 1)
                             .cols.select(col_name)
                             .collect())

                mode_result.append({col_name: filter_list(mode_list)})

            return format_dict(mode_result)

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

    return Cols()


DataFrame.cols = property(cols)

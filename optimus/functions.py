


def filter_by_data_type(col_name, data_type):
    return abstract_udf(col_name, is_data_type, "bool", data_type)


def abstract_udf(col, func, func_return_type=None, attrs=None, func_type=None):
    """
    General User defined functions. This is a helper function to create udf, pandas udf or a Column Exp
    :param col:
    :param func:
    :param attrs:
    :param func_return_type:
    :param func_type: pandas_udf or udf. The function is going to try to use pandas_udf if func_type is not defined
    :return:
    """

    attrs = val_to_list(attrs)

    if func_type != "column_exp":
        if func_type is None and is_pyarrow_installed():
            func_type = "pandas_udf"
        else:
            func_type = "udf"

    df_func = func_factory(func_type, func_return_type)
    return df_func(attrs, func)(col)


def func_factory(func_type=None, func_return_type=None):
    """
    Return column express, udf or pandas udf function.
    :param func_type:
    :param func_return_type:
    :return:
    """
    if func_return_type is not None:
        func_return_type = op_c.TYPES_SPARK_FUNC[op_c.TYPES[func_return_type]]

    def pandas_udf_func(attr=None, func=None):
        # TODO: Get the column type, so is not necessary to pass the return type a param

        # Apply the function over the whole series
        def apply_to_series(val, attr):
            if attr is None:
                args = dict(func=func, args=(None,))
            else:
                args = dict(func=func, args=tuple(attr))
            return val.apply(**args)

        return F.pandas_udf(lambda value: apply_to_series(value, attr), func_return_type)

    def udf_func(attr, func):
        return F.udf(lambda value: func(value, attr))

    def expression_func(attr, func):
        def inner(col_name):
            return func(col_name, attr)

        return inner

    if func_type is "pandas_udf":
        return pandas_udf_func

    elif func_type is "udf":
        return udf_func

    elif func_type is "column_exp":
        return expression_func


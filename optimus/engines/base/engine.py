def func(value, col):
#     value= value.astype(float)
#     return np.mod(value[col].astype(float),1)
#     return np.abs(value[col].astype(float))
#     return np.exp(value[col].astype(float))
#     return np.power(value[col].astype(float),2)
#     return np.ceil(value[col].astype(float))
#     return np.sqrt(value[col].astype(float))
#     return np.floor(value[col].astype(float))
#     return np.trunc(value[col].astype(float))

#     return np.radians(value[col].astype(float))
#     return np.degrees(value[col].astype(float))
#     return np.log(value[col].astype(float))
#     return np.log10(value[col].astype(float))

#     return np.sin(value[col].astype(float))
#     return np.cos(value[col].astype(float))
#     return np.tan(value[col].astype(float))
#     return np.arcsin(value[col].astype(float))
#     return np.arccos(value[col].astype(float))
#     return np.arctan(value[col].astype(float))
#     return np.sinh(value[col].astype(float))
#     return np.arcsinh(value[col].astype(float))
#     return np.cosh(value[col].astype(float))
#     return np.arccosh(value[col].astype(float))
#     return np.tanh(value[col].astype(float))
#     return np.arctanh(value[col].astype(float))


# CUDF
# cudf.sqrt(df.Lat)
# cudf.sin(df.Lat)
# cudf.cos(df.Lat)
# cudf.tan(df.Lat)
# cudf.arcsin(df.Lat)
# cudf.arccos(df.Lat)
# cudf.arctan(df.Lat)
# df.Lat.abs()
# df.Lat.exp()
# df.Lat.log()
# df.Lat.pow(2)
# df.Lat.round(2)
# df.Lat.floor()
# df.Lat.mod(1)
# df.Lat.sqrt()
# df.Lat.log()


class Engine:

    def func(pdf, col_name):
        return pdf.map_partitions(func, col_name, meta=float).compute()

    op_to_series_func = {
        "sqrt": {
            "cudf": "sqrt",
            "python": "sqrt",
        },
        "abs": {
            "cudf": "abs",
            "python": "fabs"

        },
        "pow": {
            "cudf": "pow",
            "python": "pow"
        },
        "exp": {
            "cudf": "exp",
            "python": "exp"
        }

    }

    @staticmethod
    def call(value, *args, method_name=None):
        """
        Process a series or number with a function
        :param value:
        :param args:
        :param method_name:
        :return:
        """

        # if is_dask_series(value):
        method = getattr(value, Engine.op_to_series_func[method_name]["pandas"])
        result = method(*args)
        # elif fastnumbers.isreal(value):  # Numeric
        #     method = getattr(math, Engine.op_to_series_func[method_name]["python"])
        #     result = method(fastnumbers.fast_real(value), *args)
        # else:  # string
        #     result = np.nan
        return result

    @staticmethod
    def mod(value):
        return Engine.call(value, method_name="mod")

    @staticmethod
    def abs(value):
        return Engine.call(value, method_name="abs")

    @staticmethod
    def exp(value):
        return Engine.call(value, method_name="exp")

    @staticmethod
    def pow(value, n):
        return Engine.call(value, n, method_name="pow")

    @staticmethod
    def ceiling(value):
        return Engine.call(value, method_name="ceiling")

    @staticmethod
    def sqrt(value):
        return Engine.call(value, method_name="sqrt")

    @staticmethod
    def floor(value):
        return Engine.call(value, method_name="floor")

    @staticmethod
    def trunc(value):
        return Engine.call(value, method_name="trunc")

    @staticmethod
    def radians(value):
        return Engine.call(value, method_name="trunc")

    @staticmethod
    def degrees(value):
        return Engine.call(value, method_name="trunc")

    # Trigonometrics
    @staticmethod
    def sin(value):
        return Engine.call(value, method_name="sin")

    @staticmethod
    def cos(value):
        return Engine.call(value, method_name="cos")

    @staticmethod
    def tan(value):
        return Engine.call(value, method_name="tan")

    @staticmethod
    def asin(value):
        return Engine.call(value, method_name="asin")

    @staticmethod
    def acos(value):
        return Engine.call(value, method_name="acos")

    @staticmethod
    def atan(value):
        return Engine.call(value, method_name="atan")

    @staticmethod
    def sinh(value):
        return Engine.call(value, method_name="sinh")

    @staticmethod
    def asinh(value):
        return Engine.call(value, method_name="asinh")

    @staticmethod
    def cosh(value):
        return Engine.call(value, method_name="cosh")

    @staticmethod
    def tanh(value):
        return Engine.call(value, method_name="tanh")

    @staticmethod
    def acosh(value):
        return Engine.call(value, method_name="acosh")

    @staticmethod
    def atanh(value):
        return Engine.call(value, method_name="atanh")
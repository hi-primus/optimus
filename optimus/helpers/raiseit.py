from optimus.helpers import checkit as c
import operator as operator_
import inspect


class RaiseIfNot:
    @staticmethod
    def _get_name(var):
        """

        :param var:
        :return:
        """
        lcls = inspect.stack()[2][0].f_locals
        for name in lcls:
            if id(var) == id(lcls[name]):
                return name
        return None

    @staticmethod
    def _apply(var, func, operator):
        if operator == "not":
            expr = operator_.not_(func(var))
        else:
            expr = func(var)
        return expr

    @staticmethod
    def type_error(var, func, operator="not"):
        """
        Raise a TypeError exception
        :param var:
        :param func:
        :param operator:
        :return:
        """

        if RaiseIfNot._apply(var, func, operator):
            raise TypeError(
                "'{var_name}' must be {type}, received '{var_type}'{value}"
                    .format(var_name=RaiseIfNot._get_name(var),
                            type=str, var_type=var,
                            value=type(var)))
        pass

    @staticmethod
    def value_error(var, _list, operator="not"):
        """
        Raise a ValueError exception
        :param var:
        :param _list:
        :param operator:
        :return:
        """

        if RaiseIfNot._apply(var, _list, operator):
            raise ValueError("'{var_name}' must be {type}, received '{var_type}'"
                             .format(var_name=RaiseIfNot._get_name(var),
                                     type=" or ".join(map(
                                         lambda x: "'" + x + "'",
                                         _list)), var_type=var))

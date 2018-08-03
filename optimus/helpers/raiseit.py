from optimus.helpers import checkit as c
import operator as operator_
import inspect
from functools import reduce

from optimus.helpers.checkit import is_function


class RaiseIfNot:
    @staticmethod
    def _get_name(var):
        """
        Get the var name form the var passed to a function
        :param var:
        :return:
        """
        lcls = inspect.stack()[2][0].f_locals
        for name in lcls:
            if id(var) == id(lcls[name]):
                return name
        return None

    @staticmethod
    def type_error(var, types):
        """
        Raise a TypeError exception
        :param var:
        :param types:
        :return:
        """

        divisor = None
        if len(types) == 2:
            divisor = " or "
        elif len(types) > 2:
            divisor = ", "

        raise TypeError(
            "'{var_name}' must be {type}, received '{var_type}'"
                .format(var_name=RaiseIfNot._get_name(var),
                        type=divisor.join(map(
                            lambda x: "'" + x + "'",
                            types)), var_type=type(var)))

    @staticmethod
    def value_error(var, _list):
        """
        Raise a ValueError exception
        :param var:
        :param _list: list of values
        :return:
        """

        #r = []
        #for element in _list:
        #    if is_function(element):
        #        r.append(element(var))
        #    else:
        #        r.append(element == var)

        #if not any(r):
        if len(_list) == 2:
            divisor = " or "
        elif len(_list) > 2:
            divisor = ", "

        print(_list)
        print(len(_list))
        raise ValueError("'{var_name}' must be {type}, received '{var_type}'"
                         .format(var_name=RaiseIfNot._get_name(var),
                                 type=divisor.join(map(
                                     lambda x: "'" + x + "'",
                                     _list)), var_type=var))

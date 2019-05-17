from optimus.helpers.convert import val_to_list
from optimus.helpers.checkit import is_int


class RaiseIt:

    @staticmethod
    def type_error(var, data_types):
        """
        Raise a TypeError exception
        :param var:
        :param data_types: data types expected as strings
        :return:
        """
        from optimus.helpers.functions import get_var_name
        if len(data_types) == 1:
            divisor = ""
        elif len(data_types) == 2:
            divisor = " or "
        elif len(data_types) > 2:
            divisor = ", "

        _type = divisor.join(map(lambda x: "'" + x + "'", data_types))

        raise TypeError(
            "'{var_name}' must be of type {type}, received '{var_type}'".format(var_name=get_var_name(var), type=_type,
                                                                                var_type=type(var)))

    @staticmethod
    def length_error(var1, var2):
        """
        Raise a ValueError exception
        :param var1:
        :param var2:
        :return:
        """
        from optimus.helpers.functions import get_var_name

        if is_int(var2):
            length_var2 = str(var2)
        else:
            length_var2 = str(len(var2))

        raise ValueError("'{var2_name}' must be length '{var1_length}', received '{var2_length}'"
                         .format(var2_name=get_var_name(var2), var1_length=str(len(var1)), var2_length=length_var2))

    @staticmethod
    def not_ready_error(message):
        raise NotReady(message)

    @staticmethod
    def value_error(var=None, data_values=None):
        """
        Raise a ValueError exception
        :param var:
        :type var:

        :param data_values: values accepted by the variable
        :type data_values: str/list
        :return:
        """
        from optimus.helpers.functions import get_var_name
        data_values = val_to_list(data_values)

        if len(data_values) == 1:
            divisor = ""
        elif len(data_values) == 2:
            divisor = " or "
        elif len(data_values) > 2:
            divisor = ", "

        raise ValueError("'{var_name}' must be {type}, received '{var_type}'"
                         .format(var_name=get_var_name(var),
                                 type=divisor.join(map(
                                     lambda x: "'" + x + "'",
                                     data_values)), var_type=var))

    @staticmethod
    def type(cls, var):
        """
        Raise and exception of the type specified
        :param var:
        :return:
        """

        from optimus.helpers.functions import get_var_name
        raise cls("'{var_name}' error".format(var_name=get_var_name(var), var_type=var))


# define Python user-defined exceptions
class Error(Exception):
    """Base class for other exceptions"""
    pass


class NotReady(Error):
    """Raised when the input value is too small"""
    pass

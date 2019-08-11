from optimus.helpers.converter import one_list_to_val


class RaiseIt:

    @staticmethod
    def type_error(var, data_types):
        """
        Raise a TypeError exception
        :param var:
        :param data_types: data types expected as strings
        :return:
        """

        from optimus.helpers.debug import get_var_name
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
    def length_error(var1: list, var2: (list, int)) -> Exception:
        """
        Raise a ValueError exception
        :param var1: variable to check for length
        :param var2: variable or integer to compare with var1
        :return:
        """
        from optimus.helpers.debug import get_var_name

        if isinstance(var2, int):
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


        :param data_values: values accepted by the variable
        :type data_values: str/list
        :return:
        """
        from optimus.helpers.debug import get_var_name

        if not isinstance(data_values, list):
            data_values = [data_values]

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
                                     data_values)), var_type=one_list_to_val(var)))

    @staticmethod
    def type(cls, var):
        """
        Raise and exception of the type specified
        :param cls: Exception Class to be Raised
        :param var:
        :return:
        """

        from optimus.helpers.debug import get_var_name
        raise cls("'{var_name}' error".format(var_name=get_var_name(var), var_type=var))

    @staticmethod
    def message(cls, message):
        """
        Raise and exception of the type specified with a message
        :param cls: Exception Class to be Raised
        :param message:
        :return:
        """
        raise cls(message)


# define Python user-defined exceptions
class Error(Exception):
    """Base class for other exceptions"""
    pass


class NotReady(Error):
    pass
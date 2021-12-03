from optimus.helpers.core import val_to_list


class RaiseIt:

    @staticmethod
    def _or(values):
        values = val_to_list(values)
        if len(values) == 1:
            return f"'{values[0]}'"
        return ", ".join([f"'{v}'" for v in values[0:-1]]) + " or " + f"'{values[-1]}'"

    @staticmethod
    def _and(values):
        values = val_to_list(values)
        if len(values) == 1:
            return f"'{values[0]}'"
        return ", ".join([f"'{v}'" for v in values[0:-1]]) + " and " + f"'{values[-1]}'"

    @classmethod
    def type_error(cls, vars, data_types):
        """
        Raise a TypeError exception
        :param var:
        :param data_types: data types expected as strings
        :return:
        """
        from optimus.helpers.debug import get_var_name

        if get_var_name(vars):
            vars = (vars,)

        var_names = []
        var_types = []

        for arg in vars:
            var_names.append(get_var_name(arg))
            var_types.append(type(arg))

        var_names = cls._and(var_names)
        var_types = cls._and(var_types)
        data_types = cls._or(data_types)

        raise TypeError(f"{var_names} must be of type {data_types}, received {var_types}")

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

    @classmethod
    def value_error(cls, vars, data_values=None, extra_text=""):
        """
        Raise a ValueError exception
        :param var:
        :param data_values: values accepted by the variable. str/list
        :param extra_text: Additional final info about the error
        :return:
        """
        from optimus.helpers.debug import get_var_name

        if get_var_name(vars):
            vars = (vars,)

        var_names = []
        var_values = []

        for arg in vars:
            var_names.append(get_var_name(arg))
            var_values.append(arg)

        var_names = cls._and(var_names)
        var_values = cls._and(var_values)

        if data_values:
            data_values = cls._or(val_to_list(data_values))
            err = f"{var_names} must be {data_values}, received {var_values}."
        else:
            err = f"Invalid {var_names}: {var_values}."

        if extra_text:
            err += f" {extra_text}."

        raise ValueError(err)

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

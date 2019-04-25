


class RaiseIt:

    @staticmethod
    def type_error(var, data_types):
        """
        Raise a TypeError exception
        :param var:
        :param data_types: data types as strings
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
    def value_error(var, data_values):
        """
        Raise a ValueError exception
        :param var:
        :param data_values:
        :return:
        """
        from optimus.helpers.functions import get_var_name
        from optimus.helpers.convert import val_to_list

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
    def length_error(var, length):
        """
        Raise a ValueError exception when the var length is nor correct
        :param var:
        :param length: Expected var length
        :return:
        """
        from optimus.helpers.functions import get_var_name
        raise ValueError("'{var_name}' must be {length}, received '{var_length}'"
                         .format(var_name=get_var_name(var),
                                 length=length, var_length=len(var)))

    @staticmethod
    def type(cls, var, message):
        """
        Raise and exception ot type specified
        :param var:
        :return:
        """
        from optimus.helpers.functions import get_var_name
        raise cls("'{var_name}' error".format(var_name=get_var_name(var), var_type=var))

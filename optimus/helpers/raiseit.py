class RaiseIt:

    @staticmethod
    def type_error(var, data_types):
        from optimus.helpers.functions import get_var_name
        """
        Raise a TypeError exception
        :param var:
        :param types:data types as strings
        :return:
        """

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
        from optimus.helpers.functions import get_var_name
        """
        Raise a ValueError exception
        :param var:
        :param _list: list of values accepted
        :return:
        """

        if len(data_values) == 1:
            divisor = ""
        if len(data_values) == 2:
            divisor = " or "
        elif len(data_values) > 2:
            divisor = ", "

        print(data_values)
        print(len(data_values))
        raise ValueError("'{var_name}' must be {type}, received '{var_type}'"
                         .format(var_name=get_var_name(var),
                                 type=divisor.join(map(
                                     lambda x: "'" + x + "'",
                                     data_values)), var_type=var))

    @staticmethod
    def type(cls, var, message):
        from optimus.helpers.functions import get_var_name

        """
        Raise and exception ot type specified
        :param var:
        :return:
        """
        raise cls("'{var_name}' error".format(var_name=get_var_name(var), var_type=var))

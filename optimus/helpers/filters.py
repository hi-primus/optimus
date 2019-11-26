def dict_filter(dic, keys: list) -> list:
    """
    Get values from a dict  given a list of keys
    :param dic: dictionary to be filtered
    :param keys: list of keys to be used as filter
    :return:
    """
    return [(dic[i]) for i in keys if i in list(dic.keys())]

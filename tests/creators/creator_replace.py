import datetime
import sys
sys.path.append("../..")


def create():
    from optimus import Optimus
    from optimus.tests.creator import TestCreator, default_configs

    op = Optimus("pandas")
    df = op.create.dataframe({
        ('names', 'str'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
        'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
        ('function', 'string'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
        ('Code', 'object'): ["123A", "456", 456, "e", None, "{code}"],
        ('Cybertronian', 'object'): ["False", "True + 1", 1, None, True, False],
        'NullType': [None, None, None, None, None, None],
        'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
        'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
        'function(binary)': [bytearray('Leader', 'utf-8'), bytearray('Espionage', 'utf-8'), bytearray('Security', 'utf-8'), bytearray('First Lieutenant', 'utf-8'), bytearray('None', 'utf-8'), bytearray('Battle Station', 'utf-8')],
        'height(ft)': [-28, 17, 26, 13, None, 300],
        ('last date seen', 'date'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'],
        'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
        'rank': [10, 7, 7, 8, 10, 8],
        ('Date Type'): [datetime.datetime(2016, 9, 10), datetime.datetime(2015, 8, 10), datetime.datetime(2014, 6, 24), datetime.datetime(2013, 6, 24), datetime.datetime(2012, 5, 10), datetime.datetime(2011, 4, 10)],
        ('age', 'int'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
        ('timestamp', 'time'): [datetime.datetime(2014, 6, 24, 0, 0), datetime.datetime(2014, 6, 24, 0, 0), datetime.datetime(2014, 6, 24, 0, 0), datetime.datetime(2014, 6, 24, 0, 0), datetime.datetime(2014, 6, 24, 0, 0), datetime.datetime(2014, 6, 24, 0, 0)],
        ('weight(t)', 'float'): [4.3, 2.0, 4.0, 1.8, 5.7, None]
    })

    t = TestCreator(op, df, name="replace", configs=default_configs)

    # chars

    t.create(method="cols.replace", variant="types_chars", cols="*", select_cols=True, search_by="chars",
             search=['at', 'an'], replace_by="this is a test")

    t.create(method="cols.replace", variant="value_chars", cols="names", select_cols=True, search_by="chars",
             search="Optimus", replace_by="optimus prime")

    t.create(method="cols.replace", variant="list_value_chars", cols="function", select_cols=True, search_by="chars",
             search=["Leader", "Firs", "None", "Secu"], replace_by="MATCH")

    t.create(method="cols.replace", variant="list_value2_chars", cols="function", select_cols=True, search_by="chars",
             search=["leader", "espionage", "security"], replace_by="this is a test", ignore_case=True)

    t.create(method="cols.replace", variant="list_chars", cols=["function"], select_cols=True, search_by="chars",
             search=["True", "False"], replace_by=["Maybe", "Unlikely"])

    t.create(method="cols.replace", variant="lists_list_chars", cols=["function", "japanese name"], select_cols=True, search_by="chars",
             search=["atro", "nochi"], replace_by=["Inochi", "Megatron"])

    t.create(method="cols.replace_regex", variant="value_chars", cols="names", select_cols=True, search_by="chars",
             search=".*atro.*", replace_by="must be Megatron")

    t.create(method="cols.replace_regex", variant="list_value_chars", cols="names", select_cols=True, search_by="chars",
             search=["^a", "^e", "^i", "^o", "^u"], replace_by="starts with a vowel", ignore_case=True)

    t.create(method="cols.replace_regex", variant="list_list_chars", cols=["function"], select_cols=True, search_by="chars",
             search=["....", ".....", "......"], replace_by=["4c", "5c", "6c"])

    t.create(method="cols.replace_regex", variant="lists_list_chars", cols=["function", "Code", "Cybertronian"], select_cols=True, search_by="chars",
             search=[["\\d"], [".*Tru.*", ".*Fal.*"]], replace_by=["has a number", "has boolean"])

    # words

    t.create(method="cols.replace", variant="types_words", cols="*", select_cols=True, search_by="words",
             search=['at', 'an'], replace_by="this is a test")

    t.create(method="cols.replace", variant="value_words", cols="names", select_cols=True, search_by="words",
             search="Optimus", replace_by="optimus prime")

    t.create(method="cols.replace", variant="list_value_words", cols="function", select_cols=True, search_by="words",
             search=["Leader", "Firs", "None", "Secu"], replace_by="MATCH")

    t.create(method="cols.replace", variant="list_value2_words", cols="function", select_cols=True, search_by="words",
             search=["leader", "espionage", "security"], replace_by="this is a test", ignore_case=True)

    t.create(method="cols.replace", variant="list_words", cols=["function"], select_cols=True, search_by="words",
             search=["True", "False"], replace_by=["Maybe", "Unlikely"])

    t.create(method="cols.replace", variant="lists_list_words", cols=["function", "japanese name"], select_cols=True, search_by="words",
             search=["atro", "Megatron", "nochi", "Inochi"], replace_by=["--", "a-match", "--", "other-match"])

    t.create(method="cols.replace_regex", variant="value_words", cols="names", select_cols=True, search_by="words",
             search=".*atro.*", replace_by="must be Megatron")

    t.create(method="cols.replace_regex", variant="list_value_words", cols="names", select_cols=True, search_by="words",
             search=["^a", "^e", "^i", "^o", "^u"], replace_by="starts with a vowel", ignore_case=True)

    t.create(method="cols.replace_regex", variant="list_list_words", cols=["function"], select_cols=True, search_by="words",
             search=["....", ".....", "......"], replace_by=["4c", "5c", "6c"])

    t.create(method="cols.replace_regex", variant="lists_list_words", cols=["function", "Code", "Cybertronian"], select_cols=True, search_by="words",
             search=[["\\d"], [".*True.*", ".*False.*"]], replace_by=["is a number", "is boolean"])

    # full

    t.create(method="cols.replace", variant="types_full", cols="*", select_cols=True, search_by="full",
             search=['at', 'an'], replace_by="this is a test")

    t.create(method="cols.replace", variant="value_full", cols="names", select_cols=True, search_by="full",
             search="Optimus", replace_by="optimus prime")

    t.create(method="cols.replace", variant="list_value_full", cols="function", select_cols=True, search_by="full",
             search=["Leader", "Firs", "None", "Secu"], replace_by="MATCH")

    t.create(method="cols.replace", variant="list_value2_full", cols="function", select_cols=True, search_by="full",
             search=["leader", "espionage", "security"], replace_by="this is a test", ignore_case=True)

    t.create(method="cols.replace", variant="list_full", cols=["function"], select_cols=True, search_by="full",
             search=["True", "False"], replace_by=["Maybe", "Unlikely"])

    t.create(method="cols.replace", variant="lists_list_full", cols=["function", "Code", "Cybertronian"], select_cols=True, search_by="full",
             search=["123", "456", "123A"], replace_by=["N/A", "Number", "String"])

    t.create(method="cols.replace_regex", variant="value_full", cols="names", select_cols=True, search_by="full",
             search=".*atro.*", replace_by="must be Megatron")

    t.create(method="cols.replace_regex", variant="list_value_full", cols="names", select_cols=True, search_by="full",
             search=["^a", "^e", "^i", "^o", "^u"], replace_by="starts with a vowel", ignore_case=True)

    t.create(method="cols.replace_regex", variant="list_list_full", cols=["function"], select_cols=True, search_by="full",
             search=["....", ".....", "......"], replace_by=["4c", "5c", "6c"])

    t.create(method="cols.replace_regex", variant="lists_list_full", cols=["function", "Code", "Cybertronian"], select_cols=True, search_by="full",
             search=[["\\d", "e"], [".*True.*", ".*False.*"]], replace_by=["is a number", "is boolean"])

    # values

    t.create(method="cols.replace", variant="types_values", cols="*", select_cols=True, search_by="values",
             search=['at', 'an'], replace_by="this is a test")

    t.create(method="cols.replace", variant="value_values", cols="names", select_cols=True, search_by="values",
             search="Optimus", replace_by="optimus prime")

    t.create(method="cols.replace", variant="list_value_values", cols="function", select_cols=True, search_by="values",
             search=["Leader", "Firs", "None", "Secu"], replace_by="MATCH")

    t.create(method="cols.replace", variant="list_value2_values", cols="function", select_cols=True, search_by="values",
             search=["leader", "espionage", "security"], replace_by="this is a test", ignore_case=True)

    t.create(method="cols.replace", variant="list_values", cols=["function"], select_cols=True, search_by="values",
             search=["True", "False"], replace_by=["Maybe", "Unlikely"])

    t.create(method="cols.replace", variant="lists_list_values", cols=["function", "japanese name"], select_cols=True, search_by="values",
             search=["atro", "nochi"], replace_by=["Inochi", "Megatron"])

    t.create(method="cols.replace_regex", variant="value_values", cols="names", select_cols=True, search_by="values",
             search=".*atro.*", replace_by="must be Megatron")

    t.create(method="cols.replace_regex", variant="list_list_values", cols=["function"], select_cols=True, search_by="values",
             search=["....", ".....", "......"], replace_by=["4c", "5c", "6c"])

    t.create(method="cols.replace_regex", variant="lists_list_values", cols=["function", "Code", "Cybertronian"], select_cols=True, search_by="values",
             search=[["\\d"], [".*True.*", ".*False.*"]], replace_by=["is a number", "is boolean"])

    t.run()


create()

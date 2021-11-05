import datetime
import sys
sys.path.append("../..")


def create():
    from optimus import Optimus
    from optimus.tests.creator import TestCreator, default_configs

    op = Optimus("pandas")
    df = op.create.dataframe({
        'NullType': [None, None, None, None, None, None],
        'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
        'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
        'function(binary)': [bytearray('Leader', 'utf-8'), bytearray('Espionage', 'utf-8'), bytearray('Security', 'utf-8'), bytearray('First Lieutenant', 'utf-8'), bytearray('None', 'utf-8'), bytearray('Battle Station', 'utf-8')],
        'height(ft)': [-28, 17, 26, 13, None, 300],
        'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
        ('last date seen', 'date'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'],
        'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
        'rank': [10, 7, 7, 8, 10, 8],
        ('Cybertronian', 'bool'): [True, True, True, True, True, False],
        ('Date Type'): [datetime.datetime(2016, 9, 10), datetime.datetime(2015, 8, 10), datetime.datetime(2014, 6, 24), datetime.datetime(2013, 6, 24), datetime.datetime(2012, 5, 10), datetime.datetime(2011, 4, 10)],
        ('age', 'int'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
        ('function', 'string'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
        ('names', 'str'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
        ('timestamp', 'time'): [datetime.datetime(2014, 6, 24, 0, 0), datetime.datetime(2014, 6, 24, 0, 0), datetime.datetime(2014, 6, 24, 0, 0), datetime.datetime(2014, 6, 24, 0, 0), datetime.datetime(2014, 6, 24, 0, 0), datetime.datetime(2014, 6, 24, 0, 0)],
        ('weight(t)', 'float'): [4.3, 2.0, 4.0, 1.8, 5.7, None]
    })

    t = TestCreator(op, df, name="preparing", configs=default_configs)

    t.create(method="cols.impute", variant="all", cols="*", strategy="constant", fill_value=float("inf"))
    t.create(method="cols.impute", variant="numeric_single_mean",cols=["height(ft)"], strategy="mean")
    t.create(method="cols.impute", variant="numeric_single_median",cols=["height(ft)"], strategy="median")
    t.create(method="cols.impute", variant="numeric_single_most_frequent",cols=["height(ft)"], strategy="most_frequent")
    t.create(method="cols.impute", variant="numeric_single_constant",cols=["height(ft)"], strategy="constant")
    t.create(method="cols.impute", variant="numeric_multiple_mean",cols=["rank","age","weight(t)"], strategy="mean", output_cols=["rk","ag","wt"])
    t.create(method="cols.impute", variant="numeric_multiple_median",cols=["rank","age","weight(t)"], strategy="median", output_cols=["rk","ag","wt"])
    t.create(method="cols.impute", variant="numeric_multiple_most_frequent",cols=["rank","age","weight(t)"], strategy="most_frequent", output_cols=["rk","ag","wt"])
    t.create(method="cols.impute", variant="numeric_multiple_constamt",cols=["rank","age","weight(t)"], strategy="constant", fill_value=12, output_cols=["rk","ag","wt"])
    t.create(method="cols.impute", variant="string_single_most_frequent",cols=["function"], strategy="most_frequent")
    t.create(method="cols.impute", variant="string_single_constant",cols=["function"], strategy="constant", fill_value=0.32132)
    t.create(method="cols.impute", variant="string_multiple_most_frequent",cols=["names","function"], strategy="most_frequent")
    t.create(method="cols.impute", variant="string_multiple_constamt",cols=["names","function"], strategy="constant", fill_value=float("-inf"))
    t.create(method="cols.impute", variant="multiple_most_frequent", cols=["rank","age","weight(t)","names","function"], strategy="most_frequent", output_cols=["rk","ag","wt","nm","fn"])
    t.create(method="cols.impute", variant="multiple_constant", cols=["rank","age","weight(t)","names","function"], strategy="constant", fill_value=-13, output_cols=["rk","ag","wt","nm","fn"])

    df = op.create.dataframe({
        'NullType': [None, None, None, None, None, None],
        'attributes': [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]],
        'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
        'function(binary)': [bytearray('Leader', 'utf-8'), bytearray('Espionage', 'utf-8'), bytearray('Security', 'utf-8'), bytearray('First Lieutenant', 'utf-8'), bytearray('None', 'utf-8'), bytearray('Battle Station', 'utf-8')],
        'japanese name': [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']],
        ('last date seen', 'date'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'],
        'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
        ('Cybertronian', 'bool'): [True, True, True, True, True, False],
        ('function', 'string'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'],
        ('names', 'str'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']
    })

    t.create(method="cols.one_hot_encode", variant="all", cols="*")
    t.create(method="cols.one_hot_encode", variant="all_prefix", cols="*", prefix="cols")
    t.create(method="cols.one_hot_encode", variant="string", cols=["names"])
    t.create(method="cols.one_hot_encode", variant="string_prefix", cols=["names"], prefix="name")
    t.create(method="cols.one_hot_encode", variant="multiple", cols=["NullType","japanese name","last date seen"])
    t.create(method="cols.one_hot_encode", variant="multiple_prefix", cols=["NullType","japanese name","last date seen"], prefix="type")

    t.run()

create()
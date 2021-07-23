import datetime
import sys
sys.path.append("../..")


def create():
    from optimus import Optimus
    from optimus.tests.creator import TestCreator

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

    configs = {
        "Pandas": {"engine": "pandas"},
        "Dask": {"engine": "dask", "n_partitions": 1},
        "PartitionDask": {"engine": "dask", "n_partitions": 2}
    }

    t = TestCreator(op, df, name="cols", configs=configs)

    operations = ["abs", "exp", "ln", "sqrt", "reciprocal", "floor", "ceil", "sin",
                  "cos", "tan", "asin", "acos", "atan", "sinh", "cosh", "tanh", "asinh", "acosh",
                  "atanh", "copy", "to_string", "infer_dtypes", "lower", "upper", "title",
                  "capitalize", "proper", "trim", "word_tokenizer", "word_count", "len", "reverse",
                  "remove_white_spaces", "normalize_spaces", "strip_html", "expand_contrated_words",
                  "lemmatize_verbs", "remove_urls", "remove_numbers", "remove_special_chars",
                  "z_score", "modified_z_score", "min_max_scaler", "standard_scaler",
                  "max_abs_scaler", "domain", "top_domain", "sub_domain", "url_scheme", "url_path",
                  "url_file", "url_query", "url_fragment", "host", "port", "email_username",
                  "email_domain", "string_to_index", "index_to_string", "fingerprint", "pos",
                  "metaphone", "double_metaphone", "nysiis", "match_rating_codex",
                  "double_methaphone", "soundex"]

    for operation in operations:
        method = "cols."+operation
        t.create(method=method, variant="all", cols="*")
        t.create(method=method, variant="string",
                 cols=["names"], select_cols=True)
        t.create(method=method, variant="numeric_int",
                 cols=["rank"], select_cols=True)
        t.create(method=method, variant="numeric_float",
                 cols=["height(ft)"], select_cols=True)
        t.create(method=method, variant="NoneType",
                 cols=["NullType"], select_cols=True)
        t.create(method=method, variant="list",
                 cols="attributes", select_cols=True)
        t.create(method=method, variant="bytearray",
                 cols="function(binary)", select_cols=True)
        t.create(method=method, variant="datetime",
                 cols="last date seen", select_cols=True)
        t.create(method=method, variant="bool",
                 cols="Cybertronian", select_cols=True)
        t.create(method=method, variant="multiple", compare_by="json",
                 cols=["NullType", "weight(t)", "japanese name", "timestamp", "function", "age", "Date Type", "last position seen", "date arrival"])

    t.run()


create()

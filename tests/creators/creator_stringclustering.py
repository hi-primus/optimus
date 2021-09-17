import datetime
import sys
sys.path.append("../..")


def create():
    from optimus import Optimus
    from optimus.tests.creator import TestCreator, default_configs

    op = Optimus("pandas")
    df = op.create.dataframe({
        'NullType': [None, None, None, None, None, None],
        'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
        'height(ft)': [-28, 17, 26, 13, None, 300],
        ('last date seen', 'date'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'],
        'last position seen': ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None],
        'rank': [10, 7, 7, 8, 10, 8],
        ('Cybertronian', 'bool'): [True, True, True, True, True, False],
        ('Date Type'): [datetime.datetime(2016, 9, 10), datetime.datetime(2015, 8, 10), datetime.datetime(2014, 6, 24), datetime.datetime(2013, 6, 24), datetime.datetime(2012, 5, 10), datetime.datetime(2011, 4, 10)],
        ('age', 'int'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000],
        ('function', 'string'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', None, 'Battle Station'],
        ('names', 'str'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'],
        ('timestamp', 'time'): [datetime.datetime(2014, 6, 24, 0, 0), datetime.datetime(2014, 6, 24, 0, 0), datetime.datetime(2014, 6, 24, 0, 0), datetime.datetime(2014, 6, 24, 0, 0), datetime.datetime(2014, 6, 24, 0, 0), datetime.datetime(2014, 6, 24, 0, 0)],
        ('weight(t)', 'float'): [4.3, 2.0, 4.0, 1.8, 5.7, None]
    })

    t = TestCreator(op, df, name="stringclustering", configs=default_configs)

    t.create(method="string_clustering", variant="all_fingerprint", cols="*", algorithm="fingerprint")
    t.create(method="string_clustering", variant="all_ngram_fingerprint", cols="*", algorithm="ngram_fingerprint")
    t.create(method="string_clustering", variant="all_metaphone", cols="*", algorithm="metaphone")
    t.create(method="string_clustering", variant="all_nysiis", cols="*", algorithm="nysiis")
    t.create(method="string_clustering", variant="all_match_rating_codex", cols="*", algorithm="match_rating_codex")
    t.create(method="string_clustering", variant="all_double_metaphone", cols="*", algorithm="double_metaphone")
    t.create(method="string_clustering", variant="all_soundex", cols="*", algorithm="soundex")
    t.create(method="string_clustering", variant="all_levenshtein", cols="*", algorithm="levenshtein")

    t.create(method="string_clustering", variant="numeric_fingerprint", cols=["rank"], algorithm="fingerprint")
    t.create(method="string_clustering", variant="numeric_ngram_fingerprint", cols=["rank"], algorithm="ngram_fingerprint")
    t.create(method="string_clustering", variant="numeric_metaphone", cols=["rank"], algorithm="metaphone")
    t.create(method="string_clustering", variant="numeric_nysiis", cols=["rank"], algorithm="nysiis")
    t.create(method="string_clustering", variant="numeric_match_rating_codex", cols=["rank"], algorithm="match_rating_codex")
    t.create(method="string_clustering", variant="numeric_double_metaphone", cols=["rank"], algorithm="double_metaphone")
    t.create(method="string_clustering", variant="numeric_soundex", cols=["rank"], algorithm="soundex")
    t.create(method="string_clustering", variant="numeric_levenshtein", cols=["rank"], algorithm="levenshtein")

    t.create(method="string_clustering", variant="string_fingerprint", cols=["names"], algorithm="fingerprint")
    t.create(method="string_clustering", variant="string_ngram_fingerprint", cols=["names"], algorithm="ngram_fingerprint")
    t.create(method="string_clustering", variant="string_metaphone", cols=["names"], algorithm="metaphone")
    t.create(method="string_clustering", variant="string_nysiis", cols=["names"], algorithm="nysiis")
    t.create(method="string_clustering", variant="string_match_rating_codex", cols=["names"], algorithm="match_rating_codex")
    t.create(method="string_clustering", variant="string_double_metaphone", cols=["names"], algorithm="double_metaphone")
    t.create(method="string_clustering", variant="string_soundex", cols=["names"], algorithm="soundex")
    t.create(method="string_clustering", variant="string_levenshtein", cols=["names"], algorithm="levenshtein")

    t.create(method="string_clustering", variant="multiple_fingerprint", cols=["NullType","Cybertronian","timestamp"], algorithm="fingerprint")
    t.create(method="string_clustering", variant="multiple_ngram_fingerprint", cols=["NullType","Cybertronian","timestamp"], algorithm="ngram_fingerprint")
    t.create(method="string_clustering", variant="multiple_metaphone", cols=["NullType","Cybertronian","timestamp"], algorithm="metaphone")
    t.create(method="string_clustering", variant="multiple_nysiis", cols=["NullType","Cybertronian","timestamp"], algorithm="nysiis")
    t.create(method="string_clustering", variant="multiple_match_rating_codex", cols=["NullType","Cybertronian","timestamp"], algorithm="match_rating_codex")
    t.create(method="string_clustering", variant="multiple_double_metaphone", cols=["NullType","Cybertronian","timestamp"], algorithm="double_metaphone")
    t.create(method="string_clustering", variant="multiple_soundex", cols=["NullType","Cybertronian","timestamp"], algorithm="soundex")
    t.create(method="string_clustering", variant="multiple_levenshtein", cols=["NullType","Cybertronian","timestamp"], algorithm="levenshtein")

    t.run()

create()
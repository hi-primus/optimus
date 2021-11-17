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

    t = TestCreator(op, df, name="numeric", configs=default_configs)

    df2 = df.cols.append({"abs_test": [-1, "10", float("-inf"), float("nan"), 0, None]})  

    t.create(df=df2, method="cols.abs", cols=["abs_test"], select_cols=True)
    t.create(method="cols.abs", variant="all", cols="*")
    t.create(method="cols.abs", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.abs", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.abs", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"exp_test": [0, "0.5", -0.5, 2.718, float("inf"), None]})

    t.create(df=df2, method="cols.exp", cols=["exp_test"], select_cols=True)
    t.create(method="cols.exp", variant="all", cols="*")
    t.create(method="cols.exp", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.exp", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.exp", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"ln_test": ["0.36", 1, float("inf"), 0, 2.7182, -100]})

    t.create(df=df2, method="cols.ln", cols=["ln_test"], select_cols=True)
    t.create(method="cols.ln", variant="all", cols="*")
    t.create(method="cols.ln", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.ln", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.ln", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"sqrt_test": ["10000", 0.25, -81, float("inf"), 0, 1]})

    t.create(df=df2, method="cols.sqrt", cols=["sqrt_test"], select_cols=True)
    t.create(method="cols.sqrt", variant="all", cols="*")
    t.create(method="cols.sqrt", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.sqrt", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.sqrt", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"reciprocal_test": [1, 0, float("-inf"), "237", None, 0.125]})

    t.create(df=df2, method="cols.reciprocal", cols=["reciprocal_test"], select_cols=True)
    t.create(method="cols.reciprocal", variant="all", cols="*")
    t.create(method="cols.reciprocal", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.reciprocal", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.reciprocal", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"floor_test": [float("inf"), "12.342", 0, None, 1004.5, -27.7]})

    t.create(df=df2, method="cols.floor", cols=["floor_test"], select_cols=True)
    t.create(method="cols.floor", variant="all", cols="*")
    t.create(method="cols.floor", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.floor", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.floor", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"ceil_test": [float("inf"), "12.342", 0, None, 1004.5, -27.7]})

    t.create(df=df2, method="cols.ceil", cols=["ceil_test"], select_cols=True)
    t.create(method="cols.ceil", variant="all", cols="*")
    t.create(method="cols.ceil", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.ceil", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.ceil", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    t.create(method="cols.z_score", variant="all", cols="*")
    t.create(method="cols.z_score", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.z_score", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.z_score", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    t.create(method="cols.modified_z_score", variant="all", cols="*", estimate=False)
    t.create(method="cols.modified_z_score", variant="numeric", cols=["height(ft)"], select_cols=True, estimate=False)
    t.create(method="cols.modified_z_score", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True, estimate=False)
    t.create(method="cols.modified_z_score", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"], estimate=False)

    t.create(method="cols.min_max_scaler", variant="all", cols="*")
    t.create(method="cols.min_max_scaler", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.min_max_scaler", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.min_max_scaler", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    t.create(method="cols.standard_scaler", variant="all", cols="*")
    t.create(method="cols.standard_scaler", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.standard_scaler", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.standard_scaler", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    t.create(method="cols.max_abs_scaler", variant="all", cols="*")
    t.create(method="cols.max_abs_scaler", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.max_abs_scaler", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.max_abs_scaler", variant="multiple", cols=["NullType", "weight(t)", "japanese name"],output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"to_float_test": [float("-inf"), 10001, 0, None, "-41", 5]})

    t.create(df=df2, method="cols.to_float", cols=["to_float_test"], select_cols=True)
    t.create(method="cols.to_float", variant="all", cols="*")
    t.create(method="cols.to_float", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.to_float", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.to_float", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"to_integer_test": [float("inf"), "12.342", 0.32, None, 1004.5, -27.7]})

    t.create(df=df2, method="cols.to_integer", cols=["to_integer_test"], select_cols=True)
    t.create(method="cols.to_integer", variant="all", cols="*")
    t.create(method="cols.to_integer", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.to_integer", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.to_integer", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"mod_test": [10, None, float("inf"), -356, 0.5314, 0]})

    t.create(df=df2, method="cols.mod", cols=["mod_test"], divisor=3, select_cols=True)
    t.create(df=df2, method="cols.mod", variant="1", cols=["mod_test"], divisor=100.3, select_cols=True)
    t.create(df=df2, method="cols.mod", variant="2", cols=["mod_test"], divisor=6, select_cols=True)
    t.create(df=df2, method="cols.mod", variant="3", cols=["mod_test"], divisor=-12, select_cols=True)
    t.create(method="cols.mod", variant="all", cols="*", divisor=5)
    t.create(method="cols.mod", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.mod", variant="string", cols=["names"], divisor=4, output_cols=["names_2"], select_cols=True)
    t.create(method="cols.mod", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], divisor=10, output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"log_test": [10, None, float("inf"), -356, 0.5314, 0]})

    t.create(df=df2, method="cols.log", cols=["log_test"], base=10, select_cols=True)
    t.create(method="cols.log", variant="1", cols=["height(ft)"], base=100.3, select_cols=True)
    t.create(method="cols.log", variant="2", cols=["height(ft)"], base=2.7182, select_cols=True)
    t.create(method="cols.log", variant="3", cols=["height(ft)"], base=-3, select_cols=True)
    t.create(method="cols.log", variant="all", cols="*", base=12)
    t.create(method="cols.log", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.log", variant="string", cols=["names"], base=2, output_cols=["names_2"], select_cols=True)
    t.create(method="cols.log", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], base=21, output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"pow_test": [10, None, float("-inf"), -356, 0.5314, 0]})

    t.create(df=df2, method="cols.pow", cols=["pow_test"], power=2, select_cols=True)
    t.create(method="cols.pow", variant="1", cols=["height(ft)"], power=0.5, select_cols=True)
    t.create(method="cols.pow", variant="2", cols=["height(ft)"], power=10, select_cols=True)
    t.create(method="cols.pow", variant="3", cols=["height(ft)"], power=-5, select_cols=True)
    t.create(method="cols.pow", variant="all", cols="*", power=3)
    t.create(method="cols.pow", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.pow", variant="string", cols=["names"], power=3.7, output_cols=["names_2"], select_cols=True)
    t.create(method="cols.pow", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], power=117, output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"round_test": [10, None, float("-inf"), -356.312312, 0.5314, 1.000009]})

    t.create(df=df2, method="cols.round", cols=["round_test"], decimals=2, select_cols=True)
    t.create(df=df2, method="cols.round", variant="1", cols=["round_test"], decimals=1, select_cols=True)
    t.create(df=df2, method="cols.round", variant="2", cols=["round_test"], decimals=2, select_cols=True)
    t.create(df=df2, method="cols.round", variant="3", cols=["round_test"], decimals=5, select_cols=True)
    t.create(method="cols.round", variant="all", cols="*", decimals=4)
    t.create(method="cols.round", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.round", variant="string", cols=["names"], decimals=5, output_cols=["names_2"], select_cols=True)
    t.create(method="cols.round", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], decimals=21, output_cols=["nt", "wt", "jn"])

    t.run()

    t = TestCreator(op, df, name="trigonometric", configs=default_configs)

    df2 = df.cols.append({"sin_test": [3.151592, None, 320, 0, float("-inf"), -10]})

    t.create(df=df2, method="cols.sin", cols=["sin_test"], select_cols=True)
    t.create(method="cols.sin", variant="all", cols="*")
    t.create(method="cols.sin", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.sin", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.sin", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"cos_test": [3.151592, None, 78, 0, float("inf"), -12]})

    t.create(df=df2, method="cols.cos", cols=["cos_test"], select_cols=True)
    t.create(method="cols.cos", variant="all", cols="*")
    t.create(method="cols.cos", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.cos", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.cos", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"tan_test": [3.151592, None, 91, 0, float("-inf"), -15]})

    t.create(df=df2, method="cols.tan", cols=["tan_test"], select_cols=True)
    t.create(method="cols.tan", variant="all", cols="*")
    t.create(method="cols.tan", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.tan", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.tan", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"asin_test": [1, "0", 10, float("nan"), float("inf"), None]})

    t.create(df=df2, method="cols.asin", cols=["asin_test"], select_cols=True)
    t.create(method="cols.asin", variant="all", cols="*")
    t.create(method="cols.asin", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.asin", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.asin", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"acos_test": [1, "0", 9, float("nan"), float("-inf"), None]})

    t.create(df=df2, method="cols.acos", cols=["acos_test"], select_cols=True)
    t.create(method="cols.acos", variant="all", cols="*")
    t.create(method="cols.acos", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.acos", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.acos", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"atan_test": [1, "0", 11, float("nan"), float("inf"), None]})

    t.create(df=df2, method="cols.atan", cols=["atan_test"], select_cols=True)
    t.create(method="cols.atan", variant="all", cols="*")
    t.create(method="cols.atan", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.atan", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.atan", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"sinh_test": [float("inf"), "3.141592", -2.7182, 0, None, 5000]})

    t.create(df=df2, method="cols.sinh", cols=["sinh_test"], select_cols=True)
    t.create(method="cols.sinh", variant="all", cols="*")
    t.create(method="cols.sinh", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.sinh", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.sinh", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"cosh_test": [float("inf"), "-3.141592", 2.7182, 0, None, -5000]})

    t.create(df=df2, method="cols.cosh", cols=["cosh_test"], select_cols=True)
    t.create(method="cols.cosh", variant="all", cols="*")
    t.create(method="cols.cosh", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.cosh", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.cosh", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"tanh_test": [float("-inf"), "3.141592", 2.7182, -1, None, 5000]})

    t.create(df=df2, method="cols.tanh", cols=["tanh_test"], select_cols=True)
    t.create(method="cols.tanh", variant="all", cols="*")
    t.create(method="cols.tanh", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.tanh", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.tanh", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"asinh_test": [None, float("nan"), 1, -0.34, float("inf"), 13]})

    t.create(df=df2, method="cols.asinh", cols=["asinh_test"], select_cols=True)
    t.create(method="cols.asinh", variant="all", cols="*")
    t.create(method="cols.asinh", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.asinh", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.asinh", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"acosh_test": [None, float("nan"), 1, 0, float("-inf"), 813]})

    t.create(df=df2, method="cols.acosh", cols=["acosh_test"], select_cols=True)
    t.create(method="cols.acosh", variant="all", cols="*")
    t.create(method="cols.acosh", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.acosh", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.acosh", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append({"atanh_test": [None, float("nan"), 9, -703, float("-inf"), 0]})

    t.create(df=df2, method="cols.atanh", cols=["atanh_test"], select_cols=True)
    t.create(method="cols.atanh", variant="all", cols="*")
    t.create(method="cols.atanh", variant="numeric", cols=["height(ft)"], select_cols=True)
    t.create(method="cols.atanh", variant="string", cols=["names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.atanh", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    t.run()

    t = TestCreator(op, df, name="math", configs=default_configs)

    df2_cols = {"add_test1": [None, float("nan"), -9, 10.234, float("-inf"), -42],
                "add_test2": [None, 1, 9, 703, float("inf"), -321]}
    df2 = df.cols.append(df2_cols)

    t.create(df=df2, method="cols.add", cols=["add_test1", "add_test2"], select_cols=True)
    t.create(method="cols.add", variant="all", cols="*")
    t.create(method="cols.add", variant="2_numerics", cols=["height(ft)", "rank"], select_cols=True)
    t.create(method="cols.add", variant="3_numerics", cols=["height(ft)", "rank", "age"], select_cols=True)
    t.create(method="cols.add", variant="all_numerics", cols=["height(ft)", "rank", "age", "weight(t)"], select_cols=True)
    t.create(method="cols.add", variant="string", cols=["names", "function"], output_col="names-function", select_cols=True)
    t.create(method="cols.add", variant="various_types", cols=["NullType", "weight(t)", "japanese name"], output_col="nt+wt+jn")

    df2_cols = {"sub_test1": [None, float("nan"), 9, 10.234, float("inf"), -42],
                "sub_test2": [None, 1, 9, 703, float("inf"), -321]}
    df2 = df.cols.append(df2_cols)

    t.create(df=df2, method="cols.sub", cols=["sub_test1", "sub_test2"], select_cols=True)
    t.create(method="cols.sub", variant="all", cols="*")
    t.create(method="cols.sub", variant="2_numerics", cols=["height(ft)", "rank"], select_cols=True)
    t.create(method="cols.sub", variant="3_numerics", cols=["height(ft)", "rank", "age"], select_cols=True)
    t.create(method="cols.sub", variant="all_numerics", cols=["height(ft)", "rank", "age", "weight(t)"], select_cols=True)
    t.create(method="cols.sub", variant="string", cols=["names", "function"], output_col="names-function", select_cols=True)
    t.create(method="cols.sub", variant="various_types", cols=["NullType", "weight(t)", "japanese name"], output_col="nt-wt-jn")

    df2_cols = {"mul_test1": [None, float("nan"), 8, 10.234, float("-inf"), -42],
                "mul_test2": [None, 1, 0.125, 703, float("inf"), -321]}
    df2 = df.cols.append(df2_cols)

    t.create(df=df2, method="cols.mul", cols=["mul_test1", "mul_test2"], select_cols=True)
    t.create(method="cols.mul", variant="all", cols="*")
    t.create(method="cols.mul", variant="2_numerics", cols=["height(ft)", "rank"], select_cols=True)
    t.create(method="cols.mul", variant="3_numerics", cols=["height(ft)", "rank", "age"], select_cols=True)
    t.create(method="cols.mul", variant="all_numerics", cols=["height(ft)", "rank", "age", "weight(t)"], select_cols=True)
    t.create(method="cols.mul", variant="string", cols=["names", "function"], output_col="names*function", select_cols=True)
    t.create(method="cols.mul", variant="various_types", cols=["NullType", "weight(t)", "japanese name"], output_col="nt*wt*jn")

    df2_cols = {"div_test1": [None, float("nan"), -8, 10.234, float("-inf"), -42],
                "div_test2": [None, 1, 0, 703, float("inf"), -321]}
    df2 = df.cols.append(df2_cols)

    t.create(df=df2, method="cols.div", cols=["div_test1", "div_test2"], select_cols=True)
    t.create(method="cols.div", variant="all", cols="*")
    t.create(method="cols.div", variant="2_numerics", cols=["height(ft)", "rank"], select_cols=True)
    t.create(method="cols.div", variant="3_numerics", cols=["height(ft)", "rank", "age"], select_cols=True)
    t.create(method="cols.div", variant="all_numerics", cols=["height(ft)", "rank", "age", "weight(t)"], select_cols=True)
    t.create(method="cols.div", variant="string", cols=["names", "function"], output_col="names/function", select_cols=True)
    t.create(method="cols.div", variant="various_types", cols=["NullType", "weight(t)", "japanese name"], output_col="nt*wt*jn")

    df2_cols = {"rdiv_test1": [None, float("nan"), -8, 10.234, float("-inf"), -42],
                "rdiv_test2": [None, 1, 0, 703, float("inf"), -321]}
    df2 = df.cols.append(df2_cols)

    t.create(df=df2, method="cols.rdiv", cols=["rdiv_test1", "rdiv_test2"], select_cols=True)
    t.create(method="cols.rdiv", variant="all", cols="*")
    t.create(method="cols.rdiv", variant="2_numerics", cols=["height(ft)", "rank"], select_cols=True)
    t.create(method="cols.rdiv", variant="3_numerics", cols=["height(ft)", "rank", "age"], select_cols=True)
    t.create(method="cols.rdiv", variant="all_numerics", cols=["height(ft)", "rank", "age", "weight(t)"], select_cols=True)
    t.create(method="cols.rdiv", variant="string", cols=["names", "function"], output_col="names*function", select_cols=True)
    t.create(method="cols.rdiv", variant="various_types", cols=["NullType", "weight(t)", "japanese name"], output_col="nt*wt*jn")

    t.run()

    t = TestCreator(op, df, name="statistics", configs=default_configs)

    all_non_date = ['NullType', 'attributes', 'date arrival', 'function(binary)', 'height(ft)', 'japanese name', 'last date seen', 'last position seen', 'rank', 'Cybertronian', 'age', 'function', 'names', 'timestamp', 'weight(t)']

    t.create(method="cols.mad", variant="all", cols=all_non_date, estimate=False)
    t.create(method="cols.mad", variant="numeric", cols="weight(t)", relative_error=0.45, estimate=False)
    t.create(method="cols.mad", variant="multiple", cols=["height(ft)", "age", "rank"], more=True, estimate=False)

    t.create(method="cols.min", variant="all", cols="*")
    t.create(method="cols.min", variant="numeric", cols="weight(t)")
    t.create(method="cols.min", variant="multiple", cols=["height(ft)", "age", "rank"])

    t.create(method="cols.max", variant="all", cols="*")
    t.create(method="cols.max", variant="numeric", cols="weight(t)")
    t.create(method="cols.max", variant="multiple", cols=["height(ft)", "age", "rank"])

    
    df2_cols = {"weight": [10.2, 10.2, 20.4, 20.5, 33.2],
                "height": [26, 17, 26, 17, None, 300],
                'date arrival': ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'],
                'age': [5000000, 5000000, 5000000, 5000000, 5000000],
                'rank': [10, 7, 7, 8, 10, 8]}
    df2 = op.create.dataframe(df2_cols)

    t.create(df=df2, method="cols.mode", variant="all", cols="*")
    t.create(df=df2, method="cols.mode", variant="numeric", cols="weight")
    t.create(df=df2, method="cols.mode", variant="multiple", cols=["height", "age", "rank"])

    t.create(method="cols.percentile", variant="all", cols="*", estimate=False)
    t.create(method="cols.percentile", variant="numeric_single", cols="weight(t)", values=0.30, estimate=False)
    t.create(method="cols.percentile", variant="numeric_multiple", cols="weight(t)", values=[0.25, 0.50, 0.75], estimate=False)
    t.create(method="cols.percentile", variant="multiple", cols=["height(ft)", "age", "rank"], values=0.95, relative_error=0.012, estimate=False)

    t.create(method="cols.range", variant="all", cols="*")
    t.create(method="cols.range", variant="numeric", cols="weight(t)")
    t.create(method="cols.range", variant="multiple", cols=["height(ft)", "age", "rank"])

    t.create(method="cols.median", variant="all", cols="*")
    t.create(method="cols.median", variant="numeric", cols="weight(t)", relative_error=0.33)
    t.create(method="cols.median", variant="multiple", cols=["height(ft)", "age", "rank"], relative_error=0.012)

    t.create(method="cols.kurtosis", variant="all", cols="*")
    t.create(method="cols.kurtosis", variant="numeric", cols="weight(t)")
    t.create(method="cols.kurtosis", variant="multiple", cols=["height(ft)", "age", "rank"])

    t.create(method="cols.skew", variant="all", cols="*")
    t.create(method="cols.skew", variant="numeric", cols="weight(t)")
    t.create(method="cols.skew", variant="multiple", cols=["height(ft)", "age", "rank"])

    t.create(method="cols.mean", variant="all", cols="*")
    t.create(method="cols.mean", variant="numeric", cols="weight(t)")
    t.create(method="cols.mean", variant="multiple", cols=["height(ft)", "age", "rank"])

    t.create(method="cols.sum", variant="all", cols="*")
    t.create(method="cols.sum", variant="numeric", cols="weight(t)")
    t.create(method="cols.sum", variant="multiple", cols=["height(ft)", "age", "rank"])

    t.create(method="cols.cumsum", variant="all", cols="*")
    t.create(method="cols.cumsum", variant="numeric", cols="weight(t)")
    t.create(method="cols.cumsum", variant="multiple", cols=["height(ft)", "age", "rank"])

    t.create(method="cols.cumprod", variant="all", cols="*")
    t.create(method="cols.cumprod", variant="numeric", cols="weight(t)")
    t.create(method="cols.cumprod", variant="multiple", cols=["height(ft)", "age", "rank"])

    t.create(method="cols.cummax", variant="all", cols="*")
    t.create(method="cols.cummax", variant="numeric", cols="weight(t)")
    t.create(method="cols.cummax", variant="multiple", cols=["height(ft)", "age", "rank"])

    t.create(method="cols.cummin", variant="all", cols="*")
    t.create(method="cols.cummin", variant="numeric", cols="weight(t)")
    t.create(method="cols.cummin", variant="multiple", cols=["height(ft)", "age", "rank"])

    t.create(method="cols.var", variant="all", cols=all_non_date)
    t.create(method="cols.var", variant="numeric", cols="weight(t)")
    t.create(method="cols.var", variant="multiple", cols=["height(ft)", "age", "rank"])

    t.create(method="cols.std", variant="all", cols=all_non_date)
    t.create(method="cols.std", variant="numeric", cols="weight(t)")
    t.create(method="cols.std", variant="multiple", cols=["height(ft)", "age", "rank"])

    t.run()

    df = op.load.csv("../../examples/data/store.csv", n_rows=10)

    t = TestCreator(op, df, name="more_statistics", configs=default_configs)

    _v = [9, 9, 9, 9, 3, 3, 3, 20, 20, 1, 1, 0, 0, 4]
    _v2 = ["nine", [9], {"nine": 9}, 9, 3, 3, 3, None, None, 1, 1, 1, 1, 1]

    df = op.create.dataframe(vf=list(map(lambda x: x * 1.1, _v)), vs=list(map(lambda x: "STR" + str(x), _v)), values=_v, o=_v2)

    t.create(df=df, method="cols.profile", variant="all", cols="*")
    t.create(df=df, method="cols.profile", variant="numeric", cols="values", bins=10, flush=True)
    t.create(df=df, method="cols.profile", variant="multiple", cols=["vs", "vf"], bins=8, flush=False)

    t.create(method="cols.quality", variant="all", cols="*")
    t.create(method="cols.quality", variant="numeric", cols="price", flush=True)
    t.create(method="cols.quality", variant="multiple", cols=["id", "code", "discount"], flush=False)

    t.create(df=df, method="cols.frequency", variant="all", cols="*", n=10, count_uniques=True)
    t.create(df=df, method="cols.frequency", variant="numeric", cols="values", n=4, percentage=True, total_rows=3)
    t.create(df=df, method="cols.frequency", variant="string", cols="vs", n=5, percentage=False)
    t.create(df=df, method="cols.frequency", variant="multiple", cols=["vs", "vf"], n=6, percentage=True)

    t.create(method="cols.hist", variant="all", cols="*", buckets=2)
    t.create(method="cols.hist", variant="numeric", cols="price", buckets=10)
    t.create(method="cols.hist", variant="multiple", cols=["id", "code", "discount"], buckets=4)

    # t.create(method="cols.crosstab", variant="numeric_numeric", col_x="discount", col_y="price", output="dict")
    # t.create(method="cols.crosstab", variant="numeric_string", col_x="price", col_y="code", output="dataframe")
    # t.create(method="cols.crosstab", variant="string_numeric", col_x="name", col_y="id", output="dataframe")
    # t.create(method="cols.crosstab", variant="string_string", col_x="code", col_y="name", output="dict")

    t.create(method="cols.boxplot", variant="all", cols="*")
    t.create(method="cols.boxplot", variant="numeric", cols="price")
    t.create(method="cols.boxplot", variant="multiple", cols=["id", "code", "discount"])

    t.create(method="cols.heatmap", variant="numeric_numeric", col_x="discount", col_y="price", bins_x=5, bins_y=10)
    t.create(method="cols.heatmap", variant="numeric_string", col_x="price", col_y="code", bins_x=3, bins_y=1)
    t.create(method="cols.heatmap", variant="string_numeric", col_x="name", col_y="id", bins_x=7, bins_y=10)
    t.create(method="cols.heatmap", variant="string_string", col_x="code", col_y="name", bins_x=4, bins_y=4)

    t.create(method="cols.correlation", variant="all_pearson", args=["*", "pearson"])
    t.create(method="cols.correlation", variant="all_spearman", args=["*", "spearman"])
    t.create(method="cols.correlation", variant="all_kendall", args=["*", "kendall"])
    t.create(method="cols.correlation", variant="numeric_pearson", args=["price","pearson"])
    t.create(method="cols.correlation", variant="numeric_spearman", args=["price","spearman"])
    t.create(method="cols.correlation", variant="numeric_kendall", args=["price","kendall"])
    t.create(method="cols.correlation", variant="multiple_pearson", args=[["id", "price"], "pearson"])
    t.create(method="cols.correlation", variant="multiple_spearman", args=[["id", "price"], "spearman"])
    t.create(method="cols.correlation", variant="multiple_kendall", args=[["id", "price"], "kendall"])

    t.create(method="cols.infer_type", variant="all", cols="*")
    t.create(method="cols.infer_type", variant="numeric", cols="price")
    t.create(method="cols.infer_type", variant="multiple", cols=["id", "code", "discount"])

    t.create(method="cols.unique_values", variant="all", cols="*")
    t.create(method="cols.unique_values", variant="numeric", cols="price", estimate=True)
    t.create(method="cols.unique_values", variant="multiple", cols=["id", "code", "discount"], estimate=False)

    t.create(method="cols.count_uniques", variant="all", cols="*")
    t.create(method="cols.count_uniques", variant="numeric", cols="price", estimate=True)
    t.create(method="cols.count_uniques", variant="multiple", cols=["id", "code", "discount"], estimate=False)

    t.create(method="cols.count_zeros", variant="all", cols="*")
    t.create(method="cols.count_zeros", variant="numeric", cols="price")
    t.create(method="cols.count_zeros", variant="multiple", cols=["id", "code", "discount"])

    t.run()

create()

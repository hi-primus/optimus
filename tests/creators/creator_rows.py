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

    t = TestCreator(op, df, name="rows", configs=default_configs)

    df2 = op.load.csv("../../examples/data/foo.csv", n_rows=2)
    df3 = op.create.dataframe({
        "email_test": ["an@example.com", "thisisatest@gmail.com", "somename@hotmail.com"],
        "ip_test": ["192.0.2.1", "192.158.1.38", "192.168.136.52"]})

    t.create(method="rows.append", variant="single_complete", dfs=[df2])
    t.create(method="rows.append", variant="single_some_cols", dfs=[df2], names_map={"First":["function","price"],"Second":["weight(t)","id"],"Third":["Cybertronian","birth"]})
    t.create(method="rows.append", variant="single_one_col", dfs=[df2], names_map={"Only":["japanese name","billingId"]})
    t.create(method="rows.append", variant="multiple_complete", dfs=[df2,df3])
    t.create(method="rows.append", variant="multiple_some_cols", dfs=[df2,df3], names_map={"First":["last position seen","product","ip_test"],"Second":["date arrival","firstName","object_test"],"Third":["names","lastName","email_test"]})
    t.create(method="rows.append", variant="multiple_one_col", dfs=[df2,df3], names_map={"Only":["attributes","dummyCol","object_test"]})

    t.create(method="rows.select", variant="mask", expr=df.mask.none("last position seen"))
    t.create(method="rows.select", variant="expression", expr=df["height(ft)"]>15)
    t.create(method="rows.select", variant="col_name_all", expr="*", contains="a")
    t.create(method="rows.select", variant="col_name_numeric", expr=["height(ft)"], contains="0", flags=0, na=True, regex=False)
    t.create(method="rows.select", variant="col_name_string", expr=["function"], contains="Le.", case=True, flags=3, na=False, regex=True)
    t.create(method="rows.select", variant="col_name_multiple", expr=["NullType", "weight(t)", "Cybertronian"], contains="T|N", case=True, flags=1, na=True, regex=True)

    t.create(method="rows.count")

    t.create(method="rows.to_list", variant="all", input_cols="*")
    t.create(method="rows.to_list", variant="single", input_cols=["names"])
    t.create(method="rows.to_list", variant="multiple", input_cols=["rank", "last date seen", "Cybertronian"])

    t.create(method="rows.sort", variant="all_asc", cols="*", order="asc")
    t.create(method="rows.sort", variant="all_desc", cols="*", order="desc")
    t.create(method="rows.sort", variant="single_asc", cols=["names"], order="asc")
    t.create(method="rows.sort", variant="single_desc", cols=["names"], order="desc")
    t.create(method="rows.sort", variant="multiple_asc", cols=["rank", "last date seen", "Cybertronian"], order="asc")
    t.create(method="rows.sort", variant="multiple_desc", cols=["rank", "last date seen", "Cybertronian"], order="desc")

    t.create(method="rows.reverse")

    t.create(method="rows.drop", variant="mask", where=df.mask.none("last position seen"))
    t.create(method="rows.drop", variant="expression", where=df["height(ft)"]>15)
    t.create(method="rows.drop", variant="col_name", where="Cybertronian")

    t.create(method="rows.between_index", variant="all", cols="*", lower_bound=0, upper_bound=1)
    t.create(method="rows.between_index", variant="single", cols=["names"], lower_bound=4, upper_bound=4)
    t.create(method="rows.between_index", variant="multiple", cols=["rank", "last date seen", "Cybertronian"], lower_bound=2, upper_bound=5)

    t.create(method="rows.limit", variant="less", count=2)
    t.create(method="rows.limit", variant="equal", count=6)
    t.create(method="rows.limit", variant="more", count=150)

    t.create(method="rows.approx_count")

    t.create(method="rows.str", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.str", variant="numeric", cols=["weight(t)"], drop=False, how="any")
    t.create(method="rows.str", variant="string", cols=["function"], drop=True, how="all")
    t.create(method="rows.str", variant="multiple", cols=["rank", "last date seen", "Cybertronian"], drop=False, how="all")

    t.create(method="rows.int", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.int", variant="numeric", cols=["weight(t)"], drop=False, how="any")
    t.create(method="rows.int", variant="string", cols=["function"], drop=True, how="all")
    t.create(method="rows.int", variant="multiple", cols=["rank", "last date seen", "Cybertronian"], drop=False, how="all")

    t.create(method="rows.float", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.float", variant="numeric", cols=["weight(t)"], drop=False, how="any")
    t.create(method="rows.float", variant="string", cols=["function"], drop=True, how="all")
    t.create(method="rows.float", variant="multiple", cols=["rank", "last date seen", "Cybertronian"], drop=False, how="all")

    t.create(method="rows.numeric", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.numeric", variant="numeric", cols=["weight(t)"], drop=False, how="any")
    t.create(method="rows.numeric", variant="string", cols=["function"], drop=True, how="all")
    t.create(method="rows.numeric", variant="multiple", cols=["rank", "last date seen", "Cybertronian"], drop=False, how="all")

    t.create(method="rows.greater_than", variant="all", cols="*", value=10, drop=True, how="any")
    t.create(method="rows.greater_than", variant="numeric", cols=["height(ft)"], value=-0.31, drop=False, how="any")
    t.create(method="rows.greater_than", variant="string", cols=["names"], value=0, drop=True, how="all")
    t.create(method="rows.greater_than", variant="multiple", cols=["age", "NullType", "weight(t)"], value="-inf", drop=False, how="all")

    t.create(method="rows.greater_than_equal", variant="all", cols="*", value=10, drop=True, how="any")
    t.create(method="rows.greater_than_equal", variant="numeric", cols=["height(ft)"], value=0.31, drop=False, how="any")
    t.create(method="rows.greater_than_equal", variant="string", cols=["names"], value=0, drop=True, how="all")
    t.create(method="rows.greater_than_equal", variant="multiple", cols=["age", "NullType", "weight(t)"], value="-inf", drop=False, how="all")

    t.create(method="rows.less_than", variant="all", cols="*", value=10, drop=True, how="any")
    t.create(method="rows.less_than", variant="numeric", cols=["height(ft)"], value=0.31, drop=False, how="any")
    t.create(method="rows.less_than", variant="string", cols=["names"], value=0, drop=True, how="all")
    t.create(method="rows.less_than", variant="multiple", cols=["age", "NullType", "weight(t)"], value="inf", drop=False, how="all")

    t.create(method="rows.less_than_equal", variant="all", cols="*", value=10, drop=True, how="any")
    t.create(method="rows.less_than_equal", variant="numeric", cols=["height(ft)"], value=0.31, drop=False, how="any")
    t.create(method="rows.less_than_equal", variant="string", cols=["names"], value=0, drop=True, how="all")
    t.create(method="rows.less_than_equal", variant="multiple", cols=["age", "NullType", "weight(t)"], value="inf", drop=False, how="all")

    t.create(method="rows.between", variant="all", cols="*", lower_bound="-inf", upper_bound="inf", equal=True, drop=True, how="any")
    t.create(method="rows.between", variant="numeric", cols=["height(ft)"], bounds=[[26, -28]], equal=False, drop=False, how="any")
    t.create(method="rows.between", variant="string", cols=["names"], upper_bound="-inf", lower_bound=0, equal=False, drop=True, how="all")
    t.create(method="rows.between", variant="multiple", cols=["age", "NullType", "weight(t)"], bounds=[["-inf", -10], [0, 1.9999], [300, 5000000]], equal=True, drop=False, how="all")

    t.create(method="rows.equal", variant="all", cols="*", value=10, drop=True, how="any")
    t.create(method="rows.equal", variant="numeric", cols=["height(ft)"], value=300, drop=False, how="any")
    t.create(method="rows.equal", variant="string", cols=["function"], value="Leader", drop=True, how="all")
    t.create(method="rows.equal", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], value=True, drop=False, how="all")

    t.create(method="rows.not_equal", variant="all", cols="*", value=10, drop=True, how="any")
    t.create(method="rows.not_equal", variant="numeric", cols=["height(ft)"], value=300, drop=False, how="any")
    t.create(method="rows.not_equal", variant="string", cols=["function"], value="Leader", drop=True, how="all")
    t.create(method="rows.not_equal", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], value=True, drop=False, how="all")
    
    t.create(method="rows.missing", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.missing", variant="numeric", cols=["height(ft)"], drop=False, how="any")
    t.create(method="rows.missing", variant="string", cols=["names"], drop=True, how="all")
    t.create(method="rows.missing", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], drop=False, how="all")

    t.create(method="rows.null", variant="all", cols="*", drop=True, how="all")
    t.create(method="rows.null", variant="numeric", cols=["height(ft)"], drop=True, how="any")
    t.create(method="rows.null", variant="string", cols=["names"], drop=False, how="all")
    t.create(method="rows.null", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], drop=False, how="any")

    t.create(method="rows.none", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.none", variant="numeric", cols=["height(ft)"], drop=False, how="any")
    t.create(method="rows.none", variant="string", cols=["names"], drop=True, how="all")
    t.create(method="rows.none", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], drop=False, how="all")

    t.create(method="rows.nan", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.nan", variant="numeric", cols=["height(ft)"], drop=False, how="any")
    t.create(method="rows.nan", variant="string", cols=["names"], drop=True, how="all")
    t.create(method="rows.nan", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], drop=False, how="all")

    t.create(method="rows.duplicated", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.duplicated", variant="all_first", cols="*", keep="first", drop=False, how="any")
    t.create(method="rows.duplicated", variant="all_last", cols="*", keep="last", drop=True, how="all")
    t.create(method="rows.duplicated", variant="numeric", cols=["rank"], drop=False, how="all")
    t.create(method="rows.duplicated", variant="numeric_first", cols=["rank"], keep="first", drop=True, how="any")
    t.create(method="rows.duplicated", variant="numeric_last", cols=["rank"], keep="last", drop=False, how="any")
    t.create(method="rows.duplicated", variant="string", cols=["names"], drop=True, how="all")
    t.create(method="rows.duplicated", variant="string_first", cols=["names"], keep="first", drop=False, how="all")
    t.create(method="rows.duplicated", variant="string_last", cols=["names"], keep="last", drop=True, how="any")
    t.create(method="rows.duplicated", variant="multiple", cols=["NullType", "timestamp", "Cybertronian"], keep="first", drop=False, how="any")
    t.create(method="rows.duplicated", variant="multiple_first", cols=["NullType", "timestamp", "Cybertronian"], keep="first", drop=True, how="all")
    t.create(method="rows.duplicated", variant="multiple_last", cols=["NullType", "timestamp", "Cybertronian"], keep="first", drop=False, how="all")

    t.create(method="rows.unique", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.unique", variant="all_first", cols="*", keep="first", drop=False, how="any")
    t.create(method="rows.unique", variant="all_last", cols="*", keep="last", drop=True, how="all")
    t.create(method="rows.unique", variant="numeric", cols=["rank"], drop=False, how="all")
    t.create(method="rows.unique", variant="numeric_first", cols=["rank"], keep="first", drop=True, how="any")
    t.create(method="rows.unique", variant="numeric_last", cols=["rank"], keep="last", drop=False, how="any")
    t.create(method="rows.unique", variant="string", cols=["names"], drop=True, how="all")
    t.create(method="rows.unique", variant="string_first", cols=["names"], keep="first", drop=False, how="all")
    t.create(method="rows.unique", variant="string_last", cols=["names"], keep="last", drop=True, how="any")
    t.create(method="rows.unique", variant="multiple", cols=["NullType", "timestamp", "Cybertronian"], keep="first", drop=False, how="any")
    t.create(method="rows.unique", variant="multiple_first", cols=["NullType", "timestamp", "Cybertronian"], keep="first", drop=True, how="all")
    t.create(method="rows.unique", variant="multiple_last", cols=["NullType", "timestamp", "Cybertronian"], keep="first", drop=False, how="all")

    t.create(method="rows.empty", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.empty", variant="numeric", cols=["height(ft)"], drop=False, how="any")
    t.create(method="rows.empty", variant="string", cols=["names"], drop=True, how="all")
    t.create(method="rows.empty", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], drop=False, how="all")
    
    t.create(method="rows.value_in", variant="all", cols="*", values=[10, True, None, "Jazz"], drop=True, how="any")
    t.create(method="rows.value_in", variant="numeric", cols=["height(ft)"], values=[300, "nan"], drop=False, how="any")
    t.create(method="rows.value_in", variant="string", cols=["function"], values="Leader", drop=True, how="all")
    t.create(method="rows.value_in", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], values=[False, None, 4], drop=False, how="all")
    
    t.create(method="rows.pattern", variant="all", cols="*", pattern="**cc**", drop=True, how="any")
    t.create(method="rows.pattern", variant="numeric", cols=["height(ft)"], pattern="##!#", drop=False, how="any")
    t.create(method="rows.pattern", variant="string", cols=["function"], pattern="Ullclc", drop=True, how="all")
    t.create(method="rows.pattern", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], pattern="****", drop=False, how="all")
    
    t.create(method="rows.starts_with", variant="all", cols="*", value="N", drop=True, how="any")
    t.create(method="rows.starts_with", variant="numeric", cols=["height(ft)"], value=1, drop=False, how="any")
    t.create(method="rows.starts_with", variant="string", cols=["function"], value="Lead", drop=True, how="all")
    t.create(method="rows.starts_with", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], value=True, drop=False, how="all")

    t.create(method="rows.ends_with", variant="all", cols="*", value="]", drop=True, how="any")
    t.create(method="rows.ends_with", variant="numeric", cols=["height(ft)"], value=0, drop=False, how="any")
    t.create(method="rows.ends_with", variant="string", cols=["function"], value="e", drop=True, how="all")
    t.create(method="rows.ends_with", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], value="one", drop=False, how="all")

    t.create(method="rows.contains", variant="all", cols="*", value="a", drop=True, how="any")
    t.create(method="rows.contains", variant="numeric", cols=["height(ft)"], value="0", drop=False, how="any")
    t.create(method="rows.contains", variant="string", cols=["function"], value="Le", drop=True, how="all")
    t.create(method="rows.contains", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], value="T", drop=False, how="all")

    t.create(method="rows.find", variant="all", cols="*", value="None", drop=True, how="any")
    t.create(method="rows.find", variant="numeric", cols=["height(ft)"], value=13, drop=False, how="any")
    t.create(method="rows.find", variant="string", cols=["function"], value="Leader", drop=True, how="all")
    t.create(method="rows.find", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], value=1, drop=False, how="all")

    df2 = df.cols.append(
        {"email_test": ["an@example.com", "thisisatest@gmail.com", "somename@hotmail.com", "an@outlook.com", "anexample@mail.com", "example@yahoo.com"]})

    t.create(df=df2, method="rows.email", cols=["email_test"], drop=False, how="any")
    t.create(method="rows.email", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.email", variant="numeric", cols=["height(ft)"], drop=False, how="any")
    t.create(method="rows.email", variant="string", cols=["names"], drop=True, how="all")
    t.create(method="rows.email", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], drop=False, how="all")

    df2 = df.cols.append(
        {"ip_test": ["192.0.2.1", "192.158.1.38", "192.168.136.52", "172.16.92.107", "10.63.215.5", "10.0.5.0"]})

    t.create(df=df2, method="rows.ip", cols=["ip_test"], drop=False, how="any")
    t.create(method="rows.ip", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.ip", variant="numeric", cols=["height(ft)"], drop=False, how="any")
    t.create(method="rows.ip", variant="string", cols=["names"], drop=True, how="all")
    t.create(method="rows.ip", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], drop=False, how="all")

    df2 = df.cols.append(
        {"urls_test": ["https://github.com/hi-primus/optimus", "localhost:3000?help=true", "http://www.images.hi-example.com:54/images.php#id?help=1&freq=2", "hi-optimus.com", "https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test", "https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALe&source=hp&ei=NL0-y4&iflsig=AINF&oq=this+is+a+test&gs_lcp=MZgBAKA&sclient=gws-wiz&ved=0ah&uact=5"]})
    
    t.create(df=df2, method="rows.url", cols=["urls_test"], drop=False, how="any")
    t.create(method="rows.url", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.url", variant="numeric", cols=["height(ft)"], drop=False, how="any")
    t.create(method="rows.url", variant="string", cols=["names"], drop=True, how="all")
    t.create(method="rows.url", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], drop=False, how="all")

    df2 = df.cols.append(
        {"gender_test": ["male", "female", "Male", "5.0", "MALE", "FEMALE"]})
    
    t.create(df=df2, method="rows.gender", cols=["gender_test"], drop=False, how="any")
    t.create(method="rows.gender", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.gender", variant="numeric", cols=["height(ft)"], drop=False, how="any")
    t.create(method="rows.gender", variant="string", cols=["names"], drop=True, how="all")
    t.create(method="rows.gender", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], drop=False, how="all")

    df2 = df.cols.append(
        {"boolean_test": ["True", "False", True, False, 1, 0]})
    
    t.create(df=df2, method="rows.boolean", cols=["boolean_test"], drop=False, how="any")
    t.create(method="rows.boolean", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.boolean", variant="numeric", cols=["height(ft)"], drop=False, how="any")
    t.create(method="rows.boolean", variant="string", cols=["names"], drop=True, how="all")
    t.create(method="rows.boolean", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], drop=False, how="all")

    df2 = df.cols.append(
        {"zip_code_test": [90210, 21252, 36104, 99801, 85001, 10]})
    
    t.create(df=df2, method="rows.zip_code", cols=["zip_code_test"], drop=False, how="any")
    t.create(method="rows.zip_code", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.zip_code", variant="numeric", cols=["height(ft)"], drop=False, how="any")
    t.create(method="rows.zip_code", variant="string", cols=["names"], drop=True, how="all")
    t.create(method="rows.zip_code", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], drop=False, how="all")

    df2 = df.cols.append(
        {"credit_card_number_test": ["2345 6362 6362 8632", 5692857295730750, "31028482204828450", "99 77 80 14 53 73 83 53", "8 5 0 0 1 5 8 1 5 8 3 7 0 0 0 1", 10]})
    
    t.create(df=df2, method="rows.credit_card_number", cols=["credit_card_number_test"], drop=False, how="any")
    t.create(method="rows.credit_card_number", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.credit_card_number", variant="numeric", cols=["height(ft)"], drop=False, how="any")
    t.create(method="rows.credit_card_number", variant="string", cols=["names"], drop=True, how="all")
    t.create(method="rows.credit_card_number", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], drop=False, how="all")

    df2 = df.cols.append(
        {"datetime_test": ['1980/04/10', '5.0', datetime.datetime(2016, 9, 10), datetime.datetime(2014, 6, 24, 0, 0), '2013/06/10', datetime.datetime(2011, 4, 10)]})
    
    t.create(df=df2, method="rows.datetime", cols=["datetime_test"], drop=False, how="any")
    t.create(method="rows.datetime", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.datetime", variant="numeric", cols=["height(ft)"], drop=False, how="any")
    t.create(method="rows.datetime", variant="string", cols=["names"], drop=True, how="all")
    t.create(method="rows.datetime", variant="multiple", cols=["Date Type", "last date seen", "timestamp"], drop=False, how="all")

    df2 = df.cols.append(
        {"object_test": ['1980/04/10', True, None, float("inf"), "yes", "bytearray(12, 'utf-8')"]})
    
    t.create(df=df2, method="rows.object", cols=["object_test"], drop=False, how="any")
    t.create(method="rows.object", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.object", variant="numeric", cols=["height(ft)"], drop=False, how="any")
    t.create(method="rows.object", variant="string", cols=["names"], drop=True, how="all")
    t.create(method="rows.object", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], drop=False, how="all")

    df2 = df.cols.append(
        {"array_test": [[1, 2, 3, 4], [[1], [2, 3]], ["one", "two"], ["one", ["two", [float("-inf")]]], ["yes"], "bytearray(12, 'utf-8')"]})
    
    t.create(df=df2, method="rows.array", cols=["array_test"], drop=False, how="any")
    t.create(method="rows.array", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.array", variant="numeric", cols=["height(ft)"], drop=False, how="any")
    t.create(method="rows.array", variant="string", cols=["names"], drop=True, how="all")
    t.create(method="rows.array", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], drop=False, how="all")

    df2 = df.cols.append(
        {"phone_number_test": [5516528967, "55 8395 1284", "+52 55 3913 1941", "+1 (210) 589-6721", 12106920692, "5532592785"]})
    
    t.create(df=df2, method="rows.phone_number", cols=["phone_number_test"], drop=False, how="any")
    t.create(method="rows.phone_number", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.phone_number", variant="numeric", cols=["height(ft)"], drop=False, how="any")
    t.create(method="rows.phone_number", variant="string", cols=["names"], drop=True, how="all")
    t.create(method="rows.phone_number", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], drop=False, how="all")

    df2 = df.cols.append(
        {"social_security_number_test": [372847278, "551-83-1284", "525 93 1941", "230 89-6721", "121-069 2062", "371847288"]})
    
    t.create(df=df2, method="rows.social_security_number", cols=["social_security_number_test"], drop=False, how="any")
    t.create(method="rows.social_security_number", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.social_security_number", variant="numeric", cols=["height(ft)"], drop=False, how="any")
    t.create(method="rows.social_security_number", variant="string", cols=["names"], drop=True, how="all")
    t.create(method="rows.social_security_number", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], drop=False, how="all")

    df2 = df.cols.append(
        {"http_code_test": ["http://github.com/hi-primus/optimus", "localhost:3000?help=true", "http://www.images.hi-example.com:54/images.php#id?help=1&freq=2", "hi-optimus.com", "http://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test", "http://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALe&source=hp&ei=NL0-y4&iflsig=AINF&oq=this+is+a+test&gs_lcp=MZgBAKA&sclient=gws-wiz&ved=0ah&uact=5"]})
    
    t.create(df=df2, method="rows.http_code", cols=["http_code_test"], drop=False, how="any")
    t.create(method="rows.http_code", variant="all", cols="*", drop=True, how="any")
    t.create(method="rows.http_code", variant="numeric", cols=["height(ft)"], drop=False, how="any")
    t.create(method="rows.http_code", variant="string", cols=["names"], drop=True, how="all")
    t.create(method="rows.http_code", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], drop=False, how="all")

    t.create(method="rows.expression", variant="all_expression", cols="*", where='df["rank"]>8', drop=True, how="any")
    t.create(method="rows.expression", variant="all_colname", where="last position seen", drop=False, how="any")
    t.create(method="rows.expression", variant="numeric_expression", where='df["height(ft)"]>=0', drop=True, how="all")
    t.create(method="rows.expression", variant="numeric_colname", where="height(ft)", drop=False, how="all")
    t.create(method="rows.expression", variant="string_expression", where='df["function"]=="Leader"', drop=True, how="any")
    t.create(method="rows.expression", variant="string_colname", where="function", drop=False, how="any")
    t.create(method="rows.expression", variant="multiple_expression", where='df["Cybertronian"]==False', drop=True, how="all")
    t.create(method="rows.expression", variant="multiple_colname", where="NullType", drop=False, how="all")

    t.create(method="rows.drop_str", variant="all", cols="*", how="any")
    t.create(method="rows.drop_str", variant="numeric", cols=["weight(t)"], how="any")
    t.create(method="rows.drop_str", variant="string", cols=["function"], how="all")
    t.create(method="rows.drop_str", variant="multiple", cols=["rank", "last date seen", "Cybertronian"], how="all")

    t.create(method="rows.drop_int", variant="all", cols="*", how="any")
    t.create(method="rows.drop_int", variant="numeric", cols=["weight(t)"], how="any")
    t.create(method="rows.drop_int", variant="string", cols=["function"], how="all")
    t.create(method="rows.drop_int", variant="multiple", cols=["rank", "last date seen", "Cybertronian"], how="all")

    t.create(method="rows.drop_float", variant="all", cols="*", how="any")
    t.create(method="rows.drop_float", variant="numeric", cols=["weight(t)"], how="any")
    t.create(method="rows.drop_float", variant="string", cols=["function"], how="all")
    t.create(method="rows.drop_float", variant="multiple", cols=["rank", "last date seen", "Cybertronian"], how="all")

    t.create(method="rows.drop_numeric", variant="all", cols="*", how="any")
    t.create(method="rows.drop_numeric", variant="numeric", cols=["weight(t)"], how="any")
    t.create(method="rows.drop_numeric", variant="string", cols=["function"], how="all")
    t.create(method="rows.drop_numeric", variant="multiple", cols=["rank", "last date seen", "Cybertronian"], how="all")

    t.create(method="rows.drop_greater_than", variant="all", cols="*", value=10, how="any")
    t.create(method="rows.drop_greater_than", variant="numeric", cols=["height(ft)"], value=-0.31, how="any")
    t.create(method="rows.drop_greater_than", variant="string", cols=["names"], value=0, how="all")
    t.create(method="rows.drop_greater_than", variant="multiple", cols=["age", "NullType", "weight(t)"], value="-inf", how="all")

    t.create(method="rows.drop_greater_than_equal", variant="all", cols="*", value=10, how="any")
    t.create(method="rows.drop_greater_than_equal", variant="numeric", cols=["height(ft)"], value=0.31, how="any")
    t.create(method="rows.drop_greater_than_equal", variant="string", cols=["names"], value=0, how="all")
    t.create(method="rows.drop_greater_than_equal", variant="multiple", cols=["age", "NullType", "weight(t)"], value="-inf", how="all")

    t.create(method="rows.drop_less_than", variant="all", cols="*", value=10, how="any")
    t.create(method="rows.drop_less_than", variant="numeric", cols=["height(ft)"], value=0.31, how="any")
    t.create(method="rows.drop_less_than", variant="string", cols=["names"], value=0, how="all")
    t.create(method="rows.drop_less_than", variant="multiple", cols=["age", "NullType", "weight(t)"], value="inf", how="all")

    t.create(method="rows.drop_less_than_equal", variant="all", cols="*", value=10, how="any")
    t.create(method="rows.drop_less_than_equal", variant="numeric", cols=["height(ft)"], value=0.31, how="any")
    t.create(method="rows.drop_less_than_equal", variant="string", cols=["names"], value=0, how="all")
    t.create(method="rows.drop_less_than_equal", variant="multiple", cols=["age", "NullType", "weight(t)"], value="inf", how="all")

    t.create(method="rows.drop_between", variant="all", cols="*", lower_bound="-inf", upper_bound="inf", equal=True, how="any")
    t.create(method="rows.drop_between", variant="numeric", cols=["height(ft)"], bounds=[[26, -28]], equal=False, how="any")
    t.create(method="rows.drop_between", variant="string", cols=["names"], upper_bound="-inf", lower_bound=0, equal=False, how="all")
    t.create(method="rows.drop_between", variant="multiple", cols=["age", "NullType", "weight(t)"], bounds=[["-inf", -10], [0, 1.9999], [300, 5000000]], equal=True, how="all")

    t.create(method="rows.drop_equal", variant="all", cols="*", value=10, how="any")
    t.create(method="rows.drop_equal", variant="numeric", cols=["height(ft)"], value=300, how="any")
    t.create(method="rows.drop_equal", variant="string", cols=["function"], value="Leader", how="all")
    t.create(method="rows.drop_equal", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], value=True, how="all")

    t.create(method="rows.drop_not_equal", variant="all", cols="*", value=10, how="any")
    t.create(method="rows.drop_not_equal", variant="numeric", cols=["height(ft)"], value=300, how="any")
    t.create(method="rows.drop_not_equal", variant="string", cols=["function"], value="Leader", how="all")
    t.create(method="rows.drop_not_equal", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], value=True, how="all")
    
    t.create(method="rows.drop_missings", variant="all", cols="*", how="any")
    t.create(method="rows.drop_missings", variant="numeric", cols=["height(ft)"], how="any")
    t.create(method="rows.drop_missings", variant="string", cols=["names"], how="all")
    t.create(method="rows.drop_missings", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], how="all")

    t.create(method="rows.drop_nulls", variant="all", cols="*", how="all")
    t.create(method="rows.drop_nulls", variant="numeric", cols=["height(ft)"], how="any")
    t.create(method="rows.drop_nulls", variant="string", cols=["names"], how="all")
    t.create(method="rows.drop_nulls", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], how="any")

    t.create(method="rows.drop_none", variant="all", cols="*", how="any")
    t.create(method="rows.drop_none", variant="numeric", cols=["height(ft)"], how="any")
    t.create(method="rows.drop_none", variant="string", cols=["names"], how="all")
    t.create(method="rows.drop_none", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], how="all")

    t.create(method="rows.drop_nan", variant="all", cols="*", how="any")
    t.create(method="rows.drop_nan", variant="numeric", cols=["height(ft)"], how="any")
    t.create(method="rows.drop_nan", variant="string", cols=["names"], how="all")
    t.create(method="rows.drop_nan", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], how="all")

    t.create(method="rows.drop_duplicated", variant="all", cols="*", how="any")
    t.create(method="rows.drop_duplicated", variant="all_first", cols="*", keep="first", how="any")
    t.create(method="rows.drop_duplicated", variant="all_last", cols="*", keep="last", how="all")
    t.create(method="rows.drop_duplicated", variant="numeric", cols=["rank"], how="all")
    t.create(method="rows.drop_duplicated", variant="numeric_first", cols=["rank"], keep="first", how="any")
    t.create(method="rows.drop_duplicated", variant="numeric_last", cols=["rank"], keep="last", how="any")
    t.create(method="rows.drop_duplicated", variant="string", cols=["names"], how="all")
    t.create(method="rows.drop_duplicated", variant="string_first", cols=["names"], keep="first", how="all")
    t.create(method="rows.drop_duplicated", variant="string_last", cols=["names"], keep="last", how="any")
    t.create(method="rows.drop_duplicated", variant="multiple", cols=["NullType", "timestamp", "Cybertronian"], keep="first", how="any")
    t.create(method="rows.drop_duplicated", variant="multiple_first", cols=["NullType", "timestamp", "Cybertronian"], keep="first", how="all")
    t.create(method="rows.drop_duplicated", variant="multiple_last", cols=["NullType", "timestamp", "Cybertronian"], keep="first", how="all")

    t.create(method="rows.drop_uniques", variant="all", cols="*", how="any")
    t.create(method="rows.drop_uniques", variant="all_first", cols="*", keep="first", how="any")
    t.create(method="rows.drop_uniques", variant="all_last", cols="*", keep="last", how="all")
    t.create(method="rows.drop_uniques", variant="numeric", cols=["rank"], how="all")
    t.create(method="rows.drop_uniques", variant="numeric_first", cols=["rank"], keep="first", how="any")
    t.create(method="rows.drop_uniques", variant="numeric_last", cols=["rank"], keep="last", how="any")
    t.create(method="rows.drop_uniques", variant="string", cols=["names"], how="all")
    t.create(method="rows.drop_uniques", variant="string_first", cols=["names"], keep="first", how="all")
    t.create(method="rows.drop_uniques", variant="string_last", cols=["names"], keep="last", how="any")
    t.create(method="rows.drop_uniques", variant="multiple", cols=["NullType", "timestamp", "Cybertronian"], keep="first", how="any")
    t.create(method="rows.drop_uniques", variant="multiple_first", cols=["NullType", "timestamp", "Cybertronian"], keep="first", how="all")
    t.create(method="rows.drop_uniques", variant="multiple_last", cols=["NullType", "timestamp", "Cybertronian"], keep="first", how="all")

    t.create(method="rows.drop_empty", variant="all", cols="*", how="any")
    t.create(method="rows.drop_empty", variant="numeric", cols=["height(ft)"], how="any")
    t.create(method="rows.drop_empty", variant="string", cols=["names"], how="all")
    t.create(method="rows.drop_empty", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], how="all")
    
    t.create(method="rows.drop_value_in", variant="all", cols="*", values=[10, True, None, "Jazz"], how="any")
    t.create(method="rows.drop_value_in", variant="numeric", cols=["height(ft)"], values=[300, "nan"], how="any")
    t.create(method="rows.drop_value_in", variant="string", cols=["function"], values="Leader", how="all")
    t.create(method="rows.drop_value_in", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], values=[False, None, 4], how="all")
    
    t.create(method="rows.drop_pattern", variant="all", cols="*", pattern="**cc**", how="any")
    t.create(method="rows.drop_pattern", variant="numeric", cols=["height(ft)"], pattern="##!#", how="any")
    t.create(method="rows.drop_pattern", variant="string", cols=["function"], pattern="Ullclc", how="all")
    t.create(method="rows.drop_pattern", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], pattern="****", how="all")
    
    t.create(method="rows.drop_starts_with", variant="all", cols="*", value="N", how="any")
    t.create(method="rows.drop_starts_with", variant="numeric", cols=["height(ft)"], value=1, how="any")
    t.create(method="rows.drop_starts_with", variant="string", cols=["function"], value="Lead", how="all")
    t.create(method="rows.drop_starts_with", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], value=True, how="all")

    t.create(method="rows.drop_ends_with", variant="all", cols="*", value="]", how="any")
    t.create(method="rows.drop_ends_with", variant="numeric", cols=["height(ft)"], value=0, how="any")
    t.create(method="rows.drop_ends_with", variant="string", cols=["function"], value="e", how="all")
    t.create(method="rows.drop_ends_with", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], value="one", how="all")

    t.create(method="rows.drop_contains", variant="all", cols="*", value="a", how="any")
    t.create(method="rows.drop_contains", variant="numeric", cols=["height(ft)"], value="0", how="any")
    t.create(method="rows.drop_contains", variant="string", cols=["function"], value="Le", how="all")
    t.create(method="rows.drop_contains", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], value="T", how="all")

    t.create(method="rows.drop_find", variant="all", cols="*", value="None", how="any")
    t.create(method="rows.drop_find", variant="numeric", cols=["height(ft)"], value=13, how="any")
    t.create(method="rows.drop_find", variant="string", cols=["function"], value="Leader", how="all")
    t.create(method="rows.drop_find", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], value=1, how="all")

    df2 = df.cols.append(
        {"emails_test": ["an@example.com", "thisisatest@gmail.com", "somename@hotmail.com", "an@outlook.com", "anexample@mail.com", "example@yahoo.com"]})

    t.create(df=df2, method="rows.drop_emails", cols=["emails_test"], how="any")
    t.create(method="rows.drop_emails", variant="all", cols="*", how="any")
    t.create(method="rows.drop_emails", variant="numeric", cols=["height(ft)"], how="any")
    t.create(method="rows.drop_emails", variant="string", cols=["names"], how="all")
    t.create(method="rows.drop_emails", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], how="all")

    df2 = df.cols.append(
        {"ips_test": ["192.0.2.1", "192.158.1.38", "192.168.136.52", "172.16.92.107", "10.63.215.5", "10.0.5.0"]})

    t.create(df=df2, method="rows.drop_ips", cols=["ips_test"], how="any")
    t.create(method="rows.drop_ips", variant="all", cols="*", how="any")
    t.create(method="rows.drop_ips", variant="numeric", cols=["height(ft)"], how="any")
    t.create(method="rows.drop_ips", variant="string", cols=["names"], how="all")
    t.create(method="rows.drop_ips", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], how="all")

    df2 = df.cols.append(
        {"urls_test": ["https://github.com/hi-primus/optimus", "localhost:3000?help=true", "http://www.images.hi-example.com:54/images.php#id?help=1&freq=2", "hi-optimus.com", "https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test", "https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALe&source=hp&ei=NL0-y4&iflsig=AINF&oq=this+is+a+test&gs_lcp=MZgBAKA&sclient=gws-wiz&ved=0ah&uact=5"]})
    
    t.create(df=df2, method="rows.drop_urls", cols=["urls_test"], how="any")
    t.create(method="rows.drop_urls", variant="all", cols="*", how="any")
    t.create(method="rows.drop_urls", variant="numeric", cols=["height(ft)"], how="any")
    t.create(method="rows.drop_urls", variant="string", cols=["names"], how="all")
    t.create(method="rows.drop_urls", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], how="all")

    df2 = df.cols.append(
        {"genders_test": ["male", "female", "Male", "5.0", "MALE", "FEMALE"]})
    
    t.create(df=df2, method="rows.drop_genders", cols=["genders_test"], how="any")
    t.create(method="rows.drop_genders", variant="all", cols="*", how="any")
    t.create(method="rows.drop_genders", variant="numeric", cols=["height(ft)"], how="any")
    t.create(method="rows.drop_genders", variant="string", cols=["names"], how="all")
    t.create(method="rows.drop_genders", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], how="all")

    df2 = df.cols.append(
        {"booleans_test": ["True", "False", True, False, 1, 0]})
    
    t.create(df=df2, method="rows.drop_booleans", cols=["booleans_test"], how="any")
    t.create(method="rows.drop_booleans", variant="all", cols="*", how="any")
    t.create(method="rows.drop_booleans", variant="numeric", cols=["height(ft)"], how="any")
    t.create(method="rows.drop_booleans", variant="string", cols=["names"], how="all")
    t.create(method="rows.drop_booleans", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], how="all")

    df2 = df.cols.append(
        {"zip_codes_test": [90210, 21252, 36104, 99801, 85001, 10]})
    
    t.create(df=df2, method="rows.drop_zip_codes", cols=["zip_codes_test"], how="any")
    t.create(method="rows.drop_zip_codes", variant="all", cols="*", how="any")
    t.create(method="rows.drop_zip_codes", variant="numeric", cols=["height(ft)"], how="any")
    t.create(method="rows.drop_zip_codes", variant="string", cols=["names"], how="all")
    t.create(method="rows.drop_zip_codes", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], how="all")

    df2 = df.cols.append(
        {"credit_card_numbers_test": ["2345 6362 6362 8632", 5692857295730750, "31028482204828450", "99 77 80 14 53 73 83 53", "8 5 0 0 1 5 8 1 5 8 3 7 0 0 0 1", 10]})
    
    t.create(df=df2, method="rows.drop_credit_card_numbers", cols=["credit_card_numbers_test"], how="any")
    t.create(method="rows.drop_credit_card_numbers", variant="all", cols="*", how="any")
    t.create(method="rows.drop_credit_card_numbers", variant="numeric", cols=["height(ft)"], how="any")
    t.create(method="rows.drop_credit_card_numbers", variant="string", cols=["names"], how="all")
    t.create(method="rows.drop_credit_card_numbers", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], how="all")

    df2 = df.cols.append(
        {"datetimes_test": ['1980/04/10', '5.0', datetime.datetime(2016, 9, 10), datetime.datetime(2014, 6, 24, 0, 0), '2013/06/10', datetime.datetime(2011, 4, 10)]})
    
    t.create(df=df2, method="rows.drop_datetimes", cols=["datetimes_test"], how="any")
    t.create(method="rows.drop_datetimes", variant="all", cols="*", how="any")
    t.create(method="rows.drop_datetimes", variant="numeric", cols=["height(ft)"], how="any")
    t.create(method="rows.drop_datetimes", variant="string", cols=["names"], how="all")
    t.create(method="rows.drop_datetimes", variant="multiple", cols=["Date Type", "last date seen", "timestamp"], how="all")

    df2 = df.cols.append(
        {"objects_test": ['1980/04/10', True, None, float("inf"), "yes", "bytearray(12, 'utf-8')"]})
    
    t.create(df=df2, method="rows.drop_objects", cols=["objects_test"], how="any")
    t.create(method="rows.drop_objects", variant="all", cols="*", how="any")
    t.create(method="rows.drop_objects", variant="numeric", cols=["height(ft)"], how="any")
    t.create(method="rows.drop_objects", variant="string", cols=["names"], how="all")
    t.create(method="rows.drop_objects", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], how="all")

    df2 = df.cols.append(
        {"arrays_test": [[1, 2, 3, 4], [[1], [2, 3]], ["one", "two"], ["one", ["two", [float("-inf")]]], ["yes"], "bytearray(12, 'utf-8')"]})
    
    t.create(df=df2, method="rows.drop_arrays", cols=["arrays_test"], how="any")
    t.create(method="rows.drop_arrays", variant="all", cols="*", how="any")
    t.create(method="rows.drop_arrays", variant="numeric", cols=["height(ft)"], how="any")
    t.create(method="rows.drop_arrays", variant="string", cols=["names"], how="all")
    t.create(method="rows.drop_arrays", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], how="all")

    df2 = df.cols.append(
        {"phone_numbers_test": [5516528967, "55 8395 1284", "+52 55 3913 1941", "+1 (210) 589-6721", 12106920692, "5532592785"]})
    
    t.create(df=df2, method="rows.drop_phone_numbers", cols=["phone_numbers_test"], how="any")
    t.create(method="rows.drop_phone_numbers", variant="all", cols="*", how="any")
    t.create(method="rows.drop_phone_numbers", variant="numeric", cols=["height(ft)"], how="any")
    t.create(method="rows.drop_phone_numbers", variant="string", cols=["names"], how="all")
    t.create(method="rows.drop_phone_numbers", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], how="all")

    df2 = df.cols.append(
        {"social_security_numbers_test": [372847278, "551-83-1284", "525 93 1941", "230 89-6721", "121-069 2062", "371847288"]})
    
    t.create(df=df2, method="rows.drop_social_security_numbers", cols=["social_security_numbers_test"], how="any")
    t.create(method="rows.drop_social_security_numbers", variant="all", cols="*", how="any")
    t.create(method="rows.drop_social_security_numbers", variant="numeric", cols=["height(ft)"], how="any")
    t.create(method="rows.drop_social_security_numbers", variant="string", cols=["names"], how="all")
    t.create(method="rows.drop_social_security_numbers", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], how="all")

    df2 = df.cols.append(
        {"http_codes_test": ["http://github.com/hi-primus/optimus", "localhost:3000?help=true", "http://www.images.hi-example.com:54/images.php#id?help=1&freq=2", "hi-optimus.com", "http://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test", "http://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALe&source=hp&ei=NL0-y4&iflsig=AINF&oq=this+is+a+test&gs_lcp=MZgBAKA&sclient=gws-wiz&ved=0ah&uact=5"]})
    
    t.create(df=df2, method="rows.drop_http_codes", cols=["http_codes_test"], how="any")
    t.create(method="rows.drop_http_codes", variant="all", cols="*", how="any")
    t.create(method="rows.drop_http_codes", variant="numeric", cols=["height(ft)"], how="any")
    t.create(method="rows.drop_http_codes", variant="string", cols=["names"], how="all")
    t.create(method="rows.drop_http_codes", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], how="all")

    t.create(method="rows.drop_by_expression", variant="all_expression", cols="*", where='df["rank"]>8', how="any")
    t.create(method="rows.drop_by_expression", variant="all_colname", where="last position seen", how="all")
    t.create(method="rows.drop_by_expression", variant="numeric_expression", where='df["height(ft)"]>=0', how="any")
    t.create(method="rows.drop_by_expression", variant="numeric_colname", where="height(ft)", how="all")
    t.create(method="rows.drop_by_expression", variant="string_expression", where='df["function"]=="Leader"', how="any")
    t.create(method="rows.drop_by_expression", variant="string_colname", where="function", how="all")
    t.create(method="rows.drop_by_expression", variant="multiple_expression", where='df["Cybertronian"]==False', how="any")
    t.create(method="rows.drop_by_expression", variant="multiple_colname", where="NullType", how="all")

    t.run()

create()
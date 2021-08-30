import datetime
import sys
sys.path.append("../..")


def create():
    from optimus import Optimus
    from optimus.tests.creator import TestCreator, default_configs

    op = Optimus("pandas")
    df = op.create.dataframe({
        'NullType': [None, None, None, None, None, None],
        ('Code', 'object'): ["123A", "456", 456, "e", None, "{code}"],
        ('Multiple', 'object'): ["12/12/12", "True", 1, "0.0", "None", "{}"],
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

    t = TestCreator(op, df, name="mask", configs=default_configs)

    t.create(method="mask.numeric", variant="all", cols="*")
    t.create(method="mask.numeric", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.numeric", variant="string", cols=["names"])
    t.create(method="mask.numeric", variant="multiple", cols=["NullType", "weight(t)", "japanese name"])

    t.create(method="mask.int", variant="all", cols="*")
    t.create(method="mask.int", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.int", variant="string", cols=["names"])
    t.create(method="mask.int", variant="multiple", cols=["NullType", "weight(t)", "japanese name"])

    t.create(method="mask.float", variant="all", cols="*")
    t.create(method="mask.float", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.float", variant="string", cols=["names"])
    t.create(method="mask.float", variant="multiple", cols=["NullType", "weight(t)", "japanese name"])

    t.create(method="mask.str", variant="all", cols="*")
    t.create(method="mask.str", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.str", variant="string", cols=["names"])
    t.create(method="mask.str", variant="multiple", cols=["NullType", "weight(t)", "japanese name"])

    t.create(method="mask.greater_than", variant="all", cols="*", value=10)
    t.create(method="mask.greater_than", variant="numeric", cols=["height(ft)"], value=-0.31)
    t.create(method="mask.greater_than", variant="string", cols=["names"], value=0)
    t.create(method="mask.greater_than", variant="multiple", cols=["age", "NullType", "weight(t)"], value="-inf")

    t.create(method="mask.greater_than_equal", variant="all", cols="*", value=10)
    t.create(method="mask.greater_than_equal", variant="numeric", cols=["height(ft)"], value=0.31)
    t.create(method="mask.greater_than_equal", variant="string", cols=["names"], value=0)
    t.create(method="mask.greater_than_equal", variant="multiple", cols=["age", "NullType", "weight(t)"], value="-inf")

    t.create(method="mask.less_than", variant="all", cols="*", value=10)
    t.create(method="mask.less_than", variant="numeric", cols=["height(ft)"], value=0.31)
    t.create(method="mask.less_than", variant="string", cols=["names"], value=0)
    t.create(method="mask.less_than", variant="multiple", cols=["age", "NullType", "weight(t)"], value="inf")

    t.create(method="mask.less_than_equal", variant="all", cols="*", value=10)
    t.create(method="mask.less_than_equal", variant="numeric", cols=["height(ft)"], value=0.31)
    t.create(method="mask.less_than_equal", variant="string", cols=["names"], value=0)
    t.create(method="mask.less_than_equal", variant="multiple", cols=["age", "NullType", "weight(t)"], value="inf")

    t.create(method="mask.between", variant="all", cols="*", lower_bound="-inf", upper_bound="inf", equal=True)
    t.create(method="mask.between", variant="numeric", cols=["height(ft)"], bounds=[[26, -28]], equal=False)
    t.create(method="mask.between", variant="string", cols=["names"], upper_bound="-inf", lower_bound=0, equal=False)
    t.create(method="mask.between", variant="multiple", cols=["age", "NullType", "weight(t)"], bounds=[["-inf", -10], [0, 1.9999], [300, 5000000]], equal=True)

    t.create(method="mask.equal", variant="all", cols="*", value=10)
    t.create(method="mask.equal", variant="numeric", cols=["height(ft)"], value=300)
    t.create(method="mask.equal", variant="string", cols=["function"], value="Leader")
    t.create(method="mask.equal", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], value=True)

    t.create(method="mask.not_equal", variant="all", cols="*", value=10)
    t.create(method="mask.not_equal", variant="numeric", cols=["height(ft)"], value=300)
    t.create(method="mask.not_equal", variant="string", cols=["function"], value="Leader")
    t.create(method="mask.not_equal", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], value=True)

    t.create(method="mask.missing", variant="all", cols="*")
    t.create(method="mask.missing", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.missing", variant="string", cols=["names"])
    t.create(method="mask.missing", variant="multiple", cols=["NullType", "weight(t)", "japanese name"])

    t.create(method="mask.value_in", variant="all", cols="*", values=[10, True, None, "Jazz"])
    t.create(method="mask.value_in", variant="numeric", cols=["height(ft)"], values=[300, "nan"])
    t.create(method="mask.value_in", variant="string", cols=["function"], values="Leader")
    t.create(method="mask.value_in", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], values=[False, None, 4])

    t.create(method="mask.pattern", variant="all", cols="*", pattern="**cc**")
    t.create(method="mask.pattern", variant="numeric", cols=["height(ft)"], pattern="##!#")
    t.create(method="mask.pattern", variant="string", cols=["function"], pattern="Ullclc")
    t.create(method="mask.pattern", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], pattern="****")

    t.create(method="mask.starts_with", variant="all", cols="*", value="N")
    t.create(method="mask.starts_with", variant="numeric", cols=["height(ft)"], value=1)
    t.create(method="mask.starts_with", variant="string", cols=["function"], value="Lead")
    t.create(method="mask.starts_with", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], value=True)

    t.create(method="mask.ends_with", variant="all", cols="*", value="]")
    t.create(method="mask.ends_with", variant="numeric", cols=["height(ft)"], value=0)
    t.create(method="mask.ends_with", variant="string", cols=["function"], value="e")
    t.create(method="mask.ends_with", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], value="one")

    t.create(method="mask.contains", variant="all", cols="*", value="a")
    t.create(method="mask.contains", variant="numeric", cols=["height(ft)"], value="0", flags=0, na=True, regex=False)
    t.create(method="mask.contains", variant="string", cols=["function"], value="Le.", case=True, flags=3, na=False, regex=True)
    t.create(method="mask.contains", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], value="T|N", case=True, flags=1, na=True, regex=True)

    # t.create(method="mask.expression", variant="all_dataframe", cols="*", where=op.create.dataframe({'NullType': [None, None, None, None, None, None],'height(ft)': [-28, 17, 26, 13, None, 300], 'rank': [10, 7, 7, 8, 10, 8]}))
    t.create(method="mask.expression", variant="all_expression", cols="*", where='df["rank"]>5')
    t.create(method="mask.expression", variant="all_colname", cols="*", value="last position seen")
    # t.create(method="mask.expression", variant="numeric_dataframe", cols=["height(ft)"], where=df["last date seen"])
    t.create(method="mask.expression", variant="numeric_expression", cols=["height(ft)"], where='df["height(ft"]>=0')
    t.create(method="mask.expression", variant="numeric_colname", cols=["height(ft)"], where="height(ft)")
    # t.create(method="mask.expression", variant="string_dataframe", cols=["function"], where=df["NullType"])
    t.create(method="mask.expression", variant="string_expression", cols=["function"], where='df["function"]>0')
    t.create(method="mask.expression", variant="string_colname", cols=["function"], where="function")
    # t.create(method="mask.expression", variant="multiple_dataframe", cols=["NullType", "weight(t)", "Cybertronian"], where=op.create.dataframe({"None":[]}))
    t.create(method="mask.expression", variant="multiple_expression", cols=["NullType", "weight(t)", "Cybertronian"], where='df["Cybertronian"]>0')
    t.create(method="mask.expression", variant="multiple_colname", cols=["NullType", "weight(t)", "Cybertronian"], where="NullType")

    t.create(method="mask.find", variant="all", cols="*", value="None")
    t.create(method="mask.find", variant="numeric", cols=["height(ft)"], value=13)
    t.create(method="mask.find", variant="string", cols=["function"], value="Leader")
    t.create(method="mask.find", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"], value=1)

    t.create(method="mask.null", variant="all", cols="*", how="all")
    t.create(method="mask.null", variant="numeric", cols=["height(ft)"], how="any")
    t.create(method="mask.null", variant="string", cols=["names"], how="all")
    t.create(method="mask.null", variant="multiple", cols=["NullType", "weight(t)", "japanese name"], how="any")

    t.create(method="mask.none", variant="all", cols="*")
    t.create(method="mask.none", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.none", variant="string", cols=["names"])
    t.create(method="mask.none", variant="multiple", cols=["NullType", "weight(t)", "japanese name"])

    t.create(method="mask.nan", variant="all", cols="*")
    t.create(method="mask.nan", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.nan", variant="string", cols=["names"])
    t.create(method="mask.nan", variant="multiple", cols=["NullType", "weight(t)", "japanese name"])

    t.create(method="mask.duplicated", variant="all", cols="*")
    t.create(method="mask.duplicated", variant="all_first", cols="*", keep="first")
    t.create(method="mask.duplicated", variant="all_last", cols="*", keep="last")
    t.create(method="mask.duplicated", variant="numeric", cols=["rank"])
    t.create(method="mask.duplicated", variant="numeric_first", cols=["rank"], keep="first")
    t.create(method="mask.duplicated", variant="numeric_last", cols=["rank"], keep="last")
    t.create(method="mask.duplicated", variant="string", cols=["names"])
    t.create(method="mask.duplicated", variant="string_first", cols=["names"], keep="first")
    t.create(method="mask.duplicated", variant="string_last", cols=["names"], keep="last")
    t.create(method="mask.duplicated", variant="multiple", cols=["NullType", "timestamp", "Cybertronian"], keep="first")
    t.create(method="mask.duplicated", variant="multiple_first", cols=["NullType", "timestamp", "Cybertronian"], keep="first")
    t.create(method="mask.duplicated", variant="multiple_last", cols=["NullType", "timestamp", "Cybertronian"], keep="first")

    t.create(method="mask.unique", variant="all", cols="*")
    t.create(method="mask.unique", variant="all_first", cols="*", keep="first")
    t.create(method="mask.unique", variant="all_last", cols="*", keep="last")
    t.create(method="mask.unique", variant="numeric", cols=["rank"])
    t.create(method="mask.unique", variant="numeric_first", cols=["rank"], keep="first")
    t.create(method="mask.unique", variant="numeric_last", cols=["rank"], keep="last")
    t.create(method="mask.unique", variant="string", cols=["names"])
    t.create(method="mask.unique", variant="string_first", cols=["names"], keep="first")
    t.create(method="mask.unique", variant="string_last", cols=["names"], keep="last")
    t.create(method="mask.unique", variant="multiple", cols=["NullType", "timestamp", "Cybertronian"], keep="first")
    t.create(method="mask.unique", variant="multiple_first", cols=["NullType", "timestamp", "Cybertronian"], keep="first")
    t.create(method="mask.unique", variant="multiple_last", cols=["NullType", "timestamp", "Cybertronian"], keep="first")

    t.create(method="mask.empty", variant="all", cols="*")
    t.create(method="mask.empty", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.empty", variant="string", cols=["names"])
    t.create(method="mask.empty", variant="multiple", cols=["NullType", "weight(t)", "japanese name"])

    df2 = df.cols.append(
        {"email_test": ["an@example.com", "thisisatest@gmail.com", "somename@hotmail.com", "an@outlook.com", "anexample@mail.com", "example@yahoo.com"]})

    t.create(df=df2, method="mask.email", cols=["email_test"])
    t.create(method="mask.email", variant="all", cols="*")
    t.create(method="mask.email", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.email", variant="string", cols=["names"])
    t.create(method="mask.email", variant="multiple", cols=["NullType", "weight(t)", "japanese name"])

    df2 = df.cols.append(
        {"ip_test": ["192.0.2.1", "192.158.1.38", "192.168.136.52", "172.16.92.107", "10.63.215.5", "10.0.5.0"]})

    t.create(df=df2, method="mask.ip", cols=["ip_test"])
    t.create(method="mask.ip", variant="all", cols="*")
    t.create(method="mask.ip", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.ip", variant="string", cols=["names"])
    t.create(method="mask.ip", variant="multiple", cols=["NullType", "weight(t)", "japanese name"])

    df2 = df.cols.append(
        {"urls_test": ["https://github.com/hi-primus/optimus", "localhost:3000?help=true", "http://www.images.hi-example.com:54/images.php#id?help=1&freq=2", "hi-optimus.com", "https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test", "https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALe&source=hp&ei=NL0-y4&iflsig=AINF&oq=this+is+a+test&gs_lcp=MZgBAKA&sclient=gws-wiz&ved=0ah&uact=5"]})
    
    t.create(df=df2, method="mask.url", cols=["url_test"])
    t.create(method="mask.url", variant="all", cols="*")
    t.create(method="mask.url", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.url", variant="string", cols=["names"])
    t.create(method="mask.url", variant="multiple", cols=["NullType", "weight(t)", "japanese name"])

    df2 = df.cols.append(
        {"gender_test": ["male", "female", "Male", "5.0", "MALE", "FEMALE"]})
    
    t.create(df=df2, method="mask.gender", cols=["gender_test"])
    t.create(method="mask.gender", variant="all", cols="*")
    t.create(method="mask.gender", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.gender", variant="string", cols=["names"])
    t.create(method="mask.gender", variant="multiple", cols=["NullType", "weight(t)", "japanese name"])

    df2 = df.cols.append(
        {"boolean_test": ["True", "False", True, False, 1, 0]})
    
    t.create(df=df2, method="mask.boolean", cols=["boolean_test"])
    t.create(method="mask.boolean", variant="all", cols="*")
    t.create(method="mask.boolean", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.boolean", variant="string", cols=["names"])
    t.create(method="mask.boolean", variant="multiple", cols=["NullType", "weight(t)", "Cybertronian"])

    df2 = df.cols.append(
        {"zip_code_test": [90210, 21252, 36104, 99801, 85001, 10]})
    
    t.create(df=df2, method="mask.zip_code", cols=["zip_code_test"])
    t.create(method="mask.zip_code", variant="all", cols="*")
    t.create(method="mask.zip_code", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.zip_code", variant="string", cols=["names"])
    t.create(method="mask.zip_code", variant="multiple", cols=["NullType", "weight(t)", "japanese name"])

    df2 = df.cols.append(
        {"credit_card_number_test": ["2345 6362 6362 8632", 5692857295730750, "31028482204828450", "99 77 80 14 53 73 83 53", "8 5 0 0 1 5 8 1 5 8 3 7 0 0 0 1", 10]})
    
    t.create(df=df2, method="mask.credit_card_number", cols=["credit_card_number_test"])
    t.create(method="mask.credit_card_number", variant="all", cols="*")
    t.create(method="mask.credit_card_number", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.credit_card_number", variant="string", cols=["names"])
    t.create(method="mask.credit_card_number", variant="multiple", cols=["NullType", "weight(t)", "japanese name"])

    df2 = df.cols.append(
        {"datetime_test": ['1980/04/10', '5.0', datetime.datetime(2016, 9, 10), datetime.datetime(2014, 6, 24, 0, 0), '2013/06/10', datetime.datetime(2011, 4, 10)]})
    
    t.create(df=df2, method="mask.datetime", cols=["datetime_test"])
    t.create(method="mask.datetime", variant="all", cols="*")
    t.create(method="mask.datetime", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.datetime", variant="string", cols=["names"])
    t.create(method="mask.datetime", variant="multiple", cols=["Date Type", "last date seen", "timestamp"])

    df2 = df.cols.append(
        {"object_test": ['1980/04/10', True, None, float("inf"), "yes", "bytearray(12, 'utf-8')"]})
    
    t.create(df=df2, method="mask.object", cols=["object_test"])
    t.create(method="mask.object", variant="all", cols="*")
    t.create(method="mask.object", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.object", variant="string", cols=["names"])
    t.create(method="mask.object", variant="multiple", cols=["NullType", "weight(t)", "japanese name"])

    df2 = df.cols.append(
        {"array_test": [[1, 2, 3, 4], [[1], [2, 3]], ["one", "two"], ["one", ["two", [float("-inf")]]], ["yes"], "bytearray(12, 'utf-8')"]})
    
    t.create(df=df2, method="mask.array", cols=["array_test"])
    t.create(method="mask.array", variant="all", cols="*")
    t.create(method="mask.array", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.array", variant="string", cols=["names"])
    t.create(method="mask.array", variant="multiple", cols=["NullType", "weight(t)", "japanese name"])

    df2 = df.cols.append(
        {"phone_number_test": [5516528967, "55 8395 1284", "+52 55 3913 1941", "+1 (210) 589-6721", 12106920692, "5532592785"]})
    
    t.create(df=df2, method="mask.phone_number", cols=["phone_number_test"])
    t.create(method="mask.phone_number", variant="all", cols="*")
    t.create(method="mask.phone_number", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.phone_number", variant="string", cols=["names"])
    t.create(method="mask.phone_number", variant="multiple", cols=["NullType", "weight(t)", "japanese name"])

    df2 = df.cols.append(
        {"social_security_number_test": [372847278, "551-83-1284", "525 93 1941", "230 89-6721", "121-069 2062", "371847288"]})
    
    t.create(df=df2, method="mask.social_security_number", cols=["social_security_number_test"])
    t.create(method="mask.social_security_number", variant="all", cols="*")
    t.create(method="mask.social_security_number", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.social_security_number", variant="string", cols=["names"])
    t.create(method="mask.social_security_number", variant="multiple", cols=["NullType", "weight(t)", "japanese name"])

    df2 = df.cols.append(
        {"http_code_test": ["http://github.com/hi-primus/optimus", "localhost:3000?help=true", "http://www.images.hi-example.com:54/images.php#id?help=1&freq=2", "hi-optimus.com", "http://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test", "http://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALe&source=hp&ei=NL0-y4&iflsig=AINF&oq=this+is+a+test&gs_lcp=MZgBAKA&sclient=gws-wiz&ved=0ah&uact=5"]})
    
    t.create(df=df2, method="mask.http_code", cols=["http_code_test"])
    t.create(method="mask.http_code", variant="all", cols="*")
    t.create(method="mask.http_code", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.http_code", variant="string", cols=["names"])
    t.create(method="mask.http_code", variant="multiple", cols=["NullType", "weight(t)", "japanese name"])

    t.create(method="mask.all", variant="all", cols="*")
    t.create(method="mask.all", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.all", variant="string", cols=["names"])
    t.create(method="mask.all", variant="multiple", cols=["NullType", "weight(t)", "japanese name"])

    t.create(method="mask.any", variant="all", cols="*")
    t.create(method="mask.any", variant="numeric", cols=["height(ft)"])
    t.create(method="mask.any", variant="string", cols=["names"])
    t.create(method="mask.any", variant="multiple", cols=["NullType", "weight(t)", "japanese name"])

    t.run()

create()
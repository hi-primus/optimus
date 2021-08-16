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

    t = TestCreator(op, df, name="string", configs=default_configs)

    df2 = df.cols.append(
        {"lower_test": ["ThIs iS A TEST", "ThIs", "iS", "a ", " TEST", "this is a test"]})

    t.create(df=df2, method="cols.lower", cols=["lower_test"], select_cols=True)
    t.create(method="cols.lower", variant="all", cols="*")
    t.create(method="cols.lower", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.lower", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.lower", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"upper_test": ["ThIs iS A TEST", "ThIs", "iS", "a ", " TEST", "this is a test"]})

    t.create(df=df2, method="cols.upper", cols=["upper_test"], select_cols=True)
    t.create(method="cols.upper", variant="all", cols="*")
    t.create(method="cols.upper", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.upper", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.upper", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"title_test": ["ThIs iS A TEST", "ThIs", "iS", "a ", " TEST", "this is a test"]})

    t.create(df=df2, method="cols.title", cols=["title_test"], select_cols=True)
    t.create(method="cols.title", variant="all", cols="*")
    t.create(method="cols.title", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.title", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.title", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"capitalize_test": ["ThIs iS A TEST", "ThIs", "iS", "a ", " TEST", "this is a test"]})

    t.create(df=df2, method="cols.capitalize", cols=["capitalize_test"], select_cols=True)
    t.create(method="cols.capitalize", variant="all", cols="*")
    t.create(method="cols.capitalize", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.capitalize", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.capitalize", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"trim_test": ["ThIs iS A TEST", "foo", "     bar", "baz      ", "      zoo     ", "   t e   s   t   "]})

    t.create(df=df2, method="cols.trim", cols=["trim_test"], select_cols=True)
    t.create(method="cols.trim", variant="all", cols="*")
    t.create(method="cols.trim", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.trim", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.trim", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"word_tokenize_test": ["THis iS a TEST", "for", " how ", "many words are in this sentence", "      ", "12"]})

    t.create(df=df2, method="cols.word_tokenize", cols=["word_tokenize_test"], select_cols=True)
    t.create(method="cols.word_tokenize", variant="all", cols="*")
    t.create(method="cols.word_tokenize", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.word_tokenize", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.word_tokenize", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"word_count_test": ["THis iS a TEST", "for", " how ", "many words are in this sentence", "      ", "12"]})

    t.create(df=df2, method="cols.word_count", cols=["word_count_test"], select_cols=True)
    t.create(method="cols.word_count", variant="all", cols="*")
    t.create(method="cols.word_count", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.word_count", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.word_count", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"len_test": ["      ", "12", "", " how many characters are in this sentence?", "!@#$%^&*()_>?:}", " 1     2      3   "]})

    t.create(df=df2, method="cols.len", cols=["len_test"], select_cols=True)
    t.create(method="cols.len", variant="all", cols="*")
    t.create(method="cols.len", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.len", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.len", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"reverse_test": ["gnitseretni", "this Is a TesT", " ", "this is another test ", "tset a si siht", "reverse esrever"]})

    t.create(df=df2, method="cols.reverse", cols=["reverse_test"], select_cols=True)
    t.create(method="cols.reverse", variant="all", cols="*")
    t.create(method="cols.reverse", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.reverse", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.reverse", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"remove_white_spaces_test": ["i   n te  rest i ng ", "This I s A Test", "     ", "ThisOneHasNoWhiteSpaces", "Th is One  Sho   uld Be    AllTogether   ", "L et  ' s G oo o o  o    o"]})

    t.create(df=df2, method="cols.remove_white_spaces", cols=["remove_white_spaces_test"], select_cols=True)
    t.create(method="cols.remove_white_spaces", variant="all", cols="*")
    t.create(method="cols.remove_white_spaces", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.remove_white_spaces", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.remove_white_spaces", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"normalize_spaces_test": ["more       close", "      for  ", "h   o    w", "many  words  are in this   sentence", "      ", "WhatIfThereAreNoSpaces"]})

    t.create(df=df2, method="cols.normalize_spaces", cols=["normalize_spaces_test"], select_cols=True)
    t.create(method="cols.normalize_spaces", variant="all", cols="*")
    t.create(method="cols.normalize_spaces", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.normalize_spaces", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.normalize_spaces", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"expand_contracted_words_test": ["y'all ain't ready for this", "i've been where you haven't", "SHE'LL DO IT BEFORE YOU", "maybe it isn't so hard after all", "he mustn't cheat in school", "IF YOU HADN'T DONE THAT, WE WOULD'VE BEEN FREE"]})

    t.create(df=df2, method="cols.expand_contracted_words", cols=["expand_contracted_words_test"], select_cols=True)
    t.create(method="cols.expand_contracted_words", variant="all", cols="*")
    t.create(method="cols.expand_contracted_words", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.expand_contracted_words", variant="string", cols=[
             "function(binary)"], output_cols=["function(binary)_2"], select_cols=True)
    t.create(method="cols.expand_contracted_words", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"lemmatize_verbs_test": ["the players are tired of studying", "feet and teeth sound simliar", "it is us connected against the world", "leave it alone", "living in the world", "I am aware that discoveries come very often nowadays"]})

    t.create(df=df2, method="cols.lemmatize_verbs", cols=["lemmatize_verbs_test"], select_cols=True)
    t.create(method="cols.lemmatize_verbs", variant="all", cols="*")
    t.create(method="cols.lemmatize_verbs", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.lemmatize_verbs", variant="string", cols=[
             "function(binary)"], output_cols=["function(binary)_2"], select_cols=True)
    t.create(method="cols.lemmatize_verbs", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"remove_numbers_test": ["2 plus 2 equals 4", "bumbl#ebéé   is about 5000000 years old", "these aren't special characters: `~!@#$%^&*()?/\|", "why is pi=3.141592... an irrational number?", "3^3=27", "don't @ me"]})

    t.create(df=df2, method="cols.remove_numbers", cols=["remove_numbers_test"], select_cols=True)
    t.create(method="cols.remove_numbers", variant="all", cols="*")
    t.create(method="cols.remove_numbers", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.remove_numbers", variant="string", cols=[
             "function(binary)"], output_cols=["function(binary)_2"], select_cols=True)
    t.create(method="cols.remove_numbers", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"remove_special_chars_test": ["2 plus 2 equals 4", "bumbl#ebéé   is about 5000000 years old", "these aren't special characters: `~!@#$%^&*()?/\|", "why is pi=3.141592... an irrational number?", "3^3=27", "don't @ me"]})

    t.create(df=df2, method="cols.remove_special_chars", cols=["remove_special_chars_test"], select_cols=True)
    t.create(method="cols.remove_special_chars", variant="all", cols="*")
    t.create(method="cols.remove_special_chars", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.remove_special_chars", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.remove_special_chars", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    t.run()

    t = TestCreator(op, df, name="web", configs=default_configs)

    df2 = df.cols.append(
        {"email_username_test": ["an@example.com", "thisisatest@gmail.com", "somename@hotmail.com", "an@outlook.com", "anexample@mail.com", "example@yahoo.com"]})

    t.create(df=df2, method="cols.email_username", cols=["email_username_test"], select_cols=True)
    t.create(method="cols.email_username", variant="all", cols="*")
    t.create(method="cols.email_username", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.email_username", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.email_username", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"email_domain_test": ["an@example.com", "thisisatest@gmail.com", "somename@hotmail.com", "an@outlook.com", "anexample@mail.com", "example@yahoo.com"]})

    t.create(df=df2, method="cols.email_domain", cols=["email_domain_test"], select_cols=True)
    t.create(method="cols.email_domain", variant="all", cols="*")
    t.create(method="cols.email_domain", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.email_domain", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.email_domain", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"strip_html_test": ["<this is a test>", "<this> <is> <a> <test>", "<this> is a <test>", "<this is> a <test>", "<>this is a test<>", ">this is a test<"]})

    t.create(df=df2, method="cols.strip_html", cols=["strip_html_test"], select_cols=True)
    t.create(method="cols.strip_html", variant="all", cols="*")
    t.create(method="cols.strip_html", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.strip_html", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.strip_html", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"remove_urls_test": ["https://github.com/hi-primus/optimus", "localhost:3000?help=true", "http://www.images.hi-example.com:54/images.php#id?help=1&freq=2", "hi-optimus.com", "https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test", "https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALe&source=hp&ei=NL0-y4&iflsig=AINF&oq=this+is+a+test&gs_lcp=MZgBAKA&sclient=gws-wiz&ved=0ah&uact=5"]})

    t.create(df=df2, method="cols.remove_urls", cols=["remove_urls_test"], select_cols=True)
    t.create(method="cols.remove_urls", variant="all", cols="*")
    t.create(method="cols.remove_urls", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.remove_urls", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.remove_urls", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"domain_test": ["https://github.com/hi-primus/optimus", "localhost:3000?help=true", "http://www.images.hi-example.com:54/images.php#id?help=1&freq=2", "hi-optimus.com", "https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test", "https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALe&source=hp&ei=NL0-y4&iflsig=AINF&oq=this+is+a+test&gs_lcp=MZgBAKA&sclient=gws-wiz&ved=0ah&uact=5"]})

    t.create(df=df2, method="cols.domain", cols=["domain_test"], select_cols=True)
    t.create(method="cols.domain", variant="all", cols="*")
    t.create(method="cols.domain", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.domain", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.domain", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"top_domain_test": ["https://github.com/hi-primus/optimus", "localhost:3000?help=true", "http://www.images.hi-example.com:54/images.php#id?help=1&freq=2", "hi-optimus.com", "https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test", "https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALe&source=hp&ei=NL0-y4&iflsig=AINF&oq=this+is+a+test&gs_lcp=MZgBAKA&sclient=gws-wiz&ved=0ah&uact=5"]})

    t.create(df=df2, method="cols.top_domain", cols=["top_domain_test"], select_cols=True)
    t.create(method="cols.top_domain", variant="all", cols="*")
    t.create(method="cols.top_domain", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.top_domain", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.top_domain", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"sub_domain_test": ["https://github.com/hi-primus/optimus", "localhost:3000?help=true", "http://www.images.hi-example.com:54/images.php#id?help=1&freq=2", "hi-optimus.com", "https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test", "https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALe&source=hp&ei=NL0-y4&iflsig=AINF&oq=this+is+a+test&gs_lcp=MZgBAKA&sclient=gws-wiz&ved=0ah&uact=5"]})

    t.create(df=df2, method="cols.sub_domain", cols=["sub_domain_test"], select_cols=True)
    t.create(method="cols.sub_domain", variant="all", cols="*")
    t.create(method="cols.sub_domain", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.sub_domain", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.sub_domain", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"url_scheme_test": ["https://github.com/hi-primus/optimus", "localhost:3000?help=true", "http://www.images.hi-example.com:54/images.php#id?help=1&freq=2", "hi-optimus.com", "https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test", "https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALe&source=hp&ei=NL0-y4&iflsig=AINF&oq=this+is+a+test&gs_lcp=MZgBAKA&sclient=gws-wiz&ved=0ah&uact=5"]})

    t.create(df=df2, method="cols.url_scheme", cols=["url_scheme_test"], select_cols=True)
    t.create(method="cols.url_scheme", variant="all", cols="*")
    t.create(method="cols.url_scheme", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.url_scheme", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.url_scheme", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"url_path_test": ["https://github.com/hi-primus/optimus", "localhost:3000?help=true", "http://www.images.hi-example.com:54/images.php#id?help=1&freq=2", "hi-optimus.com", "https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test", "https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALe&source=hp&ei=NL0-y4&iflsig=AINF&oq=this+is+a+test&gs_lcp=MZgBAKA&sclient=gws-wiz&ved=0ah&uact=5"]})

    t.create(df=df2, method="cols.url_path", cols=["url_path_test"], select_cols=True)
    t.create(method="cols.url_path", variant="all", cols="*")
    t.create(method="cols.url_path", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.url_path", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.url_path", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"url_file_test": ["https://github.com/hi-primus/optimus", "localhost:3000?help=true", "http://www.images.hi-example.com:54/images.php#id?help=1&freq=2", "hi-optimus.com", "https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test", "https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALe&source=hp&ei=NL0-y4&iflsig=AINF&oq=this+is+a+test&gs_lcp=MZgBAKA&sclient=gws-wiz&ved=0ah&uact=5"]})

    t.create(df=df2, method="cols.url_file", cols=["url_file_test"], select_cols=True)
    t.create(method="cols.url_file", variant="all", cols="*")
    t.create(method="cols.url_file", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.url_file", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.url_file", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])             

    df2 = df.cols.append(
        {"url_fragment_test": ["https://github.com/hi-primus/optimus", "localhost:3000?help=true", "http://www.images.hi-example.com:54/images.php#id?help=1&freq=2", "hi-optimus.com", "https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test", "https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALe&source=hp&ei=NL0-y4&iflsig=AINF&oq=this+is+a+test&gs_lcp=MZgBAKA&sclient=gws-wiz&ved=0ah&uact=5"]})

    t.create(df=df2, method="cols.url_fragment", cols=["url_fragment_test"], select_cols=True)
    t.create(method="cols.url_fragment", variant="all", cols="*")
    t.create(method="cols.url_fragment", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.url_fragment", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.url_fragment", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"host_test": ["https://github.com/hi-primus/optimus", "localhost:3000?help=true", "http://www.images.hi-example.com:54/images.php#id?help=1&freq=2", "hi-optimus.com", "https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test", "https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALe&source=hp&ei=NL0-y4&iflsig=AINF&oq=this+is+a+test&gs_lcp=MZgBAKA&sclient=gws-wiz&ved=0ah&uact=5"]})

    t.create(df=df2, method="cols.host", cols=["host_test"], select_cols=True)
    t.create(method="cols.host", variant="all", cols="*")
    t.create(method="cols.host", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.host", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.host", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    df2 = df.cols.append(
        {"port_test": ["https://github.com/hi-primus/optimus", "localhost:3000?help=true", "http://www.images.hi-example.com:54/images.php#id?help=1&freq=2", "hi-optimus.com", "https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test", "https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALe&source=hp&ei=NL0-y4&iflsig=AINF&oq=this+is+a+test&gs_lcp=MZgBAKA&sclient=gws-wiz&ved=0ah&uact=5"]})

    t.create(df=df2, method="cols.port", cols=["port_test"], select_cols=True)
    t.create(method="cols.port", variant="all", cols="*")
    t.create(method="cols.port", variant="numeric",
             cols=["height(ft)"], select_cols=True)
    t.create(method="cols.port", variant="string", cols=[
             "names"], output_cols=["names_2"], select_cols=True)
    t.create(method="cols.port", variant="multiple", cols=[
             "NullType", "weight(t)", "japanese name"], output_cols=["nt", "wt", "jn"])

    t.run()


create()
from optimus.tests.base import TestBase
import datetime
Timestamp = lambda t: datetime.datetime.strptime(t,"%Y-%m-%d %H:%M:%S")
nan = float("nan")
inf = float("inf")
from optimus.helpers.json import json_encoding
from optimus.helpers.functions import deep_sort, df_dicts_equal

class TestWebPandas(TestBase):
    config = {'engine': 'pandas'}
    dict = {('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan]}
    maxDiff = None
    
    def test_cols_domain(self):
        df = self.create_dataframe(dict={('domain_test', 'object'): ['https://github.com/hi-primus/optimus', 'localhost:3000?help=true', 'http://www.images.hi-example.com:54/images.php#id?help=1&freq=2', 'hi-optimus.com', 'https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test',
 'https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALeKk013x5lVsRCimJbjWReSgm-p0ztCRg%3A1628028212552&source=hp&ei=NL0JYZeJH9LR9AP-y4WACA&iflsig=AINFCbYAAAAAYQnLRFUJH1DzRYGPNRugXzIzOrF_uZes&oq=this+is+a+test&gs_lcp=Cgdnd3Mtd2l6EAMyBAgjECcyCggAEIAEEIcCEBQyBQguEIAEMgoIABCABBCHAhAUMgUIABCABDIFCAAQgAQyBQgAEIAEMgUIABCABDIFCC4QgAQyBQgAEIAEOgUIABCRAjoLCC4QgAQQxwEQ0QM6CwguEIAEEMcBEKMCOgQILhBDOgQIABBDOggIABCABBDJAzoFCAAQkgM6BQguEMsBOgUIABDLAVDgFVimKmC4LWgAcAB4AIABoQGIAcYMkgEEMy4xMZgBAKABAQ&sclient=gws-wiz&ved=0ahUKEwiX86na7ZXyAhXSKH0KHf5lAYAQ4dUDCAo&uact=5']}, force_data_types=True)
        result = df.cols.domain(cols=['domain_test'])
        expected = self.create_dataframe(dict={('domain_test', 'object'): ['github', 'github', 'hi-example', 'hi-optimus', 'computerhope', 'google']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_domain_all(self):
        df = self.df
        result = df.cols.domain(cols='*')
        expected = self.create_dataframe(dict={('NullType', 'object'): ['google', 'google', 'google', 'google', 'google', 'google'], ('attributes', 'object'): ['google', 'google', 'google', 'google', 'google', 'google'], ('date arrival', 'object'): ['google', 'google', 'google', 'google', 'google', 'google'], ('function(binary)', 'object'): ['google', 'google', 'google', 'google', 'google', 'google'], ('height(ft)', 'object'): ['google', 'google', 'google', 'google', 'google', 'google'], ('japanese name', 'object'): ['google', 'google', 'google', 'google', 'google', 'google'], ('last date seen', 'object'): ['google', 'google', 'google', 'google', 'google', 'google'], ('last position seen', 'object'): ['google', 'google', 'google', 'google', 'google', 'google'], ('rank', 'object'): ['google', 'google', 'google', 'google', 'google', 'google'], ('Cybertronian', 'object'): ['google', 'google', 'google', 'google', 'google', 'google'], ('Date Type', 'object'): ['google', 'google', 'google', 'google', 'google', 'google'], ('age', 'object'): ['google', 'google', 'google', 'google', 'google', 'google'], ('function', 'object'): ['google', 'google', 'google', 'google', 'google', 'google'], ('names', 'object'): ['google', 'google', 'google', 'google', 'google', 'google'], ('timestamp', 'object'): ['google', 'google', 'google', 'google', 'google', 'google'], ('weight(t)', 'object'): ['google', 'google', 'google', 'google', 'google', 'google']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_domain_multiple(self):
        df = self.df
        result = df.cols.domain(cols=['NullType', 'weight(t)', 'japanese name'],output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(dict={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): ['google', 'google', 'google', 'google', 'google', 'google'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): ['google', 'google', 'google', 'google', 'google', 'google'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): ['google', 'google', 'google', 'google', 'google', 'google']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_domain_numeric(self):
        df = self.df.cols.select(['height(ft)'])
        result = df.cols.domain(cols=['height(ft)'])
        expected = self.create_dataframe(dict={('height(ft)', 'object'): ['google', 'google', 'google', 'google', 'google', 'google']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_domain_string(self):
        df = self.df.cols.select(['names'])
        result = df.cols.domain(cols=['names'],output_cols=['names_2'])
        expected = self.create_dataframe(dict={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): ['google', 'google', 'google', 'google', 'google', 'google']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_email_domain(self):
        df = self.create_dataframe(dict={('email_domain_test', 'object'): ['an@example.com', 'thisisatest@gmail.com', 'somename@hotmail.com', 'an@outlook.com', 'anexample@mail.com', 'example@yahoo.com']}, force_data_types=True)
        result = df.cols.email_domain(cols=['email_domain_test'])
        expected = self.create_dataframe(dict={('email_domain_test', 'object'): ['example.com', 'gmail.com', 'hotmail.com', 'outlook.com', 'mail.com', 'yahoo.com']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_email_domain_all(self):
        df = self.df
        result = df.cols.email_domain(cols='*')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_email_domain_multiple(self):
        df = self.df
        result = df.cols.email_domain(cols=['NullType', 'weight(t)', 'japanese name'],output_cols=['nt', 'wt', 'jn'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_email_domain_numeric(self):
        df = self.df.cols.select(['height(ft)'])
        result = df.cols.email_domain(cols=['height(ft)'])
        expected = self.create_dataframe(dict={('height(ft)', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_email_domain_string(self):
        df = self.df.cols.select(['names'])
        result = df.cols.email_domain(cols=['names'],output_cols=['names_2'])
        expected = self.create_dataframe(dict={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'float64'): [nan, nan, nan, nan, nan, nan]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_email_username(self):
        df = self.create_dataframe(dict={('email_username_test', 'object'): ['an@example.com', 'thisisatest@gmail.com', 'somename@hotmail.com', 'an@outlook.com', 'anexample@mail.com', 'example@yahoo.com']}, force_data_types=True)
        result = df.cols.email_username(cols=['email_username_test'])
        expected = self.create_dataframe(dict={('email_username_test', 'object'): ['an', 'thisisatest', 'somename', 'an', 'anexample', 'example']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_email_username_all(self):
        df = self.df
        result = df.cols.email_username(cols='*')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_email_username_multiple(self):
        df = self.df
        result = df.cols.email_username(cols=['NullType', 'weight(t)', 'japanese name'],output_cols=['nt', 'wt', 'jn'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_email_username_numeric(self):
        df = self.df.cols.select(['height(ft)'])
        result = df.cols.email_username(cols=['height(ft)'])
        expected = self.create_dataframe(dict={('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_email_username_string(self):
        df = self.df.cols.select(['names'])
        result = df.cols.email_username(cols=['names'],output_cols=['names_2'])
        expected = self.create_dataframe(dict={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_host(self):
        df = self.create_dataframe(dict={('host_test', 'object'): ['https://github.com/hi-primus/optimus', 'localhost:3000?help=true', 'http://www.images.hi-example.com:54/images.php#id?help=1&freq=2', 'hi-optimus.com', 'https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test',
 'https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALeKk013x5lVsRCimJbjWReSgm-p0ztCRg%3A1628028212552&source=hp&ei=NL0JYZeJH9LR9AP-y4WACA&iflsig=AINFCbYAAAAAYQnLRFUJH1DzRYGPNRugXzIzOrF_uZes&oq=this+is+a+test&gs_lcp=Cgdnd3Mtd2l6EAMyBAgjECcyCggAEIAEEIcCEBQyBQguEIAEMgoIABCABBCHAhAUMgUIABCABDIFCAAQgAQyBQgAEIAEMgUIABCABDIFCC4QgAQyBQgAEIAEOgUIABCRAjoLCC4QgAQQxwEQ0QM6CwguEIAEEMcBEKMCOgQILhBDOgQIABBDOggIABCABBDJAzoFCAAQkgM6BQguEMsBOgUIABDLAVDgFVimKmC4LWgAcAB4AIABoQGIAcYMkgEEMy4xMZgBAKABAQ&sclient=gws-wiz&ved=0ahUKEwiX86na7ZXyAhXSKH0KHf5lAYAQ4dUDCAo&uact=5']}, force_data_types=True)
        result = df.cols.host(cols=['host_test'])
        expected = self.create_dataframe(dict={('host_test', 'object'): ['github.com', 'localhost', 'www.images.hi-example.com', 'hi-optimus.com', 'www.computerhope.com', 'www.google.com']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_host_all(self):
        df = self.df
        result = df.cols.host(cols='*')
        expected = self.create_dataframe(dict={('NullType', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('date arrival', 'object'): ['1980', '1980', '1980', '1980', '1980', '1980'], ('function(binary)', 'object'): ['bytearray', 'bytearray', 'bytearray', 'bytearray', 'bytearray', 'bytearray'], ('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0'], ('japanese name', 'object'): ['300.0', '300.0', '300.0', '300.0', '300.0', '300.0'], ('last date seen', 'object'): ['2016', '2015', '2014', '2013', '2012', '2011'], ('last position seen', 'object'): ['19.442735', '10.642707', '37.789563', '33.670666', 'None', 'None'], ('rank', 'object'): ['10', '7', '7', '8', '10', '8'], ('Cybertronian', 'object'): ['True', 'True', 'True', 'True', 'True', 'False'], ('Date Type', 'object'): ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'], ('age', 'object'): ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First', 'None', 'Battle'], ('names', 'object'): ['Optimus', 'bumbl', 'ironhide', 'Jazz', 'Megatron', 'Metroplex'], ('timestamp', 'object'): ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'], ('weight(t)', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_host_multiple(self):
        df = self.df
        result = df.cols.host(cols=['NullType', 'weight(t)', 'japanese name'],output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(dict={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): ['None', 'None', 'None', 'None', 'None', 'None'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): ['nan', 'nan', 'nan', 'nan', 'nan', 'nan'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_host_numeric(self):
        df = self.df.cols.select(['height(ft)'])
        result = df.cols.host(cols=['height(ft)'])
        expected = self.create_dataframe(dict={('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_host_string(self):
        df = self.df.cols.select(['names'])
        result = df.cols.host(cols=['names'],output_cols=['names_2'])
        expected = self.create_dataframe(dict={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): ['Optimus', 'bumbl', 'ironhide', 'Jazz', 'Megatron', 'Metroplex']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_port(self):
        df = self.create_dataframe(dict={('port_test', 'object'): ['https://github.com/hi-primus/optimus', 'localhost:3000?help=true', 'http://www.images.hi-example.com:54/images.php#id?help=1&freq=2', 'hi-optimus.com', 'https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test',
 'https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALeKk013x5lVsRCimJbjWReSgm-p0ztCRg%3A1628028212552&source=hp&ei=NL0JYZeJH9LR9AP-y4WACA&iflsig=AINFCbYAAAAAYQnLRFUJH1DzRYGPNRugXzIzOrF_uZes&oq=this+is+a+test&gs_lcp=Cgdnd3Mtd2l6EAMyBAgjECcyCggAEIAEEIcCEBQyBQguEIAEMgoIABCABBCHAhAUMgUIABCABDIFCAAQgAQyBQgAEIAEMgUIABCABDIFCC4QgAQyBQgAEIAEOgUIABCRAjoLCC4QgAQQxwEQ0QM6CwguEIAEEMcBEKMCOgQILhBDOgQIABBDOggIABCABBDJAzoFCAAQkgM6BQguEMsBOgUIABDLAVDgFVimKmC4LWgAcAB4AIABoQGIAcYMkgEEMy4xMZgBAKABAQ&sclient=gws-wiz&ved=0ahUKEwiX86na7ZXyAhXSKH0KHf5lAYAQ4dUDCAo&uact=5']}, force_data_types=True)
        result = df.cols.port(cols=['port_test'])
        expected = self.create_dataframe(dict={('port_test', 'object'): [None, '3000', '54', None, None, None]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_port_all(self):
        df = self.df
        result = df.cols.port(cols='*')
        expected = self.create_dataframe(dict={('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [None, None, None, None, None, None], ('date arrival', 'object'): [None, None, None, None, None, None], ('function(binary)', 'object'): [None, None, None, None, None, None], ('height(ft)', 'object'): [None, None, None, None, None, None], ('japanese name', 'object'): [None, None, None, None, None, None], ('last date seen', 'object'): [None, None, None, None, None, None], ('last position seen', 'object'): [None, None, None, None, None, None], ('rank', 'object'): [None, None, None, None, None, None], ('Cybertronian', 'object'): [None, None, None, None, None, None], ('Date Type', 'object'): [None, None, None, None, None, None], ('age', 'object'): [None, None, None, None, None, None], ('function', 'object'): [None, None, None, None, None, None], ('names', 'object'): [None, None, None, None, None, None], ('timestamp', 'object'): [None, None, None, None, None, None], ('weight(t)', 'object'): [None, None, None, None, None, None]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_port_multiple(self):
        df = self.df
        result = df.cols.port(cols=['NullType', 'weight(t)', 'japanese name'],output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(dict={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): [None, None, None, None, None, None], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): [None, None, None, None, None, None]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_port_numeric(self):
        df = self.df.cols.select(['height(ft)'])
        result = df.cols.port(cols=['height(ft)'])
        expected = self.create_dataframe(dict={('height(ft)', 'object'): [None, None, None, None, None, None]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_port_string(self):
        df = self.df.cols.select(['names'])
        result = df.cols.port(cols=['names'],output_cols=['names_2'])
        expected = self.create_dataframe(dict={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): [None, None, None, None, None, None]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_remove_urls(self):
        df = self.create_dataframe(dict={('remove_urls_test', 'object'): ['https://github.com/hi-primus/optimus', 'localhost:3000?help=true', 'http://www.images.hi-example.com:54/images.php#id?help=1&freq=2', 'hi-optimus.com', 'https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test',
 'https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALeKk013x5lVsRCimJbjWReSgm-p0ztCRg%3A1628028212552&source=hp&ei=NL0JYZeJH9LR9AP-y4WACA&iflsig=AINFCbYAAAAAYQnLRFUJH1DzRYGPNRugXzIzOrF_uZes&oq=this+is+a+test&gs_lcp=Cgdnd3Mtd2l6EAMyBAgjECcyCggAEIAEEIcCEBQyBQguEIAEMgoIABCABBCHAhAUMgUIABCABDIFCAAQgAQyBQgAEIAEMgUIABCABDIFCC4QgAQyBQgAEIAEOgUIABCRAjoLCC4QgAQQxwEQ0QM6CwguEIAEEMcBEKMCOgQILhBDOgQIABBDOggIABCABBDJAzoFCAAQkgM6BQguEMsBOgUIABDLAVDgFVimKmC4LWgAcAB4AIABoQGIAcYMkgEEMy4xMZgBAKABAQ&sclient=gws-wiz&ved=0ahUKEwiX86na7ZXyAhXSKH0KHf5lAYAQ4dUDCAo&uact=5']}, force_data_types=True)
        result = df.cols.remove_urls(cols=['remove_urls_test'])
        expected = self.create_dataframe(dict={('remove_urls_test', 'object'): ['', 'localhost:3000?help=true', '', 'hi-optimus.com', '', '']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_remove_urls_all(self):
        df = self.df
        result = df.cols.remove_urls(cols='*')
        expected = self.create_dataframe(dict={('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'float64'): [nan, nan, nan, nan, nan, nan], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'float64'): [nan, nan, nan, nan, nan, nan], ('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0'], ('japanese name', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'object'): ['10', '7', '7', '8', '10', '8'], ('Cybertronian', 'object'): ['True', 'True', 'True', 'True', 'True', 'False'], ('Date Type', 'object'): ['2016-09-10', '2015-08-10', '2014-06-24', '2013-06-24', '2012-05-10', '2011-04-10'], ('age', 'object'): ['5000000', '5000000', '5000000', '5000000', '5000000', '5000000'], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'object'): ['2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24', '2014-06-24'], ('weight(t)', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_remove_urls_multiple(self):
        df = self.df
        result = df.cols.remove_urls(cols=['NullType', 'weight(t)', 'japanese name'],output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(dict={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'float64'): [nan, nan, nan, nan, nan, nan], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): ['4.3', '2.0', '4.0', '1.8', '5.7', 'nan']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_remove_urls_numeric(self):
        df = self.df.cols.select(['height(ft)'])
        result = df.cols.remove_urls(cols=['height(ft)'])
        expected = self.create_dataframe(dict={('height(ft)', 'object'): ['-28.0', '17.0', '26.0', '13.0', 'nan', '300.0']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_remove_urls_string(self):
        df = self.df.cols.select(['names'])
        result = df.cols.remove_urls(cols=['names'],output_cols=['names_2'])
        expected = self.create_dataframe(dict={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_strip_html(self):
        df = self.create_dataframe(dict={('strip_html_test', 'object'): ['<this is a test>', '<this> <is> <a> <test>', '<this> is a <test>', '<this is> a <test>', '<>this is a test<>', '>this is a test<']}, force_data_types=True)
        result = df.cols.strip_html(cols=['strip_html_test'])
        expected = self.create_dataframe(dict={('strip_html_test', 'object'): ['', '   ', ' is a ', ' a ', 'this is a test', '>this is a test<']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_strip_html_all(self):
        df = self.df
        result = df.cols.strip_html(cols='*')
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_strip_html_multiple(self):
        df = self.df
        result = df.cols.strip_html(cols=['NullType', 'weight(t)', 'japanese name'],output_cols=['nt', 'wt', 'jn'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_strip_html_numeric(self):
        df = self.df.cols.select(['height(ft)'])
        result = df.cols.strip_html(cols=['height(ft)'])
        # The following value does not represent a correct output of the operation
        expected = self.dict
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_strip_html_string(self):
        df = self.df.cols.select(['names'])
        result = df.cols.strip_html(cols=['names'],output_cols=['names_2'])
        expected = self.create_dataframe(dict={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_sub_domain(self):
        df = self.create_dataframe(dict={('sub_domain_test', 'object'): ['https://github.com/hi-primus/optimus', 'localhost:3000?help=true', 'http://www.images.hi-example.com:54/images.php#id?help=1&freq=2', 'hi-optimus.com', 'https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test',
 'https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALeKk013x5lVsRCimJbjWReSgm-p0ztCRg%3A1628028212552&source=hp&ei=NL0JYZeJH9LR9AP-y4WACA&iflsig=AINFCbYAAAAAYQnLRFUJH1DzRYGPNRugXzIzOrF_uZes&oq=this+is+a+test&gs_lcp=Cgdnd3Mtd2l6EAMyBAgjECcyCggAEIAEEIcCEBQyBQguEIAEMgoIABCABBCHAhAUMgUIABCABDIFCAAQgAQyBQgAEIAEMgUIABCABDIFCC4QgAQyBQgAEIAEOgUIABCRAjoLCC4QgAQQxwEQ0QM6CwguEIAEEMcBEKMCOgQILhBDOgQIABBDOggIABCABBDJAzoFCAAQkgM6BQguEMsBOgUIABDLAVDgFVimKmC4LWgAcAB4AIABoQGIAcYMkgEEMy4xMZgBAKABAQ&sclient=gws-wiz&ved=0ahUKEwiX86na7ZXyAhXSKH0KHf5lAYAQ4dUDCAo&uact=5']}, force_data_types=True)
        result = df.cols.sub_domain(cols=['sub_domain_test'])
        expected = self.create_dataframe(dict={('sub_domain_test', 'object'): [None, None, 'www.images', None, 'www', 'www']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_sub_domain_all(self):
        df = self.df
        result = df.cols.sub_domain(cols='*')
        expected = self.create_dataframe(dict={('NullType', 'object'): ['www', 'www', 'www', 'www', 'www', 'www'], ('attributes', 'object'): ['www', 'www', 'www', 'www', 'www', 'www'], ('date arrival', 'object'): ['www', 'www', 'www', 'www', 'www', 'www'], ('function(binary)', 'object'): ['www', 'www', 'www', 'www', 'www', 'www'], ('height(ft)', 'object'): ['www', 'www', 'www', 'www', 'www', 'www'], ('japanese name', 'object'): ['www', 'www', 'www', 'www', 'www', 'www'], ('last date seen', 'object'): ['www', 'www', 'www', 'www', 'www', 'www'], ('last position seen', 'object'): ['www', 'www', 'www', 'www', 'www', 'www'], ('rank', 'object'): ['www', 'www', 'www', 'www', 'www', 'www'], ('Cybertronian', 'object'): ['www', 'www', 'www', 'www', 'www', 'www'], ('Date Type', 'object'): ['www', 'www', 'www', 'www', 'www', 'www'], ('age', 'object'): ['www', 'www', 'www', 'www', 'www', 'www'], ('function', 'object'): ['www', 'www', 'www', 'www', 'www', 'www'], ('names', 'object'): ['www', 'www', 'www', 'www', 'www', 'www'], ('timestamp', 'object'): ['www', 'www', 'www', 'www', 'www', 'www'], ('weight(t)', 'object'): ['www', 'www', 'www', 'www', 'www', 'www']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_sub_domain_multiple(self):
        df = self.df
        result = df.cols.sub_domain(cols=['NullType', 'weight(t)', 'japanese name'],output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(dict={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): ['www', 'www', 'www', 'www', 'www', 'www'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): ['www', 'www', 'www', 'www', 'www', 'www'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): ['www', 'www', 'www', 'www', 'www', 'www']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_sub_domain_numeric(self):
        df = self.df.cols.select(['height(ft)'])
        result = df.cols.sub_domain(cols=['height(ft)'])
        expected = self.create_dataframe(dict={('height(ft)', 'object'): ['www', 'www', 'www', 'www', 'www', 'www']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_sub_domain_string(self):
        df = self.df.cols.select(['names'])
        result = df.cols.sub_domain(cols=['names'],output_cols=['names_2'])
        expected = self.create_dataframe(dict={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): ['www', 'www', 'www', 'www', 'www', 'www']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_top_domain(self):
        df = self.create_dataframe(dict={('top_domain_test', 'object'): ['https://github.com/hi-primus/optimus', 'localhost:3000?help=true', 'http://www.images.hi-example.com:54/images.php#id?help=1&freq=2', 'hi-optimus.com', 'https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test',
 'https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALeKk013x5lVsRCimJbjWReSgm-p0ztCRg%3A1628028212552&source=hp&ei=NL0JYZeJH9LR9AP-y4WACA&iflsig=AINFCbYAAAAAYQnLRFUJH1DzRYGPNRugXzIzOrF_uZes&oq=this+is+a+test&gs_lcp=Cgdnd3Mtd2l6EAMyBAgjECcyCggAEIAEEIcCEBQyBQguEIAEMgoIABCABBCHAhAUMgUIABCABDIFCAAQgAQyBQgAEIAEMgUIABCABDIFCC4QgAQyBQgAEIAEOgUIABCRAjoLCC4QgAQQxwEQ0QM6CwguEIAEEMcBEKMCOgQILhBDOgQIABBDOggIABCABBDJAzoFCAAQkgM6BQguEMsBOgUIABDLAVDgFVimKmC4LWgAcAB4AIABoQGIAcYMkgEEMy4xMZgBAKABAQ&sclient=gws-wiz&ved=0ahUKEwiX86na7ZXyAhXSKH0KHf5lAYAQ4dUDCAo&uact=5']}, force_data_types=True)
        result = df.cols.top_domain(cols=['top_domain_test'])
        expected = self.create_dataframe(dict={('top_domain_test', 'object'): ['com', 'com', 'com', 'com', 'com', 'com']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_top_domain_all(self):
        df = self.df
        result = df.cols.top_domain(cols='*')
        expected = self.create_dataframe(dict={('NullType', 'object'): ['com', 'com', 'com', 'com', 'com', 'com'], ('attributes', 'object'): ['com', 'com', 'com', 'com', 'com', 'com'], ('date arrival', 'object'): ['com', 'com', 'com', 'com', 'com', 'com'], ('function(binary)', 'object'): ['com', 'com', 'com', 'com', 'com', 'com'], ('height(ft)', 'object'): ['com', 'com', 'com', 'com', 'com', 'com'], ('japanese name', 'object'): ['com', 'com', 'com', 'com', 'com', 'com'], ('last date seen', 'object'): ['com', 'com', 'com', 'com', 'com', 'com'], ('last position seen', 'object'): ['com', 'com', 'com', 'com', 'com', 'com'], ('rank', 'object'): ['com', 'com', 'com', 'com', 'com', 'com'], ('Cybertronian', 'object'): ['com', 'com', 'com', 'com', 'com', 'com'], ('Date Type', 'object'): ['com', 'com', 'com', 'com', 'com', 'com'], ('age', 'object'): ['com', 'com', 'com', 'com', 'com', 'com'], ('function', 'object'): ['com', 'com', 'com', 'com', 'com', 'com'], ('names', 'object'): ['com', 'com', 'com', 'com', 'com', 'com'], ('timestamp', 'object'): ['com', 'com', 'com', 'com', 'com', 'com'], ('weight(t)', 'object'): ['com', 'com', 'com', 'com', 'com', 'com']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_top_domain_multiple(self):
        df = self.df
        result = df.cols.top_domain(cols=['NullType', 'weight(t)', 'japanese name'],output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(dict={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): ['com', 'com', 'com', 'com', 'com', 'com'], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): ['com', 'com', 'com', 'com', 'com', 'com'], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): ['com', 'com', 'com', 'com', 'com', 'com']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_top_domain_numeric(self):
        df = self.df.cols.select(['height(ft)'])
        result = df.cols.top_domain(cols=['height(ft)'])
        expected = self.create_dataframe(dict={('height(ft)', 'object'): ['com', 'com', 'com', 'com', 'com', 'com']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_top_domain_string(self):
        df = self.df.cols.select(['names'])
        result = df.cols.top_domain(cols=['names'],output_cols=['names_2'])
        expected = self.create_dataframe(dict={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): ['com', 'com', 'com', 'com', 'com', 'com']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_url_file(self):
        df = self.create_dataframe(dict={('url_file_test', 'object'): ['https://github.com/hi-primus/optimus', 'localhost:3000?help=true', 'http://www.images.hi-example.com:54/images.php#id?help=1&freq=2', 'hi-optimus.com', 'https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test',
 'https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALeKk013x5lVsRCimJbjWReSgm-p0ztCRg%3A1628028212552&source=hp&ei=NL0JYZeJH9LR9AP-y4WACA&iflsig=AINFCbYAAAAAYQnLRFUJH1DzRYGPNRugXzIzOrF_uZes&oq=this+is+a+test&gs_lcp=Cgdnd3Mtd2l6EAMyBAgjECcyCggAEIAEEIcCEBQyBQguEIAEMgoIABCABBCHAhAUMgUIABCABDIFCAAQgAQyBQgAEIAEMgUIABCABDIFCC4QgAQyBQgAEIAEOgUIABCRAjoLCC4QgAQQxwEQ0QM6CwguEIAEEMcBEKMCOgQILhBDOgQIABBDOggIABCABBDJAzoFCAAQkgM6BQguEMsBOgUIABDLAVDgFVimKmC4LWgAcAB4AIABoQGIAcYMkgEEMy4xMZgBAKABAQ&sclient=gws-wiz&ved=0ahUKEwiX86na7ZXyAhXSKH0KHf5lAYAQ4dUDCAo&uact=5']}, force_data_types=True)
        result = df.cols.url_file(cols=['url_file_test'])
        expected = self.create_dataframe(dict={('url_file_test', 'object'): ['optimus', None, 'images.php', None, 'search.cgi', 'search']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_url_file_all(self):
        df = self.df
        result = df.cols.url_file(cols='*')
        expected = self.create_dataframe(dict={('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [None, None, None, None, None, None], ('date arrival', 'object'): ['10', '10', '10', '10', '10', '10'], ('function(binary)', 'object'): ["(b'Leader')", "(b'Espionage')", "(b'Security')", "(b'First Lieutenant')", "(b'None')", "(b'Battle Station')"], ('height(ft)', 'object'): [None, None, None, None, None, None], ('japanese name', 'object'): [None, None, None, None, None, None], ('last date seen', 'object'): ['10', '10', '10', '10', '10', '10'], ('last position seen', 'object'): [',-99.201111', ',-71.612534', ',-122.400356', ',-117.841553', None, None], ('rank', 'object'): [None, None, None, None, None, None], ('Cybertronian', 'object'): [None, None, None, None, None, None], ('Date Type', 'object'): [None, None, None, None, None, None], ('age', 'object'): [None, None, None, None, None, None], ('function', 'object'): [None, None, None, ' Lieutenant', None, ' Station'], ('names', 'object'): [None, None, '&', None, None, '_)^$'], ('timestamp', 'object'): [None, None, None, None, None, None], ('weight(t)', 'object'): [None, None, None, None, None, None]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_url_file_multiple(self):
        df = self.df
        result = df.cols.url_file(cols=['NullType', 'weight(t)', 'japanese name'],output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(dict={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): [None, None, None, None, None, None], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): [None, None, None, None, None, None]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_url_file_numeric(self):
        df = self.df.cols.select(['height(ft)'])
        result = df.cols.url_file(cols=['height(ft)'])
        expected = self.create_dataframe(dict={('height(ft)', 'object'): [None, None, None, None, None, None]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_url_file_string(self):
        df = self.df.cols.select(['names'])
        result = df.cols.url_file(cols=['names'],output_cols=['names_2'])
        expected = self.create_dataframe(dict={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): [None, None, '&', None, None, '_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_url_fragment(self):
        df = self.create_dataframe(dict={('url_fragment_test', 'object'): ['https://github.com/hi-primus/optimus', 'localhost:3000?help=true', 'http://www.images.hi-example.com:54/images.php#id?help=1&freq=2', 'hi-optimus.com', 'https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test',
 'https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALeKk013x5lVsRCimJbjWReSgm-p0ztCRg%3A1628028212552&source=hp&ei=NL0JYZeJH9LR9AP-y4WACA&iflsig=AINFCbYAAAAAYQnLRFUJH1DzRYGPNRugXzIzOrF_uZes&oq=this+is+a+test&gs_lcp=Cgdnd3Mtd2l6EAMyBAgjECcyCggAEIAEEIcCEBQyBQguEIAEMgoIABCABBCHAhAUMgUIABCABDIFCAAQgAQyBQgAEIAEMgUIABCABDIFCC4QgAQyBQgAEIAEOgUIABCRAjoLCC4QgAQQxwEQ0QM6CwguEIAEEMcBEKMCOgQILhBDOgQIABBDOggIABCABBDJAzoFCAAQkgM6BQguEMsBOgUIABDLAVDgFVimKmC4LWgAcAB4AIABoQGIAcYMkgEEMy4xMZgBAKABAQ&sclient=gws-wiz&ved=0ahUKEwiX86na7ZXyAhXSKH0KHf5lAYAQ4dUDCAo&uact=5']}, force_data_types=True)
        result = df.cols.url_fragment(cols=['url_fragment_test'])
        expected = self.create_dataframe(dict={('url_fragment_test', 'object'): [None, None, 'id', None, None, None]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_url_fragment_all(self):
        df = self.df
        result = df.cols.url_fragment(cols='*')
        expected = self.create_dataframe(dict={('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [None, None, None, None, None, None], ('date arrival', 'object'): [None, None, None, None, None, None], ('function(binary)', 'object'): [None, None, None, None, None, None], ('height(ft)', 'object'): [None, None, None, None, None, None], ('japanese name', 'object'): [None, None, None, None, None, None], ('last date seen', 'object'): [None, None, None, None, None, None], ('last position seen', 'object'): [None, None, None, None, None, None], ('rank', 'object'): [None, None, None, None, None, None], ('Cybertronian', 'object'): [None, None, None, None, None, None], ('Date Type', 'object'): [None, None, None, None, None, None], ('age', 'object'): [None, None, None, None, None, None], ('function', 'object'): [None, None, None, None, None, None], ('names', 'object'): [None, 'ebéé  ', None, None, None, None], ('timestamp', 'object'): [None, None, None, None, None, None], ('weight(t)', 'object'): [None, None, None, None, None, None]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_url_fragment_multiple(self):
        df = self.df
        result = df.cols.url_fragment(cols=['NullType', 'weight(t)', 'japanese name'],output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(dict={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): [None, None, None, None, None, None], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): [None, None, None, None, None, None]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_url_fragment_numeric(self):
        df = self.df.cols.select(['height(ft)'])
        result = df.cols.url_fragment(cols=['height(ft)'])
        expected = self.create_dataframe(dict={('height(ft)', 'object'): [None, None, None, None, None, None]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_url_fragment_string(self):
        df = self.df.cols.select(['names'])
        result = df.cols.url_fragment(cols=['names'],output_cols=['names_2'])
        expected = self.create_dataframe(dict={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): [None, 'ebéé  ', None, None, None, None]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_url_path(self):
        df = self.create_dataframe(dict={('url_path_test', 'object'): ['https://github.com/hi-primus/optimus', 'localhost:3000?help=true', 'http://www.images.hi-example.com:54/images.php#id?help=1&freq=2', 'hi-optimus.com', 'https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test',
 'https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALeKk013x5lVsRCimJbjWReSgm-p0ztCRg%3A1628028212552&source=hp&ei=NL0JYZeJH9LR9AP-y4WACA&iflsig=AINFCbYAAAAAYQnLRFUJH1DzRYGPNRugXzIzOrF_uZes&oq=this+is+a+test&gs_lcp=Cgdnd3Mtd2l6EAMyBAgjECcyCggAEIAEEIcCEBQyBQguEIAEMgoIABCABBCHAhAUMgUIABCABDIFCAAQgAQyBQgAEIAEMgUIABCABDIFCC4QgAQyBQgAEIAEOgUIABCRAjoLCC4QgAQQxwEQ0QM6CwguEIAEEMcBEKMCOgQILhBDOgQIABBDOggIABCABBDJAzoFCAAQkgM6BQguEMsBOgUIABDLAVDgFVimKmC4LWgAcAB4AIABoQGIAcYMkgEEMy4xMZgBAKABAQ&sclient=gws-wiz&ved=0ahUKEwiX86na7ZXyAhXSKH0KHf5lAYAQ4dUDCAo&uact=5']}, force_data_types=True)
        result = df.cols.url_path(cols=['url_path_test'])
        expected = self.create_dataframe(dict={('url_path_test', 'object'): ['/hi-primus/optimus', None, '/images.php', None, '/cgi-bin/search.cgi', '/search']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_url_path_all(self):
        df = self.df
        result = df.cols.url_path(cols='*')
        expected = self.create_dataframe(dict={('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [None, None, None, None, None, None], ('date arrival', 'object'): ['/04/10', '/04/10', '/04/10', '/04/10', '/04/10', '/04/10'], ('function(binary)', 'object'): ["(b'Leader')", "(b'Espionage')", "(b'Security')", "(b'First Lieutenant')", "(b'None')", "(b'Battle Station')"], ('height(ft)', 'object'): [None, None, None, None, None, None], ('japanese name', 'object'): [None, None, None, None, None, None], ('last date seen', 'object'): ['/09/10', '/08/10', '/07/10', '/06/10', '/05/10', '/04/10'], ('last position seen', 'object'): [',-99.201111', ',-71.612534', ',-122.400356', ',-117.841553', None, None], ('rank', 'object'): [None, None, None, None, None, None], ('Cybertronian', 'object'): [None, None, None, None, None, None], ('Date Type', 'object'): [None, None, None, None, None, None], ('age', 'object'): [None, None, None, None, None, None], ('function', 'object'): [None, None, None, ' Lieutenant', None, ' Station'], ('names', 'object'): [None, None, '&', None, None, '_)^$'], ('timestamp', 'object'): [None, None, None, None, None, None], ('weight(t)', 'object'): [None, None, None, None, None, None]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_url_path_multiple(self):
        df = self.df
        result = df.cols.url_path(cols=['NullType', 'weight(t)', 'japanese name'],output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(dict={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): [None, None, None, None, None, None], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): [None, None, None, None, None, None]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_url_path_numeric(self):
        df = self.df.cols.select(['height(ft)'])
        result = df.cols.url_path(cols=['height(ft)'])
        expected = self.create_dataframe(dict={('height(ft)', 'object'): [None, None, None, None, None, None]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_url_path_string(self):
        df = self.df.cols.select(['names'])
        result = df.cols.url_path(cols=['names'],output_cols=['names_2'])
        expected = self.create_dataframe(dict={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): [None, None, '&', None, None, '_)^$']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_url_scheme(self):
        df = self.create_dataframe(dict={('url_scheme_test', 'object'): ['https://github.com/hi-primus/optimus', 'localhost:3000?help=true', 'http://www.images.hi-example.com:54/images.php#id?help=1&freq=2', 'hi-optimus.com', 'https://www.computerhope.com/cgi-bin/search.cgi?q=example%20search&example=test',
 'https://www.google.com/search?q=this+is+a+test&client=safari&sxsrf=ALeKk013x5lVsRCimJbjWReSgm-p0ztCRg%3A1628028212552&source=hp&ei=NL0JYZeJH9LR9AP-y4WACA&iflsig=AINFCbYAAAAAYQnLRFUJH1DzRYGPNRugXzIzOrF_uZes&oq=this+is+a+test&gs_lcp=Cgdnd3Mtd2l6EAMyBAgjECcyCggAEIAEEIcCEBQyBQguEIAEMgoIABCABBCHAhAUMgUIABCABDIFCAAQgAQyBQgAEIAEMgUIABCABDIFCC4QgAQyBQgAEIAEOgUIABCRAjoLCC4QgAQQxwEQ0QM6CwguEIAEEMcBEKMCOgQILhBDOgQIABBDOggIABCABBDJAzoFCAAQkgM6BQguEMsBOgUIABDLAVDgFVimKmC4LWgAcAB4AIABoQGIAcYMkgEEMy4xMZgBAKABAQ&sclient=gws-wiz&ved=0ahUKEwiX86na7ZXyAhXSKH0KHf5lAYAQ4dUDCAo&uact=5']}, force_data_types=True)
        result = df.cols.url_scheme(cols=['url_scheme_test'])
        expected = self.create_dataframe(dict={('url_scheme_test', 'object'): ['https', None, 'http', None, 'https', 'https']}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_url_scheme_all(self):
        df = self.df
        result = df.cols.url_scheme(cols='*')
        expected = self.create_dataframe(dict={('NullType', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [None, None, None, None, None, None], ('date arrival', 'object'): [None, None, None, None, None, None], ('function(binary)', 'object'): [None, None, None, None, None, None], ('height(ft)', 'object'): [None, None, None, None, None, None], ('japanese name', 'object'): [None, None, None, None, None, None], ('last date seen', 'object'): [None, None, None, None, None, None], ('last position seen', 'object'): [None, None, None, None, None, None], ('rank', 'object'): [None, None, None, None, None, None], ('Cybertronian', 'object'): [None, None, None, None, None, None], ('Date Type', 'object'): [None, None, None, None, None, None], ('age', 'object'): [None, None, None, None, None, None], ('function', 'object'): [None, None, None, None, None, None], ('names', 'object'): [None, None, None, None, None, None], ('timestamp', 'object'): [None, None, None, None, None, None], ('weight(t)', 'object'): [None, None, None, None, None, None]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_url_scheme_multiple(self):
        df = self.df
        result = df.cols.url_scheme(cols=['NullType', 'weight(t)', 'japanese name'],output_cols=['nt', 'wt', 'jn'])
        expected = self.create_dataframe(dict={('NullType', 'object'): [None, None, None, None, None, None], ('nt', 'object'): [None, None, None, None, None, None], ('attributes', 'object'): [[8.5344, 4300.0], [5.334, 2000.0], [7.9248, 4000.0], [3.9624, 1800.0], [None, 5700.0], [91.44, None]], ('date arrival', 'object'): ['1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10', '1980/04/10'], ('function(binary)', 'object'): [bytearray(b'Leader'), bytearray(b'Espionage'), bytearray(b'Security'), bytearray(b'First Lieutenant'), bytearray(b'None'), bytearray(b'Battle Station')], ('height(ft)', 'float64'): [-28.0, 17.0, 26.0, 13.0, nan, 300.0], ('japanese name', 'object'): [['Inochi', 'Convoy'], ['Bumble', 'Goldback'], ['Roadbuster'], ['Meister'], ['Megatron'], ['Metroflex']], ('jn', 'object'): [None, None, None, None, None, None], ('last date seen', 'object'): ['2016/09/10', '2015/08/10', '2014/07/10', '2013/06/10', '2012/05/10', '2011/04/10'], ('last position seen', 'object'): ['19.442735,-99.201111', '10.642707,-71.612534', '37.789563,-122.400356', '33.670666,-117.841553', None, None], ('rank', 'int64'): [10, 7, 7, 8, 10, 8], ('Cybertronian', 'bool'): [True, True, True, True, True, False], ('Date Type', 'datetime64[ns]'): [Timestamp('2016-09-10 00:00:00'), Timestamp('2015-08-10 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2013-06-24 00:00:00'), Timestamp('2012-05-10 00:00:00'), Timestamp('2011-04-10 00:00:00')], ('age', 'int64'): [5000000, 5000000, 5000000, 5000000, 5000000, 5000000], ('function', 'object'): ['Leader', 'Espionage', 'Security', 'First Lieutenant', 'None', 'Battle Station'], ('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('timestamp', 'datetime64[ns]'): [Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00'), Timestamp('2014-06-24 00:00:00')], ('weight(t)', 'float64'): [4.3, 2.0, 4.0, 1.8, 5.7, nan], ('wt', 'object'): [None, None, None, None, None, None]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_url_scheme_numeric(self):
        df = self.df.cols.select(['height(ft)'])
        result = df.cols.url_scheme(cols=['height(ft)'])
        expected = self.create_dataframe(dict={('height(ft)', 'object'): [None, None, None, None, None, None]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))
    
    def test_cols_url_scheme_string(self):
        df = self.df.cols.select(['names'])
        result = df.cols.url_scheme(cols=['names'],output_cols=['names_2'])
        expected = self.create_dataframe(dict={('names', 'object'): ['Optimus', 'bumbl#ebéé  ', 'ironhide&', 'Jazz', 'Megatron', 'Metroplex_)^$'], ('names_2', 'object'): [None, None, None, None, None, None]}, force_data_types=True)
        self.assertTrue(result.equals(expected, decimal=True, assertion=True))


class TestWebDask(TestWebPandas):
    config = {'engine': 'dask', 'n_partitions': 1}


class TestWebPartitionDask(TestWebPandas):
    config = {'engine': 'dask', 'n_partitions': 2}


try:
    import cudf
except:
    pass
else:
    class TestWebCUDF(TestWebPandas):
        config = {'engine': 'cudf'}


try:
    import dask_cudf
except:
    pass
else:
    class TestWebDC(TestWebPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 1}


try:
    import dask_cudf
except:
    pass
else:
    class TestWebPartitionDC(TestWebPandas):
        config = {'engine': 'dask_cudf', 'n_partitions': 2}


class TestWebSpark(TestWebPandas):
    config = {'engine': 'spark'}


class TestWebVaex(TestWebPandas):
    config = {'engine': 'vaex'}

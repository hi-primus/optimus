import datetime
import sys
sys.path.append("../..")


def create():
    from optimus import Optimus
    from optimus.tests.creator import TestCreator, default_configs

    op = Optimus("pandas")
    df = op.create.dataframe({
        'date arrival': ["Sat Sep 30 2023 11:38:11 -0400", "Mon May 1 2021 13:25:51 -0400", "Sun Aug 30 2021 00:00:01 -0400", "Fri Dec 31 1999 23:59:59 -0400", "Wednesday Jan 15 2038 10:30:19 -0400", "Tue Jul 31 1956 12:00:00 -0400"],
        'some date': ["Fri Dec 31 1999 23:59:59 -0400", "Wednesday Jan 15 2038 10:30:19 -0400", "Mon May 1 2021 13:25:51 -0400", "Sat Sep 30 2023 11:38:11 -0400", "Tue Jul 31 1956 12:00:00 -0400", "Sun Aug 30 2021 00:00:01 -0400"],
        ('last date seen', 'date'): ['2016/09/10', '2015/08/10', '2021/12/12', '2013/06/10', '2012/05/10', "2000/05/12"],
        ('Date Type'): [datetime.datetime(2016, 9, 10), datetime.datetime(2015, 8, 10), datetime.datetime(2020, 3, 1), datetime.datetime(2013, 6, 24), datetime.datetime(2012, 5, 10), "2021/8/30"],
        ('timestamp', 'time'): ['2014/12/24', datetime.datetime(2014, 6, 24, 15, 0), datetime.datetime(2014, 6, 24, 2, 12), datetime.datetime(2014, 6, 24, 23, 3, 59), "2021-04-05 17:38:11", datetime.datetime(2021, 9, 3, 0, 0, 1)],
    })

    t = TestCreator(op, df, name="date", configs=default_configs)

    t.create(method="cols.year", variant="all", cols="*")
    t.create(method="cols.year", variant="single", cols=["date arrival"], format='%a %b %d %Y %H:%M:%S -%Y')
    t.create(method="cols.year", variant="multiple", cols=["Date Type", "timestamp", "last date seen"], output_cols=["dt", "ts", "da"])

    t.create(method="cols.month", variant="all", cols="*")
    t.create(method="cols.month", variant="single", cols=["date arrival"], format='%a %b %d %Y %H:%M:%S -%Y')
    t.create(method="cols.month", variant="multiple", cols=["Date Type", "timestamp", "last date seen"], output_cols=["dt", "ts", "da"])

    t.create(method="cols.day", variant="all", cols="*")
    t.create(method="cols.day", variant="single", cols=["date arrival"], format='%a %b %d %Y %H:%M:%S -%Y')
    t.create(method="cols.day", variant="multiple", cols=["Date Type", "timestamp", "last date seen"], output_cols=["dt", "ts", "da"])

    t.create(method="cols.hour", variant="all", cols="*")
    t.create(method="cols.hour", variant="single", cols=["date arrival"], format='%a %b %d %Y %H:%M:%S -%Y')
    t.create(method="cols.hour", variant="multiple", cols=["Date Type", "timestamp", "last date seen"], output_cols=["dt", "ts", "da"])

    t.create(method="cols.minute", variant="all", cols="*")
    t.create(method="cols.minute", variant="single", cols=["date arrival"], format='%a %b %d %Y %H:%M:%S -%Y')
    t.create(method="cols.minute", variant="multiple", cols=["Date Type", "timestamp", "last date seen"], output_cols=["dt", "ts", "da"])

    t.create(method="cols.second", variant="all", cols="*")
    t.create(method="cols.second", variant="single", cols=["date arrival"], format='%a %b %d %Y %H:%M:%S -%Y')
    t.create(method="cols.second", variant="multiple", cols=["Date Type", "timestamp", "last date seen"], output_cols=["dt", "ts", "da"])

    t.create(method="cols.weekday", variant="all", cols="*")
    t.create(method="cols.weekday", variant="single", cols=["date arrival"], format='%a %b %d %Y %H:%M:%S -%Y')
    t.create(method="cols.weekday", variant="multiple", cols=["Date Type", "timestamp", "last date seen"], output_cols=["dt", "ts", "da"])

    t.create(method="cols.years_between", variant="all", cols="*", value='2000-1-1', date_format='%Y-%d-%m', round=False)
    t.create(method="cols.years_between", variant="all_today", cols="*", round="up")
    t.create(method="cols.years_between", variant="single", cols=["date arrival"], value="Sat Sep 4 2021 10:05:39 -0400", date_format='%a %b %d %Y %H:%M:%S -%Y', round="down")
    t.create(method="cols.years_between", variant="single_timezone", cols=["date arrival", "some date"], round="round")
    t.create(method="cols.years_between", variant="multiple", cols=["Date Type", "timestamp", "last date seen"], value="29/02/2012", date_format='%d/%m/%Y', round="floor", output_cols=["dt", "ts", "da"])
    t.create(method="cols.years_between", variant="multiple_today", cols=["Date Type", "timestamp", "last date seen"], round="ceil", output_cols=["dt", "ts", "da"])

    t.create(method="cols.months_between", variant="all", cols="*", value='2000-1-1', date_format='%Y-%d-%m', round=False)
    t.create(method="cols.months_between", variant="all_today", cols="*", round="up")
    t.create(method="cols.months_between", variant="single", cols=["date arrival"], value="Sat Sep 4 2021 10:05:39 -0400", date_format='%a %b %d %Y %H:%M:%S -%Y', round="down")
    t.create(method="cols.months_between", variant="single_timezone", cols=["date arrival", "some date"], round="round")
    t.create(method="cols.months_between", variant="multiple", cols=["Date Type", "timestamp", "last date seen"], value="29/02/2012", date_format='%d/%m/%Y', round="floor", output_cols=["dt", "ts", "da"])
    t.create(method="cols.months_between", variant="multiple_today", cols=["Date Type", "timestamp", "last date seen"], round="ceil", output_cols=["dt", "ts", "da"])

    t.create(method="cols.days_between", variant="all", cols="*", value='2000-1-1', date_format='%Y-%d-%m', round=False)
    t.create(method="cols.days_between", variant="all_today", cols="*", round="up")
    t.create(method="cols.days_between", variant="single", cols=["date arrival"], value="Sat Sep 4 2021 10:05:39 -0400", date_format='%a %b %d %Y %H:%M:%S -%Y', round="down")
    t.create(method="cols.days_between", variant="single_timezone", cols=["date arrival", "some date"], round="round")
    t.create(method="cols.days_between", variant="multiple", cols=["Date Type", "timestamp", "last date seen"], value="29/02/2012", date_format='%d/%m/%Y', round="floor", output_cols=["dt", "ts", "da"])
    t.create(method="cols.days_between", variant="multiple_today", cols=["Date Type", "timestamp", "last date seen"], round="ceil", output_cols=["dt", "ts", "da"])

    t.create(method="cols.hours_between", variant="all", cols="*", value='2000-1-1', date_format='%Y-%d-%m', round=False)
    t.create(method="cols.hours_between", variant="all_today", cols="*", round="up")
    t.create(method="cols.hours_between", variant="single", cols=["date arrival"], value="Sat Sep 4 2021 10:05:39 -0400", date_format='%a %b %d %Y %H:%M:%S -%Y', round="down")
    t.create(method="cols.hours_between", variant="single_timezone", cols=["date arrival", "some date"], round="round")
    t.create(method="cols.hours_between", variant="multiple", cols=["Date Type", "timestamp", "last date seen"], value="29/02/2012", date_format='%d/%m/%Y', round="floor", output_cols=["dt", "ts", "da"])
    t.create(method="cols.hours_between", variant="multiple_today", cols=["Date Type", "timestamp", "last date seen"], round="ceil", output_cols=["dt", "ts", "da"])

    t.create(method="cols.minutes_between", variant="all", cols="*", value='2000-1-1', date_format='%Y-%d-%m', round=False)
    t.create(method="cols.minutes_between", variant="all_today", cols="*", round="up")
    t.create(method="cols.minutes_between", variant="single", cols=["date arrival"], value="Sat Sep 4 2021 10:05:39 -0400", date_format='%a %b %d %Y %H:%M:%S -%Y', round="down")
    t.create(method="cols.minutes_between", variant="single_timezone", cols=["date arrival", "same date"], round="round")
    t.create(method="cols.minutes_between", variant="multiple", cols=["Date Type", "timestamp", "last date seen"], value="29/02/2012", date_format='%d/%m/%Y', round="floor", output_cols=["dt", "ts", "da"])
    t.create(method="cols.minutes_between", variant="multiple_today", cols=["Date Type", "timestamp", "last date seen"], round="ceil", output_cols=["dt", "ts", "da"])

    t.create(method="cols.seconds_between", variant="all", cols="*", value='2000-1-1', date_format='%Y-%d-%m', round=False)
    t.create(method="cols.seconds_between", variant="all_today", cols="*", round="up")
    t.create(method="cols.seconds_between", variant="single", cols=["date arrival"], value="Sat Sep 4 2021 10:05:39 -0400", date_format='%a %b %d %Y %H:%M:%S -%Y', round="down")
    t.create(method="cols.seconds_between", variant="single_timezone", cols=["date arrival", "same date"], round="round")
    t.create(method="cols.seconds_between", variant="multiple", cols=["Date Type", "timestamp", "last date seen"], value="29/02/2012", date_format='%d/%m/%Y', round="floor", output_cols=["dt", "ts", "da"])
    t.create(method="cols.seconds_between", variant="multiple_today", cols=["Date Type", "timestamp", "last date seen"], round="ceil", output_cols=["dt", "ts", "da"])

    t.create(method="cols.date_format", variant="all", cols="*")
    t.create(method="cols.date_format", variant="single", cols=["date arrival"])
    t.create(method="cols.date_format", variant="multiple", cols=["Date Type", "timestamp", "last date seen"])

    t.create(method="cols.format_date", variant="all", cols="*", output_format='%d')
    t.create(method="cols.format_date", variant="single", cols=["date arrival"], current_format='%a %b %d %Y %H:%M:%S -%Y', output_format='%d/%Y/%m')
    t.create(method="cols.format_date", variant="multiple", cols=["Date Type", "timestamp", "last date seen"], output_format='%H:%M:%S', output_cols=["dt", "ts", "da"])

    t.run()

create()

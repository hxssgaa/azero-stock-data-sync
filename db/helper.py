import datetime


def int_2_date(int_date):
    return datetime.datetime.fromtimestamp(int_date).strftime('%Y-%m-%d %H:%M:%S')


def date_2_int(date_str):
    return int(datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").timestamp())

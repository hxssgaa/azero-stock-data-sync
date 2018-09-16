import datetime


def int_2_date(int_date, is_short=False):
    if is_short:
        return datetime.datetime.fromtimestamp(int_date).strftime('%Y%m%d %H:%M:%S')
    else:
        return datetime.datetime.fromtimestamp(int_date).strftime('%Y-%m-%d %H:%M:%S')


def int_2_date_utc(int_date):
    return (datetime.datetime.utcfromtimestamp(int_date) - datetime.timedelta(hours=4))\
        .strftime('%Y%m%d %H:%M:%S')


def date_2_int(date_str, is_short=False):
    if is_short:
        return int(datetime.datetime.strptime(date_str, "%Y%m%d %H:%M:%S").timestamp())
    else:
        return int(datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").timestamp())

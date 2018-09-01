from datetime import datetime
from pytz import timezone


def format_history_quotes_result(res):
    if not res or ('empty' in res and res['empty']) or 'error' in res:
        return res
    candles = res['candles']
    for candle in candles:
        candle['datetime'] = datetime.fromtimestamp(candle['datetime'] / 1000.0,
                                                    tz=timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')
    return res


def format_delayed_quote(res):
    if not res or 'error' in res:
        return res
    for k, v in res.items():
        v['quoteTime'] = datetime.fromtimestamp(v['quoteTimeInLong'] / 1000.0,
                                                tz=timezone('America/New_York')).strftime('%Y-%m-%d %H:%M:%S')
    return res

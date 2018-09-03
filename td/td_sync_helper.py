import asyncio
import datetime

from common.utils import StockUtils, SyncProcessHelper
from td.td_api import TDQuoteApi
from queue import Queue
from db.stock_db import query_latest_td_data, insert_td_data, query_data_dt_range
from db.helper import date_2_int

API_KEY = 'HXSSG1124@AMER.OAUTHAP'
TYPE_MAP = {
    '1': 11,
    '5': 12,
    '10': 13,
    '15': 14,
    '30': 15,
}
TYPE_FREQ_MAP = {
    1: '1M',
    11: '1M',
    12: '5M',
    13: '10M',
    14: '15M',
    15: '30M'
}


def _create_td_api():
    api_key = API_KEY
    quote_api = TDQuoteApi(api_key)
    return quote_api


def _filter_symbol_data(symbol_data):
    right = []
    wrong = []
    for data in symbol_data:
        if 'empty' in data[2] and data[2]['empty']:
            wrong.append(data)
        else:
            right.append(data)
    return right, wrong


async def async_symbol_data(quote_api, symbol_queue, parallel_cnt=25):
    loop = asyncio.get_event_loop()
    start_date = '1990-01-01 00:00:00'

    def _post(symbol, frequency, times):
        if symbol.startswith('US.'):
            symbol_api = symbol.replace('US.', '')
        else:
            symbol_api = symbol
        dt_latest = query_latest_td_data(symbol, TYPE_MAP[frequency])
        _start_date = dt_latest if dt_latest else start_date
        return symbol, frequency, quote_api.get_history_quotes(symbol_api, start_date=_start_date, period=1,
                                                               period_type='day',
                                                               frequency_type='minute', frequency=frequency,
                                                               need_extended_hours_data=True), times + 1

    symbols = []
    for _ in range(parallel_cnt):
        if symbol_queue.empty():
            break
        symbols.append(symbol_queue.get())

    futures = [
        loop.run_in_executor(
            None,
            _post,
            symbol,
            frequency,
            times
        )
        for symbol, frequency, times in symbols
    ]
    return [response for response in await asyncio.gather(*futures)]


def _get_td_bson_data(x, symbol_type):
    if not x:
        return None
    return {
        'type': symbol_type,
        'dt': date_2_int(x['datetime']),
        'open': x['open'],
        'close': x['close'],
        'high': x['high'],
        'low': x['low'],
        'volume': x['volume'],
        'pe': 0.0,
    }


def _update_symbol_data(symbol, frequency, quotes):
    t = TYPE_MAP[frequency]
    latest_date = query_latest_td_data(symbol, t)
    latest_date_int = date_2_int(latest_date)
    if not quotes or 'candles' not in quotes:
        return 0

    candles = list(filter(lambda x: 'datetime' in x and date_2_int(x['datetime']) > latest_date_int, quotes['candles']))
    candles_res = list(map(lambda x: _get_td_bson_data(x, TYPE_MAP[frequency]), candles))
    if not candles_res:
        return 0

    return insert_td_data(symbol, candles_res)


def _sync_symbol_data(quote_api, symbol_queue, parallel_cnt=25):
    total_cnt = symbol_queue.qsize()
    while not symbol_queue.empty():
        loop = asyncio.get_event_loop()
        symbol_data = async_symbol_data(quote_api, symbol_queue, parallel_cnt=parallel_cnt)
        future = loop.run_until_complete(symbol_data)
        right, wrong = _filter_symbol_data(future)

        # Put wrong symbol data to the queue
        for symbol, freq, _, times in wrong:
            if times == 3:
                continue
            symbol_queue.put((symbol, freq, times))

        for symbol, frequency, quotes, _ in right:
            res = _update_symbol_data(symbol, frequency, quotes)
            SyncProcessHelper.add_sync_record({
                'symbol': symbol,
                'frequency': frequency,
                'count': res,
                'syncDateTime': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })

        SyncProcessHelper.update_sync_progress(1 - symbol_queue.qsize() / float(total_cnt))


def start_sync_helper(symbols):
    quote_api = _create_td_api()
    q = Queue()

    asyncio.set_event_loop(asyncio.new_event_loop())
    for i, symbol in enumerate(symbols):
        for freq in ['1', '5', '10', '15', '30']:
            q.put((symbol, freq, 1))

    _sync_symbol_data(quote_api, q)


def fuzzy_query_code_list(code):
    stock_infos = StockUtils.get_stock_infos()
    res_list = list(sorted(filter(lambda x: x['symbol'].startswith(code), stock_infos),
                           key=lambda y: (y['symbol'] != code, y['symbol'])))
    return res_list[:20], len(res_list)


def query_sync_info(code):
    t_info_map = {}
    for t in [1, 11, 12, 13, 14, 15]:
        t_info_map[t] = query_data_dt_range(code, t)
    res = {}
    for t in [1, 11, 12, 13, 14, 15]:
        freq = TYPE_FREQ_MAP[t]
        value = t_info_map[t]
        if value is None:
            continue
        if freq in res:
            res[freq] = {
                'startDate': min(res[freq]['startDate'], value[0]),
                'endDate': max(res[freq]['endDate'], value[1])
            }
        else:
            res[freq] = {
                'startDate': value[0],
                'endDate': value[1]
            }
    return res

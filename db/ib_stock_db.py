import time
import logging
from collections import defaultdict

from common.utils import StockUtils
from db import _db
from db.helper import int_2_date, date_2_int, chunks
from db.td_stock_db import create_index, collection_names
from pymongo import ASCENDING, DESCENDING

from ib.ib_api import IBApp

IB_SYNC_SYMBOLS_COLLECTION_NAME = 'IB_SYNC_SYMBOLS'
IB_SYNC_METADATA_COLLECTION_NAME = 'IB_SYNC_METADATA'
IB_SYNC_REALTIME_TIME_GAP = 30


def query_ib_data_dt_range(symbol, t):
    """
    Query stock data date range from given symbol and type

    :param symbol: symbol to query
    :param t: type of the stock
    """
    if not symbol.startswith('US.'):
        symbol = 'US.%s' % symbol
    t = int(t)
    cnt = _db[symbol].count({
        'type': t
    })
    if cnt == 0:
        return None
    res = (_db[symbol].find({
        'type': t
    }).sort([('dt', ASCENDING)]).limit(1).next()['dt'], _db[symbol].find({
        'type': t
    }).sort([('dt', DESCENDING)]).limit(1).next()['dt'])
    return tuple(map(lambda x: int_2_date(x, is_short=True), res))


def query_ib_tick_dt_range(symbol):
    """
    Query stock data date range from given symbol and type

    :param symbol: symbol to query
    :param t: type of the stock
    """
    if not symbol.startswith('US.'):
        symbol = 'US.%s' % symbol
    symbol = '%s-tick' % symbol
    cnt = _db[symbol].count({})
    if cnt == 0:
        return None
    res = (_db[symbol].find({}).sort([('dt', ASCENDING)])
           .limit(1).next()['dt'], _db[symbol].find({})
           .sort([('dt', DESCENDING)]).limit(1).next()['dt'])
    return tuple(map(lambda x: int_2_date(x, is_short=True), res))


def query_ib_earliest_dt(contract, min_date):
    """
    Get earliest datetime point in given symbol

    :param contract: Given contract to query
    :param min_date: Minimum date of the contract
    :return: Earliest datetime point
    """
    # head_time, errors = app.req_head_time_stamp(req_id, contract)
    # if not head_time:
    #     return None
    symbol = contract.symbol
    if not symbol.startswith('US.'):
        symbol = 'US.%s' % symbol
    cnt = _db['US_DAY'].count({'code': symbol})
    if cnt == 0:
        return min_date
    return max(int_2_date(_db['US_DAY'].find({'code': symbol}).sort([('dt', ASCENDING)])
                          .limit(1).next()['dt']).replace('-', ''), min_date)


def get_ib_sync_symbols():
    stock_infos = StockUtils.get_stock_infos()
    res_list = list(_db[IB_SYNC_SYMBOLS_COLLECTION_NAME].find({}, {'_id': False}))
    symbol_set = set(e['symbol'] for e in res_list)
    return list(filter(lambda x: x['symbol'] in symbol_set, stock_infos))


def insert_ib_sync_symbols(symbols):
    if len(symbols) > 100:
        raise RuntimeError('symbols length exceed maximum 100 symbols.')
    _db[IB_SYNC_SYMBOLS_COLLECTION_NAME].delete_many({})
    _db[IB_SYNC_SYMBOLS_COLLECTION_NAME].insert_many(symbols)


def get_ib_sync_metadata():
    return list(_db[IB_SYNC_METADATA_COLLECTION_NAME].find({}, {'_id': False}))


def update_ib_sync_metadata(md_list):
    """
    Update ib sync metadata

    :param md_list: given metadata list
    """
    _db[IB_SYNC_METADATA_COLLECTION_NAME].delete_many({})
    _db[IB_SYNC_METADATA_COLLECTION_NAME].insert_many(md_list)
    return get_ib_sync_metadata()


def insert_ib_data(symbol, rows):
    if not symbol.startswith('US.'):
        symbol = 'US.%s' % symbol

    existed = symbol in collection_names
    res = _db[symbol].insert_many(rows)

    if not existed:
        create_index(_db[symbol])
    return len(res.inserted_ids)


def insert_ib_tick_data(symbol, rows):
    if not symbol.startswith('US.'):
        symbol = 'US.%s' % symbol

    symbol = '%s-tick' % symbol
    existed = symbol in collection_names
    res = _db[symbol].insert_many(rows)

    if not existed:
        _db[symbol].create_index([('dt', ASCENDING)])
    return len(res.inserted_ids)


def _get_bson(line):
    return {
        'dt': date_2_int(line[2]),
        'tick_type': int(line[3]),
        'tick_info': float(line[4]) if 0 <= int(line[3]) <= 9 else line[4]
    }


def _insert_ib_rt_data(symbol, rows, tracker):
    if not symbol.startswith('US.'):
        symbol = 'US.%s' % symbol

    symbol = '%s-real' % symbol
    _db[symbol].insert_many(rows)
    tracker.add_track_record('Inserted %d data' % len(rows), symbol.replace('US.', ''))
    logging.warning('%s inserted: %d' % (symbol, len(rows)))


def insert_ib_rt_data(data_queue, req_id_symbol_map, tracker):
    data_map = defaultdict(list)
    last_sync_time = time.time()
    while True:
        dt = data_queue.get()
        current_time = time.time()
        if current_time - last_sync_time > IB_SYNC_REALTIME_TIME_GAP:
            for symbol in data_map.keys():
                _insert_ib_rt_data(symbol, data_map[symbol], tracker)
            last_sync_time = current_time
            data_map.clear()
        symbol = req_id_symbol_map[dt[0]]
        bson_data = _get_bson(dt)
        data_map[symbol].append(bson_data)


def get_db_size_info():
    return _db.command("dbstats")
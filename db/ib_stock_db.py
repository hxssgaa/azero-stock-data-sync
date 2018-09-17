from db import _db
from db.helper import int_2_date
from db.td_stock_db import create_index, collection_names
from pymongo import ASCENDING, DESCENDING

from ib.ib_api import IBApp

IB_SYNC_SYMBOLS_COLLECTION_NAME = 'IB_SYNC_SYMBOLS'
IB_SYNC_METADATA_COLLECTION_NAME = 'IB_SYNC_METADATA'


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


def query_ib_earliest_dt(app, req_id, contract):
    """
    Get earliest datetime point in given symbol

    :param app IBApp
    :param contract: Given contract to query
    :return: Earliest datetime point
    """
    head_time, errors = app.req_head_time_stamp(req_id, contract)
    return '%s %s' % (head_time[1].split()[0], head_time[1].split()[1])


def get_ib_sync_symbols():
    return list(_db[IB_SYNC_SYMBOLS_COLLECTION_NAME].find({}, {'_id': False}))


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

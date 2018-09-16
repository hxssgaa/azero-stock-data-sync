from db import _db
from db.helper import int_2_date
from db.td_stock_db import create_index, collection_names
from pymongo import ASCENDING, DESCENDING

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

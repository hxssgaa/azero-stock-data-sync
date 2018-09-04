from db import _db
from db.helper import *
from pymongo import ASCENDING, DESCENDING

_collection_names = _db.collection_names()


def query_data_dt_range(symbol, t):
    """
    Query TD stock data date range from given symbol and type

    :param symbol: symbol to query
    :param t: type of the stock
    """
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
    return tuple(map(int_2_date, res))


def query_latest_td_data(symbol, t):
    """
    Query latest TD stock data date.

    :param symbol: symbol to query
    :param t: type of the stock
    """
    t = int(t)
    cnt = _db[symbol].count({
        'type': t
    })
    if cnt == 0:
        return None
    return int_2_date(_db[symbol].find({
        'type': t
    }).sort([('dt', DESCENDING)]).limit(1).next()['dt'])


def create_index(collection):
    collection.create_index([('type', ASCENDING), ('dt', ASCENDING)], unique=True)


def insert_td_data(symbol, rows):
    existed = symbol in _collection_names
    res = _db[symbol].insert_many(rows)

    if not existed:
        create_index(_db[symbol])
    return len(res.inserted_ids)

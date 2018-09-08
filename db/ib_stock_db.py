from db import _db
from db.helper import *
from pymongo import ASCENDING, DESCENDING

IB_SYNC_SYMBOLS_COLLECTION_NAME = 'IB_SYNC_SYMBOLS'


def get_ib_sync_symbols():
    return list(map(lambda x: {'symbol': x['symbol']}, _db[IB_SYNC_SYMBOLS_COLLECTION_NAME].find()))


def insert_ib_sync_symbols(symbols):
    if len(symbols) > 100:
        raise RuntimeError('symbols length exceed maximum 100 symbols.')
    _db[IB_SYNC_SYMBOLS_COLLECTION_NAME].delete_many({})
    _db[IB_SYNC_SYMBOLS_COLLECTION_NAME].insert_many(symbols)

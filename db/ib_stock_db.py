from db import _db
from db.helper import *
from pymongo import ASCENDING, DESCENDING

IB_SYNC_SYMBOLS_COLLECTION_NAME = 'IB_SYNC_SYMBOLS'
IB_SYNC_METADATA_COLLECTION_NAME = 'IB_SYNC_METADATA'


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

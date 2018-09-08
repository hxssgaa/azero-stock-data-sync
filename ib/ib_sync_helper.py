import db.ib_stock_db as db
from common.utils import ManagedProcess
from ib import _app as app
from ib.ib_utils import *
from db.ib_stock_db import *

IB_SYNC_PROCESS_NAME = 'IB_%d'


def get_sync_symbols_data_helper():
    return {
        'stocks': list(db.get_ib_sync_symbols())
    }


def insert_sync_symbols_data_helper(symbols):
    if not symbols:
        return {}
    db.insert_ib_sync_symbols(symbols)
    return get_sync_symbols_data_helper()


def get_sync_metadata_helper():
    return db.get_ib_sync_metadata()


def update_sync_metadata_helper(md_list):
    if not md_list:
        return {}
    return {
        'metadata': db.update_ib_sync_metadata(md_list)
    }


def _inner_start_1m_sync_helper(contracts):
    for contract in contracts:
        contract_dt_range = query_ib_data_dt_range(contract.symbol, 31)
        print(contract_dt_range)


def _inner_start_1s_sync_helper(contracts):
    pass


def _inner_start_tick_sync_helper(contracts):
    pass


def _inner_start_sync_helper(t, contracts):
    return {
        0: _inner_start_1m_sync_helper,
        1: _inner_start_1s_sync_helper,
        2: _inner_start_tick_sync_helper
    }.get(t)(contracts)


def start_sync_helper(t):
    t = int(t)
    symbols = list(db.get_ib_sync_symbols())
    if not symbols:
        return {'status': 2}

    existed = ManagedProcess.is_process_existed(IB_SYNC_PROCESS_NAME % t)
    if existed:
        return {'status': 1}

    contracts = [make_contract(symbol['symbol'], 'SMART') for symbol in symbols]
    ManagedProcess.create_process(IB_SYNC_PROCESS_NAME % t, _inner_start_sync_helper, (t, contracts))
    return {'status': 0}
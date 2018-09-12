import datetime
import db.ib_stock_db as db
import ib.ib_utils as utils
from common.utils import ManagedProcess
from ib.ib_api import IBApp
import time

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


def _get_offset_trading_day(trading_days, trading_day, offset_day):
    if trading_day not in trading_days:
        raise RuntimeError(
            'Specified trading day: %s not in trading days' % trading_day)
    index = trading_days.index(trading_day)
    if index + offset_day >= len(trading_days):
        return trading_days[-1]
    return '%s 23:59:59' % trading_days[index + offset_day]


def _inner_start_1m_sync_helper(contracts):
    app = IBApp("10.150.0.2", 4001, 50)
    now_datetime = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
    now_date = now_datetime.split()[0]
    trading_days = utils.get_trading_days('20040123', now_date)
    sync_days = 5
    for i, contract in enumerate(contracts):
        contract_dt_range = db.query_ib_data_dt_range(contract.symbol, 31)
        if not contract_dt_range:
            query_time = datetime.datetime.now().strftime('%Y%m%d %H:%M:%S')
        else:
            latest_sync_date = contract_dt_range[1].split()[0]
            print('last_sync_date', latest_sync_date)
            query_time = _get_offset_trading_day(
                trading_days, latest_sync_date, sync_days)
        s1 = time.time()
        hist_data = app.req_historical_data(
            1000 + i, contract, query_time, '5 D', '30 secs')
        s2 = time.time()
        print(s2 - s1)
        print(hist_data[0])


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

    contracts = [utils.make_contract(
        symbol['symbol'], 'SMART') for symbol in symbols]
    ManagedProcess.create_process(
        IB_SYNC_PROCESS_NAME % t, _inner_start_sync_helper, (t, contracts))
    return {'status': 0}

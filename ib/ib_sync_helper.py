import datetime
import db.ib_stock_db as db
import ib.ib_utils as utils
import time
import pdb
from common.utils import ManagedProcess
from ib.ib_api import IBApp
from db.helper import date_2_int, int_2_date

IB_SYNC_PROCESS_NAME = 'IB_%d'


def get_sync_symbols_data_helper():
    return {
        'stocks': list(db.get_ib_sync_symbols())
    }


def _get_ib_bson_data(hist_data, t):
    if not hist_data:
        return None
    b = hist_data[2]
    return {
        'type': t,
        'dt': date_2_int('%s %s' % (b.date.split()[0],
                                    b.date.split()[1]), is_short=True),
        'open': b.open,
        'close': b.close,
        'high': b.high,
        'low': b.low,
        'volume': b.volume,
        'average': b.average,
        'pe': 0.0,
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
        print('asdasda', i)
        contract_dt_range = db.query_ib_data_dt_range(contract.symbol, 31)
        if not contract_dt_range:
            query_time = datetime.datetime.now().strftime('%Y%m%d 23:59:59')
        else:
            latest_sync_date = contract_dt_range[1].split()[0]
            query_time = _get_offset_trading_day(
                trading_days, latest_sync_date, sync_days)
        while True:
            print(query_time)
            s1 = time.time()
            hist_data = app.req_historical_data(
                1000 + i, contract, query_time, '%d D' % sync_days, '30 secs')
            s2 = time.time()

            if not hist_data:
                print('hist data not exists')
                break
            print(hist_data[-1])
            bson_list = list(map(lambda x: _get_ib_bson_data(x, 31),
                                 hist_data[:-1]))
            last_date = int_2_date(bson_list[-1]['dt'], is_short=True)
            print(bson_list[0])
            print(s2 - s1, contract.symbol, int_2_date(bson_list[0]['dt']))
            print(len(bson_list))
            query_time = _get_offset_trading_day(
                trading_days, last_date.split()[0], sync_days)
            if query_time > datetime.datetime.now() \
                    .strftime('%Y%m%d 23:59:59'):
                query_time = datetime.datetime.now() \
                    .strftime('%Y%m%d 23:59:59')
                print('wow', query_time)
            elif query_time == datetime.datetime.now() \
                    .strftime('%Y%m%d 23:59:59'):
                print('wwww', query_time)
                break


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

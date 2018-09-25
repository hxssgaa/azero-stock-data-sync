import datetime
import logging
import queue
from bisect import bisect_left

import db.ib_stock_db as db
import ib.ib_utils as utils
import time
import pdb
from common.utils import ManagedProcess
from ib.ib_api import IBApp
from db.helper import date_2_int, int_2_date, int_2_date_for_tick

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


def _get_ib_tick_bson_data(tick_data):
    if not tick_data:
        return None
    return {
        'dt': date_2_int('%s %s' % (tick_data[0].split()[0],
                                    tick_data[0].split()[1]), is_short=True),
        'mask': tick_data[1],
        'size': tick_data[2],
        'price': tick_data[3],
        'exchange': tick_data[4],
        'specialConditions': tick_data[5]
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


def _get_offset_trading_day(trading_days, trading_day, offset_day, make_time='23:59:59'):
    if trading_day not in trading_days:
        raise RuntimeError(
            'Specified trading day: %s not in trading days' % trading_day)
    index = trading_days.index(trading_day)
    if index + offset_day >= len(trading_days):
        return trading_days[-1]
    return '%s %s' % (trading_days[index + offset_day], make_time)


def _get_offset_trading_datetime(trading_days, dt_str, offset_seconds):
    dt_date = dt_str.split()[0]
    if dt_date not in trading_days:
        raise RuntimeError(
            'Specified trading day: %s not in trading days' % dt_date)
    dt_time = dt_str.split()[1]
    if dt_time >= '20:00:00':
        index = trading_days.index(dt_date) + 1
        offset_time = (datetime.datetime.strptime('04:00:00', '%H:%M:%S')
                       + datetime.timedelta(seconds=offset_seconds)).strftime('%H:%M:%S')
        return '%s %s' % (trading_days[index], offset_time)

    dt_time = max('04:00:00', dt_time)
    dt_time = min('20:00:00', dt_time)
    offset_time = (datetime.datetime.strptime(dt_time, '%H:%M:%S') +
                   datetime.timedelta(seconds=offset_seconds)).strftime('%H:%M:%S')
    offset_time = min('20:00:00', offset_time)
    return '%s %s' % (dt_date, offset_time)


def _is_datetime_up_to_date(trading_days, dt_str):
    dt_date = datetime.datetime.now().strftime('%Y%m%d')
    index = bisect_left(trading_days, dt_date)
    return dt_str.split()[0] >= trading_days[index]


def _inner_start_1m_sync_helper(contracts):
    app = IBApp("10.150.0.2", 4001, 50)
    trading_days = utils.get_trading_days('20040123', (datetime.datetime.now()
                                                       + datetime.timedelta(30)).strftime('%Y%m%d'))
    sync_days = 5
    base_req_id = 1000
    tick_base_req_id = 10000
    for i, contract in enumerate(contracts):
        contract_dt_range = db.query_ib_data_dt_range(contract.symbol, 31)
        while True:
            if tick_base_req_id > (1 << 30):
                app = IBApp("10.150.0.2", 4001, 50)
                tick_base_req_id = 10000
            earliest_dt = db.query_ib_earliest_dt(app, tick_base_req_id, contract)
            tick_base_req_id += 1
            if earliest_dt:
                break
            logging.warning('%s query_ib_earliest_dt failed, waitting to retry...' % contract.symbol)
            time.sleep(5)

        if not contract_dt_range:
            earliest_dt = max('20040123 23:59:59', earliest_dt)
            latest_sync_date_time = earliest_dt
            query_time = _get_offset_trading_day(
                trading_days, earliest_dt.split()[0], sync_days - 1)
        else:
            latest_sync_date_time = contract_dt_range[1]
            latest_sync_date = contract_dt_range[1].split()[0]
            query_time = _get_offset_trading_day(
                trading_days, latest_sync_date, sync_days)
        query_time = max('20040123 23:59:59', query_time)
        while True:
            if _is_datetime_up_to_date(trading_days, query_time):
                logging.warning('1M %s up to date' % contract.symbol)
                break

            logging.warning('1M ' + contract.symbol + " " + str((contract.symbol, query_time)))
            s1 = time.time()
            hist_data = app.req_historical_data(base_req_id, contract, query_time, '%d D' % sync_days, '30 secs')
            base_req_id += 1
            s2 = time.time()

            if hist_data[0][1] == 'error' and hist_data[0][2] == 162 and 'no data' in hist_data[0][3]:
                logging.warning('1M %s no data, skipped' % contract.symbol)
                break

            if len(hist_data) == 1:
                logging.warning('1M hist data not exists')
                break

            if hist_data[0][1] == 'error':
                base_req_id += 1
                time.sleep(1)
                logging.warning('1M %s error: %s' % (contract.symbol, str(hist_data[0])))
                continue

            bson_list = list(map(lambda x: _get_ib_bson_data(x, 31),
                                 hist_data[:-1]))
            latest_sync_date_time_int = date_2_int(latest_sync_date_time, is_short=True)
            bson_list = list(filter(lambda x: x['dt'] > latest_sync_date_time_int, bson_list))

            logging.warning('1M %s~%s~%s~%s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                contract.symbol, int_2_date(bson_list[0]['dt'], is_short=True),
                                                int_2_date(bson_list[-1]['dt'], is_short=True)))

            latest_sync_date_time = '%s %s' % (hist_data[-2][2].date.split()[0], hist_data[-2][2].date.split()[1])
            if bson_list:
                db.insert_ib_data(contract.symbol, bson_list)

            last_date = int_2_date(bson_list[-1]['dt'], is_short=True)
            if query_time == datetime.datetime.now().strftime('%Y%m%d 23:59:59'):
                logging.warning('1M %s %s complete' % (contract.symbol, query_time))
                break

            query_time = _get_offset_trading_day(
                trading_days, last_date.split()[0], sync_days)
            if query_time > datetime.datetime.now().strftime('%Y%m%d 23:59:59'):
                query_time = datetime.datetime.now().strftime('%Y%m%d 23:59:59')


def _inner_start_1s_sync_helper(contracts):
    app = IBApp("10.150.0.2", 4001, 60)
    trading_days = utils.get_trading_days('20040123', (datetime.datetime.now()
                                                       + datetime.timedelta(30)).strftime('%Y%m%d'))
    sync_seconds = 1800
    tmp_sync_count = 0
    for i, contract in enumerate(contracts):
        contract_dt_range = db.query_ib_data_dt_range(contract.symbol, 32)
        contract_earliest_time = max('20180601 00:00:00',
                                     db.query_ib_earliest_dt(app, 10000 + i, contract))
        if not contract_dt_range:
            query_time = _get_offset_trading_datetime(
                trading_days, contract_earliest_time, sync_seconds)
            latest_sync_date_time = contract_earliest_time
        else:
            latest_sync_date_time = contract_dt_range[1]
            latest_sync_date_time = (datetime.datetime.strptime(latest_sync_date_time, '%Y%m%d %H:%M:%S')
                                     + datetime.timedelta(seconds=1)).strftime('%Y%m%d %H:%M:%S')
            query_time = _get_offset_trading_datetime(
                trading_days, latest_sync_date_time, sync_seconds)
        base_req_id = 3000
        while True:
            if tmp_sync_count == 60:
                tmp_sync_count = 0
                logging.warning('1S %s pacing violation, pausing...' % contract.symbol)
                time.sleep(600)

            if _is_datetime_up_to_date(trading_days, query_time):
                logging.warning('1S %s update to date' % contract.symbol)
                break

            hist_data = app.req_historical_data(
                base_req_id, contract, query_time, '%d S' % sync_seconds, '1 secs')
            time.sleep(1)
            base_req_id += 1
            if hist_data[0][1] == 'error' and hist_data[0][2] == 162 and 'pacing' in hist_data[0][3]:
                logging.warning('1S %s pacing violation, pausing...' % contract.symbol)
                tmp_sync_count = 0
                time.sleep(600)
                base_req_id += 1
                continue

            if hist_data[0][1] == 'error' and hist_data[0][2] == 162 and 'no data' in hist_data[0][3]:
                query_time = _get_offset_trading_datetime(
                    trading_days, query_time, sync_seconds)
                if _is_datetime_up_to_date(trading_days, query_time):
                    logging.warning('1S %s update to date' % contract.symbol)
                    break
                logging.warning('1S %s no data, try another time' % contract.symbol)
                tmp_sync_count += 1
                time.sleep(1)
                continue

            if hist_data[0][1] == 'error' and hist_data[0][2] == 322 and 'Duplicate ticker' in hist_data[0][3]:
                logging.warning('1S %s Duplicate ticker, try again' % contract.symbol)
                base_req_id += 1
                time.sleep(1)
                continue

            if hist_data[0][1] == 'error':
                logging.warning('1S %s other error:%s, try again' % (contract.symbol, str(hist_data[0])))
                base_req_id += 1
                time.sleep(1)
                continue

            bson_list = list(map(lambda x: _get_ib_bson_data(x, 32),
                                 hist_data[:-1]))
            if not bson_list:
                base_req_id += 1
                time.sleep(1)
                continue

            if bson_list[0]['dt'] < date_2_int(latest_sync_date_time, is_short=True):
                logging.warning('1S %s, %s skipped' % (contract.symbol, query_time))
                query_time = _get_offset_trading_datetime(
                    trading_days, '%s 20:00:00' % query_time.split()[0], sync_seconds)
                continue
            logging.warning('1S %s~%s~%s~%s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                contract.symbol, hist_data[0][2].date, hist_data[-2][2].date))
            db.insert_ib_data(contract.symbol, bson_list)

            latest_sync_date_time = query_time
            query_time = _get_offset_trading_datetime(
                trading_days, query_time, sync_seconds)
            tmp_sync_count += 1


def _inner_start_tick_sync_helper(contracts):
    app = IBApp("10.150.0.2", 4001, 70)
    trading_days = utils.get_trading_days(
        '20040123', (datetime.datetime.now() +
                     datetime.timedelta(30)).strftime('%Y%m%d'))
    base_req_id = 100
    retry_cnt = 0
    for i, contract in enumerate(contracts):
        contract_dt_range = db.query_ib_tick_dt_range(contract.symbol)
        ib_earliest_dt = db.query_ib_earliest_dt(app, 10 + i, contract)
        contract_earliest_time = max('20180601 00:00:00', ib_earliest_dt)
        if not contract_dt_range:
            query_time = contract_earliest_time
        else:
            latest_sync_date_time = contract_dt_range[1]
            query_time = _get_offset_trading_datetime(
                trading_days, latest_sync_date_time, 1)

        last_synced_time = None
        while True:
            if _is_datetime_up_to_date(trading_days, query_time):
                logging.warning('Tick %s update to date.' % contract.symbol)
                break
            try:
                hist_tick_data = app.req_historical_ticks(
                    base_req_id, contract, query_time, '')
                base_req_id += 1
                retry_cnt = 0
            except queue.Empty:
                base_req_id += 1
                if retry_cnt >= 2:
                    retry_cnt = 0
                    logging.warning('%s retry break' % contract.symbol)
                    break
                query_time = _get_offset_trading_datetime(trading_days, query_time, 1)
                logging.warning('Tick %s skipped' % contract.symbol)
                retry_cnt += 1
                continue
            if hist_tick_data[1] == 'error':
                logging.warning('Tick ' + contract.symbol + ' ' + query_time + ' ' + str(hist_tick_data))
                base_req_id += 1
                continue
            if not hist_tick_data[2]:
                query_time = _get_offset_trading_datetime(
                    trading_days, '%s 20:00:00' % query_time.split()[0], 1)
                logging.warning('Tick %s last, skipped.' % contract.symbol)
                continue

            if hist_tick_data[2]:
                hist_tick_data = list(map(lambda x: (int_2_date_for_tick(x.time),
                                                     x.mask,
                                                     x.size,
                                                     x.price,
                                                     x.exchange,
                                                     x.specialConditions), hist_tick_data[2]))

            if hist_tick_data[-1][0] == last_synced_time:
                last_synced_time = hist_tick_data[-1][0]
                continue

            query_time = _get_offset_trading_datetime(
                trading_days, hist_tick_data[-1][0], 1)
            bson_data = list(map(_get_ib_tick_bson_data, hist_tick_data))

            last_synced_time = hist_tick_data[-1][0]
            db.insert_ib_tick_data(contract.symbol, bson_data)
            logging.warning('Tick %s~%s~%s' % (contract.symbol, hist_tick_data[0][0], hist_tick_data[-1][0]))


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


def stop_sync_helper(t):
    t = int(t)
    if not ManagedProcess.is_process_existed(IB_SYNC_PROCESS_NAME % t):
        return {'status': 1}

    ManagedProcess.remove_process(IB_SYNC_PROCESS_NAME % t)
    return {'status': 0}

import datetime
import logging
import queue
from bisect import bisect_left

import db.ib_stock_db as db
import ib.ib_utils as utils
import time
from common.utils import ManagedProcess, IBProgressTracker
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
    tmp_error_cnt = 0
    tracker = IBProgressTracker('1M')
    num_contracts = len(contracts)
    per_progress = 1 / float(num_contracts)
    for i, contract in enumerate(contracts):
        contract_dt_range = db.query_ib_data_dt_range(contract.symbol, 31)
        base_progress = i / float(num_contracts)
        earliest_dt = db.query_ib_earliest_dt(contract, '20040123 23:59:59')

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
        first_query_time_int = date_2_int(query_time, is_short=True)
        end_query_time_int = date_2_int(datetime.datetime.now().strftime('%Y%m%d %H:%M:%S'), is_short=True)
        while True:
            if tmp_error_cnt >= 4:
                app.disconnect()
                time.sleep(2)
                app = IBApp("10.150.0.2", 4001, 50)
                tmp_error_cnt = 0
                base_req_id = 1000
                tick_base_req_id = 10000
                logging.warning('1M %s app has been reset' % contract.symbol)
                tracker.add_track_record('App has been reset', contract.symbol)

            if _is_datetime_up_to_date(trading_days, query_time):
                logging.warning('1M %s up to date' % contract.symbol)
                tracker.add_track_record('Contract up to date', contract.symbol)
                tmp_error_cnt = 0
                break

            logging.warning('1M ' + str((contract.symbol, query_time)))
            s1 = time.time()
            try:
                hist_data = app.req_historical_data(base_req_id, contract, query_time, '%d D' % sync_days, '30 secs')
            except queue.Empty:
                base_req_id += 1
                tmp_error_cnt += 1
                logging.warning('1M ' + contract.symbol + ' ' + query_time +
                                ' req historical data timeout, try again...')
                tracker.add_track_record(query_time + ' req historical data timeout, try again...', contract.symbol)
                continue

            base_req_id += 1
            s2 = time.time()

            if hist_data[0][1] == 'error' and hist_data[0][2] == 162 and 'no data' in hist_data[0][3]:
                logging.warning('1M %s %s no data, skipped' % (contract.symbol, query_time))
                tmp_error_cnt = 0
                tracker.add_track_record('%s no data, skipped' % query_time, contract.symbol)
                query_time = _get_offset_trading_day(trading_days, query_time.split()[0], sync_days)
                continue

            if len(hist_data) == 1:
                logging.warning('1M %s hist data not exists(%s)' % (contract.symbol, query_time))
                tracker.add_track_record('hist data not exists(%s)' % query_time, contract.symbol)
                tmp_error_cnt = 0
                break

            if hist_data[0][1] == 'error':
                base_req_id += 1
                time.sleep(1)
                logging.warning('1M %s %s error: %s' % (contract.symbol, query_time, str(hist_data[0])))
                tracker.add_track_record('%s error: %s' % (query_time, str(hist_data[0])), contract.symbol)
                tmp_error_cnt += 1
                time.sleep(1)
                continue

            bson_list = list(map(lambda x: _get_ib_bson_data(x, 31),
                                 hist_data[:-1]))
            latest_sync_date_time_int = date_2_int(latest_sync_date_time, is_short=True)
            bson_list = list(filter(lambda x: x['dt'] > latest_sync_date_time_int, bson_list))
            # Clear temp error count.
            tmp_error_cnt = 0

            latest_sync_date_time = '%s %s' % (hist_data[-2][2].date.split()[0], hist_data[-2][2].date.split()[1])
            if bson_list:
                db.insert_ib_data(contract.symbol, bson_list)
                progress = base_progress + (bson_list[-1]['dt'] - first_query_time_int) * per_progress / float(
                    end_query_time_int - first_query_time_int)
                logging.warning('1M %s~%s~%s~%s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                    contract.symbol, int_2_date(bson_list[0]['dt'], is_short=True),
                                                    int_2_date(bson_list[-1]['dt'], is_short=True)))
                tracker.add_track_record('SYNC %s~%s->%.2fs' % (int_2_date(bson_list[0]['dt'], is_short=True),
                                                                int_2_date(bson_list[-1]['dt'], is_short=True),
                                                                float(s2 - s1)), contract.symbol)
                tracker.update_track_progress(progress)

            last_date = int_2_date(bson_list[-1]['dt'], is_short=True) if bson_list else query_time
            if query_time == datetime.datetime.now().strftime('%Y%m%d 23:59:59'):
                logging.warning('1M %s %s complete' % (contract.symbol, query_time))
                tracker.add_track_record('%s complete' % query_time, contract.symbol)
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
    tmp_error_cnt = 0
    tracker = IBProgressTracker('1S')
    num_contracts = len(contracts)
    per_progress = 1 / float(num_contracts)
    for i, contract in enumerate(contracts):
        contract_dt_range = db.query_ib_data_dt_range(contract.symbol, 32)
        base_progress = i / float(num_contracts)
        contract_earliest_time = db.query_ib_earliest_dt(contract, '20180601 00:00:00')
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
        first_query_time_int = date_2_int(query_time, is_short=True)
        end_query_time_int = date_2_int(datetime.datetime.now().strftime('%Y%m%d %H:%M:%S'), is_short=True)
        base_req_id = 100
        while True:
            if tmp_error_cnt >= 4:
                app.disconnect()
                time.sleep(2)
                app = IBApp("10.150.0.2", 4001, 60)
                tmp_error_cnt = 0
                base_req_id = 100
                logging.warning('1S %s app has been reset' % contract.symbol)
                tracker.add_track_record('App has been reset', contract.symbol)

            if tmp_sync_count == 60:
                tmp_sync_count = 0
                logging.warning('1S %s pacing violation, pausing...' % contract.symbol)
                tracker.add_track_record('Pacing violation, pausing...', contract.symbol)
                time.sleep(600)

            if _is_datetime_up_to_date(trading_days, query_time):
                logging.warning('1S %s update to date' % contract.symbol)
                tracker.add_track_record('Update to date', contract.symbol)
                break

            try:
                s1 = time.time()
                hist_data = app.req_historical_data(
                    base_req_id, contract, query_time, '%d S' % sync_seconds, '1 secs')
                s2 = time.time()
            except queue.Empty:
                logging.warning('1S %s,%s req historical data timeout, try again...' % (contract.symbol, query_time))
                tracker.add_track_record('%s req historical data timeout, try again...' % query_time, contract.symbol)
                tmp_error_cnt += 1
                base_req_id += 1
                time.sleep(1)
                continue
            time.sleep(1)
            base_req_id += 1
            if hist_data[0][1] == 'error' and hist_data[0][2] == 162 and 'pacing' in hist_data[0][3]:
                logging.warning('1S %s pacing violation, pausing...' % contract.symbol)
                tracker.add_track_record('Pacing violation, pausing...', contract.symbol)
                tmp_sync_count = 0
                time.sleep(600)
                base_req_id += 1
                continue

            if hist_data[0][1] == 'error' and hist_data[0][2] == 162 and 'no data' in hist_data[0][3]:
                query_time = _get_offset_trading_datetime(
                    trading_days, query_time, sync_seconds)
                if _is_datetime_up_to_date(trading_days, query_time):
                    logging.warning('1S %s update to date' % contract.symbol)
                    tracker.add_track_record('Update to date', contract.symbol)
                    break
                logging.warning('1S %s no data, try another time' % contract.symbol)
                tracker.add_track_record('No data found at %s, try another time' % query_time, contract.symbol)
                tmp_sync_count += 1
                time.sleep(1)
                continue

            if hist_data[0][1] == 'error' and hist_data[0][2] == 322 and 'Duplicate ticker' in hist_data[0][3]:
                logging.warning('1S %s Duplicate ticker, try again' % contract.symbol)
                tracker.add_track_record('Duplicate ticker, try again', contract.symbol)
                base_req_id += 1
                time.sleep(1)
                continue

            if hist_data[0][1] == 'error':
                logging.warning('1S %s other error:%s, try again' % (contract.symbol, str(hist_data[0])))
                tracker.add_track_record('Other error at %s: %s, try again' %
                                         (query_time, str(hist_data[0])), contract.symbol)
                base_req_id += 1
                tmp_error_cnt += 1
                time.sleep(1)
                continue

            bson_list = list(map(lambda x: _get_ib_bson_data(x, 32),
                                 hist_data[:-1]))
            if not bson_list:
                base_req_id += 1
                time.sleep(1)
                tmp_error_cnt += 1
                continue

            tmp_error_cnt = 0
            if bson_list[0]['dt'] < date_2_int(latest_sync_date_time, is_short=True):
                logging.warning('1S %s, %s skipped' % (contract.symbol, query_time))
                tracker.add_track_record('%s skipped' % query_time, contract.symbol)
                query_time = _get_offset_trading_datetime(
                    trading_days, '%s 20:00:00' % query_time.split()[0], sync_seconds)
                continue

            progress = base_progress + (bson_list[-1]['dt'] - first_query_time_int) * per_progress / float(
                end_query_time_int - first_query_time_int)
            tracker.update_track_progress(progress)

            logging.warning('1S %s~%s~%s~%s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                contract.symbol, hist_data[0][2].date, hist_data[-2][2].date))
            tracker.add_track_record('SYNC %s~%s-->%.2f' % (hist_data[0][2].date, hist_data[-2][2].date,
                                                            float(s2 - s1)), contract.symbol)
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
    tmp_error_cnt = 0
    tracker = IBProgressTracker('TICK')
    num_contracts = len(contracts)
    per_progress = 1 / float(num_contracts)
    for i, contract in enumerate(contracts):
        contract_dt_range = db.query_ib_tick_dt_range(contract.symbol)
        base_progress = i / float(num_contracts)
        contract_earliest_time = db.query_ib_earliest_dt(contract, '20180601 00:00:00')
        if not contract_dt_range:
            query_time = contract_earliest_time
        else:
            latest_sync_date_time = contract_dt_range[1]
            query_time = _get_offset_trading_datetime(
                trading_days, latest_sync_date_time, 1)

        first_query_time_int = date_2_int(query_time, is_short=True)
        end_query_time_int = date_2_int(datetime.datetime.now().strftime('%Y%m%d %H:%M:%S'), is_short=True)
        last_synced_time = None
        while True:
            if tmp_error_cnt >= 4:
                app.disconnect()
                time.sleep(2)
                app = IBApp("10.150.0.2", 4001, 70)
                tmp_error_cnt = 0
                base_req_id = 1000
                logging.warning('Tick %s app has been reset' % contract.symbol)
                tracker.add_track_record('App has been reset', contract.symbol)

            if _is_datetime_up_to_date(trading_days, query_time):
                logging.warning('Tick %s update to date.' % contract.symbol)
                tracker.add_track_record('Update to date', contract.symbol)
                break
            try:
                s1 = time.time()
                hist_tick_data = app.req_historical_ticks(
                    base_req_id, contract, query_time, '')
                s2 = time.time()
                base_req_id += 1
            except Exception:
                app.disconnect()
                time.sleep(2)
                app = IBApp("10.150.0.2", 4001, 70)
                tmp_error_cnt = 0
                base_req_id = 1000
                logging.warning('Tick %s app has been reset' % contract.symbol)
                tracker.add_track_record('App has been reset', contract.symbol)
                continue
            if hist_tick_data[1] == 'error':
                logging.warning('Tick ' + contract.symbol + ' ' + query_time + ' ' + str(hist_tick_data))
                tracker.add_track_record('%s %s' % (query_time, str(hist_tick_data)), contract.symbol)
                base_req_id += 1
                tmp_error_cnt += 1
                time.sleep(1)
                continue
            if not hist_tick_data[2]:
                query_time = _get_offset_trading_datetime(
                    trading_days, '%s 20:00:00' % query_time.split()[0], 1)
                logging.warning('Tick %s last, skipped.' % contract.symbol)
                tracker.add_track_record('%s last, skipped' % query_time, contract.symbol)
                continue

            if hist_tick_data[2]:
                hist_tick_data = list(map(lambda x: (int_2_date_for_tick(x.time),
                                                     x.mask,
                                                     x.size,
                                                     x.price,
                                                     x.exchange,
                                                     x.specialConditions), hist_tick_data[2]))

            tmp_error_cnt = 0
            if hist_tick_data[-1][0] == last_synced_time:
                last_synced_time = hist_tick_data[-1][0]
                continue

            query_time = _get_offset_trading_datetime(
                trading_days, hist_tick_data[-1][0], 1)
            bson_data = list(map(_get_ib_tick_bson_data, hist_tick_data))
            progress = base_progress + (bson_data[-1]['dt'] - first_query_time_int) * per_progress / float(
                end_query_time_int - first_query_time_int)
            tracker.update_track_progress(progress)

            last_synced_time = hist_tick_data[-1][0]
            db.insert_ib_tick_data(contract.symbol, bson_data)
            logging.warning('Tick %s~%d~%s~%s~%s' % (contract.symbol, base_req_id, query_time,
                                                     hist_tick_data[0][0], hist_tick_data[-1][0]))
            tracker.add_track_record('SYNC %s~%s-->%.2f' % (hist_tick_data[0][0], hist_tick_data[-1][0],
                                                            float(s2 - s1)), contract.symbol)
            time.sleep(1)


def _inner_start_realtime_sync_helper(contracts):
    if not contracts:
        return
    tracker = IBProgressTracker('REAL')
    contracts.append(utils.get_usd_contract())
    q = queue.Queue()
    app = IBApp("10.150.0.2", 4001, 80)

    def _handler(data):
        q.put(data)

    req_id_symbol_map = {}
    for i, contract in enumerate(contracts):
        app.req_market_data(1000 + i, contract, _handler, generic_tick_list='236')
        req_id_symbol_map[1000 + i] = contract.symbol

    db.insert_ib_rt_data(q, req_id_symbol_map, tracker)


def _inner_start_sync_helper(t, contracts):
    return {
        0: _inner_start_1m_sync_helper,
        1: _inner_start_1s_sync_helper,
        2: _inner_start_tick_sync_helper,
        3: _inner_start_realtime_sync_helper
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


def get_sync_progress_helper():
    res = {}
    for i, t in enumerate(['1M', '1S', 'TICK', 'REAL']):
        tracker = IBProgressTracker(t)
        sync_logs = tracker.get_track_records()
        sync_logs = list(
            map(lambda x: {'datetime': ''.join(' '.join(x.split(maxsplit=2)[:-1]).split('][')[1][:-1]),
                           'log': '[%s] %s' % (''.join(''.join(x.split(maxsplit=2)[:-1]).split('][')[0][1:]),
                                               x.split(maxsplit=2)[2])}, sync_logs))
        synced_symbols = list(map(lambda x: {'symbol': x}, tracker.get_synced_symbols()))
        sync_logs = sorted(sync_logs, key=lambda x: x['datetime'])
        hist_data_sync_track = {
            'histDataSyncTrack': {
                'syncLogs': sync_logs,
                'histDataSyncProgress': tracker.get_track_progress(),
                'syncedSymbols': synced_symbols
            }
        }
        res[t] = hist_data_sync_track
    return {'data': res}


def get_sync_status_helper(t):
    return {'status': int(ManagedProcess.is_process_existed(IB_SYNC_PROCESS_NAME % t))}

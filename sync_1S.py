import argparse
import datetime
import queue
import time
from bisect import bisect_left

import ib.ib_utils as utils

from ib.ib_api import IBApp
from db.ib_lite_db import LiteDB, SyncTypesEnum
from db.helper import date_2_int, int_2_date

DB_PATH = r'C:\Users\paperspace\Documents\tmp_stock_data'


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
    if dt_str.split()[0] > dt_str:
        return True
    index = bisect_left(trading_days, dt_date)
    return dt_str.split()[0] >= trading_days[index]


def sync_1S():
    db = LiteDB(DB_PATH)
    symbols = db.get_sync_symbols(SyncTypesEnum.TYPE_1S)
    contracts = [utils.make_contract(symbol, 'SMART') for symbol in symbols]

    app = IBApp("localhost", 4001, 60)
    trading_days = utils.get_trading_days('20040123', (datetime.datetime.now()
                                                       + datetime.timedelta(30)).strftime('%Y%m%d'))
    sync_seconds = 1800
    tmp_sync_count = 0
    tmp_error_cnt = 0
    num_contracts = len(contracts)
    per_progress = 1 / float(num_contracts)
    base_td = 1
    for i, contract in enumerate(contracts):
        db.download_db(contract.symbol)
        contract_dt_range = db.query_ib_data_dt_range(contract.symbol, 32)
        base_progress = i / float(num_contracts)
        contract_earliest_time = db.query_ib_earliest_dt(base_td, app, contract, '20180702 00:00:00')
        base_td += 1
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
        previous_conn = None
        while True:
            if tmp_error_cnt >= 4:
                app.disconnect()
                time.sleep(2)
                app = IBApp("localhost", 4001, 60)
                tmp_error_cnt = 0
                base_req_id = 100
                print('1S %s app has been reset' % contract.symbol)

            if tmp_sync_count == 60:
                tmp_sync_count = 0
                print('1S %s pacing violation, pausing...' % contract.symbol)
                time.sleep(600)

            if _is_datetime_up_to_date(trading_days, query_time):
                print('1S %s update to date' % contract.symbol)
                break

            try:
                s1 = time.time()
                hist_data = app.req_historical_data(
                    base_req_id, contract, query_time, '%d S' % sync_seconds, '1 secs')
                s2 = time.time()
            except queue.Empty:
                print(
                    '1S %s,%s req historical data timeout, try again...' % (contract.symbol, query_time))
                tmp_error_cnt += 1
                base_req_id += 1
                time.sleep(1)
                continue
            time.sleep(1)
            base_req_id += 1
            if hist_data[0][1] == 'error' and hist_data[0][2] == 162 and 'pacing' in hist_data[0][3]:
                print('1S %s pacing violation, pausing...' % contract.symbol)
                tmp_sync_count = 0
                time.sleep(600)
                base_req_id += 1
                continue

            if hist_data[0][1] == 'error' and hist_data[0][2] == 162 and 'no data' in hist_data[0][3]:
                query_time = _get_offset_trading_datetime(
                    trading_days, query_time, sync_seconds)
                if _is_datetime_up_to_date(trading_days, query_time):
                    print('1S %s update to date' % contract.symbol)
                    break
                print('1S %s no data, try another time' % contract.symbol)
                tmp_sync_count += 1
                time.sleep(1)
                continue

            if hist_data[0][1] == 'error' and hist_data[0][2] == 322 and 'Duplicate ticker' in hist_data[0][3]:
                print('1S %s Duplicate ticker, try again' % contract.symbol)
                base_req_id += 1
                time.sleep(1)
                continue

            if hist_data[0][1] == 'error' and hist_data[0][2] == 162 and 'Starting time must occur' in hist_data[0][
                3]:
                print('1S %s Starting error:%s, try again' % (contract.symbol, str(hist_data[0])))
                query_time = _get_offset_trading_day(trading_days, query_time.split()[0], 1)
                base_req_id += 1
                time.sleep(1)
                continue

            if hist_data[0][1] == 'error':
                print('1S %s other error:%s, try again' % (contract.symbol, str(hist_data[0])))
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
                print('1S %s, %s skipped' % (contract.symbol, query_time))
                query_time = _get_offset_trading_datetime(
                    trading_days, '%s 20:00:00' % query_time.split()[0], sync_seconds)
                continue

            progress = base_progress + (bson_list[-1]['dt'] - first_query_time_int) * per_progress / float(
                end_query_time_int - first_query_time_int)

            print('1S %s~%s~%s~%s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                contract.symbol, hist_data[0][2].date, hist_data[-2][2].date))
            conn = db.insert_ib_data(previous_conn, contract.symbol, bson_list)
            previous_conn = conn

            latest_sync_date_time = query_time
            query_time = _get_offset_trading_datetime(
                trading_days, query_time, sync_seconds)
            tmp_sync_count += 1
        if previous_conn:
            previous_conn.close()


if __name__ == '__main__':
    sync_1S()

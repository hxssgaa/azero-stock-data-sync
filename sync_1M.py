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


def _is_datetime_up_to_date(trading_days, dt_str):
    dt_date = datetime.datetime.now().strftime('%Y%m%d')
    if dt_str.split()[0] > dt_str:
        return True
    index = bisect_left(trading_days, dt_date)
    return dt_str.split()[0] >= trading_days[index]


def sync_1M():
    db = LiteDB(DB_PATH)
    symbols = db.get_sync_symbols(SyncTypesEnum.TYPE_1M)
    contracts = [utils.make_contract(symbol, 'SMART') for symbol in symbols]
    app = IBApp("localhost", 4001, 50)
    trading_days = utils.get_trading_days('20040123', (datetime.datetime.now()
                                                       + datetime.timedelta(30)).strftime('%Y%m%d'))
    sync_days = 5
    base_req_id = 1000
    tmp_error_cnt = 0
    num_contracts = len(contracts)
    for i, contract in enumerate(contracts[:5]):
        db.download_db(contract.symbol)
        contract_dt_range = db.query_ib_data_dt_range(contract.symbol, 31)
        earliest_dt = db.query_ib_earliest_dt(app, contract, '20040123 23:59:59')
        print(contract_dt_range)

        if not contract_dt_range:
            earliest_dt = max('20040123 23:59:59', earliest_dt)
            latest_sync_date_time = earliest_dt
            query_time = _get_offset_trading_day(
                trading_days, earliest_dt.split()[0], sync_days - 1)
        else:
            latest_sync_date_time = contract_dt_range[1]
            latest_sync_date = contract_dt_range[1].split()[0]
            query_time = _get_offset_trading_day(
                trading_days, latest_sync_date, sync_days - 1)
        query_time = max('20040123 23:59:59', query_time)
        first_query_time_int = date_2_int(query_time, is_short=True)
        end_query_time_int = date_2_int(datetime.datetime.now().strftime('%Y%m%d %H:%M:%S'), is_short=True)
        previous_conn = None
        while True:
            if tmp_error_cnt >= 4:
                app.disconnect()
                time.sleep(2)
                app = IBApp("10.150.0.2", 4001, 50)
                tmp_error_cnt = 0
                base_req_id = 1000
                print('1M %s app has been reset' % contract.symbol)

            if _is_datetime_up_to_date(trading_days, query_time):
                print('1M %s up to date' % contract.symbol)
                tmp_error_cnt = 0
                break

            print('1M ' + str((contract.symbol, query_time)))
            s1 = time.time()
            try:
                hist_data = app.req_historical_data(base_req_id, contract, query_time, '%d D' % sync_days,
                                                    '30 secs')
            except queue.Empty:
                base_req_id += 1
                tmp_error_cnt += 1
                print('1M ' + contract.symbol + ' ' + query_time +
                      ' req historical data timeout, try again...')
                continue

            base_req_id += 1
            s2 = time.time()

            if hist_data[0][1] == 'error' and hist_data[0][2] == 162 and 'no data' in hist_data[0][3]:
                print().warning('1M %s %s no data, skipped' % (contract.symbol, query_time))
                tmp_error_cnt = 0
                query_time = _get_offset_trading_day(trading_days, query_time.split()[0], sync_days)
                continue

            if hist_data[0][1] == 'error':
                base_req_id += 1
                print('1M %s %s error: %s' % (contract.symbol, query_time, str(hist_data[0])))
                tmp_error_cnt += 1
                time.sleep(1)
                continue

            if len(hist_data) == 1:
                base_req_id += 1
                print('1M %s hist data not exists(%s)' % (contract.symbol, query_time))
                query_time = _get_offset_trading_day(trading_days, query_time.split()[0], 200)
                tmp_error_cnt = 0
                continue

            bson_list = list(map(lambda x: _get_ib_bson_data(x, 31),
                                 hist_data[:-1]))
            latest_sync_date_time_int = date_2_int(latest_sync_date_time, is_short=True)
            bson_list = list(filter(lambda x: x['dt'] > latest_sync_date_time_int, bson_list))
            # Clear temp error count.
            tmp_error_cnt = 0

            latest_sync_date_time = '%s %s' % (hist_data[-2][2].date.split()[0], hist_data[-2][2].date.split()[1])
            if bson_list:
                conn = db.insert_ib_data(previous_conn, contract.symbol, bson_list)
                previous_conn = conn
                # progress = base_progress + (bson_list[-1]['dt'] - first_query_time_int) * per_progress / float(
                #     end_query_time_int - first_query_time_int)
                print('SYNC 1M %s~%s~%s~%s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                    contract.symbol, int_2_date(bson_list[0]['dt'], is_short=True),
                                                    int_2_date(bson_list[-1]['dt'], is_short=True)))
            last_date = int_2_date(bson_list[-1]['dt'], is_short=True) if bson_list else query_time
            if query_time == datetime.datetime.now().strftime('%Y%m%d 23:59:59'):
                print('1M %s %s complete' % (contract.symbol, query_time))
                break

            query_time = _get_offset_trading_day(
                trading_days, last_date.split()[0], sync_days)
            if query_time > datetime.datetime.now().strftime('%Y%m%d 23:59:59'):
                query_time = datetime.datetime.now().strftime('%Y%m%d 23:59:59')
        if previous_conn:
            previous_conn.close()
    print(symbols)


if __name__ == '__main__':
    sync_1M()

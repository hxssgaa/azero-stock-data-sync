import pandas as pd
import multiprocessing
import json
import datetime

from db import _db
from flask import Response


def parse_resp(data, success=True):
    return Response(json.dumps({
        'success': success,
        'data': data
    }), mimetype='application/json')


class StockUtils(object):
    _cache_symbols = set()
    _cached_infos = set()

    @staticmethod
    def get_stock_symbols():
        if not StockUtils._cache_symbols:
            csv_data = pd.read_csv('stock_code.csv')
            StockUtils._cache_symbols = list(map(lambda x: x[1], csv_data.values))
        return StockUtils._cache_symbols

    @staticmethod
    def get_stock_infos():
        if not StockUtils._cached_infos:
            csv_data = pd.read_csv('stock_code.csv')
            StockUtils._cached_infos = list(map(lambda x: {
                'symbol': x[1],
                'title': x[2],
                'date': x[-2]
            }, csv_data.values))
        return StockUtils._cached_infos

    @staticmethod
    def clear_cache():
        StockUtils._cache_symbols = set()


class ManagedProcess(object):
    _process_map = {}

    @staticmethod
    def create_process(name, handler, args=(), kwargs={}):
        if name not in ManagedProcess._process_map:
            ManagedProcess._process_map[name] = \
                multiprocessing.Process(target=handler, name=name, args=args, kwargs=kwargs)
        ManagedProcess._process_map[name].daemon = True
        ManagedProcess._process_map[name].start()
        return ManagedProcess._process_map[name]

    @staticmethod
    def remove_process(name):
        if name not in ManagedProcess._process_map:
            raise RuntimeError('%s not in the process map' % name)
        ManagedProcess._process_map[name].terminate()
        SyncProcessHelper.clear()
        del ManagedProcess._process_map[name]

    @staticmethod
    def is_process_existed(name):
        return name in ManagedProcess._process_map


class DbCache(object):
    def __init__(self, name):
        self.name = 'cache_%s' % name
        self.collection = _db[self.name]

    def put(self, k, v):
        exists = self.collection.count({
            k: {'$exists': True}
        })
        if exists == 0:
            self.collection.insert_one({
                k: v
            })
        else:
            self.collection.update_one({
                k: {'$exists': True}
            }, {
                '$set': {
                    k: v
                }
            })

    def get(self, k, d=None):
        exists = self.collection.count({
            k: {'$exists': True}
        })
        if exists == 0:
            return d
        return self.collection.find({
            k: {'$exists': True}
        }).next().get(k, d)

    def clear(self):
        self.collection.delete_many({})


class SyncProcessHelper(object):
    MAX_NUM_SHOW_RECORD = 25
    _cache = DbCache('azero')

    @staticmethod
    def add_sync_record(record):
        if 'symbol' in record:
            data = set(SyncProcessHelper._cache.get('synced_symbols', list()))
            data.add(record['symbol'])
            SyncProcessHelper._cache.put('synced_symbols', list(data))
        records = SyncProcessHelper._cache.get('records', list())
        records.append(record)
        if len(records) > 25:
            del records[0]
        SyncProcessHelper._cache.put('records', records)

    @staticmethod
    def get_synced_symbols_count():
        return len(SyncProcessHelper._cache.get('synced_symbols', list()))

    @staticmethod
    def get_sync_records():
        return SyncProcessHelper._cache.get('records', list())

    @staticmethod
    def clear():
        SyncProcessHelper._cache.clear()

    @staticmethod
    def update_sync_progress(progress):
        if progress < 0:
            progress = 0
        last_progress = float(SyncProcessHelper._cache.get('progress', 0))
        SyncProcessHelper._cache.put('progress', progress)
        last_update_time = int(SyncProcessHelper._cache.get('progress_update_time', 0))
        now_time = int(datetime.datetime.now().timestamp())
        SyncProcessHelper._cache.put('progress_update_time', now_time)
        if last_update_time > 0 and last_progress > 0:
            SyncProcessHelper._cache.put('progress_eta', (1 - progress) * (now_time - last_update_time)
                                         / (progress - last_progress))

    @staticmethod
    def get_sync_progress():
        return float(SyncProcessHelper._cache.get('progress', 0))

    @staticmethod
    def get_sync_progress_eta():
        return float(SyncProcessHelper._cache.get('progress_eta', 0))

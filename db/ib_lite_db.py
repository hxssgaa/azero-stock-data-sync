import os
import sqlite3

from enum import Enum

from ib import drive, stock_index


class SyncTypesEnum(Enum):
    TYPE_1M = "1M",
    TYPE_1S = "1S"


class LiteDB(object):

    def __init__(self, db_path) -> None:
        super().__init__()
        self.db_path = db_path

    def _get_path_for_symbol(self, symbol):
        return os.path.join(self.db_path, '%s.db' % symbol)

    def get_sync_symbols(self, option: SyncTypesEnum):
        with open('sync_symbols_%s.txt' % option.value) as f:
            symbols = f.readlines()
        return list(sorted(set(['US.' + e.strip() for e in symbols])))

    def download_db(self, symbol: str):
        if not symbol.startswith('US.'):
            symbol = 'US.%s' % symbol
        full_path = self._get_path_for_symbol(symbol)
        if symbol not in stock_index:
            return os.path.join(self.db_path, '%s.db' % symbol)
        stock_index[symbol].GetContentFile(full_path)
        return full_path

    def query_ib_data_dt_range(self, symbol: str, t: int):
        if not symbol.startswith('US.'):
            symbol = 'US.%s' % symbol
        conn = sqlite3.connect(self._get_path_for_symbol(symbol))
        count = [e for e in conn.execute('select count(*) from stocks where type=%d' % t)]
        if count[0] == 0:
            return None
        return [e for e in conn.execute('select dt from stocks where type=%d order by dt asc' % t)][0], \
               [e for e in conn.execute('select dt from stocks where type=%d order by dt desc' % t)][0]

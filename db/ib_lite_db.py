import os
import sqlite3

from enum import Enum

from db.helper import int_2_date
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
        if os.path.exists(full_path):
            return full_path
        symbol_key = '%s.db' % symbol
        if symbol_key not in stock_index:
            conn = sqlite3.connect(full_path)
            conn.execute('''CREATE TABLE stocks 
                            (dt text, open real, close real, high real, low real, volume real, 
                            pe real, average real, type int)''')
            conn.execute('''CREATE INDEX stocks_dt_idx ON stocks(dt)''')
            conn.commit()
            conn.close()
            print('stock %s has been created' % symbol)
            return full_path
        stock_index[symbol_key].GetContentFile(full_path)
        return full_path

    def query_ib_data_dt_range(self, symbol: str, t: int):
        if not symbol.startswith('US.'):
            symbol = 'US.%s' % symbol
        conn = sqlite3.connect(self._get_path_for_symbol(symbol))
        count = [e for e in conn.execute('select count(*) from stocks where type=%d' % t)]
        if count[0][0] == 0:
            return None
        return [e for e in conn.execute('select dt from stocks where type=%d order by dt asc' % t)][0][0] \
                   .replace('-', ''), \
               [e for e in conn.execute('select dt from stocks where type=%d order by dt desc' % t)][0][0] \
                   .replace('-', '')

    def query_ib_earliest_dt(self, app, symbol, min_date):
        time_s = app.req_head_time_stamp(1, symbol)
        return max('%s %s' % (time_s[0][1].split()[0], time_s[0][1].split()[1]), min_date)

    def _update_rows_datetime(self, rows):
        for row in rows:
            row['dt'] = int_2_date(row['dt'])

    def insert_ib_data(self, conn, symbol, rows):
        if not symbol.startswith('US.'):
            symbol = 'US.%s' % symbol

        full_path = self._get_path_for_symbol(symbol)
        if conn is None:
            conn = sqlite3.connect(full_path)
        sq_rows = [(e['dt'], e['open'], e['close'], e['high'], e['low'],
                            e['volume'], e['pe'], e.get('average', -1), e['type']) for e in rows]
        self._update_rows_datetime(sq_rows)
        conn.executemany('''
                        INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', sq_rows)
        conn.commit()
        return conn
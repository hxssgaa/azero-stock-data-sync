import argparse
import datetime

import ib.ib_utils as utils

from ib.ib_api import IBApp
from db.ib_lite_db import LiteDB, SyncTypesEnum


DB_PATH = r'C:\Users\paperspace\Documents\tmp_stock_data'


def sync_1M():
    db = LiteDB(DB_PATH)
    symbols = db.get_sync_symbols(SyncTypesEnum.TYPE_1M)
    contracts = [utils.make_contract(
        symbol['symbol'], 'SMART') for symbol in symbols]
    app = IBApp("localhost", 4001, 50)
    trading_days = utils.get_trading_days('20040123', (datetime.datetime.now()
                                                       + datetime.timedelta(30)).strftime('%Y%m%d'))
    base_req_id = 1000
    num_contracts = len(contracts)
    for i, contract in enumerate(contracts[:5]):
        db.download_db(contract.symbol)
        contract_dt_range = db.query_ib_data_dt_range(contract.symbol, 31)
        print(contract_dt_range)
    print(symbols)


if __name__ == '__main__':
    sync_1M()

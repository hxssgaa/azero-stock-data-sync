from db.helper import int_2_date
from pymongo import MongoClient, DESCENDING
from utils import get_config
#
#
def peek_tick_for_symbol(db, symbol):
    # db['%s-tick' % symbol].delete_many({})
    tick_data = list(db['%s' % symbol].find({'type': 31}).sort([('dt', DESCENDING)]).limit(1000))
    if tick_data:
        print(tick_data[0])
        for e in tick_data:
            print(int_2_date(e['dt']))


if __name__ == '__main__':
    cfg = get_config('db')
    db = MongoClient(cfg['DbHost'], int(cfg['DbPort']))['azero-stock']
    peek_tick_for_symbol(db, 'US.BGNE')

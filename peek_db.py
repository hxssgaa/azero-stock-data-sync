from db.helper import int_2_date
from pymongo import MongoClient, DESCENDING, ASCENDING
from utils import get_config


#
#
def peek_tick_for_symbol(db, symbol):
    print(db.command("dbstats"))
    # db['%s-tick' % symbol].delete_many({})
    # print(db['US.CSCO-tick'].find().sort([('dt', DESCENDING)]).limit(10000))
    # print(db['%s-tick' % symbol].count())
    # tick_data = list(db['%s-tick' % symbol].find().sort([('dt', DESCENDING)]).limit(5000).skip(1000000))
    # if tick_data:
    #     print(tick_data[0])
    #     for e in tick_data:
    #         print(int_2_date(e['dt']))


if __name__ == '__main__':
    cfg = get_config('db')
    db = MongoClient(cfg['DbHost'], int(cfg['DbPort']))['azero-stock']
    peek_tick_for_symbol(db, 'US.CSCO')

import datetime
import pandas_market_calendars as mcal
from ibapi.contract import Contract

nyse = mcal.get_calendar('NASDAQ')


def queue_consumer(q, handler):
    while True:
        data = q.get()
        handler(data)


def make_contract(symbol, exchange):
    if symbol.startswith('US.'):
        symbol = ''.join(symbol[3:])
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.currency = "USD"
    contract.exchange = exchange
    contract.primaryExchange = "ISLAND"
    return contract


def get_usd_contract():
    contract = Contract()
    contract.symbol = 'USD'
    contract.secType = "CASH"
    contract.currency = "CNH"
    contract.exchange = 'IDEALPRO'
    return contract


def format_bar_date(bar_date):
    return '%s %s' % (bar_date.split()[0], bar_date.split()[1])


def get_trading_days(start_date, end_date):
    if not start_date or not end_date:
        return []
    start_date = datetime.datetime.strptime(
        start_date, '%Y%m%d').strftime('%Y-%m-%d')
    end_date = datetime.datetime.strptime(
        end_date, '%Y%m%d').strftime('%Y-%m-%d')
    result = nyse.schedule(start_date, end_date)
    return list(map(lambda x: x.strftime("%Y%m%d"), result.values[:, 0]))

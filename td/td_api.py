import requests as r
import json
from td.td_utils import *


class TDBaseApi(object):
    def __init__(self, api_key):
        self.api_key = api_key

    def make_request(self, url, params=None):
        res = r.get(url, params=params).content.decode('utf-8')
        try:
            return json.loads(res)
        except Exception:
            return res


class TDQuoteApi(TDBaseApi):
    GET_QUOTE_URL = 'https://api.tdameritrade.com/v1/marketdata/%s/quotes'
    GET_QUOTES_URL = 'https://api.tdameritrade.com/v1/marketdata/quotes'
    GET_HISTORY_QUOTE_URL = 'https://api.tdameritrade.com/v1/marketdata/%s/pricehistory'

    def get_delayed_quote(self, symbol):
        '''
        Get delayed quote with given symbol, case sensitive, eg: HUYA.
        '''

        if not isinstance(symbol, str):
            return None
        res = self.make_request(self.GET_QUOTE_URL % symbol, params={
            'apikey': self.api_key
        })
        return format_delayed_quote(res)

    def get_delayed_quotes(self, symbols):
        '''
        Get delayed quotes with given symbols, case sensitive, symbols are seperated by comma, eg: HUYA,IQ
        '''

        if not isinstance(symbols, str):
            return None
        return self.make_request(self.GET_QUOTES_URL, params={
            'apikey': self.api_key,
            'symbol': symbols
        })

    def get_history_quotes(self, symbol, period_type=None, period=None, frequency_type=None, frequency=None,
                           start_date=None, end_date=None, need_extended_hours_data=True):
        '''
        Get history quotes from given symbol and query params, case sensitive.
        
        :param period_type The type of period to show. Valid values are day, month, year, or ytd (year to date). Default is day.
        :param period The number of periods to show. Valid values are for day: 1, 2, 3, 4, 5, 10*, for month: 1*, 2, 3, 6 and for year: 1*, 2, 3, 5, 10, 15, 20
        :param frequency_type The type of frequency with which a new candle is formed. Valid values are minute, daily, weekly, monthly.
        :param frequency The number of the frequencyType to be included in each candle. For minute, valid values including 1, 5, 10, 15, 30.
        :param start_date Start date as milliseconds since epoch. If startDate and endDate are provided, period should not be provided.
        :param end_date End date as milliseconds since epoch. If startDate and endDate are provided, period should not be provided. Default is previous trading day.
        :param need_extended_hours_data true to return extended hours data, false for regular market hours only. Default is true
        '''
        if not isinstance(symbol, str):
            return None
        if isinstance(start_date, str):
            start_date = int(datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
                             .replace(tzinfo=timezone('America/New_York')).timestamp() * 1000)
        if isinstance(end_date, str):
            end_date = int(datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
                           .replace(tzinfo=timezone('America/New_York')).timestamp() * 1000)

        res = self.make_request(self.GET_HISTORY_QUOTE_URL % symbol, params={
            'apikey': self.api_key,
            'periodType': period_type,
            'period': period,
            'frequencyType': frequency_type,
            'frequency': frequency,
            'startDate': start_date,
            'endDate': end_date,
            'needExtendedHoursData': need_extended_hours_data
        })
        return format_history_quotes_result(res)


class TDTimeApi(TDBaseApi):
    GET_HOUR_URL = 'https://api.tdameritrade.com/v1/marketdata/%s/hours'

    def get_hour_for_single_market(self, market, date):
        """
        Get valid market hour for a single market.
        
        :param market query market. Valid values are EQUITY, OPTION, FUTURE
        :param date "The date for which market hours information is requested. Valid ISO-8601 formats are : yyyy-MM-dd and yyyy-MM-dd'T'HH:mm:ssz."
        """
        if not isinstance(market, str) or not isinstance(date, str):
            return None
        return self.make_request(self.GET_HOUR_URL % market, params={
            'apikey': self.api_key,
            'date': date
        })

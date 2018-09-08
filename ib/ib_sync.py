import os
from flask import Blueprint, Response
from utils import *
from common.utils import parse_resp
from ib.ib_sync_helper import *

ib_sync_app = Blueprint('ib_sync_app', __name__)


@ib_sync_app.route("/ib/get_sync_symbols")
@gzipped
def get_sync_symbols():
    """
    get symbols which has been or need syncing.

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "stocks": [
                "symbol": "HUYA"
            ] // 当前已同步或者需要同步的股票数据
        }
    }
    """
    if not os.path.exists('stock_sync_codes.txt'):
        return parse_resp({'message': 'Stock data not exists, please configure it first.'})

    with open('stock_sync_codes.txt') as f:
        symbols = list(map(lambda x: x.strip(), f.readlines()))

    return parse_resp(get_sync_symbols_data_helper(symbols))


@ib_sync_app.route("/ib/config_sync_symbols", methods=['POST'])
@gzipped
def config_sync_symbols():
    """
    Config symbols which has been or need syncing.
    :request:
    {
        "stocks": [
            "symbol": "HUYA"
        ] // 传入股票symbols数据
    }

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "stocks": [
                "symbol": "HUYA"
            ] // 当前已同步或者需要同步的股票数据
        }
    }
    """

    data = request.get_json()
    stocks = data.get('stocks')
    print(stocks)
    return Response('ok')


@ib_sync_app.route("/ib/get_sync_metadata")
@gzipped
def get_sync_metadata():
    """
    get stock sync metadata which could configure the query date, start date, duration or bar size

    :return:
    {
        "success": true  // 当前接口是否成功
        "errorMessage": "跨度时间错误"  // 若success为false，为具体的错误信息
        "data": {
            "historicalDataMeta": {
                "queryTime": "20180606 00:00:00"  // 查询时间或者最终时间
                "startDate": "20140606 00:00:00"  // 开始时间，同步股票数据不会超过开始时间
                "duration": "2 M"  // 一次请求同步的跨度时间，可选的有D, M, Y等
                "barSize": "1 min"  // 请求的精度，可选的有 min, sec, hour, day等
            }  // 历史股票元数据配置信息
        }
    }
    """
    return Response('hello, world')


@ib_sync_app.route("/ib/config_sync_metadata", methods=['POST'])
@gzipped
def config_sync_metadata():
    """
        Config stock sync metadata
        :request:
        {
            "historicalDataMeta": {
                "queryTime": "20180606 00:00:00"  // 查询时间或者最终时间
                "startDate": "20140606 00:00:00"  // 开始时间，同步股票数据不会超过开始时间
                "duration": "2 M"  // 一次请求同步的跨度时间，可选的有D, M, Y等
                "barSize": "1 min"  // 请求的精度，可选的有 min, sec, hour, day等
            }  // 历史股票元数据配置信息
        }

        :return:
        {
            "success": true  // 当前接口是否成功
            "errorMessage": "跨度时间错误"  // 若success为false，为具体的错误信息
            "data": {
                "historicalDataMeta": {
                    "queryTime": "20180606 00:00:00"  // 查询时间或者最终时间
                    "startDate": "20140606 00:00:00"  // 开始时间，同步股票数据不会超过开始时间
                    "duration": "2 M"  // 一次请求同步的跨度时间，可选的有D, M, Y等
                    "barSize": "1 min"  // 请求的精度，可选的有 min, sec, hour, day等
                }  // 历史股票元数据配置信息
            }
        }
        """

    data = request.get_json()
    historical_data_meta = data.get('historicalDataMeta')
    print(historical_data_meta)
    return Response('hello, world')


@ib_sync_app.route("/ib/start_sync")
@gzipped
def start_sync():
    """
    Start sync stock data.

    :request:
    restart=0  // 是否重启同步流程, 0为不重启, 1为重启

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "startSyncStatus": 0|1 //0:同步进程未开启，正在开启，1:表示同步进程已经开启，
        }
    }
    """
    pass


@ib_sync_app.route("/ib/stopSync")
@gzipped
def stop_sync():
    """
    Stop sync stock data.

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "stopSyncStatus": 0|1 //0:同步进程已开启，正在挂壁，1:表示同步进程已经关闭，
        }
    }
    """
    pass


@ib_sync_app.route("/ib/getSyncStatus")
@gzipped
def sync_status():
    """
    Get current sync status

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "syncStatus": 0|1 //0:当前未开启同步，1:当前已开启同步
        }
    }
    """
    pass


@ib_sync_app.route("/ib/getProgress")
@gzipped
def get_progress():
    """
    Get current sync progress

    :return:
    {
        "success": true  // 当前接口是否成功

        "data": {
             "historicalDataProgress": {
                "HUYA": {  // 股票Symbol
                    "startDate": "20100410 00:00:00"  // 同步开始时间
                    "endDate": "20180510 00:00:00"  // 同步结束时间
                },
                "currentSyncHistory": [  // 当前同步历史记录，最多显示20条，一条记录即一次得到的同步记录数据
                    {
                        "symbol": "AAPL",  // 当前同步的股票symbol
                        "startDate": "20150410 00:00:00"  // 当前同步开始时间
                        "endDate": "20150416 00:00:00"  // 当前同步结束时间
                    }
                ]
            }  // 历史股票数据同步进度
        }
    }
    """
    pass

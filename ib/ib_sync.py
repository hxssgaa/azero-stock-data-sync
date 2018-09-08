import os
from flask import Blueprint, Response
from utils import *
from common.utils import parse_resp
from ib.ib_sync_helper import *

ib_sync_app = Blueprint('ib_sync_app', __name__)


@ib_sync_app.route("/ib/getSyncSymbols.do")
@gzipped
def get_sync_symbols():
    """
    get symbols which has been or need syncing.

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "codeList": [  // symbol列表
                {
                    "symbol": "US.HUYA",  // 股票代码
                    "title": "虎牙",  // 公司名称
                    "date": "1969-12-31"  // 交易所日期
                },
                {
                    "symbol": "US.HMI",  // 股票代码
                    "title": "華米科技",  // 公司名称
                    "date": "1969-12-31"  // 交易所日期
                },
            ],
        }
    }
    """
    try:
        return parse_resp(get_sync_symbols_data_helper())
    except Exception as e:
        return parse_resp({'message': str(e)}, False)


@ib_sync_app.route("/ib/config_sync_symbols", methods=['POST'])
@gzipped
def config_sync_symbols():
    """
    Config symbols which has been or need syncing.
    :request:
    {
        "codeList": [  // symbol列表
            {
                "symbol": "US.HUYA",  // 股票代码
            },
            {
                "symbol": "US.HMI",  // 股票代码
            },
        ],
    }

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "codeList": [  // symbol列表
                {
                    "symbol": "US.HUYA",  // 股票代码
                    "title": "虎牙",  // 公司名称
                    "date": "1969-12-31"  // 交易所日期
                },
                {
                    "symbol": "US.HMI",  // 股票代码
                    "title": "華米科技",  // 公司名称
                    "date": "1969-12-31"  // 交易所日期
                },
            ],
        }
    }
    """

    data = request.get_json()
    stocks = data.get('stocks')
    if not stocks:
        return parse_resp({'message': 'Stocks can\'t be empty.'}, False)
    try:
        return parse_resp(insert_sync_symbols_data_helper(stocks))
    except Exception as e:
        return parse_resp({'message': str(e)}, False)


@ib_sync_app.route("/ib/get_sync_metadata")
@gzipped
def get_sync_metadata():
    """
    get stock sync metadata which could configure the query date, start date, duration or bar size

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": [
            {
                "type": 1  // 配置类型为分钟级别数据(实际间隔为30s，我们认为30s同为分钟数据)
                "startDate": "20140606 00:00:00"  // 开始时间，同步股票数据不会超过开始时间
            },
            {
                "type": 2  // 配置类型为秒级别数据
                "startDate": "20140606 00:00:00"  // 开始时间，同步股票数据不会超过开始时间
            },
            {
                "type": 3  // 配置类型为tick类型数据
                "startDate": "20140606 00:00:00"  // 开始时间，同步股票数据不会超过开始时间
            }
              // 历史股票元数据配置信息
        ]
    }
    """
    return Response('hello, world')


@ib_sync_app.route("/ib/config_sync_metadata", methods=['POST'])
@gzipped
def config_sync_metadata():
    """
        Config stock sync metadata
        :request:
        type = 1 // url param
        {
            "startDate": "20140606 00:00:00"  // 开始时间，同步股票数据不会超过开始时间
        }

        :return:
        {
            "success": true  // 当前接口是否成功
            "errorMessage": "跨度时间错误"  // 若success为false，为具体的错误信息
            "data": [
            {
                "type": 1  // 配置类型为分钟级别数据(实际间隔为30s，我们认为30s同为分钟数据)
                "startDate": "20140606 00:00:00"  // 开始时间，同步股票数据不会超过开始时间
            },
            {
                "type": 2  // 配置类型为秒级别数据
                "startDate": "20140606 00:00:00"  // 开始时间，同步股票数据不会超过开始时间
            },
            {
                "type": 3  // 配置类型为tick类型数据
                "startDate": "20140606 00:00:00"  // 开始时间，同步股票数据不会超过开始时间
            }
              // 历史股票元数据配置信息
        ]
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
    type=0|1|2  // 0 为同步分钟级别数据(实际间隔为30s，我们认为30s同为分钟数据)
                // 1 为同步1秒级别数据，实际间隔为1s
                // 2 为同步tick数据

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "status": 0|1 //0:同步进程未开启，已经开启，1:表示同步进程已经开启，不需要再开启
        }
    }
    """
    pass


@ib_sync_app.route("/ib/stopSync")
@gzipped
def stop_sync():
    """
    Stop sync stock data.
    :request:
    type=0|1|2  // 0 为同步分钟级别数据(实际间隔为30s，我们认为30s同为分钟数据)
                // 1 为同步1秒级别数据，实际间隔为1s
                // 2 为同步tick数据

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "status": 0|1 //0:同步进程已开启，正在关闭，1:表示同步进程已经关闭，无需再关闭
        }
    }
    """
    pass


@ib_sync_app.route("/ib/getSyncStatus")
@gzipped
def sync_status():
    """
    Get current sync status
    :request:
    type=0|1|2  // 0 为同步分钟级别数据(实际间隔为30s，我们认为30s同为分钟数据)
                // 1 为同步1秒级别数据，实际间隔为1s
                // 2 为同步tick数据

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "status": 0|1 //0:当前未开启同步，1:当前已开启同步
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

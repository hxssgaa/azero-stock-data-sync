import os
from flask import Blueprint, Response
from utils import *
from common.utils import parse_resp
from ib.ib_sync_helper import *

ib_sync_app = Blueprint('ib_sync_app', __name__)
SUPPORTED_SYNC_TYPES = [0, 1, 2, 3]


@ib_sync_app.route("/ib/getSyncSymbols.do")
@gzipped
def get_sync_symbols():
    """
    get symbols which has been or need syncing.
    :request
    code=US.AAPL  // 股票代码
    type=0  // 类型

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "stocks": [  // symbol列表
                {
                    "symbol": "US.HUYA",  // 股票代码
                    "title": "虎牙",  // 公司名称
                    "date": "1969-12-31"  // 交易所日期,
                },
                {
                    "symbol": "US.HMI",  // 股票代码
                    "title": "華米科技",  // 公司名称
                    "date": "1969-12-31"  // 交易所日期
                },
            ],
            "syncInfo": {
                "startDate": "2016-01-28 00:00:00",
                "endDate": "2018-01-28 00:00:00"
            } // 同步信息
        }
    }
    """
    try:
        code = request.args.get('code')
        t = request.args.get('type')
        if t is not None:
            t = int(t)
        return parse_resp(get_sync_symbols_data_helper(code, t))
    except Exception as e:
        return parse_resp({'message': str(e)}, False)


@ib_sync_app.route("/ib/configSyncSymbols.do", methods=['POST'])
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
    stocks = data.get('codeList')
    if not stocks:
        return parse_resp({'message': 'Stocks can\'t be empty.'}, False)
    try:
        return parse_resp(insert_sync_symbols_data_helper(stocks))
    except Exception as e:
        return parse_resp({'message': str(e)}, False)


@ib_sync_app.route("/ib/getSyncMetadata.do")
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
    try:
        return parse_resp(get_sync_metadata_helper())
    except Exception as e:
        return parse_resp({'message': str(e)}, False)


@ib_sync_app.route("/ib/configSyncMetadata.do", methods=['POST'])
@gzipped
def config_sync_metadata():
    """
        Config stock sync metadata
        :request:
        {
            "metadata":[
                 {
                    "type": 1,  // 配置类型为分钟级别数据(实际间隔为30s，我们认为30s同为分钟数据)
                    "startDate": "20140606 00:00:00"  // 开始时间，同步股票数据不会超过开始时间
                 },
                 {
                    "type": 2,  // 配置类型为秒级别数据
                    "startDate": "20140606 00:00:00"  // 开始时间，同步股票数据不会超过开始时间
                 },
                 {
                    "type": 3,  // 配置类型为tick类型数据
                    "startDate": "20140606 00:00:00"  // 开始时间，同步股票数据不会超过开始时间
                 } // 历史股票元数据配置信息
            ]
        }
        :return:
        {
            "success": true  // 当前接口是否成功
            "data": {
                "metadata": [
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
                    } // 历史股票元数据配置信息
                ]
            }
        }
        """

    data = request.get_json()
    meta_data_list = data.get('metadata')
    if not meta_data_list:
        return parse_resp({'message': 'Metadata can\'t be empty.'}, False)
    try:
        return parse_resp(update_sync_metadata_helper(meta_data_list))
    except Exception as e:
        return parse_resp({'message': str(e)}, False)


@ib_sync_app.route("/ib/startSync.do")
@gzipped
def start_sync():
    """
    Start sync stock data.

    :request:
    type=0|1|2|3  // 0 为同步分钟级别数据(实际间隔为30s，我们认为30s同为分钟数据)
                  // 1 为同步1秒级别数据，实际间隔为1s
                  // 2 为同步tick数据
                  // 3 为同步Realtime实时数据，30s间隔同步一次数据库（提前停止会导致最后30s数据丢失）

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "status": 0|1|2 //0:同步进程未开启，已经开启，1:表示同步进程已经开启，不需要再开启，2:当前没有需要同步的股票数据
        }
    }
    """
    t = int(request.args.get('type'))
    try:
        if t not in SUPPORTED_SYNC_TYPES:
            return parse_resp({'message': 'Type not supported.'}, False)
        return parse_resp(start_sync_helper(t))
    except Exception as e:
        return parse_resp({'message': str(e)}, False)


@ib_sync_app.route("/ib/stopSync.do")
@gzipped
def stop_sync():
    """
    Stop sync stock data.
    :request:
    type=0|1|2|3 // 0 为同步分钟级别数据(实际间隔为30s，我们认为30s同为分钟数据)
                 // 1 为同步1秒级别数据，实际间隔为1s
                 // 2 为同步tick数据
                 // 3 为同步Realtime实时数据，30s间隔同步一次数据库（提前停止会导致最后30s数据丢失）

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "status": 0|1 //0:同步进程已开启，正在关闭，1:表示同步进程已经关闭，无需再关闭
        }
    }
    """
    t = int(request.args.get('type'))
    try:
        if t not in SUPPORTED_SYNC_TYPES:
            return parse_resp({'message': 'Type not supported.'}, False)
        return parse_resp(stop_sync_helper(t))
    except Exception as e:
        return parse_resp({'message': str(e)}, False)


@ib_sync_app.route("/ib/getSyncStatus.do")
@gzipped
def sync_status():
    """
    Get current sync status
    :request:
    type=0|1|2|3  // 0 为同步分钟级别数据(实际间隔为30s，我们认为30s同为分钟数据)
                  // 1 为同步1秒级别数据，实际间隔为1s
                  // 2 为同步tick数据
                  // 3 为同步Realtime实时数据，30s间隔同步一次数据库（提前停止会导致最后30s数据丢失）


    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "status": 0|1 //0:当前未开启同步，1:当前已开启同步
        }
    }
    """
    t = int(request.args.get('type'))
    try:
        if t not in SUPPORTED_SYNC_TYPES:
            return parse_resp({'message': 'Type not supported.'}, False)
        return parse_resp(get_sync_status_helper(t))
    except Exception as e:
        return parse_resp({'message': str(e)}, False)


@ib_sync_app.route("/ib/getProgress.do")
@gzipped
def get_progress():
    """
    Get current sync progress

    :return:
    {
        "success": true  // 当前接口是否成功

        "data": {
            "1M": {
                 "histDataSyncTrack": {
                    "syncLogs": [  // 当前同步历史记录，最多显示20条，一条记录即一次得到的同步记录数据
                        {
                            "datetime": "20180510 12:00:03",  // log时间
                            "log": "[US.HUYA] xxxxxxx"  // 同步内容
                        }
                    ]
                }  // 历史股票数据同步轨迹
                "histDataSyncProgress": 0.4621, // 46.21% 同步进度
                "syncedSymbols": [{
                    "symbol": "US.HUYA"
                }]
            }, // 分钟级同步数据
            "1S": {
                ...
            }, // 秒级同步数据，格式同上
            "TICK": {
                ...
            } // Tick级同步数据，格式同上
            "REAL": {
                ...
            } // Real级同步数据，格式同上
        }
    }
    """
    try:
        return parse_resp(get_sync_progress_helper())
    except Exception as e:
        return parse_resp({'message': str(e)}, False)

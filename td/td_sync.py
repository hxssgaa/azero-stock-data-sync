import multiprocessing
from flask import Blueprint
from utils import *
from common.utils import StockUtils, ManagedProcess, parse_resp
from td.td_sync_helper import *

td_sync_app = Blueprint('td_sync_app', __name__)
TD_SYNC_PROCESS_NAME = 'TD'


@td_sync_app.route("/td/syncStatus.do")
@gzipped
def sync_status():
    """
    Current sync status indicating if the server is syncing.

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "status": 0|1|  //0:当前服务器没有同步数据，1:当前服务器正在同步数据
        }
    }
    """
    try:
        return parse_resp({'status': int(ManagedProcess.is_process_existed(TD_SYNC_PROCESS_NAME))})
    except Exception as e:
        return parse_resp({'message': str(e)}, False)


@td_sync_app.route("/td/startSync.do")
@gzipped
def start_sync():
    """
    Start syncing td stock data.

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "status": 0|1|  //0:当前数据同步成功开启，1:当前数据同步已经开启，不需要再开启
        }
    }
    """
    try:
        symbols = StockUtils.get_stock_symbols()
        is_td_process_existed = ManagedProcess.is_process_existed(TD_SYNC_PROCESS_NAME)
        if is_td_process_existed:
            return parse_resp({
                'status': 1
            })

        ManagedProcess.create_process(TD_SYNC_PROCESS_NAME, start_sync_helper, (symbols,))
        return parse_resp({
            'status': 0
        })
    except Exception as e:
        return parse_resp({'message': str(e)}, False)


@td_sync_app.route("/td/getSymbolsInfo.do")
@gzipped
def get_symbols_info():
    """
    Start symbols syncing info.

    :request:
    isFuzzy=1
    code=US.H

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "status": 0|1,  // 0为当前股票没有同步数据或者为模糊搜索状态，1为当前股票有同步数据
            "codeList": [  // 模糊搜索列表
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
            "codeCnt": "US"  // 股票总数
            "syncInfo": { // 同步信息
                "1M": { // 分钟级数据, 秒级数据为1S，天级为1D，还有5分钟(5M)，10分钟(10M)，15分钟(15M)，30分钟(30M)
                    "startDate": "2016-01-28 00:00:00", // 开始日期
                    "endDate": "2018-01-28 00:00:00" // 结束日期
                }
            }
        }
    }
    """
    try:
        data_map = {}
        is_fuzzy = bool(int(request.args.get('isFuzzy')))
        code = request.args.get('code')
        if not code:
            return parse_resp({'message': 'Code can\'t be empty'}, False)
        if is_fuzzy:
            res_list, cnt = fuzzy_query_code_list(code)
            data_map['codeList'] = res_list[0]
            data_map['codeCnt'] = cnt
        else:
            data_map['syncInfo'] = query_sync_info(code)
        return parse_resp({'data': data_map})
    except Exception as e:
        return parse_resp({'message': str(e)}, False)


@td_sync_app.route("/td/getSyncProgress.do")
@gzipped
def get_sync_progress():
    """
    Get syncing progress.

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "lastSyncStocks": [  // 最新同步的25条股票数据
                "symbol": "HUYA",
                "frequency": "1M",
                "syncDateTime": "2018-08-14 11:10:00"  // 我们所有的时间都是New York Region时间
            ]  // 最新的同步的股票数据
            "currentProgress": "0.223",  // 表示当前同步的进度比例为22.3%
            "isSyncing": true,  // 表示当前股票是否在同步，如果出现异常则为false，并且进程就会关闭
            "eta": "3600"  //表示当前股票还需3600s=1h的时间才能同步完毕.
            "syncedSymbol": 200  // 表示有200支股票已经是最新状态.
        }
    }
    """
    try:
        data_map = {'isSyncing': bool(int(ManagedProcess.is_process_existed(TD_SYNC_PROCESS_NAME))),
                    'lastSyncStocks': SyncProcessHelper.get_sync_records(),
                    'currentProgress': SyncProcessHelper.get_sync_progress(),
                    'syncedSymbol': SyncProcessHelper.get_synced_symbols_count()}
        return parse_resp({'data': data_map})
    except Exception as e:
        return parse_resp({'message': str(e)}, False)


@td_sync_app.route("/td/stopSync.do")
@gzipped
def stop_sync():
    """
    Stop syncing.

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "status": 0|1|  //0:正在同步，已经关闭，1: 已经关闭
        }
    }
    """
    try:
        is_td_process_existed = ManagedProcess.is_process_existed(TD_SYNC_PROCESS_NAME)
        if not is_td_process_existed:
            return parse_resp({
                'status': 1
            })

        ManagedProcess.remove_process(TD_SYNC_PROCESS_NAME)
        return parse_resp({
            'status': 0
        })
    except Exception as e:
        return parse_resp({'message': str(e)}, False)

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


@td_sync_app.route("/td/getSyncSymbolsCnt.do")
@gzipped
def get_sync_symbols_cnt():
    """
    Start symbols count to sync.

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "syncSymbolsCnt": 1020  // 当前需要同步1020支股票
        }
    }
    """
    pass


@td_sync_app.route("/td/getSyncProgress.do")
@gzipped
def get_sync_progress():
    """
    Get syncing progress.

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "lastSyncStocks": [
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
    pass


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

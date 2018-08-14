from flask import Blueprint
from utils import *

td_sync_app = Blueprint('td_sync_app', __name__)


@td_sync_app.route("/td/startSync")
@gzipped
def start_sync():
    """
    Start syncing td stock data.

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "status": 0|1|2  //0:当前数据已经是最新，所有股票数据均为最新，1:开启同步，正在同步中, 2: 已经开启了同步进程，
                             //不能再次开启
        }
    }
    """
    pass


@td_sync_app.route("/td/getSyncSymbolsCnt")
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


@td_sync_app.route("/td/getSyncProgress")
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


@td_sync_app.route("/td/stop_sync")
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
    pass
from flask import Blueprint, Response
from utils import *
from futu.futu_sync_api import *
import subprocess


futu_sync_app = Blueprint('futu_sync_app', __name__)
FUTU_INSTANCE_NAME = 'azero-stock'


@futu_sync_app.route("/futu/startSync")
@gzipped
def start_sync():
    """
    Start syncing futu stock data.

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "startSyncStatus": 0|1|2 //0:同步机器未开启，正在开启，1:同步机器正在开启中，2:表示同步机器已经开启，
        }
    }
    """
    status = get_gce_instance_status(FUTU_INSTANCE_NAME)
    output = subprocess.check_output(['gcloud', 'compute', 'instances', 'list'])
    return Response(output)


@futu_sync_app.route("/futu/stopSync")
@gzipped
def stop_sync():
    """
    Stop syncing futu stock data.

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "stopSyncStatus": 0|1|2 //0:同步机器已开启，正在关闭，1:同步机器正在关闭中，2:表示同步机器已经关闭，
        }
    }
    """
    pass


@futu_sync_app.route("/futu/newestSyncDate")
@gzipped
def newest_sync_date():
    """
    Get newest sync date.

    :return:
    {
        "success": true  // 当前接口是否存在
        "data": {
            "newestDate": "2018-08-10" // 当前同步股票数据的最新日期
        }
    }
    """
    pass


@futu_sync_app.route("/futu/syncNewestSymbols")
@gzipped
def sync_newest_symbols():
    """
    Sync newest symbols and config the server to use these data

    :return:
    {
        "success": true  // 当前接口是否存在
        "data": {
            "status": true  // 有新增symbols股票数据, 否则无
            "addedSymbolsCnt": 5  // 新增了5个股票数据
            "totalSymbolsCnt": 1030  // 总共有1030支股票被同步.
        }
    }
    """

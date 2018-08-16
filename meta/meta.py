from flask import Blueprint, Response
from utils import *

meta_app = Blueprint('meta_app', __name__)


@meta_app.route("/meta/getMachineMetadata")
@gzipped
def get_machine_metadata():
    """
    get machine metadata.

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "capacity": {
                "ib": {   // IB的容量数据
                    "historicalData": "42G"  // 历史股票数据占用了42G
                } // 当前已同步或者需要同步的股票数据
                "futu": {
                    "historicalData": "200G"  // 富涂数据占用了200G
                }
                "td": {
                    "historicalData"： "90G"  // TD数据占用了90G
                }
            }
        }
    }
    """

    pass

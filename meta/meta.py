from flask import Blueprint, Response

from common.utils import parse_resp
from meta.meta_helper import get_sync_size_info_helper
from utils import *

meta_app = Blueprint('meta_app', __name__)


@meta_app.route("/meta/getSyncSizeInfo.do")
@gzipped
def get_sync_size_info():
    """
    Get how much capacity used in syncing.

    :return:
    {
        "success": true  // 当前接口是否成功
        "data": {
            "capacity": {
                "usedSize": 120527912960.0,  // 磁盘占有的空间
                "totalSize": 315993423872.0   // 磁盘总共的空间
            }
        }
    }
    """
    try:
        return parse_resp(get_sync_size_info_helper())
    except Exception as e:
        return parse_resp({'message': str(e)}, False)

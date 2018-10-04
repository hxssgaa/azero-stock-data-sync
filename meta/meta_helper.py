import db.ib_stock_db as db


def get_sync_size_info_helper():
    info = db.get_db_size_info()
    return {
        'capacity': {
            'usedSize': float(info['fsUsedSize']),
            'totalSize': float(info['fsTotalSize'])
        }
    }
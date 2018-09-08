import db.ib_stock_db as db


def get_sync_symbols_data_helper():
    return {
        'stocks': list(db.get_ib_sync_symbols())
    }


def insert_sync_symbols_data_helper(symbols):
    if not symbols:
        return
    db.insert_ib_sync_symbols(symbols)
    return get_sync_symbols_data_helper()

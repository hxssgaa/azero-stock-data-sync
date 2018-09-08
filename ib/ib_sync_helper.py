def get_sync_symbols_data_helper(symbols):
    if not symbols:
        return {'stocks': []}
    return {
        'stocks': list(map(lambda x: {'symbol': x}, symbols))
    }
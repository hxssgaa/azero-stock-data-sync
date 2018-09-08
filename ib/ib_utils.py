from ibapi.contract import Contract


def queue_consumer(q, handler):
    while True:
        data = q.get()
        handler(data)


def make_contract(symbol, exchange):
    contract = Contract()
    contract.symbol = symbol
    contract.secType = "STK"
    contract.currency = "USD"
    contract.exchange = exchange
    contract.primaryExchange = "ISLAND"
    return contract

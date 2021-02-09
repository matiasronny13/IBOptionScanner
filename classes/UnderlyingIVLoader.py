from ibapi.client import EClient
from classes.IBWrapper import *
from classes.AppUtils import *
from datetime import datetime
from datetime import timedelta

class UnderlyingIVLoader(IBWrapper, EClient):
    def __init__(self, db, limitCount, data):
        IBWrapper.__init__(self, db, limitCount, data)
        EClient.__init__(self, wrapper=self)

    def execute(self, item):
        contract = Contract()
        contract.conId = item['underConId']
        contract.symbol = item['symbol']
        contract.exchange = item['underExchange']
        logger.info("UnderlyingPriceLoader: {0} = {1}".format(item['rowId'], str(contract)))

        self.reqHistoricalData(item['rowId'], contract, '', "2 M", "1 day", "OPTION_IMPLIED_VOLATILITY", 0, 1, False, [])

    @iswrapper
    def historicalData(self, reqId: int, bar: BarData):
        print("HistoricalData. ReqId:", reqId, "BarData.", bar)

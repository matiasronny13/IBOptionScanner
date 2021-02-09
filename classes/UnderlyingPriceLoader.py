from ibapi.client import EClient
from classes.IBWrapper import *
from classes.AppUtils import *

class UnderlyingPriceLoader(IBWrapper, EClient):
    def __init__(self, db, limitCount, data):
        IBWrapper.__init__(self, db, limitCount, data)
        EClient.__init__(self, wrapper=self)

        if 'close' not in self.data.columns:
            self.data['close'] = None

        if 'last' not in self.data.columns:
            self.data['last'] = None

    def execute(self, item):
        contract = Contract()
        contract.conId = item['underConId']
        contract.symbol = item['symbol']
        contract.exchange = item['underExchange']
        logger.info("UnderlyingPriceLoader: {0} = {1}".format(item['rowId'], str(contract)))

        self.reqMktData(item['rowId'], contract, "", True, False, [])

    @iswrapper
    def tickPrice(self, reqId:TickerId , tickType:TickType, price:float, attrib:TickAttrib):
        super().tickPrice(reqId, tickType, price, attrib)
        colName = ''
        if tickType == 9:
            colName = 'close'
        elif tickType == 4:
            colName = 'last'

        if colName != '':
            self.data.loc[self.data['rowId'] == reqId, colName] = price

from ibapi.client import EClient
from classes.IBWrapper import *
from classes.AppUtils import *
import pandas as pd


class OptionPriceLoader(IBWrapper, EClient):
    def __init__(self, db, limitCount, data):
        IBWrapper.__init__(self, db, limitCount, data)
        EClient.__init__(self, wrapper=self)

        if 'bid' not in self.data.columns:
            self.data['bid'] = None
        if 'ask' not in self.data.columns:
            self.data['ask'] = None
        if 'iv' not in self.data.columns:
            self.data['iv'] = None
        if 'delta' not in self.data.columns:
            self.data['delta'] = None
        if 'gamma' not in self.data.columns:
            self.data['gamma'] = None
        if 'vega' not in self.data.columns:
            self.data['vega'] = None
        if 'theta' not in self.data.columns:
            self.data['theta'] = None
        if 'optPrice' not in self.data.columns:
            self.data['optPrice'] = None
        if 'underprice' not in self.data.columns:
            self.data['underprice'] = None

    def execute(self, item):
        contract = Contract()
        contract.conId = item['chainConId']
        contract.symbol = item['symbol']
        contract.exchange = item['optExchange']
        logger.info("OptionPriceLoader: {0} = {1}".format(item['rowId'], str(contract)))

        if not pd.isnull(item['chainConId']):
            self.reqMktData(item['rowId'], contract, "", True, False, [])


    @iswrapper
    def tickPrice(self, reqId:TickerId , tickType:TickType, price:float, attrib:TickAttrib):
        super().tickPrice(reqId, tickType, price, attrib)
        colName = ''
        if tickType == 1:
            colName = 'bid'
        elif tickType == 2:
            colName = 'ask'

        if colName != '':
            self.data.loc[self.data['rowId'] == reqId, colName] = price

    @iswrapper
    def tickOptionComputation(self, reqId: TickerId, tickType: TickType,
                              impliedVol: float, delta: float, optPrice: float, pvDividend: float,
                              gamma: float, vega: float, theta: float, undPrice: float):

        self.logAnswer(current_fn_name(), vars())
        self.data.loc[self.data['rowId'] == reqId, ['iv', 'delta', 'gamma', 'vega', 'theta', 'optPrice', 'underprice']] = [impliedVol, delta, gamma, vega, theta, optPrice, undPrice]
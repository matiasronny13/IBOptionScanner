from ibapi.client import EClient
from classes.IBWrapper import *
from classes.AppUtils import *

class OptionExpirationLoader(IBWrapper, EClient):
    def __init__(self, db, limitCount, data):
        IBWrapper.__init__(self, db, limitCount, data)
        EClient.__init__(self, wrapper=self)
        self.data['strikes'] = None
        self.data['expirations'] = None

    def execute(self, item):
        logger.info("OptionExpirationLoader: {0}\n{1}".format(item['rowId'], str(item)))
        self.reqSecDefOptParams(item['rowId'], item['symbol'], '', item['underSecType'], item['underConId'])

    @iswrapper
    def securityDefinitionOptionParameter(self, reqId: int, exchange: str,
                                          underlyingConId: int, tradingClass: str, multiplier: str,
                                          expirations: SetOfString, strikes: SetOfFloat):
        super().securityDefinitionOptionParameter(reqId, exchange, underlyingConId, tradingClass, multiplier, expirations, strikes)

        if exchange == 'SMART':
            self.data.loc[self.data['rowId'] == reqId, ['strikes', 'expirations']] = [' '.join(str(x) for x in strikes),
                                                                                       ' '.join(expirations)]
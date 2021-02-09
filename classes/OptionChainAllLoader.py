from ibapi.client import EClient
from classes.IBWrapper import *
from classes.AppUtils import *
import pandas as pd

class OptionChainAllLoader(IBWrapper, EClient):
    def __init__(self, db, limitCount, data, expiryDate):
        IBWrapper.__init__(self, db, limitCount, data)
        EClient.__init__(self, wrapper=self)
        self.optionChains = []
        self.expiryDate = expiryDate

    def execute(self, item):
        contract = Contract()
        contract.secType = 'OPT'
        contract.symbol = item['symbol']
        contract.exchange = item['optExchange']
        #contract.right = item['queryRight']
        contract.lastTradeDateOrContractMonth = self.expiryDate
        #contract.strike = item['queryStrike']
        logger.info("OptionChainLoader: {0} = {1}".format(item['rowId'], str(contract)))

        self.reqContractDetails(item['rowId'], contract)

    @iswrapper
    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().contractDetails(reqId, contractDetails)
        self.optionChains.append(
            {
                'chainConId': contractDetails.contract.conId,
                'symbol': contractDetails.contract.symbol,
                'expiry': contractDetails.contract.lastTradeDateOrContractMonth,
                'right': contractDetails.contract.right,
                'strike': contractDetails.contract.strike,

                'optCurrency': contractDetails.contract.currency,
                'optSecType': contractDetails.contract.secType,
                'optExchange': contractDetails.contract.exchange,

                'underConId': contractDetails.underConId,
                'underSecType': contractDetails.underSecType,
                'underExchange': contractDetails.validExchanges
            }
        )

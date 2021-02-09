from ibapi.client import EClient
from classes.IBWrapper import *
from classes.AppUtils import *

class OptionChainLoader(IBWrapper, EClient):
    def __init__(self, db, limitCount, data):
        IBWrapper.__init__(self, db, limitCount, data)
        EClient.__init__(self, wrapper=self)

    def execute(self, item):
        contract = Contract()
        contract.secType = 'OPT'
        contract.symbol = item['symbol']
        contract.exchange = item['optExchange']
        contract.right = item['queryRight']
        contract.lastTradeDateOrContractMonth = item['queryExpiration']
        contract.strike = item['queryStrike']
        logger.info("OptionChainLoader: {0} = {1}".format(item['rowId'], str(contract)))

        self.reqContractDetails(item['rowId'], contract)

    @iswrapper
    def contractDetails(self, reqId:int, contractDetails:ContractDetails):
        super().contractDetails(reqId, contractDetails)
        self.data.loc[self.data['rowId'] == reqId, 'chainConId'] = str(contractDetails.contract.conId)

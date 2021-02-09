from ibapi.client import EClient
from classes.IBWrapper import *
from classes.AppUtils import *


class OptionUnderlyingLoader(IBWrapper, EClient):
    def __init__(self, db, limitCount, data):
        IBWrapper.__init__(self, db, limitCount, data)
        EClient.__init__(self, wrapper=self)

        if 'underExchange' not in self.data.columns:
            self.data['underExchange'] = None

    def execute(self, item):
        contract = Contract()
        contract.conId = item['underConId']
        contract.symbol = item['symbol']
        contract.secType = item['underSecType']
        contract.currency = item['currency']
        logger.info("OptionUnderlyingLoader: {0} = {1}".format(item['rowId'], str(contract)))

        self.reqContractDetails(item['rowId'], contract)

    @iswrapper
    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().contractDetails(reqId, contractDetails)
        self.data.loc[self.data['rowId'] == reqId, 'underExchange'] = contractDetails.contract.exchange
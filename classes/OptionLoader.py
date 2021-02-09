from ibapi.client import EClient
from classes.IBWrapper import *
from classes.AppUtils import *


class OptionLoader(IBWrapper, EClient):
    def __init__(self, db, limitCount, data):
        IBWrapper.__init__(self, db, limitCount, data)
        EClient.__init__(self, wrapper=self)

        if 'optExchange' not in self.data.columns:
            self.data['optExchange'] = None

        if 'currency' not in self.data.columns:
            self.data['currency'] = None

        if 'underConId' not in self.data.columns:
            self.data['underConId'] = None

        if 'underSecType' not in self.data.columns:
            self.data['underSecType'] = None

    def execute(self, item):
        contract = Contract()
        contract.conId = item['optConId']
        contract.symbol = item['symbol']
        logger.info("OptionLoader: {0} = {1}".format(item['rowId'], str(contract)))

        self.reqContractDetails(item['rowId'], contract)

    @iswrapper
    def contractDetails(self, reqId: int, contractDetails: ContractDetails):
        super().contractDetails(reqId, contractDetails)
        self.data.loc[self.data['rowId'] == reqId, ['optExchange', 'currency', 'underConId', 'underSecType']] = [
            contractDetails.contract.exchange,
            contractDetails.contract.currency,
            contractDetails.underConId,
            contractDetails.underSecType]

from classes.AppUtils import *
import numpy
import pandas as pd

class ChainNormalizer2:
    def __init__(self):
        self.result = []

    def do(self, data, start, end, spread):
        if len(data) > 0:
            self.strikeRange = spread
            self.exp_start_date = datetime.strptime(start, '%Y-%m-%d')
            self.exp_end_date = datetime.strptime(end, '%Y-%m-%d')

            data.apply(self.breakChain, axis=1)

            return pd.DataFrame(self.result)

    def breakChain(self, row):
        try:
            obj = row.copy()
            del obj['expirations']
            del obj['strikes']
            obj['queryExpiration'] = '20200110'
            #obj['queryStrike'] = chain[1]
            logger.info(obj)

            obj['queryRight'] = 'C'
            self.result.append(obj.copy())

            obj['queryRight'] = 'P'
            self.result.append(obj.copy())
        except Exception as ex:
            logger.error("ERROR BreakChain {0}\nERROR Message: {1}".format(row, ex))

    def filterContracts(self, contract, strikes, expirations):
        # check if has valid expiry
        newExpirations = [dt for dt in expirations if self.exp_start_date <= dt <= self.exp_end_date]
        newStrikes = []

        if len(newExpirations) > 0 and len(strikes) > 4:
            lastPrice = contract['last']

            # search the first OTM call strike price
            strikes = sorted(strikes)
            otmIndices = [idx for idx, val in enumerate(strikes) if val > float(lastPrice)]
            if len(otmIndices) > 0:
                firstOTMIndex = otmIndices[0]

                # get OTM
                endIndex = firstOTMIndex + self.strikeRange
                newStrikes = strikes[firstOTMIndex:endIndex]

                # get ITM
                if firstOTMIndex > 0:
                    startIndex = firstOTMIndex - self.strikeRange
                    if startIndex < 0:
                        startIndex = 0

                    if firstOTMIndex-1 == startIndex:
                        newStrikes += [strikes[startIndex]]
                    else:
                        newStrikes += strikes[startIndex: firstOTMIndex]
            else:
                # abort
                newExpirations = []
        else:
            # abort
            newExpirations = []

        return (sorted(newExpirations), sorted(newStrikes))


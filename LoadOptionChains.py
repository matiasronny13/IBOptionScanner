from pymongo import MongoClient
from classes.UnderlyingPriceLoader import *
from classes.ChainNormalizer import *
from classes.OptionChainLoader import *


class ChainLoader:
    def __init__(self):
        self.ip = "127.0.0.1"
        self.port = 7497
        self.clientId = 3
        self.dbName = 'IBOptions'
        self.symbolTableName = 'OptionSymbols'
        self.optionChainTableName = 'OptionChains'

        self.limitCount = 45
        self.pageIndex = 0
        self.start = '2020-2-7'
        self.end = '2020-2-7'
        self.spread = 5
        self.upperPriceLimit = 700
        self.bottomPriceLimit = 200

    def get_database(self):
        client = MongoClient('127.0.0.1', 27017)
        return client[self.dbName]

    def run(self):
        db = self.get_database()

        productList = pd.DataFrame(db[self.symbolTableName].find({}))

        # get current underlying price
        loader = UnderlyingPriceLoader(db, self.limitCount, productList)
        loader.connect(self.ip, self.port, self.clientId)
        loader.run()

        # merge close price into last price
        productList['last'].fillna(productList['close'], inplace=True)
        productList['last'].fillna(0, inplace=True)

        # remove product with no last price
        productList = productList[(productList['last'] != 0) & (productList['last'] >= self.bottomPriceLimit) & (productList['last'] <= self.upperPriceLimit)]

        # drill down option chains
        normalizer = ChainNormalizer()
        optionChains = normalizer.do(productList,
                                     self.start,
                                     self.end,
                                     self.spread)

        logger.info("Inserting to DB . . .")
        optionChains.apply(
            lambda item: db[self.optionChainTableName].replace_one({'optConId': item['optConId'],
                                                                    'queryRight': item['queryRight'],
                                                                    'queryStrike': item['queryStrike'],
                                                                    'expiration': item['expiration']},
                                                                   {'symbol': item['symbol'],
                                                                    'currency': item['currency'],
                                                                    'underConId': item['underConId'],
                                                                    'underExchange': item['underExchange'],
                                                                    'underSecType': item['underSecType'],
                                                                    'optConId': item['optConId'],
                                                                    'optExchange': item['optExchange'],
                                                                    'expiration': item['expiration'],
                                                                    'queryExpiration': item['queryExpiration'],
                                                                    'queryRight': item['queryRight'],
                                                                    'queryStrike': item['queryStrike']
                                                                    },
                                                                   upsert=True), axis=1)

        # get option chain id
        loader = OptionChainLoader(db, self.limitCount, optionChains)
        loader.connect(self.ip, self.port, self.clientId)
        loader.run()


        logger.info("Inserting chains to DB . . .")
        optionChains.apply(
            lambda item: db[self.optionChainTableName].replace_one({'optConId': item['optConId'],
                                                                    'queryRight': item['queryRight'],
                                                                    'queryStrike': item['queryStrike'],
                                                                    'expiration': item['expiration']},
                                                                   {'symbol': item['symbol'],
                                                                    'currency': item['currency'],
                                                                    'underConId': item['underConId'],
                                                                    'underExchange': item['underExchange'],
                                                                    'underSecType': item['underSecType'],
                                                                    'optConId': item['optConId'],
                                                                    'optExchange': item['optExchange'],
                                                                    'expiration': item['expiration'],
                                                                    'queryExpiration': item['queryExpiration'],
                                                                    'queryRight': item['queryRight'],
                                                                    'queryStrike': item['queryStrike'],
                                                                    'chainConId': item['chainConId']
                                                                    },
                                                                    upsert=True), axis=1)

        print("Main() end " + str(datetime.now()))


if __name__ == "__main__":
    configure_log('ChainLoader')
    app = ChainLoader()
    app.run()

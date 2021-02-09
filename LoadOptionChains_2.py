from pymongo import MongoClient
from classes.UnderlyingPriceLoader import *
from classes.ChainNormalizer2 import *
from classes.OptionChainAllLoader import *


class ChainLoader:
    def __init__(self):
        self.ip = "127.0.0.1"
        self.port = 7497
        self.clientId = 2
        self.dbName = 'IBOptions'
        self.symbolTableName = 'OptionSymbols'
        self.optionChainTableName = 'OptionChains'

        self.limitCount = 45
        self.expiryDate = '20200110'

    def get_database(self):
        client = MongoClient('127.0.0.1', 27017)
        return client[self.dbName]

    def run(self):
        db = self.get_database()

        productList = pd.DataFrame(db[self.symbolTableName].find({'expirations': {'$regex': self.expiryDate}}).limit(100))

        # get option chain id
        loader = OptionChainAllLoader(db, self.limitCount, productList, self.expiryDate)
        loader.connect(self.ip, self.port, self.clientId)
        loader.run()

        logger.info("Inserting chains to DB . . .")
        db[self.optionChainTableName].insert(loader.optionChains)

        print("Main() end " + str(datetime.now()))


if __name__ == "__main__":
    configure_log('ChainLoader')
    app = ChainLoader()
    app.run()

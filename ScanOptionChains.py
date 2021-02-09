from pymongo import MongoClient
from classes.UnderlyingPriceLoader import *
from classes.OptionPriceLoader import *
import json


class ScanOptionChains():
    def __init__(self):
        self.result = None
        self.limitCount = 100
        self.ip = "127.0.0.1"
        self.port = 7497
        self.clientId = 1
        self.optionChainTableName = 'OptionChains'
        self.resultTableName = 'Scan_RESULT_' + str(datetime.now().strftime("%Y%m%d_%H%M%S"))

        with open('configs/watchlist.json', 'r') as f:
            self.watchlist = json.load(f)

        self.priceRange = 2
        self.start = '2020-2-7'
        self.end = '2020-2-7'
        self.rights = ['C', 'P']

    def get_database(self):
        client = MongoClient('127.0.0.1', 27017)
        return client.IBOptions

    def run(self):
        print("Main() start " + str(datetime.now()))
        db = self.get_database()

        collection = db[self.resultTableName]
        collection.drop()

        # apply filter by DB columns (right, expiration. etc)
        input = pd.DataFrame(db[self.optionChainTableName].find({
            '$and': [
                #{'symbol': {'$in': self.watchlist}},
                {'chainConId': {'$ne': float('nan')}},
                {'expiration': {"$gte": datetime.strptime(self.start, '%Y-%m-%d'), "$lte": datetime.strptime(self.end, '%Y-%m-%d')}}
            ]
        }))

        # get unique contract
        uniqueContract = input[['optConId', 'symbol', 'underConId', 'underExchange']].drop_duplicates()

        # load current price for each unique contract
        loader = UnderlyingPriceLoader(db, self.limitCount, uniqueContract)
        loader.connect(self.ip, self.port, self.clientId)
        loader.run()

        # apply filter by current underlying price
        input = self.filterByPrice(input, uniqueContract)

        # load option price
        loader = OptionPriceLoader(db, self.limitCount, input)
        loader.connect(self.ip, self.port, self.clientId)
        loader.run()

        input['bid'].fillna(0, inplace=True)
        input['ask'].fillna(0, inplace=True)

        logger.info("Inserting to DB . . .")
        data_dict = input[
            ['queryRight', 'queryExpiration', 'symbol', 'last', 'queryStrike', 'bid', 'ask',
             'iv', 'delta', 'gamma', 'vega', 'theta', 'optPrice', 'underprice',
             'optConId', 'optExchange', 'expiration', 'chainConId',
             'currency', 'underConId', 'underExchange', 'underSecType']
        ].to_dict('records')
        result_table = db[self.resultTableName]
        result_table.insert_many(data_dict)

        print("Main() end " + str(datetime.now()))

    def filterByPrice(self, data, uniqueContract):
        logger.info("Start filtering by price . . .")

        # merge close price into last price
        uniqueContract['last'].fillna(uniqueContract['close'], inplace=True)
        uniqueContract['last'].fillna(0, inplace=True)

        result = pd.DataFrame(columns=data.columns.to_list())

        for idx in range(0, len(uniqueContract)):
            contract = uniqueContract.iloc[idx]
            for right in self.rights:
                result = result.append(data[(data['queryStrike'] <= contract['last']) & (data['queryRight'] == right) & (data['optConId'] == contract['optConId'])].sort_values(by=['queryStrike'], ascending=True).tail(self.priceRange), sort=False, ignore_index=True)
                result = result.append(data[(data['queryStrike'] >= contract['last']) & (data['queryRight'] == right) & (data['optConId'] == contract['optConId'])].sort_values(by=['queryStrike'], ascending=True).head(self.priceRange), sort=False, ignore_index=True)

            # update input current price
            result.loc[result['underConId'] == contract['underConId'], 'last'] = contract['last']

        result['rowId'] = range(0, len(result))
        return result


if __name__ == "__main__":
    configure_log('Scanner')
    app = ScanOptionChains()
    app.run()





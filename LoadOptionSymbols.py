from pymongo import MongoClient
from classes.OptionSymbolScrapper import *
from classes.OptionLoader import *
from classes.OptionUnderlyingLoader import *
from classes.OptionExpirationLoader import *

class ContractScrapper:
    def __init__(self):
        self.ip = "127.0.0.1"
        self.port = 7497
        self.clientId = 3
        self.dbName = 'IBOptions'
        self.symbolTableName = 'OptionSymbols'

        self.limitCount = 45
        self.pageIndex = 0
        self.ibExchange = 'cboe'
        self.productListUrl = "https://www.interactivebrokers.com/en/index.php?f=2222&exch={0}&showcategories=OPTGRP".format(self.ibExchange)

    def get_database(self):
        client = MongoClient('127.0.0.1', 27017)
        return client[self.dbName]

    def run(self):
        db = self.get_database()

        #productList = pd.DataFrame(db[self.symbolTableName].find({}))

        # scrap from the website
        scrapper = OptionSymbolScrapper(self.productListUrl)
        productList = scrapper.scrap(self.pageIndex)

        # get option details
        app = OptionLoader(db, self.limitCount, productList)
        app.connect(self.ip, self.port, self.clientId)
        app.run()

        # get underlying exchange
        app = OptionUnderlyingLoader(db, self.limitCount, productList)
        app.connect(self.ip, self.port, self.clientId)
        app.run()

        # get options strike and expiration
        loader = OptionExpirationLoader(db, self.limitCount, productList)
        loader.connect(self.ip, self.port, self.clientId)
        loader.run()

        # upsert additional data
        logger.info("Inserting to DB . . .")
        productList.apply(lambda item: db[self.symbolTableName].replace_one({'optConId': item['optConId']},
                                                                            {'symbol': item['symbol'],
                                                                             'currency': item['currency'],
                                                                             'underConId': item['underConId'],
                                                                             'underExchange': item['underExchange'],
                                                                             'underSecType': item['underSecType'],
                                                                             'optConId': item['optConId'],
                                                                             'optExchange': item['optExchange'],
                                                                             'strikes': item['strikes'],
                                                                             'expirations': item['expirations']
                                                                             },
                                                                            upsert=True), axis=1)

        print("Main() end " + str(datetime.now()))

    def removeUnusedProduct(self, data):
        '''numericSymbol = ['AABA1', 'AABA2', 'ABCB1', 'AHT1', 'AKBA1', 'ALIM1', 'AMCR1', 'APDN1', 'APEX1', 'APRN1',
                         'APTV1', 'ARAV1', 'ARTX1', 'ASNA1', 'AXGT1', 'BAM1', 'BAP1', 'BBD1', 'BBD1', 'BBD1', 'BBI1',
                         'BGCP1', 'BHP1', 'BHR1', 'BMY1', 'BOIL1', 'BPY1', 'BW1', 'CAG1', 'CARB1', 'CGIX1', 'CI1',
                         'CLDR1', 'CNX1', 'CPE1', 'CRES1', 'CRM1', 'CRM2', 'CUZ2', 'CVIA1', 'CVS1', 'CYOU1', 'DD1',
                         'DD2', 'DELL1', 'DIS1', 'DIS2', 'DRIP1', 'DRV1', 'EC1', 'ECA1', 'EGO1', 'ELAN1', 'ENIA2',
                         'EQT2', 'ET1', 'ET2', 'EXAS1', 'FANG1', 'FAZ1', 'FIS1', 'FISV1', 'FMC1', 'FRAN1', 'FVE1',
                         'GCI1', 'GE1', 'GOLD1', 'GPN1', 'GTS1', 'GUSH1', 'HDB2', 'HI1', 'HMNY1', 'HON2',
                         'HON3', 'IIVI1', 'IMUX1', 'INFY2', 'JEF1', 'JNK1', 'JNUG1', 'KAR1', 'KDP1', 'LABD1', 'LC1',
                         'LEN1', 'LHX1', 'LITE1', 'MACK2', 'MDR2', 'MIDZ1', 'MPC1', 'MPLX1', 'NEM1', 'NEX1', 'NTR2',
                         'NUAN1', 'NVAX1', 'NVS1', 'ONTO1', 'OXY1', 'PAAS1', 'PB1', 'PBCT1', 'PLX1', 'PSDO1', 'PSTI1',
                         'QID1', 'QUIK1', 'RAD1', 'RBS2', 'REPH1', 'RIO2', 'RYCE1', 'SALT1', 'SATS3', 'SDOW1', 'SFUN2',
                         'SIRI1', 'SNCA1', 'SOS1', 'SPNV1', 'SPXU1', 'SQQQ1', 'STNG1', 'SUNW1', 'SVXY1', 'SXC1', 'T1',
                         'TA1', 'TAK1', 'TCF1', 'TFC1', 'TNK1', 'TRI1', 'TRN1', 'TRXC1', 'TTPH1', 'TVTY1', 'TZA1',
                         'UGI1', 'UNG1', 'UTX1', 'UVXY2', 'VAC1', 'VAL2', 'VFC1', 'VIAC1', 'VIAC2', 'VIIX1', 'VIVE1',
                         'VST1', 'WMB1', 'WYND1', 'XEC1', 'XRF1']
        data.loc[data['symbol'].isin(numericSymbol), 'optExchange'] = 'BASKET'
        data = data[data['underSecType'] != 'IND']'''
        data = data[~data['symbol'].isin(['HLTH', 'EMESQ', 'GWR', 'PVTL'])]

        return data


if __name__ == "__main__":
    configure_log('Contract')
    app = ContractScrapper()
    app.run()

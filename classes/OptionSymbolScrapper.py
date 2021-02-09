import pandas as pd
from bs4 import BeautifulSoup
import re
from classes.AppUtils import *


class OptionSymbolScrapper:
    def __init__(self, productListUrl):
        self.productListUrl = productListUrl

    def get_page_count(self):
        html = BeautifulSoup(simple_get(self.productListUrl), 'html.parser')
        last_page = html.select('ul[class=pagination]')[0].select('li')[-2].text
        return int(last_page)

    def scrap_page(self, page_index=0):
        result = pd.DataFrame(columns=['optConId', 'symbol'])
        html = BeautifulSoup(simple_get(self.productListUrl + "&page=" + str(page_index)), 'html.parser')

        for a in html.select('a[class=linkexternal][href^=javascript]'):
            try:
                tr = a.parent.parent
                tds = tr.select('td')
                symbol = tds[0].text
                conid = int(
                    re.search("conid=\d*", tds[1].select('a')[0].get_attribute_list('href')[0])[0].replace('conid=',
                                                                                                           ''))

                logger.info(">> {0} {1}".format(symbol, conid))
                result = result.append({'optConId': conid, 'symbol': symbol}, ignore_index=True)
            except Exception as ex:
                logger.error(ex)

        return result

    def scrap(self, page_index=0):
        pageCount = self.get_page_count()
        logger.info("{0} pages found".format(pageCount))

        result = pd.DataFrame(columns=['optConId', 'symbol'])
        if page_index <= 0:
            for page in range(1, pageCount + 1):
                result = result.append(self.scrap_page(page), ignore_index=True)
        else:
            result = result.append(self.scrap_page(page_index), ignore_index=True)

        return result

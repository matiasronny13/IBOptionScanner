from classes.AppUtils import *
from bs4 import BeautifulSoup
import pandas as pd
import asyncio
from aiohttp import ClientSession


class OCCStrikeLoader():
    def __init__(self, data):
        self.data = data
        self.data['rowId'] = range(0, len(self.data))
        self.url = "https://oic.ivolatility.com/oic_adv_options.j;jsessionid=aNYQJga6ulra"
        self.result = pd.DataFrame()

    async def fetch(session, url):
        async with session.get(url) as response:
            return await response.text()

    async def downloadChains(self, session, product):
        payload = {
            "__is_form_sent": 1
            , "exp_date": 7
            , "percent": 2
            , "scp": 3
            , "service": "detailed"
            , "ticker": product.symbol
            , "tm": 4
            # ,"x": ""
            # ,"y": ""
        }

        logger.info("OCCStrikeLoader: {0} = {1}".format('', payload))
        async with session.post(self.url, data=payload) as response:
            responseText = await response.text()
            html = BeautifulSoup(responseText, 'html.parser')

            logger.info("OCCStrikeLoader END: {0} = {1}".format('', payload))
            expirySpans = html.select('span[class=s4] b')
            if len(expirySpans) > 0:
                expiry = datetime.strptime(expirySpans[0].text[8:20], '%b %d, %Y').strftime('%Y%m%d')
                strikes = " ".join([a.text.strip() for a in html.select('td[rowspan] span')])

                return {'rowId': product.rowId, 'strikes': strikes, 'expiry': expiry}

    async def main_loop(self):
        async with ClientSession() as session:
            html = await self.downloadChains(session, self.data.iloc[0])
            print(html)

    async def run(self):
        tasks = []

        # Fetch all responses within one Client session,
        # keep connection alive for all requests.
        async with ClientSession() as session:
            count = len(self.data)
            for i in range(0, count):
                paramData = self.data.iloc[i]
                task = asyncio.ensure_future(self.downloadChains(session, paramData))
                tasks.append(task)

            responses = await asyncio.gather(*tasks)
            # you now have all response bodies in this variable
            print(responses)

    def execute(self):
        #logger.info("OCCStrikeLoader processing {0}".format(len(self.data)))

        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.run())
        loop.run_until_complete(future)


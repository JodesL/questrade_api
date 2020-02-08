""""
This is the implementation of the Questrade API. It will setup an API to be able to obtain market candles data
as well as information on each ticker.

"""
import requests
import time
from datetime import datetime


class QTApi:
    def __init__(self, token):

        self.conn_init_time = time.time()
        self.conn = requests.post('https://login.questrade.com/oauth2/token',
                                  data={
                                      'grant_type': 'refresh_token',
                                      'refresh_token': token
                                  })

        if self.conn.status_code != 200:
            raise ConnectionError(f'Connection to Questrade API failed. \n'
                                  f'Message: {self.conn.content} \n'
                                  f'Error code: {self.conn.status_code}')
        else:
            print(f'Questrade API connection success!')

        self.conn = self.conn.json()

    def _refresh_conn(self):
        """ Refreshes api connection
        Will refresh token if token is expired.

        """

        if time.time() - self.conn_init_time > self.conn['expires_in']:
            tries = 5
            for i in range(tries):
                try:
                    conn_init_time = time.time()
                    conn = requests.post('https://login.questrade.com/oauth2/token',
                                         data={
                                             'grant_type': 'refresh_token',
                                             'refresh_token': self.conn['refresh_token']
                                         })

                    if conn.status_code != 200:
                        raise ConnectionError(f'Connection to Questrade API failed. \n'
                                              f'Message: {conn.content} \n'
                                              f'Error code: {conn.status_code}')
                    else:
                        print(f'Connection Success!')
                        self.conn_init_time = conn_init_time
                        self.conn = conn.json()

                except:
                    if i < tries - 1:
                        time.sleep(10)
                    else:
                        print('QTApi max number of retries has been hit, will raise error')
                        raise

                break

    def symbols_search(self, symbol_name):
        """ Searches for symbol

        Searches for symbol and returns Questrade symbolId as well as the description,
        security types and other information.

        Args:
            symbol_name (str): ticker symbol

        Returns:
            dict:
                {'symbol': (str),
                 'symbolId': (int),
                 'description': (str),
                 'securityType': (str),
                 'listingExchange': (str),
                 'isTradable': (bool),
                 'isQuotable': (bool),
                 'currency': (str)}

        """

        self._refresh_conn()
        response = requests.get(url=f"{self.conn['api_server']}v1/symbols/search", params={'prefix': symbol_name},
                                headers={'Authorization': f"{self.conn['token_type']} {self.conn['access_token']}"})

        return response.json()

    def symbols_info(self, symbol_id):
        """ Get additional information on symbol

        Obtain fundamental information on ticker. Requires the symbolId obtainable from self.symbol_search().

        Args:
            symbol_id (int): symbolId of ticker

        Returns:
            dict:
                {'symbols':
                    [
                    {'symbol': (str),
                    'symbolId': (int),
                    'prevDayClosePrice': (float),
                    'highPrice52': (float),
                    'lowPrice52': (float),
                    'averageVol3Months': (float),
                    ....}
                    ]
                }

        """

        self._refresh_conn()
        response = requests.get(url=f"{self.conn['api_server']}v1/symbols/{symbol_id}",
                                headers={'Authorization': f"{self.conn['token_type']} {self.conn['access_token']}"})

        return response.json()

    def market_candles(self, symbol_id, start_date, end_date):
        """ Obtain daily market candles quotes
        Using symbolId and defined start and end date in (y-m-d) obtain a list of candles information; open, high, low,
        low, close, volume. Candles are at the daily level.

        Args:
            symbol_id (int): symbolId number
            start_date (str): string representing the start date in %Y-%m-%d format (2020-01-01)
            end_date (str): string representing the start date in %Y-%m-%d format (2020-12-31)

        Returns:
            dict:
                {'candles':
                    [{'start': (datetime),
                       'end': (datetime),
                       'low': (float),
                       'high': (float),
                       'open': (float),
                       'close': (float),
                       'volume': (int),
                       'VWAP': (float)},
                      {'start': (datetime),
                       'end': (datetime),
                       'low': (float),
                       'high': (float),
                       'open': (float),
                       'close': (float),
                       'volume': (int),
                       'VWAP': (float)}
                       ....
                    ]
                }

        """

        self._refresh_conn()
        begin = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        response = requests.get(url=f"{self.conn['api_server']}v1/markets/candles/{symbol_id}",
                                params={
                                    'startTime': f'{begin.isoformat()}-05:00',
                                    'endTime': f'{end.isoformat()}-05:00',
                                    'interval': "OneDay"
                                },
                                headers={'Authorization': f'Bearer {self.conn["access_token"]}'})

        return response.json()

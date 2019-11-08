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
            print(f'Connection Success!')

        self.conn = self.conn.json()

    def _refresh_conn(self):

        if time.time() - self.conn_init_time > self.conn['expires_in']:
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


    def symbols_search(self, symbol_name):

        self._refresh_conn()
        response = requests.get(url=f"{self.conn['api_server']}v1/symbols/search", params={'prefix': symbol_name},
                                headers={'Authorization': f"{self.conn['token_type']} {self.conn['access_token']}"})

        return response.json()

    def symbols_info(self, symbol_id):

        self._refresh_conn()
        response = requests.get(url=f"{self.conn['api_server']}v1/symbols/{symbol_id}",
                                headers={'Authorization': f"{self.conn['token_type']} {self.conn['access_token']}"})

        return response.json()

    def market_candles(self, symbol_id, start_date, end_date):

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

if __name__ == '__main__':

    test_API = QTApi('test_string')

    bmo_symbol = test_API.symbols_search('BMO.TO')['symbols'][0]

    test_API.symbols_info(bmo_symbol['symbolId'])


    test_API.market_candles(bmo_symbol['symbolId'],
                            '2014-11-01',
                            '2014-12-01')
from questrade_api import QTApi


test_API = QTApi('test_string')

bmo_symbol = test_API.symbols_search('BMO.TO')['symbols'][0]

test_API.symbols_info(bmo_symbol['symbolId'])


test_API.market_candles(bmo_symbol['symbolId'],
                        '2014-11-01',
                        '2014-12-01')
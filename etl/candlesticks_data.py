import datetime
import pandas as pd
from sqlalchemy import create_engine, types
from configparser import ConfigParser
from tqdm import tqdm

from questrade_api import QTApi

parser = ConfigParser()
parser.read('config.ini')
config = parser._sections

tsx_config = config['tsx_website']
sql_config = config['postgresql_server']
qt_config = config['questrade_api']

psql_user_name = sql_config['user']
psql_password = sql_config['pwd']
psql_hostname = sql_config['host_name']
psql_url = sql_config['local_url']
psql_schema = sql_config['schema']

engine = create_engine(f'postgresql://{sql_config["user"]}:{sql_config["pwd"]}'
                       f'@{sql_config["local_url"]}/{tsx_config["schema"]}')

candlestick_table = 'backup_candlestick_data'
test = pd.read_sql("SELECT * FROM backup_candlestick_data", con=engine)
test.to_sql('backup_candlestick_data', con=engine, if_exists='replace', index=False)


def remove_duplicates_candlestick_table(candlestick_table, engine):
    """ Remove duplicate rows from candlestick data table
    Will remove duplicate historical candlestick quotes. canelstick_table must contain must be output of
    update_candlestick_data and contain following columns: start, end, low, high, open. close. volumne, VWAP,
    symbol_id, symbol.

    Args:
        candlestick_table (str): name of table containing historical candlestick data in postgresql
        engine (sqlalchemy.engine): sql aclhemy connection engine

    """

    with engine.connect() as con:
        con.execute(f'DELETE FROM {candlestick_table} WHERE "start" = "end"')  # removes rows with start == end
        distinct_row_count = con.execute(
            f'SELECT COUNT(distinct("start", "symbol", "symbol_id")) FROM {candlestick_table};')
        distinct_row_count = distinct_row_count.fetchone()[0]
        total_row_count = con.execute(f'SELECT COUNT(*) FROM {candlestick_table};')
        total_row_count = total_row_count.fetchone()[0]

    if total_row_count > distinct_row_count:
        print(f'Removing duplicate rows from {candlestick_table}')
        print(f'Creating backup of original data in backup_{candlestick_table}')
        with engine.connect() as con:

            con.execute(f'DROP table backup_{candlestick_table};')
            con.execute(f'''
                ALTER TABLE {candlestick_table} 
                RENAME TO backup_{candlestick_table};
            ''')

            print(f'Keeping only distinct rows in {candlestick_table}')
            con.execute(f'''
                CREATE TABLE {candlestick_table} AS
                SELECT DISTINCT "start", "end", "low", "high", "open", "close", "volume", "VWAP", "symbol_id", "symbol"
                FROM backup_{candlestick_table};
                ''')

        print('Removed duplicates successful!')

    else:
        print(f'No duplicates in {candlestick_table}!')


def update_candlestick_data(candlestick_table, symbol_name_ids, engine, qt_api, start_date=None):
    """ Update candlestick historical data from questrade API
    Will call the questrade api market_candles method to get candlestick data for symbolId  provided in symbol_name_ids.
    Will update candlestick_table in postgresql.

    Args:
        candlestick_table (str): name of candlestick_table to update
        symbol_name_ids (iterable, tuples): iterable of tuples (ticker_id, ticker_symbol)
        engine (sqlalchemy.engine): sqlachmey connection engine
        qt_api (QTApi): questrade api object of QTApi class
        start_date (str): string representing the start date in %Y-%m-%d format (2020-12-31):


    """
    if not start_date:
        start_date = pd.read_sql(f"SELECT MAX(start) FROM {candlestick_table};", con=engine).iloc[0, 0]
        start_date += datetime.timedelta(days=1)
    end_date = datetime.date.today()

    if start_date >= end_date:
        raise (f'{candlestick_table} already up to date!')

    print(f'Updating {candlestick_table} from {str(start_date)} to {str(end_date)}')

    for symbol_tuple in tqdm(symbol_name_ids):
        daily_candles = None

        symbol_id = symbol_tuple[0]
        symbol = symbol_tuple[1]
        print(f'Extracting {symbol}')

        candles_info = qt_api.market_candles(symbol_id=symbol_id,
                                             start_date=str(start_date),
                                             end_date=str(end_date))

        if candles_info.get('code') == 1019 or len(candles_info['candles']) == 0:
            continue

        candles_info = candles_info['candles']
        for day in candles_info:
            daily_candle = pd.DataFrame(day, index=[0])

            daily_candle['symbol_id'] = int(symbol_id)
            daily_candle['symbol'] = symbol

            if daily_candles is not None:
                daily_candles = daily_candles.append(daily_candle, ignore_index=True)
            else:
                daily_candles = daily_candle

        daily_candles.to_sql(name=candlestick_table,
                             con=engine,
                             if_exists='append',
                             index=False,
                             dtype={
                                 'start': types.DATE,
                                 'end': types.DATE,
                                 'low': types.FLOAT,
                                 'high': types.FLOAT,
                                 'open': types.FLOAT,
                                 'close': types.FLOAT,
                                 'volume': types.INT,
                                 'VWAP': types.FLOAT,
                                 'symbol_id': types.INT,
                                 'symbol': types.String
                             })

    print('Update successful!')


if __name__ == '__main__':
    parser = ConfigParser()
    parser.read('config.ini')
    config = parser._sections

    tsx_config = config['tsx_website']
    sql_config = config['postgresql_server']
    qt_config = config['questrade_api']

    psql_user_name = sql_config['user']
    psql_password = sql_config['pwd']
    psql_hostname = sql_config['host_name']
    psql_url = sql_config['local_url']
    psql_schema = sql_config['schema']

    engine = create_engine(f'postgresql://{sql_config["user"]}:{sql_config["pwd"]}'
                           f'@{sql_config["local_url"]}/{tsx_config["schema"]}')

    qt_api = QTApi(qt_config['token'])

    symbol_info = pd.read_sql('symbol_info',
                              con=engine)

    update_candlestick_data(candlestick_table='candlestick_data',
                            symbol_name_ids=zip(symbol_info['symbolId'], symbol_info['symbol']),
                            qt_api=qt_api,
                            engine=engine,
                            start_date=None)

    remove_duplicates_candlestick_table(candlestick_table='candlestick_data',
                                        engine=engine)

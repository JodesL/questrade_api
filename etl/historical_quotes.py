import pandas as pd
import datetime
from sqlalchemy import create_engine, types
from configparser import ConfigParser
from tqdm import tqdm
import psycopg2

from questrade_api import QTApi


def remove_duplicates_candlestick_data(db_name, user, password, host):


    conn = psycopg2.connect(
        dbname=db_name,
        user=user,
        password=password,
        host=host)

    curr = conn.cursor()

    # removes rows with start == end
    curr.execute('DELETE FROM candlestick_data WHERE "start" = "end"')
    curr.execute('SELECT COUNT(distinct("start", "symbol", "symbol_id")) FROM candlestick_data;')
    distinct_row_count = curr.fetchone()
    curr.execute('SELECT COUNT(*) FROM candlestick_data;')
    total_row_count = curr.fetchone()

    if total_row_count[0] > distinct_row_count[0]:
        print('Removing duplicate rows from candlestick_data')
        print('Creating backup of original data (backup_candlestick_data)')
        curr.execute('DROP table backup_candlestick_data;')
        curr.execute('''
            ALTER TABLE candlestick_data 
            RENAME TO backup_candlestick_data;
        ''')

        print('Keeping only distinct rows in candlestick_data')
        curr.execute('''
            CREATE TABLE candlestick_data AS
            SELECT DISTINCT "start", "end", "low", "high", "open", "close", "volume", "VWAP", "symbol_id", "symbol"
            FROM backup_candlestick_data;
            ''')
        conn.commit()

        curr.close()
        conn.close()
        print('Removed duplicates successful!')

    else:
        print('No duplicates in candlestick_data!')


def update_candlestick_data(user, password, url, schema, qt_api_token, start_date=None):

    qt_api = QTApi(qt_api_token)
    engine = create_engine(f'postgresql://{user}:{password}@{url}/{schema}')

    if not start_date:
        start_date = pd.read_sql("SELECT MAX(start) FROM candlestick_data;", con=engine).iloc[0, 0]
        start_date += datetime.timedelta(days=1)
    end_date = datetime.date.today()

    if start_date >= end_date:
        raise(f'Candlestick_data already up to date!')

    print(f'Updating candlestick_data from {str(start_date)} to {str(end_date)}')

    symbol_info = pd.read_sql('symbol_info',
                              con=engine)

    for i in tqdm(range(len(symbol_info))):
        daily_candles = None

        symbol_id = symbol_info.iloc[i]['symbolId']
        symbol = symbol_info.iloc[i]['symbol']
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

        daily_candles.to_sql(name='candlestick_data',
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

    update_candlestick_data(user=psql_user_name,
                            password=psql_password,
                            url=psql_url,
                            schema=psql_schema,
                            qt_api_token=qt_config['token'],
                            start_date=None)

    remove_duplicates_candlestick_data(db_name=psql_schema,
                                       user=psql_user_name,
                                       password=psql_password,
                                       host=psql_hostname)

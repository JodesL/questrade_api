import time
from datetime import datetime
import pandas as pd
from configparser import ConfigParser
from selenium import webdriver
from sqlalchemy import create_engine, types
from tqdm import tqdm

from questrade_api import QTApi


def scrape_symbols_tsx_website(
        tsx_company_directory_url='https://www.tmxmoney.com/en/research/listed_company_directory.html',
        webdriver_exec_path='/usr/lib/chromium-browser/chromedriver'):
    driver = webdriver.Chrome(executable_path=webdriver_exec_path)
    driver.get(tsx_company_directory_url)
    elem = driver.find_element_by_id("SearchKeyword")
    elem.clear()
    elem.send_keys("^")
    button = driver.find_element_by_id("btn-search")
    driver.execute_script("arguments[0].click();", button)
    time.sleep(5)

    table = driver.find_element_by_id('tresults')
    rows = table.find_elements_by_tag_name('tr')

    extracted_table = pd.DataFrame(columns=['company_name', 'symbol'])


    for row in tqdm(rows[1:]):
        cols = row.find_elements_by_tag_name('td')
        to_append = [None, None]

        for i in range(len(cols)):
            to_append[i] = cols[i].text.lstrip()

        extracted_table = extracted_table.append(
            {'company_name': to_append[0],
             'symbol': to_append[1]},
            ignore_index=True)

    driver.close()

    final_table = extracted_table.loc[~extracted_table['symbol'].isnull(), :]

    return final_table


def insert_tsx_symbols_psql(tsx_symbol_table,
                            psql_user_name,
                            psql_password,
                            psql_url='localhost:5432',
                            psql_schema='questrade_api',
                            write_table_name='tsx_symbols'):
    engine = create_engine(f'postgresql://{psql_user_name}:{psql_password}@{psql_url}/{psql_schema}')

    tsx_symbol_table.to_sql(name=write_table_name,
                            con=engine,
                            index=False,
                            if_exists='replace')

    tsx_symbol_table.to_sql(name=f'{write_table_name}_{datetime.date(datetime.now())}',
                            con=engine,
                            index=False)


def insert_symbol_info_psql(tsx_symbols_table_name,
                            qt_api_token,
                            psql_user_name,
                            psql_password,
                            psql_url='localhost:5432',
                            psql_schema='questrade_api',
                            write_table_name='symbol_info'):
    engine = create_engine(f'postgresql://{psql_user_name}:{psql_password}@{psql_url}/{psql_schema}')

    tsx_symbols = pd.read_sql_table(table_name=tsx_symbols_table_name,
                                    con=engine)

    QT_api = QTApi(qt_api_token)

    for i in tqdm(range(len(tsx_symbols))):
        search_response = QT_api.symbols_search(f'{tsx_symbols.iloc[i]["symbol"]}.TO')
        if not search_response['symbols']:
            continue
        info_response = QT_api.symbols_info(search_response['symbols'][0]['symbolId'])
        info_response_dict = info_response['symbols'][0]

        info_df = pd.DataFrame({
            'info_date': datetime.date(datetime.now()),
            'symbol': info_response_dict['symbol'],
            'symbolId': info_response_dict['symbolId'],
            'prevDayClosePrice': info_response_dict['prevDayClosePrice'],
            'highPrice52': info_response_dict['highPrice52'],
            'lowPrice52': info_response_dict['lowPrice52'],
            'averageVol3Months': info_response_dict['averageVol3Months'],
            'averageVol20Days': info_response_dict['averageVol20Days'],
            'outstandingShares': info_response_dict['outstandingShares'],
            'eps': info_response_dict['eps'],
            'pe': info_response_dict['pe'],
            'dividend': info_response_dict['dividend'],
            'yield': info_response_dict['yield'],
            'exDate': info_response_dict['exDate'],
            'marketCap': info_response_dict['marketCap'],
            'listingExchange': info_response_dict['listingExchange'],
            'description': info_response_dict['description'],
            'securityType': info_response_dict['securityType'],
            'dividendDate': info_response_dict['dividendDate'],
            'isTradable': info_response_dict['isTradable'],
            'isQuotable': info_response_dict['isQuotable'],
            'currency': info_response_dict['currency'],
            'industrySector': info_response_dict['industrySector'],
            'industryGroup': info_response_dict['industryGroup'],
            'industrySubgroup': info_response_dict['industrySubgroup']

        },
            index=[0],
        )

        info_df.to_sql(name=write_table_name,
                       con=engine,
                       if_exists='append',
                       index=False,
                       dtype={
                           'info_date': types.DATE,
                           'symbol': types.String,
                           'symbolId': types.BIGINT,
                           'prevDayClosePrice': types.FLOAT,
                           'highPrice52': types.FLOAT,
                           'lowPrice52': types.FLOAT,
                           'averageVol3Months': types.FLOAT,
                           'averageVol20Days': types.FLOAT,
                           'outstandingShares': types.FLOAT,
                           'eps': types.FLOAT,
                           'pe': types.FLOAT,
                           'dividend': types.FLOAT,
                           'yield': types.FLOAT,
                           'exDate': types.DATE,
                           'marketCap': types.FLOAT,
                           'listingExchange': types.String,
                           'description': types.String,
                           'securityType': types.String,
                           'dividendDate': types.DATE,
                           'isTradable': types.BOOLEAN,
                           'isQuotable': types.BOOLEAN,
                           'currency': types.String,
                           'industrySector': types.String,
                           'industryGroup': types.String,
                           'industrySubgroup': types.String
                       }
                       )

if __name__ == '__main__':
    parser = ConfigParser()
    parser.read('config.ini')
    config = parser._sections

    tsx_config = config['tsx_website']
    sql_config = config['postgresql_server']
    qt_config = config['questrade_api']

    print('Scraping TSX website for symbols')
    tsx_symbols = scrape_symbols_tsx_website(tsx_company_directory_url=tsx_config['tsx_company_directory_url'],
                                             webdriver_exec_path=tsx_config['webdriver_exec_path'])

    print('Inserting TSX symbols on postgresql')
    insert_tsx_symbols_psql(tsx_symbol_table=tsx_symbols,
                            psql_user_name=sql_config['user'],
                            psql_password=sql_config['pwd'],
                            psql_url=sql_config['local_url'],
                            psql_schema=tsx_config['schema'],
                            write_table_name=tsx_config['tsx_symbols_table_name']
                            )

    print('Getting additional symbol informatinon')
    insert_symbol_info_psql(tsx_symbols_table_name=tsx_config['tsx_symbols_table_name'],
                            qt_api_token=qt_config['token'],
                            psql_user_name=sql_config['user'],
                            psql_password=sql_config['pwd'],
                            psql_url=sql_config['local_url'],
                            psql_schema=tsx_config['schema'],
                            write_table_name=tsx_config['tsx_symbols_info_table_name']
                            )

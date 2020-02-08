"""
Contains function to scrape the tsx website to obtain all the ticker symbols that are currently listed on
the Toronto Stock Exchange (TSX). Also has utilities to write to postgresql database.

"""

import time
import pandas as pd
from datetime import datetime
from configparser import ConfigParser
from selenium import webdriver
from sqlalchemy import create_engine, types
from tqdm import tqdm

from questrade_api import QTApi


def scrape_symbols_tsx_website(
        tsx_company_directory_url='https://www.tmxmoney.com/en/research/listed_company_directory.html',
        webdriver_exec_path='/usr/lib/chromium-browser/chromedriver'):
    """ Scrape ticker symbols from TSX website
    Will go to company directory url and use a provided webdriver executable to scrape the ticker symbol
    strings.

    Args:
        tsx_company_directory_url (str): TSX company directory url
        webdriver_exec_path (str): local path to webdriver executable

    Returns:
        dataframe: table with the ticker's name as well as its symbol

    """
    print('Scraping TSX website for symbols')
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


def insert_dataframe_timestamp_psql(dataframe, psql_table_name, psql_engine):
    """ Write TSX symbols dataframe to postgresql
    Will write the table to postgresql database as well as create a timestamped version of it.

    Args:
        dataframe (dataframe): name of the pandas dataframe to insert in postgres
        psql_engine (sqlalchemy.engine): sqlalchemy connection engine
        psql_table_name (str): name of the table to write to


    """
    print(f'Inserting dataframe in {psql_table_name}')
    dataframe.to_sql(name=psql_table_name,
                     con=psql_engine,
                     index=False,
                     if_exists='replace')

    dataframe.to_sql(name=f'{psql_table_name}_{datetime.date(datetime.now())}',
                     con=psql_engine,
                     index=False)

    print(f'Insertion success!')


def insert_additional_ticker_info_psql(ticker_symbols, psql_table_name, psql_engine, qt_api):
    """ Inserts additonal ticker information to postgres
    Will write additional information obtained from QTApi.symbols_info() method to postgresql table for all tickers
    provided in iterable

    Args:
        ticker_symbols (iterable): iterable containg ticker symbols (str)
        psql_table_name (str): name of the postgresql table to write to
        psql_engine (sqlalchemy.egnine): connection engine to postgresql
        qt_api (QTApi): questrade_api.QTApi object to call additional ticker info with

    """

    print('Getting additional ticker information')

    if_exists = 'replace'
    for symbol in tqdm(ticker_symbols):
        search_response = qt_api.symbols_search(f'{symbol}.TO')
        if not search_response['symbols']:
            continue
        info_response = qt_api.symbols_info(search_response['symbols'][0]['symbolId'])
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

        info_df.to_sql(name=psql_table_name,
                       con=psql_engine,
                       if_exists=if_exists,
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

        if_exists = 'append'

    print(f'Additional information written to {psql_table_name}')


if __name__ == '__main__':

    # Will write to postgres scraped tsx ticker symbol as well as additional information for each ticker
    parser = ConfigParser()
    parser.read('config.ini')
    config = parser._sections

    tsx_config = config['tsx_website']
    sql_config = config['postgresql_server']
    qt_config = config['questrade_api']

    tsx_symbols = scrape_symbols_tsx_website(tsx_company_directory_url=tsx_config['tsx_company_directory_url'],
                                             webdriver_exec_path=tsx_config['webdriver_exec_path'])

    engine = create_engine(f'postgresql://{sql_config["user"]}:{sql_config["pwd"]}'
                           f'@{sql_config["local_url"]}/{tsx_config["schema"]}')

    insert_dataframe_timestamp_psql(dataframe=tsx_symbols,
                                    psql_table_name=tsx_config['tsx_symbols_table_name'],
                                    psql_engine=engine
                                    )

    qt_api = QTApi(qt_config['token'])
    insert_additional_ticker_info_psql(ticker_symbols=tsx_symbols['symbol'],
                                       psql_table_name='symbol_info',
                                       psql_engine=engine,
                                       qt_api=qt_api)

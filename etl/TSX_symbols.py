import time
from datetime import datetime
import pandas as pd
from configparser import ConfigParser
from selenium import webdriver
from sqlalchemy import create_engine
from progress.bar import FillingSquaresBar


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

    bar = FillingSquaresBar('Extracting', max=len(rows))

    for row in rows[1:]:
        cols = row.find_elements_by_tag_name('td')
        to_append = [None, None]

        for i in range(len(cols)):
            to_append[i] = cols[i].text.lstrip()

        extracted_table = extracted_table.append(
            {'company_name': to_append[0],
             'symbol': to_append[1]},
            ignore_index=True)

        bar.next()

    driver.close()
    bar.finish()

    final_table = extracted_table.loc[~extracted_table['symbol'].isnull(), :]

    return final_table


def insert_tsx_symbols_postgresql(tsx_symbol_table,
                                  user_name,
                                  password,
                                  postgresl_url='localhost:5432',
                                  schema_name='questrade_api',
                                  table_name='tsx_symbols'):
    engine = create_engine(f'postgresql://{user_name}:{password}@{postgresl_url}/{schema_name}')

    tsx_symbol_table.to_sql(name=table_name,
                            con=engine,
                            index=False,
                            if_exists='replace')

    tsx_symbol_table.to_sql(name=f'{table_name}_{datetime.date(datetime.now())}',
                            con=engine,
                            index=False)


if __name__ == '__main__':
    parser = ConfigParser()
    parser.read('config.ini')
    config = parser._sections

    tsx_config = config['tsx_website']
    sql_config = config['postgresql_server']

    tsx_symbols = scrape_symbols_tsx_website(tsx_company_directory_url=tsx_config['tsx_company_directory_url'],
                                             webdriver_exec_path=tsx_config['webdriver_exec_path'])

    insert_tsx_symbols_postgresql(tsx_symbol_table=tsx_symbols,
                                  user_name=sql_config['user'],
                                  password=sql_config['pwd'],
                                  postgresl_url=sql_config['local_url'],
                                  schema_name=tsx_config['schema'],
                                  table_name=tsx_config['postgres_table_name']
                                  )

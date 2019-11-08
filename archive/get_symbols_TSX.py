from selenium import webdriver
from bs4 import BeautifulSoup
import time
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

engine = create_engine('postgresql://python_user:qwerty@localhost:5432/questrade_api', )

driver = webdriver.Chrome(executable_path='/usr/lib/chromium-browser/chromedriver')
driver.get("https://www.tmxmoney.com/en/research/listed_company_directory.html")
elem = driver.find_element_by_id("SearchKeyword")
elem.clear()
elem.send_keys("^")
button = driver.find_element_by_id("btn-search")
driver.execute_script("arguments[0].click();", button)
time.sleep(5)

soup = BeautifulSoup(driver.page_source, 'html.parser')

table = driver.find_element_by_id('tresults')
rows = table.find_elements_by_tag_name('tr')

extracted_table = pd.DataFrame(columns=['company_name', 'symbol'])

for row in rows[1:]:
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
final_table.to_sql(name='tsx_symbols',
                   con=engine,
                   index=False,
                   if_exists='replace')
final_table.to_sql(name=f'tsx_symbols_{datetime.date(datetime.now())}',
                   con=engine,
                   index=False)

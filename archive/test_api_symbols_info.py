import pandas as pd
from tqdm import tqdm
from datetime import datetime
from sqlalchemy import create_engine, types

from questrade_api import QTApi

engine = create_engine(f'postgresql://python_user:qwerty2@localhost:5432/questrade_api')

tsx_symbols = pd.read_sql_table(table_name='tsx_symbols',
                                con=engine)

QT_api = QTApi('ir6cM0zZy67MpOJefc8bM8d_9wBX2_kl0')

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

    info_df.to_sql(name='symbol_info',
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

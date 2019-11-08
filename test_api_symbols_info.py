import pandas as pd
from sqlalchemy import create_engine, MetaData
from sqlalchemy.sql import select
from questrade_api import QTApi

engine = create_engine(f'postgresql://python_user:qwerty@localhost:5432/questrade_api')

tsx_symbols = pd.read_sql_table(table_name='tsx_symbols',
                                con=engine)

QT_api = QTApi('VwOscxIADJ6bwqPn_wX9wt7D2q_g9j_60')

symbol_search = f'{tsx_symbols.iloc[0, 1]}.TO'
search_response = QT_api.symbols_search(f'{tsx_symbols.iloc[0, 1]}.TO')
info_response = QT_api.symbols_info(search_response['symbols'][0]['symbolId'])

pd.DataFrame(info_response['symbols'][0])


# conn = engine.connect()
# metadata = MetaData(engine)
# metadata.reflect()
#
# tsx_symbols = metadata.tables['tsx_symbols']
# selection_query = tsx_symbols.select()
# result = conn.execute(selection_query)
#
# for row in result:
#     print(row)



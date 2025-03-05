import sqlite3
import pandas as pd

columns_to_select = ['trade_id', 'strategy', 'instrument_type', 'stop_price']
df = pd.read_csv('../migration/files/trades.csv', usecols=columns_to_select)

print(df)
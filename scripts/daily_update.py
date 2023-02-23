import os
import json
import datetime
import pandas as pd
from utils import log
from utils import database_engine
from get_quotes import get_quotes
from get_ticker_data import get_ticker_data

# create a connection to the PostgreSQL database
engine = database_engine()
conn = engine.connect()
business_dates = pd.read_sql('market_callender', conn)['Date']
conn.commit()

os.chdir('/home/naveen/code/financial_data_analysis/')
# file_path = sys.argv[1]

file_path = './watchlist.json'
global watch_list
global watch_list_name
watch_list_name = os.path.basename(file_path).split('.')[0]

with open(file_path) as f:
    watch_list = json.load(f)

if datetime.date.today() in business_dates:
    # Quotes and Related data for Tickers in given Json File.
    for sector, value in watch_list.items():
        for ticker in value:
            get_quotes(watch_list=watch_list_name, sector=sector, ticker=ticker)
            get_ticker_data(watch_list=watch_list_name, sector=sector,ticker=ticker)

    log(message='Quotes & Ticker Data')
    
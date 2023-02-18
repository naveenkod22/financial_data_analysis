import os
import json
import logging
import datetime
from get_quotes import get_quotes
from get_ticker_data import get_ticker_data

os.chdir('/home/naveen/code/financial_data_analysis/')
# file_path = sys.argv[1]

file_path = './watchlist.json'
global watch_list
global watch_list_name
watch_list_name = os.path.basename(file_path).split('.')[0]

with open(file_path) as f:
    watch_list = json.load(f)
    
# Quotes and Related data for Tickers in given Json File.
for sector, value in watch_list.items():
    for ticker in value:
        get_quotes(watch_list=watch_list_name, sector=sector, ticker=ticker)
        get_ticker_data(watch_list=watch_list_name, sector=sector,ticker=ticker)

timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
logging.basicConfig(filename='logs.log', level=logging.INFO)
logging.info("; Quotes & Ticker Data ; {timestamp}".format(timestamp=timestamp))
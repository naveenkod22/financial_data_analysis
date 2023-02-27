import os
import json
import datetime
import pandas as pd
from utils import log
from utils import conn_url
from get_quotes import update_quotes
from sqlalchemy import create_engine
from utils import get_sql_data
from get_ticker_data import get_ticker_data

conn = create_engine(conn_url()).connect()

business_dates = get_sql_data("SELECT * FROM market_calender")
business_dates = [i[0] for i in business_dates]

tickers = get_sql_data("SELECT ticker FROM  fact_tickers")
tickers = [i[0] for i in tickers]


if datetime.date.today() in business_dates:
    for ticker in tickers:
        update_quotes(ticker=ticker, conn=conn)
        get_ticker_data(ticker=ticker, conn = conn)
        log(message='Quotes & Ticker Data')

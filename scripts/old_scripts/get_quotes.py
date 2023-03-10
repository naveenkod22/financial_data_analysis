import os
import datetime
import pandas as pd
import yfinance as yf
from utils import conn_url
from sqlalchemy import create_engine
from numpy import number as np_number

cols_dict = {'Ticker':'ticker', 'Date':'quote_date', 'Open':'open_price', 
            'High':'high_price', 'Low':'low_price', 'Close':'close_price', 
            'Adj Close':'adj_close_price', 'Volume':'volume', 
            'Dividends':'dividend', 'Stock Splits': 'splits'}

def get_quotes(ticker, conn):
    quote = yf.download(ticker, period = 'max', interval = "1d", actions = True)
    quote.reset_index(drop=False, inplace = True)
    quote["Date"] = pd.to_datetime(quote['Date'], utc=True).dt.date
    quote['ticker'] = ticker
    quote[quote.select_dtypes(include=[np_number]).columns] = quote.select_dtypes(include=[np_number]).apply(lambda x: round(x, 4))  
    quote.reset_index()   
    quote.rename(columns=cols_dict, inplace=True)        
    quote.to_sql('dim_quotes', conn, if_exists='append', index=False)

def update_quotes(ticker,conn):
    start = datetime.datetime.today()-datetime.timedelta(5)
    end  = datetime.datetime.today()
    quote = yf.download(ticker, start=start, end=end, interval = "1d", actions = True)
    quote.reset_index(drop=False, inplace = True)
    quote["Date"] = pd.to_datetime(quote['Date'], utc=True).dt.date
    quote['ticker'] = ticker
    quote[quote.select_dtypes(include=[np_number]).columns] = quote.select_dtypes(include=[np_number]).apply(lambda x: round(x, 4))
    quote.reset_index()
    quote.rename(columns=cols_dict, inplace=True)
    quote.to_sql('dim_quotes', conn, if_exists='append', index=False)

    # else:
    #     df = pd.read_csv(path)
    #     start = datetime.datetime.strptime(df['Date'].iloc[-1], "%Y-%m-%d")
    #     start = start+datetime.timedelta(1)
    #     end  = datetime.datetime.today()

    #     quote = yf.download(ticker, start=start, end=end, interval = "1d", actions = True)
    #     quote.reset_index(drop=False, inplace = True)
    #     quote["Date"] = pd.to_datetime(quote['Date'], utc=True).dt.date
    #     quote[quote.select_dtypes(include=[np_number]).columns] = quote.select_dtypes(include=[np_number]).apply(lambda x: round(x, 4))
        
    #     df = pd.concat([df, quote], axis=0, ignore_index=False)
    #     df.reset_index()
    #     df.to_csv(path, index = False)

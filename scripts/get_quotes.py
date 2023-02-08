import os
import datetime
import pandas as pd
import yfinance as yf
from numpy import number as np_number

def get_quotes(watch_list, sector,ticker):
    path = "./data/{watch_list}/{sector}/{ticker}/{ticker}.csv".format(watch_list = watch_list, sector=sector,ticker=ticker)
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)

        quote = yf.download(ticker, period = 'max', interval = "1d", actions = True)
        quote.reset_index(drop=False, inplace = True)
        quote["Date"] = pd.to_datetime(quote['Date'], utc=True).dt.date
        quote[quote.select_dtypes(include=[np_number]).columns] = quote.select_dtypes(include=[np_number]).apply(lambda x: round(x, 4))  
        quote.reset_index()           
        quote.to_csv(path, index=True)

    else:
        df = pd.read_csv(path)
        start = datetime.datetime.strptime(df['Date'].iloc[-1], "%Y-%m-%d")
        start = start+datetime.timedelta(1)
        end  = datetime.datetime.today()

        quote = yf.download(ticker, start=start, end=end, interval = "1d", actions = True)
        quote.reset_index(drop=False, inplace = True)
        quote["Date"] = pd.to_datetime(quote['Date'], utc=True).dt.date
        quote[quote.select_dtypes(include=[np_number]).columns] = quote.select_dtypes(include=[np_number]).apply(lambda x: round(x, 4))
        
        df = pd.concat([df, quote], axis=0, ignore_index=False)
        df.reset_index()
        df.to_csv(path, index = False)

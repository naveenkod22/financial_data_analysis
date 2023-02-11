import os
import json
import time
import datetime
import pandas as pd
from finvizfinance import quote

os.chdir('C://Users//navee//Documents//code//financial_data_analysis/')
global base_path

def _transform_save_rating(df,path):
    if df['Rating'].str.contains(" → ").any():
        df[['Previous_ratting', 'Current_ratting']] = df['Rating'].str.split(" → ", expand=True)
        df['Current_ratting'].fillna(df['Previous_ratting'], inplace=True)
    else:
        df['Current_Rating'] = df['Rating']
        df['Previous_rating'] = df['Rating']
    
    if df['Price'].str.contains(" → ").any():    
        df[['Previous_price', 'Current_price']] = df['Price'].str.split(" → ", expand=True)
        df[['Previous_price', 'Current_price']] = df[['Previous_price', 'Current_price']].apply(lambda x: x.str.replace('$', '',regex=False))
        df['Current_price'].fillna(df['Previous_price'], inplace=True)
    else:
        df['Current_price'] = df['Price']
        df['Previous_price'] = df['Price']       
    
    
    df.drop(columns=['Rating', 'Price'], inplace=True)
    if os.path.exists(path):
        ratings = pd.read_csv(path)
        ratings['Date'] = pd.to_datetime(ratings['Date'], format='%Y-%m-%d')
        df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
        ratings = pd.concat( [df,ratings],ignore_index=True)
        ratings.drop_duplicates(subset = ['Date','Status','Outer'],inplace=True)
        ratings.reset_index(inplace=True,drop=True)
        ratings.to_csv(path, index = False)
    else:
        df.to_csv(path, index = False)

def _transform_save_news(df,path):
    df['Source'] = df['Link'].str.extract(r'//(.*?).com/')
    if os.path.exists(path):
        news = pd.read_csv(path)
        news = pd.concat([df,news], ignore_index=True)
        news = news.drop_duplicates(subset=['Title'])
        news.reset_index(inplace=True, drop=True)
        news.to_csv(path, index = False)
    else:
        df.to_csv(path, index = False)

def _transform_save_inside_trade(df,path):
    df.to_csv(path, index=False)
    if os.path.exists(path):
        inside_trade = pd.read_csv(path)
        inside_trade = pd.concat( [df,inside_trade],ignore_index=True)
        inside_trade = inside_trade.drop_duplicates()
        inside_trade.reset_index(inplace=True,drop=True)
        inside_trade.to_csv(path, index = False)
    else:
        df.to_csv(path, index = False)
 

# This function gets Ticker description, Inside Trade,  Fundamentals, Daily chart, Ticker Rating, Ticker News
def _get_ticker_data(watch_list, sector, ticker):
    base_path = "./data/{watch_list}/{sector}/{ticker}/".format(watch_list = watch_list,sector=sector,ticker=ticker)
    os.makedirs(os.path.dirname(base_path), exist_ok=True)

    # Setting tick variable to get the data
    tick = quote.finvizfinance(ticker=ticker)

# Ticker Description
    path = "{base_path}{ticker}_description.txt".format(base_path=base_path, ticker=ticker)
    if not os.path.exists(path):
        ticker_description = tick.ticker_description()
        time.sleep(0.1)
        with open(path, "w") as file:
            file.write(ticker_description)

# Ticker Chart
    path = "{base_path}charts/".format(base_path=base_path)
    os.makedirs(path, exist_ok=True)
    tick.ticker_charts(out_dir=path)
    time.sleep(0.1)
    date = str(datetime.datetime.now())[0:10].replace("-", "")
    src = "{path}{ticker}.jpg".format(path = path, ticker=ticker)
    dst = "{path}{ticker}_{date}.jpg".format(path = path, ticker=ticker, date=date)
    os.replace(src,dst)

    try:
# Ticker Full Info.
        full_info = tick.ticker_full_info()
        time.sleep(0.1)
        for key in full_info.keys():
            
            # Ticker Fundament
            if key == 'fundament':
                path = "{base_path}{ticker}_fundament.json".format(base_path=base_path, ticker=ticker)
                if not os.path.exists(path):
                    fundament = full_info['fundament']
                    with open(path, 'w') as f:
                        json.dump(fundament,f,indent=4)

            # Ticker Ratting
            if key == 'ratings_outer':
                path = "{base_path}{ticker}_ratting.csv".format(base_path=base_path, ticker=ticker)
                ratings_outer = full_info['ratings_outer']
                _transform_save_rating(ratings_outer,path=path)
            
            # Ticker News
            if key == 'news':
                path = "{base_path}{ticker}_news.csv".format(base_path=base_path, ticker=ticker)
                news = full_info['news']
                _transform_save_news(df=news,path=path)

            # Inside Trade
            if key == 'inside trader':
                path = "{base_path}{ticker}_inside_trade.csv".format(base_path=base_path, ticker=ticker)
                inside_trade = full_info['inside trader']
                _transform_save_inside_trade(df=inside_trade,path=path)
            
    except AttributeError:
        fundament = tick.ticker_fundament()
        time.sleep(0.1)
        path = "{base_path}{ticker}_fundament.json".format(base_path=base_path, ticker=ticker)
        if not os.path.exists(path):
            with open(path, 'w') as f:
                json.dump(fundament,f,indent=4)

        ratings_outer = tick.ticker_outer_ratings()
        time.sleep(0.1)
        path = "{base_path}{ticker}_ratting.csv".format(base_path=base_path, ticker=ticker)
        _transform_save_rating(ratings_outer,path=path)

        news = tick.ticker_news()
        path = "{base_path}{ticker}_news.csv".format(base_path=base_path, ticker=ticker)
        _transform_save_news(df=news,path=path)

def get_ticker_data(watch_list, sector, ticker):
    try:
        _get_ticker_data(watch_list, sector, ticker)
    except ConnectionError:
        print('Connection Error Occurred for Get Data')
        _get_ticker_data(watch_list, sector, ticker)
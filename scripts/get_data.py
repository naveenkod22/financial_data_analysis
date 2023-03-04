import pandas as pd
import datetime
import time
import yfinance as yf
from numpy import number as np_number
from finvizfinance import quote
from finvizfinance.news import News
from finvizfinance.insider import Insider
from finvizfinance.calendar import Calendar
from transform_load_data import TransformLoad


class GetData():
    def __init__(self, conn_url):
        self.transform_load = TransformLoad(conn_url = conn_url)
    
    # This function gets Ticker description, Inside Trade,  Fundamentals, Ticker Rating, Ticker News
    def update_ticker_data(self, ticker, max_retries=2):
        tick = quote.finvizfinance(ticker=ticker)
        try:
            try:
                full_info = tick.ticker_full_info()
                time.sleep(0.1)
                for key in full_info.keys():
                    if key == 'fundament':
                        dct = full_info['fundament']
                        self.transform_load.transform_load_fundament(dct=dct,ticker=ticker)

                    elif key == 'ratings_outer':
                        ratings_outer = full_info['ratings_outer']
                        self.transform_load.transform_load_ratings(ratings_outer,ticker=ticker)
                    
                    elif key == 'news':
                        news = full_info['news']
                        self.transform_load.transform_load_tick_news(df=news,ticker=ticker)

                    elif key == 'inside trader':
                        inside_trade = full_info['inside trader']
                        self.transform_load.transform_load_tick_inside_trade(df=inside_trade,ticker=ticker)
                  
            except AttributeError:
                dct = tick.ticker_fundament()
                time.sleep(0.1)
                self.transform_load.transform_load_fundament(dct=dct,ticker=ticker)

                ratings_outer = tick.ticker_outer_ratings()
                time.sleep(0.1)
                self.transform_load.transform_load_ratings(ratings_outer,ticker=ticker)

                news = tick.ticker_news()
                time.sleep(0.1)
                self.transform_load.transform_load_tick_news(df=news,ticker=ticker)

        except ConnectionError:
            if max_retries > 0:
                self.update_ticker_data(ticker, max_retries-1)
            else:
                print("Connection Error")


    # This function gets Ticker Quote

    def update_ticker_quotes(self, ticker):
        start = datetime.datetime.today() - datetime.timedelta(days=1)
        end = datetime.datetime.today()
        df = yf.download(ticker, start=start, end=end, interval='1d', actions=True)
        self.transform_load.transform_load_quotes(df=df, ticker=ticker)
        

    def get_ticker_quotes(self, ticker):
        df = yf.download(ticker, period = 'max', interval = "1d", actions = True)
        self.transform_load.transform_load_quotes(df=df, ticker=ticker) 


    # This function gets News, Blogs, Calendar, Insider
    def update_calendar(self):
        calendar = Calendar().calendar()
        time.sleep(0.1)
        self.transform_load.transform_load_calendar(df=calendar)


    def update_news_blogs(self):
        news_blogs = News().get_news()
        time.sleep(0.1)
        self.transform_load.transform_load_news_blogs(dct = news_blogs)
        
    def update_insider(self):
        insider = Insider().get_insider()
        time.sleep(0.1)
        self.transform_load.transform_load_insider_trades(df=insider)

import requests
import time
import os
import configparser
import pandas as pd
from finvizfinance import quote
from finvizfinance.news import News
from finvizfinance.insider import Insider
from finvizfinance.calendar import Calendar
from transform_load_data import TransformLoad
from finvizfinance.screener.overview import Overview

os.chdir('/home/naveen/code/financial_data_analysis/')

class GetData():
    def __init__(self, conn_url):
        self.transform_load = TransformLoad(conn_url = conn_url)
    
    # This function gets Ticker description, Inside Trade,  Fundamentals, Ticker Rating, Ticker News
    def update_ticker_data(self, ticker):
        tick = quote.finvizfinance(ticker=ticker)
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
                    self.transform_load.transform_load_dim_news(df=news,ticker=ticker)

                elif key == 'inside trader':
                    inside_trade = full_info['inside trader']
                    self.transform_load.transform_load_dim_inside_trade(df=inside_trade,ticker=ticker)
                
        except AttributeError:
            dct = tick.ticker_fundament()
            time.sleep(0.1)
            self.transform_load.transform_load_fundament(dct=dct,ticker=ticker)

            ratings_outer = tick.ticker_outer_ratings()
            time.sleep(0.1)
            self.transform_load.transform_load_ratings(ratings_outer,ticker=ticker)

            news = tick.ticker_news()
            time.sleep(0.1)
            self.transform_load.transform_load_dim_news(df=news,ticker=ticker)


    def update_fact_tickers(self, ticker):
        tick = quote.finvizfinance(ticker=ticker)
        description = tick.ticker_description()
        time.sleep(0.1)
        fundament = tick.ticker_fundament()
        time.sleep(0.1)
        self.transform_load.transform_load_fact_tickers(ticker=ticker, description=description, fundament=fundament)

    # This function gets Ticker Quotes
    def update_ticker_quotes(self, ticker):
        self.transform_load.transform_load_quotes(ticker=ticker) 

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

    # financial statements
    def update_financial_statements(self, ticker):
        credentials = configparser.ConfigParser()
        credentials.read('./scripts/config.ini')
        api_key = credentials.get('alphavintage', 'api_key')
        functions = ['BALANCE_SHEET', 'CASH_FLOW', 'INCOME_STATEMENT']
        for function in functions:
            url = "https://www.alphavantage.co/query?function={function}&symbol={ticker}&apikey={api_key}".format(function=function, ticker=ticker, api_key=api_key)
            data = requests.get(url)
            data = data.json()
            if function == 'BALANCE_SHEET':
                self.transform_load.transform_load_balance_sheets(data=data, ticker=ticker)
            elif function == 'CASH_FLOW':
                self.transform_load.transform_load_cash_flows(data=data, ticker=ticker)
            elif function == 'INCOME_STATEMENT':
                self.transform_load.transform_load_income_statements(data=data, ticker=ticker)

    # earnings
    def update_earnings(self, ticker):
        credentials = configparser.ConfigParser()
        credentials.read('./scripts/config.ini')
        api_key = credentials.get('alphavintage', 'api_key')
        function = 'EARNINGS'
        url = "https://www.alphavantage.co/query?function={function}&symbol={ticker}&apikey={api_key}".format(function=function, ticker=ticker, api_key=api_key)
        data = requests.get(url)
        data = data.json()
        self.transform_load.transform_load_earnings(data=data, ticker=ticker)


    # This function gets screener data
    def update_screeners(self):
        signals = Overview().get_signal()
        signals_df = pd.DataFrame()
        for i in signals:
            screener = Overview()
            screener.set_filter(signal=i)
            one_signal_df = screener.screener_view(verbose=False)
            try:
                one_signal_df['signal'] = i
                signals_df = pd.concat([signals_df, one_signal_df], ignore_index=True)
                time.sleep(0.1)
            except TypeError:
                pass

        self.transform_load.transform_load_screener_data(df=signals_df)
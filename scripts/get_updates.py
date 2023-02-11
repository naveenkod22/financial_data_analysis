import os
import time
import datetime
import pandas as pd
from finvizfinance.news import News
from finvizfinance.insider import Insider
from finvizfinance.calendar import Calendar


os.chdir('C://Users//navee//Documents//code//financial_data_analysis/')

base_path = "./data/"

def _add_year_to_date(df):
    year = str(pd.to_datetime('today').year)
    df['Date'] = df['Date']+ " " +year 
    df['Date'] = pd.to_datetime(df['Date'], format='%b %d %Y', errors='coerce')

    if (datetime.datetime.now().month in [1,2,3]):
        df['Date'] = df['Date'].apply(lambda x: x.replace(year=datetime.datetime.now().year-1) 
                                      if (x - datetime.datetime.now()).days > 120 
                                      else x.replace(year=datetime.datetime.now().year))
    if (datetime.datetime.now().month in [10,11,12]):
        df['Date'] = df['Date'].apply(lambda x: x.replace(year=datetime.datetime.now().year+1) 
                                      if (datetime.datetime.now() - x).days > 200 
                                      else x.replace(year=datetime.datetime.now().year))
    return df

def update_calendar():
    path = "{base_path}calendar.csv".format(base_path=base_path)
    calendar = Calendar().calendar()
    time.sleep(0.1)
    if not os.path.exists(path):
        calendar.to_csv(path,index=False)
    else:
        df = pd.read_csv(path)
        calendar = pd.concat([calendar,df], ignore_index=True)
        calendar = calendar.drop_duplicates(subset=['Datetime', 'Release'], keep='first')
        calendar.to_csv(path, index=False)
    print('Calendar updated')

def update_news():
    path = "{base_path}news.csv".format(base_path=base_path)
    news_blogs = News().get_news()
    time.sleep(0.1)
    news = news_blogs['news']
    news['News_type'] = 'News'
    blogs = news_blogs['blogs']
    blogs['News_type'] = 'Blogs'
    news = pd.concat([news, blogs], ignore_index=True)
    if not os.path.exists(path):
        news.to_csv(path,index=False)
    else:
        df = pd.read_csv(path)
        news = pd.concat([news,df], ignore_index=True)
        news = news.drop_duplicates(subset=['Date', 'Title'], keep='first')
        news.to_csv(path, index=False)
    print('News updated')


def update_insider():
    path = "{base_path}insider.csv".format(base_path=base_path)
    insider = Insider().get_insider()
    time.sleep(0.1)
    insider = _add_year_to_date(insider)
    if not os.path.exists(path):
        insider.to_csv(path,index=False)
    else:
        df = pd.read_csv(path)
        insider = pd.concat([insider,df], ignore_index=True)
        insider = insider.drop_duplicates(subset= ['Ticker','Owner','Relationship','Transaction','Cost','#Shares','Value ($)'], keep='first')
        insider.to_csv(path, index=False)
    print('Insider updated')
    


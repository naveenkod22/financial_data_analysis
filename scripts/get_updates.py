import os
import time
import logging
import datetime
import pandas as pd
from finvizfinance.news import News
from finvizfinance.insider import Insider
from finvizfinance.calendar import Calendar


os.chdir('/home/naveen/code/financial_data_analysis/')

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

def _format_date(df):
    df['Date'] = df['Date'].str.extract('(\d\d\:\d\d\w{2})', expand=False)
    df.dropna(subset=['Date'],inplace=True)
    today = datetime.datetime.now().date().strftime('%Y-%m-%d')
    df['Date'] =  today + '-' + df['Date']
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d-%I:%M%p', errors='coerce')
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


news_path = "{base_path}news.csv".format(base_path=base_path)
blogs_path = "{base_path}blogs.csv".format(base_path=base_path)
news_blogs = News().get_news()
time.sleep(0.1)

def update_news_blogs(df=news_blogs, news_path = news_path, blogs_path = blogs_path):
    paths = {'news': news_path, 'blogs':blogs_path}
    for key, path in paths.items():
        df_temp = df[key]
        df_temp = _format_date(df_temp)
        if not os.path.exists(path):
            df_temp.to_csv(path,index=False)
        else:
            df_old = pd.read_csv(path)
            df_temp = df[key]
            df_temp = pd.concat([df_temp,df_old], ignore_index=True)
            df_temp = df_temp.drop_duplicates(subset=['Title'], keep='first')
            df_temp.to_csv(path, index=False)
    print('News and Blogs updated')
    

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

if __name__ == "__main__":
    try:    
        update_insider()
        update_calendar()
        update_news_blogs()
    except ConnectionError:
        update_insider()
        update_calendar()
        update_news_blogs()

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.basicConfig(filename='logs.log', level=logging.INFO)
    logging.info("; News, Blogs, Insider, Calender Data ; {timestamp}".format(timestamp=timestamp))
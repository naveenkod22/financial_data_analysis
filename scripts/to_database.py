from sqlalchemy import text
import pandas as pd
import glob
import os
from utils import database_engine

engine = database_engine()
conn = engine.connect()

# TO FACT_TICKER TABLE
files = glob.glob('/home/naveen/code/financial_data_analysis/data/watchlist/**/*fundament*.csv', recursive=True)
fundamentals_df = pd.DataFrame()
for file in files:
    df = pd.read_csv(file)
    df['Ticker'] = os.path.basename(file).split('_')[0]
    fundamentals_df = pd.concat([fundamentals_df, df], ignore_index=True)

fact_ticker_df = fundamentals_df[['Ticker', 'Sector', 'Industry', 'Country', 'Index']]
fact_ticker_df =  fact_ticker_df.drop_duplicates(subset=['Ticker'])

files = glob.glob('/home/naveen/code/financial_data_analysis/data/watchlist/**/*descrip*.txt', recursive=True)

def text(path):
    with open(path, 'r') as f:
        return f.read()
desc_series = pd.Series(dtype='string')
description_df = pd.DataFrame()
for file in files:
    desc = text(file)
    ticker = os.path.basename(file).split('_')[0]
    desc_series[ticker] = desc


fact_ticker_df.set_index('Ticker', inplace=True)
fact_ticker_df['Description'] = desc_series
fact_ticker_df.reset_index(inplace=True)
fact_ticker_df.head()
fact_ticker_df.rename(columns={'Ticker':'ticker', 'Description':'description',
                               'Country':'country','Index':'index', 'Sector': 'sector', 'Industry':'industry'}, inplace=True)

fact_ticker_df.to_sql('fact_tickers', conn, if_exists='append', index=False)
conn.commit()

# TO QUOTES TABLE
from utils import database_engine
from sqlalchemy import text
import pandas as pd
import glob
import os
import json

file_path = '/home/naveen/code/financial_data_analysis/watchlist.json'
with open(file_path) as f:
    watch_list = json.load(f)
type(watch_list)

quotes = pd.DataFrame()
for sector, value in watch_list.items():
    for ticker in value:
        path = "./data/{watch_list}/{sector}/{ticker}/{ticker}.csv".format(watch_list = 'watchlist', sector=sector,ticker=ticker)
        df = pd.read_csv(path)
        df['Ticker'] = ticker
        quotes = pd.concat([quotes, df], ignore_index=True)

quotes.rename(columns={'Ticker':'ticker', 'Date':'quote_date', 'Open':'open_price', 
                        'High':'high_price', 'Low':'low_price', 'close':'close_price', 
                        'Adj Close':'adj_close_price', 'Volume':'volume', 'Dividends':'dividend', 
                        'Stock Splits': 'splits'}, inplace=True)

engine = database_engine()
conn = engine.connect()
quotes.to_sql('dim_quotes', conn, if_exists='replace', index=False)
conn.commit()

# TO NEWS TABLE
from sqlalchemy import text
import pandas as pd
import glob
import os
from utils import database_engine

engine = database_engine()
conn = engine.connect()

files = glob.glob('/home/naveen/code/financial_data_analysis/data/watchlist/**/*news.csv', recursive=True)
news_df = pd.DataFrame()
for file in files:
    df = pd.read_csv(file)
    df['ticker'] = os.path.basename(file).split('_')[0]
    news_df = pd.concat([news_df, df], ignore_index=True)

source = news_df['source']

news_df['Source'].fillna(source, inplace=True)
news_df.drop(columns=['source'], inplace=True)
news_df.rename(columns={'ticker':'ticker', 'Date':'news_date', 'Title':'news_title', 'Source':'news_source', 'Link':'news_link'}, inplace=True)    
news_df.to_sql('dim_news', conn, if_exists='replace', index=False)
conn.commit()

# TO DIM_RATING TABLE
from sqlalchemy import text
import pandas as pd
import glob
import os
from utils import database_engine

engine = database_engine()
conn = engine.connect()

files = glob.glob('/home/naveen/code/financial_data_analysis/data/watchlist/**/*ratting.csv', recursive=True)
rating_df = pd.DataFrame()
for file in files:
    df = pd.read_csv(file)
    df['ticker'] = os.path.basename(file).split('_')[0]
    rating_df = pd.concat([rating_df, df], ignore_index=True)

rating_df.rename(columns={'Date':'rating_date', 'Status':'rating_status', 'Outer':'rating_agency', 'Previous_rating':'previous_rating', 
                           'Current_rating':'current_rating','Previous_price':'previous_price', 'Current_price':'current_price', 
                           'ticker':'ticker'}, inplace=True)


rating_df.to_sql('dim_ratings', conn, if_exists='replace', index=False)
conn.commit()

# TO INSIDE TRADING TABLE
from sqlalchemy import text
import pandas as pd
import glob
import os
from utils import database_engine

engine = database_engine()
conn = engine.connect()

files = glob.glob('/home/naveen/code/financial_data_analysis/data/watchlist/**/*inside_t*.csv', recursive=True)
inside_df = pd.DataFrame()
for file in files:
    df = pd.read_csv(file)
    df['ticker'] = os.path.basename(file).split('_')[0]
    inside_df = pd.concat([inside_df, df], ignore_index=True)

inside_df.rename(columns={'Insider Trading':'traded_by', 'Relationship':'relationship', 
                        'Date':'trading_date', 'Transaction':'transaction_type', 'Cost':'share_price',
                        '#Shares':'no_of_shares', 'Value ($)':'transaction_value', 
                        '#Shares Total':'shares_total', 'SEC Form 4':'sec_form_4',
                        'SEC Form 4 Link':'sec_form_4_link', 'Insider_id':'insider_id', 'ticker':'ticker'}, inplace=True)

inside_df.to_sql('dim_inside_trades', conn, if_exists='replace', index=False)
conn.commit()

# TO ANNUAL EARNINGS TABLE
from sqlalchemy import text
import pandas as pd
import glob
import os
from utils import database_engine

engine = database_engine()
conn = engine.connect()

files = glob.glob('/home/naveen/code/financial_data_analysis/data/watchlist/**/*annual_earnings*.csv', recursive=True)
earn_df = pd.DataFrame()
for file in files:
    df = pd.read_csv(file)
    df['ticker'] = os.path.basename(file).split('_')[0]
    earn_df = pd.concat([earn_df, df], ignore_index=True)

earn_df.rename(columns={'ticker':'ticker', 'fiscalDateEnding':'fiscal_date_ending', 'reportedEPS':'reported_eps'}, inplace=True)
earn_df.to_sql('dim_annual_earnings', conn, if_exists='append', index=False)
conn.commit()

# TO QUARTERLY EARNINGS TABLE
from sqlalchemy import text
import pandas as pd
import glob
import os
import numpy as np
from utils import database_engine

engine = database_engine()
conn = engine.connect()

files = glob.glob('/home/naveen/code/financial_data_analysis/data/watchlist/**/*quarterly_earnings*.csv', recursive=True)
q_earn_df = pd.DataFrame()
for file in files:
    df = pd.read_csv(file)
    df['ticker'] = os.path.basename(file).split('_')[0]
    q_earn_df = pd.concat([q_earn_df, df], ignore_index=True)

q_earn_df.columns
q_earn_df.rename(columns={'ticker':'ticker', 'fiscalDateEnding':'fiscal_date_ending', 'reportedEPS':'reported_eps',
                        'reportedDate':'reported_date', 'estimatedEPS':'estimated_eps',
                        'surprise':'surprise', 'surprisePercentage':'surprise_percentage'}, inplace=True)
q_earn_df.replace('None', np.nan, inplace=True)
q_earn_df.to_sql('dim_quarterly_earnings', conn, if_exists='append', index=False)
conn.commit()

# TO BALANCE SHEET TABLE


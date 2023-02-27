import time
import pandas as pd
from finvizfinance import quote
from utils import get_sql_table, format_fundamentals



def _transform_save_rating(df,ticker, conn):
    rating_cols = {'Date':'rating_date', 'Status':'rating_status', 'Outer':'rating_agency'}
    
    if df['Rating'].str.contains(" → ").any():
        df[['previous_rating', 'current_rating']] = df['Rating'].str.split(" → ", expand=True)
        df['current_rating'].fillna(df['previous_rating'], inplace=True)
    else:
        df['current_rating'] = df['Rating']
        df['previous_rating'] = df['Rating']
    
    if df['Price'].str.contains(" → ").any():    
        df[['previous_price', 'current_price']] = df['Price'].str.split(" → ", expand=True)
        df[['previous_price', 'current_price']] = df[['previous_price', 'current_price']].apply(lambda x: x.str.replace('$', '',regex=False))
        df['current_price'].fillna(df['previous_price'], inplace=True)
    else:
        df['Price'] = df['Price'].str.replace('$', '',regex=False)
        df['current_price'] = df['Price']
        df['previous_price'] = df['Price']       
    
    df.drop(columns=['Rating', 'Price'], inplace=True)
    df['ticker'] = ticker
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    df.rename(columns=rating_cols, inplace=True)

    ratings = get_sql_table("SELECT * FROM dim_ratings WHERE ticker = '{}'".format(ticker))
    ratings['rating_date'] = pd.to_datetime(ratings['rating_date'], format='%Y-%m-%d')
        
    # selcet rows that are not in the database
    ratings = df[~df[['rating_date', 'rating_status', 'rating_agency']].apply(tuple, 1).isin(ratings[['rating_date', 'rating_status', 'rating_agency']].apply(tuple, 1))]
    ratings.to_sql('dim_ratings', conn, if_exists='replace', index=False)
 
def _transform_save_news(df,ticker, conn):
    df['news_source'] = df['Link'].str.extract(r'//(.*?).com/')
    df['ticker'] = ticker
    news_cols = {'Date':'news_date', 'Title':'news_title', 'Link':'news_link'}
    df.rename(columns=news_cols, inplace=True)
    news = get_sql_table("SELECT * FROM dim_news WHERE ticker = '{}'".format(ticker))
    # selcet rows that are not in the database
    news = df[~df[['news_title']].apply(tuple, 1).isin(news[['news_title']].apply(tuple, 1))]
    news.to_sql('dim_news', conn, if_exists='append', index=False)


def _transform_save_inside_trade(df,ticker, conn):
    df['ticker'] = ticker
    inside_trade_cols = {'Insider Trading':'traded_by', 'Relationship':'relationship', 
                        'Date':'trading_date', 'Transaction':'transaction_type', 'Cost':'share_price',
                        '#Shares':'no_of_shares', 'Value ($)':'transaction_value', 
                        '#Shares Total':'shares_total', 'SEC Form 4':'sec_form_4',
                        'SEC Form 4 Link':'sec_form_4_link', 'Insider_id':'insider_id'}
    df.rename(columns=inside_trade_cols, inplace=True)
    inside_trade = get_sql_table("SELECT * FROM dim_inside_trades WHERE ticker = '{}'".format(ticker))
    # selcet rows that are not in the database
    subset_cols = ['traded_by', 'relationship', 'trading_date', 'transaction_type', 
                   'share_price', 'no_of_shares', 'transaction_value', 'shares_total', 
                   'sec_form_4', 'sec_form_4_link', 'insider_id']
    inside_trade = df[~df[subset_cols].apply(tuple, 1).isin(inside_trade[subset_cols].apply(tuple, 1))]
    inside_trade.to_sql('dim_inside_trades', conn, if_exists='append', index=False)


def _transform_save_fundament(dct,ticker, conn):
    series = pd.Series(dct)
    today = pd.to_datetime('today').strftime('%Y-%m-%d')
    series.name = today
    df = series.to_frame().transpose(copy=True)
    df.reset_index(drop=False, inplace=True)
    df.rename(columns={'index':'Date'}, inplace=True)
    df['ticker'] = ticker
    df = format_fundamentals(df)
    df.to_sql('dim_fundamentals', conn, if_exists='append', index=False)

 

# This function gets Ticker description, Inside Trade,  Fundamentals, Ticker Rating, Ticker News
def _get_ticker_data(ticker, conn):
    tick = quote.finvizfinance(ticker=ticker)

# # Ticker Description
#     path = "{base_path}{ticker}_description.txt".format(base_path=base_path, ticker=ticker)
#     if not os.path.exists(path):
#         ticker_description = tick.ticker_description()
#         time.sleep(0.1)
#         with open(path, "w") as file:
#             file.write(ticker_description)

    try:
# Ticker Full Info.
        full_info = tick.ticker_full_info()
        time.sleep(0.1)
        for key in full_info.keys():
            # Ticker Fundament
            if key == 'fundament':
                dct = full_info['fundament']
                _transform_save_fundament(dct=dct,ticker=ticker, conn=conn)


            # Ticker Ratting
            if key == 'ratings_outer':
                ratings_outer = full_info['ratings_outer']
                _transform_save_rating(ratings_outer,ticker=ticker, conn=conn)
            
            # Ticker News
            if key == 'news':
                news = full_info['news']
                _transform_save_news(df=news,ticker=ticker, conn=conn)

            # Inside Trade
            if key == 'inside trader':
                inside_trade = full_info['inside trader']
                _transform_save_inside_trade(df=inside_trade,ticker=ticker, conn=conn)
            
    except AttributeError:
        dct = tick.ticker_fundament()
        time.sleep(0.1)
        _transform_save_fundament(dct=dct,ticker=ticker, conn=conn)

        ratings_outer = tick.ticker_outer_ratings()
        time.sleep(0.1)
        _transform_save_rating(ratings_outer,ticker=ticker, conn=conn)

        news = tick.ticker_news()
        _transform_save_news(df=news,ticker=ticker, conn=conn)

def get_ticker_data(ticker, conn):
    try:
        _get_ticker_data(ticker, conn)
    
    except ConnectionError:
        print('Connection Error Occurred for Get Data')
        _get_ticker_data(ticker, conn)
    

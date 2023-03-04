import pandas as pd
import numpy as np
import datetime
from numpy import number as np_number
from sqlalchemy import create_engine
from databasse_connection import DatabaseConnection

database_connection = DatabaseConnection()

class TransformLoad():
    def __init__(self, conn_url):
        self.conn = create_engine(conn_url).connect()
        

    def convert_to_number(self, value):
        """Converts a string to a number"""
        value = str(value).replace(',', '')
        if value.endswith('K'):
            return float(value.strip('K')) * 1000
        elif value.endswith('M'):
            return float(value.strip('M')) * 1000_000
        elif value.endswith('B'):
            return float(value.strip('B')) * 1000_000_000
        elif value.endswith('T'):
            return float(value.strip('T')) * 1000_000_000_0000
        elif value.endswith('-'):
            return np.nan
        else:
            return float(value)

    def format_earnings(self, df):
        # earnings to earnings_date and earnings_timing
        df['earnings'].replace('-', np.nan, inplace=True)
        df[['earnings_date', 'earnings_timing']] = df['earnings'].str.extract(r"([a-zA-Z]+\s\d+)\s([a-zA-Z]*)")
        df.drop(columns=['earnings'], inplace=True)
        current_year = str(pd.to_datetime('today').year)
        df['earnings_date'] = df['earnings_date']+ " " +current_year 
        df['earnings_date'] = pd.to_datetime(df['earnings_date'], format='%b %d %Y', errors='coerce')

        if (datetime.datetime.now().month in [1,2,3]):
            df['earnings_date'] = df['earnings_date'].apply(lambda x: x.replace(year=datetime.datetime.now().year-1) \
                                                            if (x - datetime.datetime.now()).days > 120 \
                                                                else x.replace(year=datetime.datetime.now().year))
        if (datetime.datetime.now().month in [10,11,12]):
            df['earnings_date'] = df['earnings_date'].apply(lambda x: x.replace(year=datetime.datetime.now().year+1) \
                                                            if (datetime.datetime.now() - x).days > 200 \
                                                                else x.replace(year=datetime.datetime.now().year))

        df['earnings_date'] = pd.to_datetime(df['earnings_date'], format='%b %d %Y', errors='coerce')
        return df

    def add_year_to_date(self, df, date_col='Date'):
        year = str(pd.to_datetime('today').year)
        df[date_col] = df[date_col]+ " " +year 
        df[date_col] = pd.to_datetime(df[date_col], format='%b %d %Y', errors='coerce')

        if (datetime.datetime.now().month in [1,2,3]):
            df[date_col] = df[date_col].apply(lambda x: x.replace(year=datetime.datetime.now().year-1) 
                                        if (x - datetime.datetime.now()).days > 120 
                                        else x.replace(year=datetime.datetime.now().year))
        if (datetime.datetime.now().month in [10,11,12]):
            df[date_col] = df[date_col].apply(lambda x: x.replace(year=datetime.datetime.now().year+1) 
                                        if (datetime.datetime.now() - x).days > 200 
                                        else x.replace(year=datetime.datetime.now().year))
        return df

    def format_date(self, df, date_col = 'Date'):
        df[date_col] = df[date_col].str.extract('(\d\d\:\d\d\w{2})', expand=False)
        df.dropna(subset=[date_col],inplace=True)
        today = datetime.datetime.now().date().strftime('%Y-%m-%d')
        df[date_col] =  today + '-' + df[date_col]
        df[date_col] = pd.to_datetime(df[date_col], format='%Y-%m-%d-%I:%M%p', errors='coerce')
        return df  
        
    def format_fundamentals(self, df):
        df['Short Float / Ratio'].replace('-', np.nan, inplace=True)
        df[['short_float', 'short_float_ratio']] = df['Short Float / Ratio'].str.split('/', expand=True)
        df.drop(columns=['Short Float / Ratio'], inplace=True)

        df.columns = df.columns.str.lower().str.replace(" ", "_",regex=False).\
                                                    str.replace("/", "_", regex=False).\
                                                    str.replace(".","", regex=False).\
                                                    str.replace("(", "", regex=False).\
                                                    str.replace(")", "", regex=False)

        df.drop(columns=['company', 'sector', 'industry', 'country', 'index'], inplace=True)

        percent_cols =['insider_own', 'perf_week', 'insider_trans','eps_this_y', 'inst_trans', 'perf_half_y', 
                    'eps_next_y_percentage', 'roa', 'perf_year', 'eps_next_5y', 'roe', 'perf_ytd', 'eps_past_5y', 
                    'roi', '52w_high', 'dividend_%', 'sales_past_5y', 'gross_margin', '52w_low', 'sales_q_q', 
                    'oper_margin', 'volatility_w', 'volatility_m', 'eps_q_q', 'profit_margin', 'sma20', 'sma50', 
                    'sma200', 'inst_own', 'perf_quarter', 'perf_month', 'payout', 'short_float']

        new_percent_col_names = [(str(i)+'_percentage') for i in percent_cols]
        df[percent_cols] = df[percent_cols].apply(lambda x: pd.to_numeric(x.str.replace('%', '').str.replace('-','')))
        df.rename(columns=dict(zip(percent_cols, new_percent_col_names)), inplace=True)
        df.rename(columns = {'dividend_%_percentage':'dividend_percentage'}, inplace=True)
        df.rename(columns={'52w_high_percentage':'high_52w_percentage', '52w_low_percentage':'low_52w_percentage'}, inplace=True)

        df['change'] = df['change'].replace('-', np.nan)
        df['change'] = df['change'].apply(lambda x: pd.to_numeric(x.replace('%', '')))
        df.rename(columns={'change':'change_percentage'}, inplace=True)


        float_cols = ['shs_outstand', 'p_e','employees','market_cap','volume', 'shs_float', 'income', 'sales', 'short_interest', 
                    'avg_volume', 'eps_ttm', 'forward_p_e', 'eps_next_y', 'peg', 'eps_next_q', 
                    'p_s', 'book_sh', 'p_b', 'target_price', 'cash_sh', 'p_c', '52w_range_from', 
                    'dividend', 'p_fcf', 'beta', 'quick_ratio', 'current_ratio', 'rsi_14', 'debt_eq', 
                    'lt_debt_eq', 'recom', 'short_float_ratio']
        for col in float_cols:
            if df[col].dtypes == 'object':
                df[col] = df[col].apply(self.convert_to_number)


        df.rename(columns={'52w_range_from':'range_from_52w', '52w_range_to':'range_to_52w'}, inplace=True)

        df = self.format_earnings(df)
        # df.drop(columns=['earnings_date', 'earnings_timing'], inplace=True)

        bool_cols = ['optionable', 'shortable']
        bool_dict = {'Yes': True, 'No': False}
        df[bool_cols] = df[bool_cols].applymap(lambda x: bool_dict[x])
        return df


    def transform_load_ratings(self, df, ticker):
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

        ratings = database_connection.get_sql_table("SELECT * FROM dim_ratings WHERE ticker = '{}'".format(ticker), conn=self.conn)
        ratings['rating_date'] = pd.to_datetime(ratings['rating_date'], format='%Y-%m-%d')
            
        # selcet rows that are not in the database
        ratings = df[~df[['rating_date', 'rating_status', 'rating_agency']].apply(tuple, 1).isin(ratings[['rating_date', 'rating_status', 'rating_agency']].apply(tuple, 1))]
        ratings.to_sql('dim_ratings', self.conn, if_exists='replace', index=False)


    def transform_load_tick_news(self, df,ticker):
        df['news_source'] = df['Link'].str.extract(r'//(.*?).com/')
        df['ticker'] = ticker
        news_cols = {'Date':'news_date', 'Title':'news_title', 'Link':'news_link'}
        df.rename(columns=news_cols, inplace=True)
        news = database_connection.get_sql_table("SELECT * FROM dim_news WHERE ticker = '{}'".format(ticker), conn=self.conn)
        # selcet rows that are not in the database
        news = df[~df[['news_title']].apply(tuple, 1).isin(news[['news_title']].apply(tuple, 1))]
        news.to_sql('dim_news', self.conn, if_exists='append', index=False)

    def transform_load_tick_inside_trade(self, df,ticker):
        df['ticker'] = ticker
        inside_trade_cols = {'Insider Trading':'traded_by', 'Relationship':'relationship', 
                            'Date':'trading_date', 'Transaction':'transaction_type', 'Cost':'share_price',
                            '#Shares':'no_of_shares', 'Value ($)':'transaction_value', 
                            '#Shares Total':'shares_total', 'SEC Form 4':'sec_form_4',
                            'SEC Form 4 Link':'sec_form_4_link', 'Insider_id':'insider_id'}
        df.rename(columns=inside_trade_cols, inplace=True)
        inside_trade = database_connection.get_sql_table("SELECT * FROM dim_inside_trades WHERE ticker = '{}'".format(ticker), conn=self.conn)
        # selcet rows that are not in the database
        subset_cols = ['traded_by', 'relationship', 'trading_date', 'transaction_type', 
                    'share_price', 'no_of_shares', 'transaction_value', 'shares_total', 
                    'sec_form_4', 'sec_form_4_link', 'insider_id']
        inside_trade = df[~df[subset_cols].apply(tuple, 1).isin(inside_trade[subset_cols].apply(tuple, 1))]
        inside_trade.to_sql('dim_inside_trades', self.conn, if_exists='append', index=False)


    def transform_load_fundament(self, dct,ticker):
        series = pd.Series(dct)
        today = pd.to_datetime('today').strftime('%Y-%m-%d')
        series.name = today
        df = series.to_frame().transpose(copy=True)
        df.reset_index(drop=False, inplace=True)
        df.rename(columns={'index':'Date'}, inplace=True)
        df['ticker'] = ticker
        df = self.format_fundamentals(df)
        df.to_sql('dim_fundamentals', self.conn, if_exists='append', index=False)

    def transform_load_calendar(self, df):
        calendar_cols={'datetime':'news_date', 'release':'release_title', 
                        'for':'release_for','prior':'previous'}
        df.columns = df.columns.str.lower()
        current_year = datetime.datetime.now().year

        # add the current year to the input string
        df['datetime'] = df['datetime'] + ", " + str(current_year)
        df['datetime'] = pd.to_datetime(df['datetime'], format='%a %b %d, %I:%M %p, %Y', errors='coerce')

        df.rename(columns = calendar_cols, inplace=True)

        calendar = database_connection.get_sql_table("SELECT * FROM calendar", conn=self.conn)
        calendar.drop(columns=['calendar_id'], inplace=True)
        # select rows that are not in the database
        subset_cols = ['news_date', 'release_title', 'release_for']
        calendar = df[~df[subset_cols].apply(tuple, 1).isin(calendar[subset_cols].apply(tuple, 1))]
        calendar.to_sql('calendar', self.conn, if_exists='append', index=False)


    def transform_load_news_blogs(self, dct):
        tables = ['news', 'blogs']
        for table in tables:
            df = dct[table]
            df = self.format_date(df)
            df.columns = df.columns.str.lower()
            df = df.add_prefix(table + '_')
            database_table = database_connection.get_sql_table("SELECT * FROM {}".format(table), conn=self.conn)
            # select rows that are not in the database
            subset_cols = [table + '_title']
            database_table = df[~df[subset_cols].apply(tuple, 1).isin(database_table[subset_cols].apply(tuple, 1))]
            database_table.to_sql(table, self.conn, if_exists='append', index=False)

    def transform_load_insider_trades(self, df):
        df = self.add_year_to_date(df)
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        insider_cols={'owner':'traded_by', 'date':'trading_date',
                    'transaction':'transaction_type', 'cost':'share_price', 
                    '#shares':'no_of_shares', 'value_($)':'transaction_value', 
                    '#shares_total':'total_shares'}
        df.rename(columns=insider_cols, inplace=True)
        insider_trades = database_connection.get_sql_table("SELECT * FROM inside_trades", conn=self.conn)
        insider_trades.drop(columns=['inside_trade_id'], inplace=True)
        # select rows that are not in the database
        subset_cols = ['traded_by', 'ticker', 'transaction_type', 'transaction_type', 'share_price', 'no_of_shares', 'transaction_value', 'total_shares']
        insider_trades = df[~df[subset_cols].apply(tuple, 1).isin(insider_trades[subset_cols].apply(tuple, 1))]
        insider_trades.to_csv('./insider_trades.csv', index = False)
        insider_trades.to_sql('inside_trades', self.conn, if_exists='append', index=False)
        insider_trades.to_csv('./insider_trades.csv', index = False)

    def transform_load_quotes(self,df, ticker):
        quote_cols = {'Date':'quote_date', 'Open':'open_price', 
                'High':'high_price', 'Low':'low_price', 'Close':'close_price', 
                'Adj Close':'adj_close_price', 'Volume':'volume', 'Dividends':'dividend', 
                'Stock Splits': 'splits'}
        df.reset_index(drop = False, inplace=True)
        df['Date'] = pd.to_datetime(df['Date'], utc = True).dt.date
        df['ticker'] = ticker
        df[df.select_dtypes(include=[np_number]).columns] = df.select_dtypes(include=[np_number]).apply(lambda x: round(x, 4))
        df.rename(columns=quote_cols, inplace=True)
        df.to_sql('dim_quotes', self.conn, if_exists='append', index=False, method='multi')
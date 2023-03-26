import re
import time
import warnings
import pandas as pd
import numpy as np
import datetime
import yfinance as yf
from finvizfinance import quote
from numpy import number as np_number
from sqlalchemy import create_engine
from database_connection import DatabaseConnection

warnings.filterwarnings('ignore')
database_connection = DatabaseConnection()

class TransformLoad():
    """
    This Class transforms and loads data into database, this class is initialized in GetData class
    The connection url is passed to this class from GetData class during initialization.

    Parameters:
    conn_url (str): connection url to database

    Methods:
    convert_to_number(value)
    add_year_to_date(df, date_col='Date')
    format_earnings(df)
    format_date(df, date_col = 'Date')
    format_fundamentals(df)
    format_statements(data, ticker, table)
    transform_load_ratings(df, ticker)
    transform_load_dim_news(df,ticker)
    transform_load_dim_inside_trade(df,ticker)
    transform_load_fundament(dct,ticker)
    transform_load_calendar(df)
    transform_load_news_blogs(dct)
    transform_load_insider_trades(df)
    transform_load_quotes(ticker)
    transform_load_fact_tickers(ticker,description, fundament)
    transform_load_balance_sheets(data, ticker)
    transform_load_cash_flows(data, ticker)
    transform_load_income_statements(data, ticker)
    transform_load_earnings(data, ticker)
    transform_load_screener_data(df)
    """
    def __init__(self, conn_url):
        self.conn = create_engine(conn_url).connect()
        

    def convert_to_number(self, value):
        """
        Converts a string to a number

        Parameters:
        value (str): string to be converted to number
        """
        value = str(value).replace(',', '').replace('%', '').replace('- ', '-').replace('None', '-')
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

    def add_year_to_date(self, df, date_col='Date'):
        """
        Adds year to date column

        Parameters:
        df (dataframe): dataframe with date column
        date_col (str): date column name
        """
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


    def format_earnings(self, df):
        """
        Formats earnings column in dataframe to earnings_date and earnings_timing

        Parameters:
        df (dataframe): dataframe with earnings column
        """
        df['earnings'].replace('-', np.nan, inplace=True)
        df[['earnings_date', 'earnings_timing']] = df['earnings'].str.extract(r"([a-zA-Z]+\s\d+)\s([a-zA-Z]*)")
        df.drop(columns=['earnings'], inplace=True)
        df = self.add_year_to_date(df, date_col='earnings_date')
        df['earnings_date'] = pd.to_datetime(df['earnings_date'], format='%b %d %Y', errors='coerce')
        return df


    def format_date(self, df, date_col = 'Date'):
        """
        Formats date column in dataframe to datetime
        
        Parameters:
        df (dataframe): dataframe with date column
        date_col (str): date column name
        """
        df[date_col] = df[date_col].str.extract('(\d\d\:\d\d\w{2})', expand=False)
        df.dropna(subset=[date_col],inplace=True)
        today = datetime.datetime.now().date().strftime('%Y-%m-%d')
        df[date_col] =  today + '-' + df[date_col]
        df[date_col] = pd.to_datetime(df[date_col], format='%Y-%m-%d-%I:%M%p', errors='coerce')
        return df  


    def format_fundamentals(self, df):
        """
        Formats fundamentals dataframe to match columns of dataframe to database table

        Parameters:
        df (dataframe): dataframe with fundamentals data
        """
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
                    'sma200', 'inst_own', 'perf_quarter', 'perf_month', 'payout', 'short_float', 'change']

        new_percent_col_names = [(str(i)+'_percentage') for i in percent_cols]
        for col in percent_cols:
            if df[col].dtypes == 'object':
                df[col] = df[col].apply(self.convert_to_number)
        df.rename(columns=dict(zip(percent_cols, new_percent_col_names)), inplace=True)
        df.rename(columns = {'dividend_%_percentage':'dividend_percentage'}, inplace=True)
        df.rename(columns={'52w_high_percentage':'high_52w_percentage', '52w_low_percentage':'low_52w_percentage'}, inplace=True)

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

        bool_cols = ['optionable', 'shortable']
        bool_dict = {'Yes': True, 'No': False}
        df[bool_cols] = df[bool_cols].applymap(lambda x: bool_dict[x])
        return df
    
    
    def format_statements(self, data, ticker, table):
        """
        Formats financial statements dataframe to match columns of dataframe to database table

        Parameters:
        data (dict of csv's): dictionary with financial statements data
        ticker (str): ticker
        table (str): table name(dictionary key name)
        """
        quarter_reports = pd.DataFrame(data=data['quarterlyReports'])
        quarter_reports['report_type'] = 'quarterly'

        annual_reports = pd.DataFrame(data = data['annualReports'])
        annual_reports['report_type'] = 'annual'
        data = pd.concat([annual_reports,quarter_reports])
        data['ticker'] = ticker

        def to_lowercase(match):
            return match.group(0).title()

        columns = data.columns.str.replace(r'([A-Z]{2,})', to_lowercase ,regex= True)
        regex = re.compile(r'([A-Z])')
        data.columns = [regex.sub(r'_\1', col).lower() for col in columns]
        non_numeric_columns = ['ticker', 'report_type','reported_currency', 'fiscal_date_ending']
        data = data.apply(lambda x: pd.to_numeric(x, errors='coerce') if x.name not in non_numeric_columns else x)
        data['fiscal_date_ending'] = pd.to_datetime(data['fiscal_date_ending']).dt.date
        db_data = database_connection.get_sql_table("SELECT * FROM {table} WHERE ticker = '{ticker}'".format(ticker = ticker, table = table), conn=self.conn)
        subset = ['ticker', 'report_type', 'fiscal_date_ending']
        data = data[~data[subset].apply(tuple, 1).isin(db_data[subset].apply(tuple, 1))]        
        return data


    def transform_load_ratings(self, df, ticker):
        """
        Formats ratings dataframe to match columns of dataframe to database table dim_ratings and loads data to database

        Parameters:
        df (dataframe): dataframe with ratings data
        ticker (str): ticker
        """
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
        subset_cols = ['rating_date', 'rating_status', 'rating_agency']
        if ratings.shape[0] > 0:
            df = df[~df[subset_cols].apply(tuple, 1).isin(ratings[subset_cols].apply(tuple, 1))]
        df[['previous_price', 'current_price']] = df[['previous_price', 'current_price']].apply(pd.to_numeric, errors='coerce')
        df.to_sql('dim_ratings', self.conn, if_exists='append', index=False)


    def transform_load_dim_news(self, df,ticker):
        """
        Formats news dataframe to match columns of dataframe to database table dim_news

        Parameters:
        df (dataframe): dataframe with news data
        ticker (str): ticker
        """
        df['news_source'] = df['Link'].str.extract(r'//(.*?).com/')
        df['ticker'] = ticker
        news_cols = {'Date':'news_date', 'Title':'news_title', 'Link':'news_link'}
        df.rename(columns=news_cols, inplace=True)
        date = database_connection.get_sql_data("SELECT max(news_date) FROM dim_news WHERE ticker = '{}'".format(ticker), conn=self.conn)[0][0]
        if date:
            titles = database_connection.get_sql_table("SELECT news_title FROM dim_news WHERE ticker = '{}' and news_date = '{}'".format(ticker, date), conn=self.conn)
            titles = pd.Series(titles['news_title'])
            titles.name = 'news_title'

            df = df[df['news_date']>=date]
            df = df[~df['news_title'].isin(titles)]
        df.to_sql('dim_news', self.conn, if_exists='append', index=False)


    def transform_load_dim_inside_trade(self, df,ticker):
        """
        Formats inside trade dataframe to match columns of dataframe to database table dim_inside_trades

        Parameters:
        df (dataframe): dataframe with inside trade data
        ticker (str): ticker
        """
        df['ticker'] = ticker
        inside_trade_cols = {'Insider Trading':'traded_by', 'Relationship':'relationship', 
                            'Date':'trading_date', 'Transaction':'transaction_type', 'Cost':'share_price',
                            '#Shares':'no_of_shares', 'Value ($)':'transaction_value', 
                            '#Shares Total':'shares_total', 'SEC Form 4':'sec_form_4',
                            'SEC Form 4 Link':'sec_form_4_link', 'Insider_id':'insider_id'}
        df.rename(columns=inside_trade_cols, inplace=True)
        inside_trade = database_connection.get_sql_table("SELECT * FROM dim_inside_trades WHERE ticker = '{}'".format(ticker), conn=self.conn)
        subset_cols = ['traded_by', 'trading_date', 'transaction_type', 'share_price', 'no_of_shares', 'ticker', 'shares_total']
        inside_trade = df[~df[subset_cols].apply(tuple, 1).isin(inside_trade[subset_cols].apply(tuple, 1))]
        inside_trade.to_sql('dim_inside_trades', self.conn, if_exists='append', index=False)


    def transform_load_fundament(self, dct,ticker):
        """
        Formats fundamentals dataframe to match columns of dataframe to database table dim_fundamentals

        Parameters:
        dct (dict): dictionary with fundamentals data
        ticker (str): ticker
        """
        series = pd.Series(dct)
        today = pd.to_datetime('today').strftime('%Y-%m-%d %H:%M:%S')
        series.name = today
        df = series.to_frame().transpose(copy=True)
        df.reset_index(drop=False, inplace=True)
        df.rename(columns={'index':'Date'}, inplace=True)
        df['ticker'] = ticker
        df = self.format_fundamentals(df)
        df.to_sql('dim_fundamentals', self.conn, if_exists='append', index=False)


    def transform_load_calendar(self, df):
        """
        Formats calendar dataframe to match columns of dataframe to database table calendar

        Parameters:
        df (dataframe): dataframe with calendar data
        """
        calendar_cols={'datetime':'news_date', 'release':'release_title', 
                        'for':'release_for','prior':'previous'}
        df.columns = df.columns.str.lower()
        current_year = datetime.datetime.now().year

        # add the current year to the input string
        df['datetime'] = df['datetime'] + ", " + str(current_year)
        df['datetime'] = pd.to_datetime(df['datetime'], format='%a %b %d, %I:%M %p, %Y', errors='coerce')

        df.rename(columns = calendar_cols, inplace=True)
        date = (datetime.datetime.now()-datetime.timedelta(15)).strftime('%Y-%m-%d')
        df = df[df['news_date']>=pd.to_datetime(date, format='%Y-%m-%d')]

        calendar = database_connection.get_sql_table("SELECT * FROM calendar WHERE news_date >= '{}'".format(date), conn=self.conn)
        subset_cols = ['news_date', 'release_title', 'release_for', 'previous']
        df = df[~df[subset_cols].apply(tuple, 1).isin(calendar[subset_cols].apply(tuple, 1))]

        df.to_sql('calendar', self.conn, if_exists='append', index=False)


    def transform_load_news_blogs(self, dct):
        """
        Formats news and blogs dataframe to match columns of dataframe to database tables news and blogs

        Parameters:
        dct (dict): dictionary with news and blogs data
        """
        tables = ['news', 'blogs']
        date = (datetime.datetime.now()-datetime.timedelta(1)).strftime('%Y-%m-%d')
        for table in tables:
            df = dct[table]
            df = self.format_date(df)
            df = df[df['Date']>=pd.to_datetime(date, format='%Y-%m-%d')]
            df.columns = df.columns.str.lower()
            df = df.add_prefix(table + '_')
            database_table = database_connection.get_sql_table("SELECT * FROM {table} WHERE {table}_date >= '{date}'".format(table = table, date = date), conn=self.conn)
            subset_cols = [table + '_date', table + '_title']
            df = df[~df[subset_cols].apply(tuple, 1).isin(database_table[subset_cols].apply(tuple, 1))]
            df.to_sql(table, self.conn, if_exists='append', index=False)


    def transform_load_insider_trades(self, df):
        """
        Formats insider trades dataframe to match columns of dataframe to database table inside_trades

        Parameters:
        df (dataframe): dataframe with insider trades data
        """
        df = self.add_year_to_date(df)
        date = (datetime.datetime.now()-datetime.timedelta(10)).strftime('%Y-%m-%d')
        df = df[df['Date']>=pd.to_datetime(date, format='%Y-%m-%d')].copy(deep= True)
        df.columns = df.columns.str.lower().str.replace(' ', '_')
        insider_cols={'owner':'traded_by', 'date':'trading_date',
                    'transaction':'transaction_type', 'cost':'share_price', 
                    '#shares':'no_of_shares', 'value_($)':'transaction_value', 
                    '#shares_total':'total_shares'}
        df.rename(columns=insider_cols, inplace=True)
        insider_trades = database_connection.get_sql_table("SELECT * FROM inside_trades WHERE trading_date >= '{}'".format(date), conn=self.conn)
        subset_cols = ['ticker','traded_by', 'transaction_type', 'share_price', 'no_of_shares']
        df = df[~df[subset_cols].apply(tuple, 1).isin(insider_trades[subset_cols].apply(tuple, 1))]
        df.to_sql('inside_trades', self.conn, if_exists='append', index=False)
    

    def transform_load_quotes(self, ticker):
        """
        Formats dataframe to match columns of dataframe to database table dim_quotes
        Note: This function both downloads and loads data into the database when ticker is provided
        Parameters:
        ticker (str): ticker
        """
        quote_cols = {'Date':'quote_date', 'Open':'open_price', 
                'High':'high_price', 'Low':'low_price', 'Close':'close_price', 
                'Adj Close':'adj_close_price', 'Volume':'volume', 'Dividends':'dividend', 
                'Stock Splits': 'splits'}
        max_date = database_connection.get_sql_data("SELECT max(quote_date) FROM dim_quotes WHERE ticker = '{}'".format(ticker), conn=self.conn)[0][0]
        if max_date != None:
            from_date = (max_date + datetime.timedelta(1)).strftime('%Y-%m-%d')
            to_date = (datetime.datetime.now() + datetime.timedelta(1)).strftime('%Y-%m-%d')
            df = yf.download(ticker, start=from_date, end=to_date, interval='1d', actions=True, progress=False)
        else:
            df = yf.download(ticker, period='max', interval='1d', actions=True, progress=False)

        df.reset_index(drop = False, inplace=True)
        df['Date'] = pd.to_datetime(df['Date'], utc = True).dt.date
        df['ticker'] = ticker
        df[df.select_dtypes(include=[np_number]).columns] = df.select_dtypes(include=[np_number]).apply(lambda x: round(x, 4))
        df.rename(columns=quote_cols, inplace=True)
        df.to_sql('dim_quotes', self.conn, if_exists='append', index=False, method='multi')


    def transform_load_fact_tickers(self, ticker,description, fundament):
        """
        Formats  dataframe to match columns of dataframe to database table fact_tickers

        Parameters:
        ticker (str): ticker of the company. example: AAPL
        description (str): description of the company
        fundament (pandas series): pandas series with fundamental data
        """
        df = pd.Series(fundament).to_frame().T
        df['Description'] = description
        df['Ticker'] = ticker
        df = df[['Company', 'Sector', 'Industry', 'Country', 'Index', 'Description', 'Ticker']]
        df.columns = df.columns.str.lower()
        df.to_sql('fact_tickers', self.conn, if_exists='append', index=False)


    # financial statements
    def transform_load_balance_sheets(self, data, ticker):
        """
        Formats balance sheet dataframe to match columns of dataframe to database table dim_balance_sheets using format_statements method
        
        Parameters:
        data (dataframe): dataframe with balance sheet data
        ticker (str): ticker of the company
        """
        data = self.format_statements(data, ticker, table='dim_balance_sheets')
        data.to_sql('dim_balance_sheets', self.conn, if_exists='append', index=False)


    def transform_load_cash_flows(self, data, ticker):
        """
        Formats cash flow dataframe to match columns of dataframe to database table dim_cash_flows using format_statements method
        
        Parameters:
        data (dataframe): dataframe with cash flow data
        ticker (str): ticker of the company
        """
        data = self.format_statements(data, ticker, table = 'dim_cash_flows')
        data.to_sql('dim_cash_flows', self.conn, if_exists='append', index=False)

    def transform_load_income_statements(self, data, ticker):
        """
        Formats income statement dataframe to match columns of dataframe to database table dim_income_statements using format_statements method

        Parameters:
        data (dataframe): dataframe with income statement data
        ticker (str): ticker of the company
        """
        data = self.format_statements(data, ticker, table = 'dim_income_statements')
        data.to_sql('dim_income_statements', self.conn, if_exists='append', index=False)


    # Earning reports
    def transform_load_earnings(self, data, ticker):
        """
        Formats earnings dataframe to match columns of dataframe to database table dim_quarterly_earnings and dim_annual_earnings
        
        Parameters:
        data (dict): dictionary with earnings data; keys: 'annualEarnings', 'quarterlyEarnings'
        ticker (str): ticker of the company
        """
        df = pd.DataFrame(data=data['quarterlyEarnings'])
        df['ticker'] = ticker
        quarterly_earning_cols = {'fiscalDateEnding':'fiscal_date_ending', 'reportedEPS':'reported_eps','reportedDate':'reported_date', 
                                  'estimatedEPS':'estimated_eps','surprisePercentage':'surprise_percentage'}
        df.rename(columns=quarterly_earning_cols, inplace=True)
        df.replace('None', np.nan, inplace=True)
        df['fiscal_date_ending'] = pd.to_datetime(df['fiscal_date_ending']).dt.date

        max_date_query = "SELECT MAX(fiscal_date_ending) FROM {} WHERE ticker = '{}'".format('dim_quarterly_earnings',ticker)
        max_date = database_connection.get_sql_data(max_date_query, conn=self.conn)[0][0]
        if max_date != None:
            delete_query = "DELETE FROM {} WHERE ticker = '{}' AND fiscal_date_ending = '{}'".format('dim_quarterly_earnings',ticker,max_date)
            database_connection.update_sql_table(delete_query, conn=self.conn)
            df = df[df['fiscal_date_ending'] >= max_date]
        
        df.to_sql('dim_quarterly_earnings', self.conn, if_exists='append', index=False)
        
        df = pd.DataFrame(data=data['annualEarnings'])
        df['ticker'] = ticker
        annual_earning_cols = {'fiscalDateEnding':'fiscal_date_ending', 'reportedEPS':'reported_eps'}
        df.rename(columns=annual_earning_cols, inplace=True)
        df.replace('None', np.nan, inplace=True)
        df['fiscal_date_ending'] = pd.to_datetime(df['fiscal_date_ending']).dt.date

        max_date_query = "SELECT MAX(fiscal_date_ending) FROM {} WHERE ticker = '{}'".format('dim_annual_earnings',ticker)
        max_date = database_connection.get_sql_data(max_date_query, conn=self.conn)[0][0]
        if max_date != None:
            delete_query = "DELETE FROM {} WHERE ticker = '{}' AND fiscal_date_ending = '{}'".format('dim_annual_earnings',ticker,max_date)
            database_connection.update_sql_table(delete_query, conn=self.conn)
            df = df[df['fiscal_date_ending'] >= max_date]
        
        df.to_sql('dim_annual_earnings', self.conn, if_exists='append', index=False)


    def transform_load_screener_data(self, df):
        """
        Formats screener dataframe to match columns of dataframe to database table all_signal_screener and ticker_info:
        Note: This function downloads all fundamental data, ticker description for all tickers in the screener dataframe.


        Parameters:
        df (dataframe): dataframe with screener data
        """
        df.columns = df.columns.str.lower()
        repeated_tickers = df['ticker'].value_counts()>1
        repeated_tickers = repeated_tickers[repeated_tickers].index.tolist()
        df['signals'] = 0
        df['no_of_signals'] = 1
        for i in repeated_tickers:
            signals = df[df['ticker'] == i]['signal'].to_list()
            signals = sorted(signals)
            df.loc[df['ticker'] == i, 'no_of_signals'] = len(signals)
            df.loc[df['ticker'] == i, 'signals'] = str(signals)
            df['signals'] = np.where(df['signals'] == 0, df['signal'], df['signals'])
        
        df.drop_duplicates(subset=['ticker'], inplace=True)
        df.drop(columns=['signal'], inplace=True)

        # Transforming fundamental data to  match with all_signal_screener table
        for_columns = quote.finvizfinance(ticker = 'AAPL').ticker_fundament()
        fundamental_df = pd.DataFrame(index=for_columns.keys())

        tickers = df['ticker'].to_list()
        tickers = tickers
        for ticker in tickers:
            try:
                fundament_dct = quote.finvizfinance(ticker = ticker).ticker_fundament()
                time.sleep(0.2)
                fundamental_df[ticker] = fundament_dct.values()
            except:
                pass
        
        fundamental_df = fundamental_df.transpose(copy = True)
        fundamental_df = fundamental_df.reset_index(drop = False)
        fundamental_df = fundamental_df.rename(columns={'index':'ticker'})
        new_ticker_info = fundamental_df[['ticker', 'Company','Sector','Industry','Country']]
        new_ticker_info.columns = new_ticker_info.columns.str.lower()
        fundamental_df = fundamental_df.drop(columns=['Company', 'Sector', 'Industry', 'Country', 'Market Cap', 'P/E', 'Price', 'Change', 'Volume'])
        signals_df = pd.merge(df, fundamental_df, on='ticker')        
        signals_df = self.format_fundamentals(signals_df)
        signals_df['date'] = datetime.datetime.now().strftime("%Y%m%d_%H%M")
        signals_df['date'] = pd.to_datetime(signals_df['date'], format='%Y%m%d_%H%M')

        # Updating ticker_info table
        ticker_info = pd.read_sql('ticker_info', self.conn)
        tickers = new_ticker_info['ticker'][~new_ticker_info['ticker'].isin(ticker_info['ticker'])]

        description = pd.Series(name='description', dtype='str')
        for ticker in tickers:
            try:
                description[ticker] = quote.finvizfinance(ticker = ticker).ticker_description()
                time.sleep(0.2)
            except:
                pass
        
        new_ticker_info['description'] = new_ticker_info['ticker'].map(description)
        new_ticker_info.dropna(inplace=True)
        new_ticker_info.drop(new_ticker_info[new_ticker_info['company']=='Nano Labs Ltd'].index, inplace= True)
        new_ticker_info.to_sql('ticker_info', self.conn, if_exists='append', index=False)

        # Loading into all_signal_screener 
        signals_df.drop(signals_df[signals_df['ticker'] == 'NA'].index, inplace = True)
        signals_df.to_sql('all_signal_screener', self.conn, if_exists='append', index=False)
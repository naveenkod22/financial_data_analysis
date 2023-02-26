import os
import logging
import datetime
import configparser
import numpy as np
import pandas as pd

os.chdir('/home/naveen/code/financial_data_analysis/')

def conn_url(user = 'naveen', host = 'localhost', database = 'stock_database'):
    """
    Creates a connection to the PostgreSQL database
    by default it connects to the database with the following credentials:
        user = 'naveen'
        host = 'localhost'
        database = 'stock_database'
    Returns: engine
    
    """
    config = configparser.ConfigParser()
    config.read('./scripts/config.ini')
    password = config.get('postgresql', 'password')
    port = config.get('postgresql', 'port')
    user = user
    host = host
    database = database
    conn_url = "postgresql://{user}:{password}@{host}:{port}/{database}"\
                           .format(user = user, password = password, host = host, port = port, database = database)
    return conn_url

def log(message):
    """Logs messages to a file called logs.log"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.basicConfig(filename='logs.log', level=logging.INFO)
    logging.info("; {message} ; {timestamp}".format(message=message, timestamp=timestamp))

def format_earnings(df):
    # earnings to earnings_date and earnings_timing
    df['earnings'].replace('-', np.nan, inplace=True)
    df[['earnings_date', 'earnings_timing']] = df['earnings'].str.extract(r"([a-zA-Z]+\s\d+)\s([a-zA-Z]*)")
    df.drop(columns=['earnings'], inplace=True)
    current_year = str(pd.to_datetime('today').year)
    df['earnings_date'] = df['earnings_date']+ " " +current_year 
    df['earnings_date'] = pd.to_datetime(df['earnings_date'], format='%b %d %Y', errors='coerce')

    if (datetime.datetime.now().month in [1,2,3]):
        df['earnings_date'] = df['earnings_date'].apply(lambda x: x.replace(year=datetime.datetime.now().year-1) if (x - datetime.datetime.now()).days > 120 else x.replace(year=datetime.datetime.now().year))
    if (datetime.datetime.now().month in [10,11,12]):
        df['earnings_date'] = df['earnings_date'].apply(lambda x: x.replace(year=datetime.datetime.now().year+1) if (datetime.datetime.now() - x).days > 200 else x.replace(year=datetime.datetime.now().year))
    return df

def convert_to_number(value):
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
    
def format_fundamentals(df):
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
            df[col] = df[col].apply(convert_to_number)


    df.rename(columns={'52w_range_from':'range_from_52w', '52w_range_to':'range_to_52w'}, inplace=True)

    df = format_earnings(df)
    df.drop(columns=['earnings_date', 'earnings_timing'], inplace=True)

    bool_cols = ['optionable', 'shortable']
    bool_dict = {'Yes': True, 'No': False}
    df[bool_cols] = df[bool_cols].applymap(lambda x: bool_dict[x])
    return df

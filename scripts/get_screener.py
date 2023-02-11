import os
import time
import csv
import datetime
import warnings
import numpy as np
import pandas as pd
from finvizfinance import quote
from finvizfinance.quote import finvizfinance
from finvizfinance.screener.overview import Overview

start = time.time()
warnings.filterwarnings('ignore')
os.chdir('C://Users//navee//Documents//code//financial_data_analysis/')

def _get_signals_df():
    print('Getting Signals Dataframe')
    signals = Overview().get_signal()
    signals_df = pd.DataFrame()
    for i in signals:
        screener = Overview()
        screener.set_filter(signal = i)
        one_signal_df = screener.screener_view()
        try:
            one_signal_df['Signal'] = i
            signals_df = pd.concat([signals_df,one_signal_df], ignore_index=True)
            time.sleep(0.1)
        except TypeError:
            print('No Tickers for signal ', i)

    return signals_df

def _transform_signals_df(signals_df):
    repeated_tickers = signals_df['Ticker'].value_counts()>1
    repeated_tickers = repeated_tickers.loc[repeated_tickers].index.to_list()
    signals_df['Signals'] = 0
    signals_df['NoOf Signals'] = 1
    for i in repeated_tickers:
        signals = signals_df[signals_df['Ticker'] == i]['Signal'].to_list()
        signals = sorted(signals)
        signals_df.loc[signals_df['Ticker'] == i, 'NoOf Signals'] = len(signals)
        signals_df.loc[signals_df['Ticker'] == i, 'Signals'] = str(signals)
        signals_df['Signals'] = np.where(signals_df['Signals']==0, signals_df['Signal'], signals_df['Signals'])

    signals_df.drop_duplicates(subset=['Ticker'], inplace=True)
    signals_df.drop(columns=['Signal'], inplace=True)
    return signals_df

def _add_fundamentals(df):
    print('Adding Fundamentals')
    for_columns = quote.finvizfinance(ticker='AAPL').ticker_fundament()
    fundamentals_df = pd.DataFrame(index=for_columns.keys())

    tickers = df['Ticker'].to_list()
    for ticker in tickers:
        try:
            fundament_series = quote.finvizfinance(ticker=ticker).ticker_fundament()
            time.sleep(0.1)
            fundamentals_df[ticker] = fundament_series.values()
            time.sleep(0.2)
        except:
            print('An Exception occurred while adding fundamentals')


    fundamentals_df = fundamentals_df.transpose(copy=True)
    fundamentals_df = fundamentals_df.reset_index(drop=False)
    fundamentals_df = fundamentals_df.rename(columns={'index': 'Ticker'})
    fundamentals_df = fundamentals_df.drop(columns=['Company', 'Sector', 'Industry', 'Country', 'Market Cap', 'P/E', 'Price', 'Change', 'Volume'])
    df = pd.merge(df, fundamentals_df, on='Ticker')
    return df

def _update_ticker_info(df, ticker_info_path):
    if not os.path.exists(ticker_info_path):
        header = ['Ticker', 'Company','Sector','Industry','Country', 'Description']
        with open(ticker_info_path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header)
    print('Updating Ticker Info')
    ticker_info = pd.read_csv(ticker_info_path)
    new_ticker_info = df[['Ticker', 'Company','Sector','Industry','Country']]
    tickers = new_ticker_info['Ticker'][~new_ticker_info['Ticker'].isin(ticker_info['Ticker'])]
    
    description = pd.Series(name='Description')

    for ticker in tickers:
        try:
            description[ticker] = finvizfinance(ticker).ticker_description()
            time.sleep(0.2)
        except:
            print('An Exception occurred while adding description')
    
    new_ticker_info['Description'] = new_ticker_info['Ticker'].map(description)

    all_ticker_info = pd.concat([ticker_info, new_ticker_info], ignore_index=True)
    all_ticker_info.dropna(inplace=True)
    all_ticker_info.to_csv(ticker_info_path, index=False)
    

def _convert_to_number(value):
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

def _format_earnings(df):
    # Earnings to Earnings_Date and Earnings_Timing
    df['Earnings'].replace('-', np.nan, inplace=True)
    df[['Earnings_Date', 'Earnings_Timing']] = df['Earnings'].str.extract(r"([a-zA-Z]+\s\d+)\s([a-zA-Z]*)")
    current_year = str(pd.to_datetime('today').year)
    df['Earnings_Date'] = df['Earnings_Date']+ " " +current_year 
    df['Earnings_Date'] = pd.to_datetime(df['Earnings_Date'], format='%b %d %Y', errors='coerce')

    if (datetime.datetime.now().month in [1,2,3]):
        df['Earnings_Date'] = df['Earnings_Date'].apply(lambda x: x.replace(year=datetime.datetime.now().year-1) if (x - datetime.datetime.now()).days > 120 else x.replace(year=datetime.datetime.now().year))
    if (datetime.datetime.now().month in [10,11,12]):
        df['Earnings_Date'] = df['Earnings_Date'].apply(lambda x: x.replace(year=datetime.datetime.now().year+1) if (datetime.datetime.now() - x).days > 200 else x.replace(year=datetime.datetime.now().year))
    df.drop(columns=['Earnings'], inplace=True)
    return df
    
def _transform_load_data(df, path):
    df['Date'] = pd.Timestamp.today().date()
    df['Date'] = pd.to_datetime(df['Date'])

    df['Short Float / Ratio'].replace('-', np.nan, inplace=True)
    df[['Short Float Percentage', 'Short Float Ratio']] = df['Short Float / Ratio'].str.split(" / ", expand=True)
    df.drop(columns=['Short Float / Ratio'], inplace=True)

    # converting to float
    percent_cols = ['Insider Own', 'Perf Week','Insider Trans', 'EPS this Y', 'Inst Trans', 'Perf Half Y', 'EPS next Y Percentage', 'ROA', 'Perf Year', 'EPS next 5Y', 'ROE', 'Perf YTD',
            'EPS past 5Y', 'ROI', '52W High', 'Dividend %', 'Sales past 5Y', 'Gross Margin', '52W Low', 'Sales Q/Q', 'Oper. Margin', 'Volatility W', 'Volatility M',
            'EPS Q/Q', 'Profit Margin', 'SMA20', 'SMA50', 'SMA200', 'Inst Own', 'Perf Quarter', 'Perf Month', 'Payout', 'Short Float Percentage']

    df[percent_cols] = df[percent_cols].apply(lambda x: pd.to_numeric(x.str.replace('%', '').str.replace('-','')))

    new_percent_col_names = [i+'(%)' for i in percent_cols]
    df.rename(columns=dict(zip(percent_cols, new_percent_col_names)), inplace=True)
  
    # converting to float
    float_cols = ['Shs Outstand','Employees', 'Shs Float','Income', 'Sales', 'Short Interest' ,'Avg Volume',
                    'EPS (ttm)', 'Forward P/E', 'EPS next Y', 'PEG', 'EPS next Q', 'P/S',
                    'Book/sh', 'P/B', 'Target Price', 'Cash/sh', 'P/C', '52W Range From',
                    'Dividend', 'P/FCF', 'Beta', 'Quick Ratio', 'Current Ratio', 'RSI (14)',
                    'Debt/Eq', 'LT Debt/Eq', 'Recom', 'Short Float Ratio']
    for col in float_cols:
        df[col] = df[col].apply(_convert_to_number)


    int_cols = ['Shs Outstand', 'Market Cap', 'Volume', 'Employees', 'Avg Volume'] 
    df[int_cols] = df[int_cols].fillna(0)
    df[int_cols] = df[int_cols].astype(int)

    bool_cols = ['Optionable', 'Shortable']
    bool_dict = {'Yes': True, 'No': False}
    df[bool_cols] = df[bool_cols].applymap(lambda x: bool_dict[x])

    # Earnings columns to Earnings_Date and Earnings_Timing
    df = _format_earnings(df)

    # converting to category
    cat_cols = ['Signals', 'Index', 'Earnings_Timing']
    df[cat_cols] = df[cat_cols].astype('category')
    
    df.columns = df.columns.str.replace(' ', '_').str.replace('/', '_')
    df.rename(columns={'Dividend_%_percentage': 'Dividend_percentage'}, inplace=True)
    df.to_csv(path, index=False)

def _get_allSignalScreener():
    print('Transforming and Saving all Signal Screener')
    signals_df = _get_signals_df()
    signals_df = _transform_signals_df(signals_df)
    fundamentals = _add_fundamentals(signals_df)
    ticker_info_path = './data/screeners/ticker_info.csv'
    _update_ticker_info(df = fundamentals, ticker_info_path= ticker_info_path)
    date = str(datetime.datetime.now())[0:10].replace("-", "")
    path = './data/screeners/allSignalScreener{date}.csv'.format(date= date)
    _transform_load_data(fundamentals, path=path)

def get_allSignalScreener():
    try:
        _get_allSignalScreener()
    except ConnectionError:
        print('Connection Error Occurred for all Signal Scanner')
        _get_allSignalScreener()

    end = time.time()-start
    print('Time taken to run allSignalScreener: ', end)
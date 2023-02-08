import os
import time
import csv
import datetime
import warnings
import numpy as np
import pandas as pd
from finvizfinance import quote
from finvizfinance.screener.overview import Overview

warnings.filterwarnings('ignore')
os.chdir('C://Users//navee//Documents//code//financial_data_analysis/')

def _get_allSignalScreener():
    def _add_description(path_description, df):
        if not os.path.exists(path_description):
            header = ['Ticker', 'Description']
            # fake_row = ['Fake_Ticker', 'Fake description']
            with open(path_description, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(header)
                # writer.writerow(fake_row)

        description = pd.read_csv(path_description,header=0, index_col=0).squeeze("columns")
        df = df.merge(description, on='Ticker', how='left')
        df.to_csv('./temp/temp1.csv')
        mask = df['Description'].isna()
        df_noDescription = df[mask]
        df = df[~mask]
        df.to_csv('./temp/temp1.csv')
        df_noDescription.to_csv('./temp/temp2.csv')
        df_noDescription = df_noDescription.drop(columns=['Description'])

        new_description = pd.Series(dtype='str', name='Description')
        new_description.index.name = 'Ticker'
        tickers = df_noDescription['Ticker']
        for ticker in tickers:
            try:
                new_description.loc[ticker] = quote.finvizfinance(ticker=ticker).ticker_description()
                time.sleep(0.2)
            except:
                print('An Exception occurred while adding Description')

        signal_Desc = df_noDescription.merge(new_description, on='Ticker', how='left')
        df = pd.concat([df, signal_Desc], ignore_index=True)
    
        all_description = pd.concat([description, new_description])
        all_description.dropna(inplace=True)
        all_description.to_csv(path_description ,header=True, index_label='Ticker')
        return df

    
    signals = Overview().get_signal()
    signals_df = pd.DataFrame()
    for i in signals:
        screener = Overview()
        screener.set_filter(signal = i)
        signal = screener.screener_view()
        signal['Signal'] = i
        signals_df = pd.concat([signals_df,signal], ignore_index=True)
        time.sleep(0.1)

    repeated_tickers = signals_df['Ticker'].value_counts()>1
    repeated_tickers = repeated_tickers.loc[repeated_tickers].index.to_list()
    signals_df['Signals'] = 0
    signals_df['NoOf Signals'] = 1
    for i in repeated_tickers:
        signal = signals_df[signals_df['Ticker'] == i]['Signal'].to_list()
        signals_df.loc[signals_df['Ticker'] == i, 'NoOf Signals'] = len(signal)
        signals_df.loc[signals_df['Ticker'] == i, 'Signals'] = str(signal)
        signals_df['Signals'] = np.where(signals_df['Signals']==0, signals_df['Signal'], signals_df['Signals'])

    signals_df.drop_duplicates(subset=['Ticker'], inplace=True)
    signals_df.drop(columns=['Signal'], inplace=True)

    for_index = quote.finvizfinance(ticker='AAPL').ticker_fundament()
    fundamentals = pd.DataFrame(index=for_index.keys())

    tickers = signals_df['Ticker'].to_list()
    for ticker in tickers:
        try:
            fundament_series = quote.finvizfinance(ticker=ticker).ticker_fundament()
            time.sleep(0.1)
            fundamentals[ticker] = fundament_series.values()
            time.sleep(0.2)
        except:
            print('An Exception occurred while adding fundamentals')


    fundamentals = fundamentals.transpose(copy=True)
    fundamentals = fundamentals.reset_index(drop=False)
    fundamentals.rename(columns={'index': 'Ticker'}, inplace=True)
    fundamentals.drop(columns=['Company', 'Sector', 'Industry', 'Country', 'Market Cap', 'P/E', 'Price', 'Change', 'Volume'], inplace=True)
    result = pd.merge(signals_df, fundamentals, on='Ticker')

    path_description = './data/screeners/descriptions.csv'
    result = _add_description(path_description = path_description, df = result)

    date = str(datetime.datetime.now())[0:10].replace("-", "")
    path = './data/screeners/allSignalScreener{date}.csv'.format(date= date)
    result.to_csv(path, index=False)

def get_allSignalScreener():
    try:
        _get_allSignalScreener()
    except ConnectionError:
        print('Connection Error Occurred for all Signal Scanner')
        _get_allSignalScreener()
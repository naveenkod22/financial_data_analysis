import os
import time
import pandas as pd
from finvizfinance.quote import Statements

os.chdir('C://Users//navee//Documents//code//financial_data_analysis/') 


def get_statements_df(watch_list, sector, ticker):
    base_path = "./data/{watch_list}/{sector}/{ticker}/".format(watch_list=watch_list, sector=sector, ticker=ticker)
    statements = ['I', 'B', 'C']
    time_frames = ['Q', 'A']
    for s in statements:
        for t in time_frames:
            path = "{base_path}{ticker}_statement_{s}_{t}.csv".format(ticker = ticker, base_path=base_path, s=s, t=t)
            statements = Statements().get_statements(ticker, statement=s, timeframe=t)
            statements = statements.transpose(copy=True)
            if not os.path.exists(path):
                os.makedirs(os.path.dirname(base_path), exist_ok=True)
                statements = Statements().get_statements(ticker, statement=s, timeframe=t)
                statements = statements.transpose(copy=True)
                statements.to_csv(path,index=False)
                time.sleep(0.2)
            else:
                df = pd.read_csv(path)
                statements = pd.concat([statements,df], ignore_index=True)
                statements = statements.drop_duplicates(subset=['Period End Date'], keep='first')
                statements.to_csv(path, index=False)
                time.sleep(0.2)


                

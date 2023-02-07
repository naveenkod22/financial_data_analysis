import os
import datetime
import pandas as pd
from yahoo_fin import stock_info as si

time = str(datetime.datetime.now())
time = time[0:16].replace("-", "").replace(" ", "_").replace(":","")
os.chdir('C://Users//navee//Documents//code//financial_data_analysis/')

def convert_to_numeric(x):
    try:
        return pd.to_numeric(x)
    except ValueError:
        if x.endswith("T"):
            return float(x[:-1]) * 1e12
        elif x.endswith("B"):
            return float(x[:-1]) * 1e9
        elif x.endswith("M"):
            return float(x[:-1]) * 1e6

def transform_and_save(df, path):
    df["Market Cap"] = df["Market Cap"].apply(convert_to_numeric)
    df['Market Cap'] = df['Market Cap']/1000000000
    df.rename(columns={"Market Cap" : "Market Cap(Billions)"}, inplace=True)
    df.to_csv(path, index=False)    


def get_active(count = 200):
    path = "./data/active/active/active_{time}.csv".format(time=time)
    df = si.get_day_most_active(count = count)
    transform_and_save(df = df, path=path)

    path = "./data/active/losers/losers_{time}.csv".format(time=time)
    df = si.get_day_losers(count = count)
    transform_and_save(df = df, path=path)

    path = "./data/active/gainers_{time}.csv".format(time=time)
    df = si.get_day_gainers(count = count)
    transform_and_save(df = df, path=path)

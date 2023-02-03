import datetime
if datetime.datetime.today().weekday() in [0, 1, 2, 3, 4]:
    from numpy import number as np_number
    import pandas as pd
    import yfinance as yf
    from yahoo_fin import stock_info as si

    now = str(datetime.datetime.now())
    now = now[0:16].replace("-", "").replace(" ", "_").replace(":","")

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

    file_name = './data/active/active'+now+'.csv'
    day_active = si.get_day_most_active(count = 200)
    day_active["Market Cap"] = day_active["Market Cap"].apply(convert_to_numeric)
    day_active['Market Cap'] = day_active['Market Cap']/1000000000
    day_active.rename(columns={"Market Cap" : "Market Cap(Billions)"}, inplace=True)
    day_active.to_csv(file_name, index=False)

    file_name = './data/losers/losers'+now+'.csv'
    day_losers = si.get_day_losers(count = 200)
    day_losers["Market Cap"] = day_losers["Market Cap"].apply(convert_to_numeric)
    day_losers['Market Cap'] = day_losers['Market Cap']/1000000000
    day_losers.rename(columns={"Market Cap" : "Market Cap(Billions)"}, inplace=True)
    day_losers.to_csv(file_name, index=False)

    file_name = './data/gainers/gainers_'+now+'.csv'
    day_gainers = si.get_day_gainers(count = 200)
    day_gainers["Market Cap"] = day_gainers["Market Cap"].apply(convert_to_numeric)
    day_gainers['Market Cap'] = day_gainers['Market Cap']/1000000000
    day_gainers.rename(columns={"Market Cap" : "Market Cap(Billions)"}, inplace=True)
    day_gainers.to_csv(file_name, index=False)


    start = datetime.datetime.today() - datetime.timedelta(1)
    to = datetime.datetime.today()

    tech = ["AAPL", "MSFT", "GOOG", "AMZN", "META","BABA", "ASML", "AVGO", "ORCL", "ADBE", "TXN"]
    semi_conductor = ["NVDA", "TSM", "ASML", "QCOM", "INTC", "AMD", "MU"]
    Automakers = ["TSLA", "F", "GM", "LCID", "RIVN"]


    watch_list = {"tech": tech,"semi_conductor": semi_conductor, "Automakers": Automakers}

    for key, value in watch_list.items():
        for ticker in value:
            temp = yf.download(ticker, start=start, end=to, interval = "1d", actions = True)
            temp.reset_index(drop=False, inplace = True)
            temp["Date"] = pd.to_datetime(temp['Date'], utc=True).dt.date
            temp[temp.select_dtypes(include=[np_number]).columns] = temp.select_dtypes(include=[np_number]).apply(lambda x: round(x, 4))
            
            temp2 = pd.read_csv("./data/watchlist/"+ticker+".csv")
            
            df = pd.concat([temp2, temp], axis=0, ignore_index=False)
            df.drop_duplicates(subset=['Date'], inplace=True)
            df.reset_index()
            df.to_csv("./data/watchlist/"+ticker+".csv", index = False)
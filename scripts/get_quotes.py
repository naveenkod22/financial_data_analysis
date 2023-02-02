import pandas as pd
import yfinance as yf

tech = ["AAPL", "MSFT", "GOOG", "AMZN", "META","BABA", "ASML", "AVGO", "ORCL", "ADBE", "TXN"]
semi_conductor = ["NVDA", "TSM", "ASML", "QCOM", "INTC", "AMD", "MU"]
Automakers = ["TSLA", "F", "GM", "LCID", "RIVN"]


watch_list = {"tech": tech,"semi_conductor": semi_conductor, "Automakers": Automakers}

# Download the historical for the tickers inside the `watch_list`
for key, value in watch_list.items():
    for ticker in value:
        data = yf.download(ticker, period = 'max', interval = "1d", actions = True)
        data.to_csv("./data/watchlist/"+ticker+".csv", index=True)


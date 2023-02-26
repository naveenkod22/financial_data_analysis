from multiprocessing import Process
import pandas as pd
import yfinance as yf
import numpy as np
import time


start = time.time()

def download(tickers, path):
    for ticker in tickers:
        quote = yf.download(ticker, period = '1mo', interval = "5m", actions = True, threads = True)
        quote = quote.reset_index(drop=False)
        # append to existing table
        quote.to_csv(path + ticker + '.csv', index=False)
       

df = pd.read_csv('./data/screeners/ticker_info.csv')
tickers = df['Ticker'].tolist()
sub_length = int(np.ceil(len(tickers)/6))
tickers = [tickers[i:i + sub_length] for i in range(0, len(tickers), sub_length)]

path = './data/screeners/quotes/'

if __name__ == "__main__":  # confirms that the code is under main function 
    procs = []
    proc = Process(target=download)  # instantiating without any argument
    procs.append(proc)
    proc.start()

    # instantiating process with arguments
    for ticker in tickers:
        # print(name)
        proc = Process(target=download, args=(ticker,path))
        procs.append(proc)
        proc.start()

    # complete the processes
    for proc in procs:
        proc.join()

end = time.time()
print(end - start)
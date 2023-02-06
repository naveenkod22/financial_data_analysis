import os
import json
import datetime
from get_active import get_active
from get_quotes import get_quotes, update_quotes
from get_data import get_data

os.chdir('C://Users//navee//Documents//code//financial_data_analysis/')
# file_path = sys.argv[1]
file_path = './watchlist.json'

global watch_list
global watch_list_name
watch_list_name = os.path.basename(file_path).split('.')[0]

with open(file_path) as f:
    watch_list = json.load(f)

if datetime.datetime.today().weekday() in [0, 1, 2, 3, 4]:
    get_active()
    
for sector, value in watch_list.items():
    for ticker in value:
        if datetime.datetime.today().weekday() in [0, 1, 2, 3, 4]:
            try:
                update_quotes(watch_list=watch_list_name, sector=sector, ticker=ticker)
            except FileNotFoundError:
                get_quotes(watch_list=watch_list_name, sector=sector, ticker=ticker)

    
        try:
            get_data(watch_list=watch_list_name, sector=sector,ticker=ticker)

        except ConnectionError:
            get_data(watch_list=watch_list_name, sector=sector,ticker=ticker)

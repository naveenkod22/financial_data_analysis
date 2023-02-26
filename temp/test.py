import os
import json
import sys
os.chdir('C://Users//navee//Documents//code//financial_data_analysis/')
sys.path.append('./scripts/')
from get_data import get_data


# file_path = sys.argv[1]
file_path = './watchlist.json'

global watch_list
global watch_list_name
watch_list_name = os.path.basename(file_path).split('.')[0]

with open(file_path) as f:
    watch_list = json.load(f)


for sector, value in watch_list.items():
    for ticker in value:  
        get_data(watch_list=watch_list_name, sector=sector,ticker=ticker)
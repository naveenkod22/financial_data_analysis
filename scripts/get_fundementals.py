# Importing Functions
import os
import json
import time
import requests
import pandas as pd

# Assigning Api Key 
api_key = 'OWBLXVL83U05HM1N'

# Setting default working directory for this script
os.chdir("C:\\Users\\navee\\Desktop\\Stock_prediction")

with open('./data/watch_list.json', 'r') as f:
    watch_list = json.load(f)


def get_fundamentals(watch_list = watch_list, api_key=api_key, last_sector = 'Automakers'):
    """
    1. This function creates csv files with fundamental data for all the tickers in `watch_list` dictionary
        - fundamental data includes:
            - Company overview
            - Earning reports
            - income statements
            - cash flows
            - balance sheets
    2. Inputs: 
        - `watch_list`: A dictionary with lists, each list have tickers belonging to different sector
        - `api_key`: API key to request data from alpha vintage 
        - `last_sector`: last sector in the `watch_list` dictionary
    """
    # Assigning last ticker value in watch list to `last_ticker``
    last_ticker = watch_list[last_sector][-1]
    
    for sector, tickers in watch_list.items():
        for ticker in tickers:
    
            # overview
            fnc = 'OVERVIEW'
            print(fnc)
            print(ticker)
            url = str("https://www.alphavantage.co/query?function="+fnc+"&symbol="+ticker+"&apikey="+api_key)
            temp = requests.get(url)
            temp = temp.json()

            # creating directories for sectors and tickers if they do not exist
            directory = str("./data/fundamentals/"+sector+"/"+ticker)
            os.makedirs(directory, exist_ok=True)
            
            # Saving dictionary to json file
            with open(str("./data/fundamentals/"+sector+"/"+ticker+"/"+ticker+"_overview.json"), 'w') as f:
                json.dump(temp, f, indent=4)
            

            # balance_sheet
            fnc = 'BALANCE_SHEET'    
            print(fnc)
            print(ticker)
            url = str("https://www.alphavantage.co/query?function="+fnc+"&symbol="+ticker+"&apikey="+api_key)
            temp = requests.get(url)
            temp = temp.json()

            # Converting json data to dataframe and saving into csv files
            quarter_reports = pd.DataFrame(data=temp['quarterlyReports'])
            quarter_reports['report_type'] = 'quarterly'

            annual_reports = pd.DataFrame(data = temp['annualReports'])
            annual_reports['report_type'] = 'annual'
            temp = pd.concat([annual_reports,quarter_reports])
            temp.to_csv(str("./data/fundamentals/"+sector+"/"+ticker+"/"+ticker+"_balance_sheet.csv"), index=False)
            

            # INCOME_STATEMENT
            # Fetching data
            fnc = 'INCOME_STATEMENT'
            url = str("https://www.alphavantage.co/query?function="+fnc+"&symbol="+ticker+"&apikey="+api_key)
            temp = requests.get(url)
            temp = temp.json()

            quarter_reports = pd.DataFrame(data=temp['quarterlyReports'])
            quarter_reports['report_type'] = 'quarterly'

            annual_reports = pd.DataFrame(data = temp['annualReports'])
            annual_reports['report_type'] = 'annual'
            temp = pd.concat([annual_reports,quarter_reports])
            temp.to_csv(str("./data/fundamentals/"+sector+"/"+ticker+"/"+ticker+"_income_statement.csv"), index=False)
            

            # Cash_flow
            fnc = 'CASH_FLOW'   
            url = str("https://www.alphavantage.co/query?function="+fnc+"&symbol="+ticker+"&apikey="+api_key)
            temp = requests.get(url)
            temp = temp.json()

            quarter_reports = pd.DataFrame(data=temp['quarterlyReports'])
            quarter_reports['report_type'] = 'quarterly'

            annual_reports = pd.DataFrame(data = temp['annualReports'])
            annual_reports['report_type'] = 'annual'
            temp = pd.concat([annual_reports,quarter_reports])
            temp.to_csv(str("./data/fundamentals/"+sector+"/"+ticker+"/"+ticker+"_cash_flow.csv"), index=False)
            
            
            # Earnings
            fnc = 'EARNINGS'
            url = str("https://www.alphavantage.co/query?function="+fnc+"&symbol="+ticker+"&apikey="+api_key)
            temp = requests.get(url)
            temp = temp.json()

            quatre_earnings = pd.DataFrame(data = temp['quarterlyEarnings'])
            quatre_earnings.to_csv(str("./data/fundamentals/"+sector+"/"+ticker+"/"+ticker+"_quarterly_earnings.csv"))
            annual_earnings = pd.DataFrame(data = temp['annualEarnings'])
            annual_earnings.to_csv(str("./data/fundamentals/"+sector+"/"+ticker+"/"+ticker+"_annual_earnings.csv"))
            
            if ticker == last_ticker:
                break
            time.sleep(61)


get_fundamentals()
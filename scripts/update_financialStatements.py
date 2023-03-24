import datetime
import time
from get_data import GetData
from database_connection import DatabaseConnection

database_connection = DatabaseConnection()
conn_url = database_connection.conn_url()

get_data = GetData(conn_url=conn_url)
transform_load_conn = get_data.transform_load.conn

remaining_tickers_query = """SELECT remaining_tickers
                            FROM api_requests
                            WHERE request_date = (
                                SELECT MAX(request_date)
                                FROM api_requests
                                WHERE request_date < '{}'
                            )""".format(datetime.date.today().strftime("%Y-%m-%d"))

remaining_tickers = database_connection.get_sql_data(remaining_tickers_query, conn=transform_load_conn)
if remaining_tickers != []:
    remaining_tickers = str(remaining_tickers[0][0])
    remaining_tickers = remaining_tickers.split(', ')
    remaining_tickers = [i.strip("'") for i in remaining_tickers]
    if remaining_tickers == ['None']:
        remaining_tickers = []


date = (datetime.date.today() - datetime.timedelta(1)).strftime("%Y-%m-%d")
earning_tickers_query = "SELECT ticker FROM dim_fundamentals WHERE earnings_date =  '{}' ".format(date)
earning_tickers = database_connection.get_sql_data(earning_tickers_query, conn=transform_load_conn)
earning_tickers = [i[0] for i in earning_tickers]

all_tickers_query = "SELECT ticker FROM fact_tickers"
all_tickers = database_connection.get_sql_data(all_tickers_query, conn=transform_load_conn)
all_tickers = [i[0] for i in all_tickers]

balance_sheet_tickers_query = "SELECT DISTINCT ticker FROM dim_balance_sheets"
balance_sheet_tickers = database_connection.get_sql_data(balance_sheet_tickers_query, conn=transform_load_conn)
balance_sheet_tickers = [i[0] for i in balance_sheet_tickers]

# including the tickers that are newly added to fact_tickers
new_tickers = [i for i in all_tickers if i not in balance_sheet_tickers]
tickers = remaining_tickers + earning_tickers + new_tickers

today = datetime.date.today().strftime("%Y-%m-%d")
no_of_requests_query = "SELECT no_of_requests FROM api_requests WHERE request_date = '{}'".format(today)
no_of_requests = database_connection.get_sql_data(no_of_requests_query, conn=transform_load_conn)
if no_of_requests != []:
    no_of_requests = int(no_of_requests[0][0])
else:
    no_of_requests = 0
    database_connection.update_sql_table("INSERT INTO api_requests (request_date, no_of_requests, website) VALUES ('{}', {}, '{}')".format(today, no_of_requests,'alphavantage'), conn=transform_load_conn)

available_requests = (400 - no_of_requests)//4
remaining_tickers = tickers[available_requests:]
tickers = tickers[:available_requests]

if len(tickers) != 0:
    updated_tickers = str(tickers).strip('[').strip(']').replace("'", "")
    database_connection.update_sql_table("UPDATE api_requests SET updated_tickers = '{}'WHERE request_date = '{}'".format(updated_tickers, today), conn=transform_load_conn)

if len(remaining_tickers) != 0:
    remaining_tickers = str(remaining_tickers).strip('[').strip(']').replace("'", "")
    database_connection.update_sql_table("UPDATE api_requests SET remaining_tickers = '{}'WHERE request_date = '{}'".format(remaining_tickers, today), conn=transform_load_conn)


for ticker in tickers:
    get_data.update_financial_statements(ticker=ticker)
    get_data.update_earnings(ticker=ticker)
    print(" {} financial statements and earnings; {}".format(ticker, datetime.datetime.now()))
    no_of_requests += 4
    if ticker != tickers[-1]:
        time.sleep(61)
        
database_connection.update_sql_table("UPDATE api_requests SET no_of_requests = {} WHERE request_date = '{}'".format(no_of_requests, today), conn=transform_load_conn)
transform_load_conn.close()
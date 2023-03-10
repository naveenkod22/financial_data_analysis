import datetime
import time
from get_data import GetData
from database_connection import DatabaseConnection

database_connection = DatabaseConnection()
conn_url = database_connection.conn_url()

get_data = GetData(conn_url=conn_url)
transform_load_conn = get_data.transform_load.conn

date = (datetime.date.today() - datetime.timedelta(1)).strftime("%Y-%m-%d")
print(date+ " *******************************************************" + str(datetime.datetime.now()))

earning_tickers_query = "select ticker from dim_fundamentals where earnings_date =  '{}' ".format(date)
earning_tickers = database_connection.get_sql_data(earning_tickers_query, conn=transform_load_conn)
earning_tickers = [i[0] for i in earning_tickers]
print(earning_tickers)

all_tickers_query = "select ticker from fact_tickers"
all_tickers = database_connection.get_sql_data(all_tickers_query, conn=transform_load_conn)
all_tickers = [i[0] for i in all_tickers]
print(all_tickers)

for ticker in all_tickers:
    print(ticker+ " *******************************************************" + str(datetime.datetime.now()))
    new_tickers_query = "select DISTINCT ticker from dim_balance_sheets where ticker = '{}''".format(ticker)
    new_tickers = database_connection.get_sql_data(new_tickers_query, conn=transform_load_conn)
    print(new_tickers)
    

# for ticker in tickers:
#     get_data.update_financial_statements(ticker=ticker)
#     get_data.update_earnings(ticker=ticker)
#     print(" {} financial statements and earnings; {}".format(ticker, datetime.datetime.now()))
#     if ticker != tickers[-1]:
#         time.sleep(61)

transform_load_conn.close()

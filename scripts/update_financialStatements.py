import datetime
import time
from get_data import GetData
from database_connection import DatabaseConnection

database_connection = DatabaseConnection()
conn_url = database_connection.conn_url()

get_data = GetData(conn_url=conn_url)
transform_load_conn = get_data.transform_load.conn

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
tickers = earning_tickers + new_tickers

for ticker in tickers:
    get_data.update_financial_statements(ticker=ticker)
    get_data.update_earnings(ticker=ticker)
    print(" {} financial statements and earnings; {}".format(ticker, datetime.datetime.now()))
    if ticker != tickers[-1]:
        time.sleep(61)

transform_load_conn.close()

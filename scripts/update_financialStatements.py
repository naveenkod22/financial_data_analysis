import datetime
import time
from get_data import GetData
from database_connection import DatabaseConnection

database_connection = DatabaseConnection()
conn_url = database_connection.conn_url()

get_data = GetData(conn_url=conn_url)
transform_load_conn = get_data.transform_load.conn

date = (datetime.date.today() - datetime.timedelta(1)).strftime("%Y-%m-%d")

query = "select ticker from dim_fundamentals where earnings_date =  '{}' ".format(date)
tickers = database_connection.get_sql_data(query, conn=transform_load_conn)
tickers = [i[0] for i in tickers]
for ticker in tickers:
    get_data.update_financial_statements(ticker=ticker)
    get_data.update_earnings(ticker=ticker)
    print(" {} financial statements and earnings; {}".format(ticker, datetime.datetime.now()))
    if ticker != tickers[-1]:
        time.sleep(61)

transform_load_conn.close()

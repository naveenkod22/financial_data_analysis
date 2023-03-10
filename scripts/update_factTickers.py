from get_data import GetData
from database_connection import DatabaseConnection

database_connection = DatabaseConnection()
conn_url = database_connection.conn_url()

get_data = GetData(conn_url=conn_url)
transform_load_conn = get_data.transform_load.conn

existing_tickers_query = "select ticker from fact_tickers"
existing_tickers = database_connection.get_sql_data(existing_tickers_query, conn=transform_load_conn)
existing_tickers = [i[0] for i in existing_tickers]

tickers = input("Enter tickers separated by commas: ")
tickers = tickers.split(",")
tickers = [i.strip().upper() for i in tickers]

# tickers that are not in existing_tickers
tickers = [i for i in tickers if i not in existing_tickers]

for ticker in tickers:
    try:
        get_data.update_fact_tickers(ticker=ticker)
    except ConnectionError as e:
        get_data.update_fact_tickers(ticker=ticker)

transform_load_conn.close()
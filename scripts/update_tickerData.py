import datetime
from get_data import GetData
from database_connection import DatabaseConnection

database_connection = DatabaseConnection()
conn_url = database_connection.conn_url()

get_data = GetData(conn_url=conn_url)
transform_load_conn = get_data.transform_load.conn

# Get Business Dates from market_calender table
business_dates = database_connection.get_sql_data("SELECT trading_dates FROM market_calender", conn=transform_load_conn)
business_dates = [i[0] for i in business_dates]

# Get Tickers from fact_tickers table
tickers = database_connection.get_sql_data("SELECT ticker FROM  fact_tickers", conn=transform_load_conn)
tickers = [i[0] for i in tickers]

if datetime.date.today() in business_dates:
    for ticker in tickers:
        try:
            get_data.update_ticker_data(ticker=ticker)
            get_data.update_ticker_quotes(ticker=ticker)
        except ConnectionError as e:
            get_data.update_ticker_data(ticker=ticker)
            get_data.update_ticker_quotes(ticker=ticker)

transform_load_conn.close()
print('Ticker Data; {}'.format(datetime.datetime.now()))
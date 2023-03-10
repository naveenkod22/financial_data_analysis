import datetime
from get_data import GetData
from database_connection import DatabaseConnection

database_connection = DatabaseConnection()
conn_url = database_connection.conn_url()

get_data = GetData(conn_url=conn_url)
transform_load_conn = get_data.transform_load.conn
try:
    get_data.update_screeners()
except ConnectionError as e:
    get_data.update_screeners()

transform_load_conn.close()
print('All Signal Screener; {}'.format(datetime.datetime.now()))
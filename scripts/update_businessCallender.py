import datetime
from database_connection import DatabaseConnection
from sqlalchemy import create_engine
import pandas_market_calendars as mcal

database_connection = DatabaseConnection()
conn_url = database_connection.conn_url

# create a connection to the PostgreSQL database
def update_business_calender():
    """
    Creates a table called market_calender in the database which contains the Trading business dates for the next 60 days.
    """
    engine = create_engine(conn_url())
    conn = engine.connect()
    nyse = mcal.get_calendar('NYSE')

    start_date = datetime.datetime.now().strftime("%Y-%m-%d")
    end_date = (datetime.datetime.now()+datetime.timedelta(60)).strftime("%Y-%m-%d")

    schedule = nyse.schedule(start_date=start_date, end_date=end_date)
    schedule = schedule.reset_index(drop = False)
    schedule.drop(columns=['market_open', 'market_close'], inplace=True)
    schedule = schedule.rename(columns={'index': 'trading_dates'})
    schedule['trading_dates'] = schedule['trading_dates'].dt.date
    schedule.to_sql('market_calender', conn, if_exists='replace', index=False)
    conn.close()

if __name__ == '__main__':
    update_business_calender()
    print('Business Calender; {}'.format(datetime.datetime.now()))

import datetime
import logging
from utils import database_connection
import pandas_market_calendars as mcal

# create a connection to the PostgreSQL database
conn = database_connection()

def get_business_callender():
    """
    Creates a table called market_callender in the database which contains the Trading business dates for the next 60 days.
    """
    nyse = mcal.get_calendar('NYSE')

    start_date = datetime.datetime.now().strftime("%Y-%m-%d")
    end_date = (datetime.datetime.now()+datetime.timedelta(60)).strftime("%Y-%m-%d")

    schedule = nyse.schedule(start_date=start_date, end_date=end_date)
    schedule = schedule.reset_index(drop = False)
    schedule.drop(columns=['market_open', 'market_close'], inplace=True)
    schedule = schedule.rename(columns={'index': 'Date'})

    schedule.to_sql('Market_Callender', conn, if_exists='replace', index=False)
    conn.commit()

if __name__ == '__main__':
    get_business_callender()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.basicConfig(filename='logs.log', level=logging.INFO)
    logging.info("; Callender Updated ; {timestamp}".format(timestamp=timestamp))
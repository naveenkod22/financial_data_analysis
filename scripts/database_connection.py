from sqlalchemy import create_engine
import pandas as pd
import configparser
import os

os.chdir('/home/naveen/code/financial_data_analysis/')

class DatabaseConnection():

    def conn_url(self, user = 'naveen', host = 'localhost', database = 'stock_database'):
        """
        Creates a connection to the PostgreSQL database
        by default it connects to the database with the following credentials:
            user = 'naveen'
            host = 'localhost'
            database = 'stock_database'
        Returns: conn_url
        """
        config = configparser.ConfigParser()
        config.read('./scripts/config.ini')
        password = config.get('postgresql', 'password')
        port = config.get('postgresql', 'port')
        user = user
        host = host
        database = database
        conn_url = "postgresql://{user}:{password}@{host}:{port}/{database}"\
                    .format(user = user, password = password, host = host, port = port, database = database)
        return conn_url

    def get_sql_table(self, statement, conn):
        """Returns a dataframe from a SQL statement"""
        result = conn.execute(statement)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df

    def get_sql_data(self, statement, conn):
        """Returns a data from a SQL statement"""
        result = conn.execute(statement)
        data = result.fetchall()
        return data

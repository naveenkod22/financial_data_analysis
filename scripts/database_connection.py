from sqlalchemy import create_engine
import pandas as pd
import configparser
import os

os.chdir('/home/naveen/code/financial_data_analysis/')

class DatabaseConnection():
    """
    Class contains methods to connect to database and get and update the data of a database

    Methods:
    conn_url(user, host, database, password, port)
    get_sql_table(statement, conn)
    get_sql_data(statement, conn)
    update_sql_table(statement, conn)
    """

    config = configparser.ConfigParser()
    config.read('./scripts/config.ini')
    password = config.get('postgresql', 'password')
    port = config.get('postgresql', 'port')

    def conn_url(self, user = 'naveen', host = 'localhost', database = 'stock_database', password = password, port = port):
        """
        Creates a connection url to connect to database

        Parameters:
        user (str): user name of the database
        host (str): host name of the database
        database (str): database name
        password (str): password of the database
        port (str): port number of the database

        Returns: conn_url
        """
        conn_url = "postgresql://{user}:{password}@{host}:{port}/{database}"\
                    .format(user = user, password = password, host = host, port = port, database = database)
        return conn_url

    def get_sql_table(self, statement, conn):
        """
        Returns a dataframe from a SQL statement
        
        Parameters:
        statement (str): SQL statement
        conn (str): connection object to database
        
        Returns: dataframe
        """
        result = conn.execute(statement)
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df

    def get_sql_data(self, statement, conn):
        """
        Returns a data from a SQL statement
        
        Parameters:
        statement (str): SQL statement
        conn (str): connection object to database

        Returns: data
        """
        result = conn.execute(statement)
        data = result.fetchall()
        return data
    
    def update_sql_table(self, statement, conn):
        """
        Updates a SQL table from the SQL statement
        
        Parameters:
        statement (str): SQL statement
        conn (str): connection object to database
        
        Returns: message if table is updated successfully
        """
        conn.execute(statement)
        message = 'Table updated successfully'
        return message
from utils import database_connection
import pandas as pd

engine = database_connection()
ticker_info = pd.read_sql('ticker_info', engine)
print(ticker_info.shape)

ticker_info.to_sql('ticker_info_dymmy1', engine, index = False)
engine.commit()
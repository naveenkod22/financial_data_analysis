import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, Date
from sqlalchemy.orm import declarative_base

Base = declarative_base()

def _create_table_class(df, table_name):

    class AllSignalScreener(Base):
        __tablename__ = table_name

        id = Column(Integer, primary_key=True)

    for col in df.columns:
        col_type = df[col].dtype
        if col_type == 'int64':
            setattr(AllSignalScreener, col, Column(Integer))
        elif col_type == 'int32':
            setattr(AllSignalScreener, col, Column(Integer))
        elif col_type == 'float32':
            setattr(AllSignalScreener, col, Column(Float))
        elif col_type == 'float64':
            setattr(AllSignalScreener, col, Column(Float))
        elif col_type == 'bool':
            setattr(AllSignalScreener, col, Column(Boolean))
        elif col_type == 'datetime64[ns]':
            setattr(AllSignalScreener, col, Column(Date))
        else:
            setattr(AllSignalScreener, col, Column(String(200)))

    return AllSignalScreener

def write_to_db(df, table_name, db_url):
    engine = create_engine(db_url)
    MyTable = _create_table_class(df, table_name)
    Base.metadata.create_all(engine)
    df.to_sql(table_name, engine, if_exists='append', index=False)


# Project Objective.

*Developing python code to gather and analyze the data which could possibly influence stock market*
---


## Getting Stock Data

1. Historical stock Data.
2. Stock Fundamental data:
    - Balance sheets
    - Income Statements
    - Cash flows
    - Earning reports
3. News articles.
4. Analyst Ratings
5. Micro Economic Data
6. Macro Economic Data
7. Daily Active, gainer, looser stocks

---
# Repository structure:
```
.
├── logs.log : All the log messages from cron jobs are piped into this file.
├── notebooks : Folder containing Jupyter notebooks
├── README.md
├── scripts
│   ├── database_connection.py: Code for python class DatabaseConnection.
│   ├── get_data.py: Code for python class GetData.
│   ├── sql: This folder contains SQL scripts to create databases.
│   ├── transform_load_data.py: Code for python class TransformLoad.
│   ├── update_businessCalender.py: Code to update market_calender.
│   ├── update_factTickers.py: Code to add new tickers to fact_tickers table.
│   ├── update_financialStatements.py: Code to update cashflow, balance sheets, income statements and earning tables.
│   ├── update_news.py: Code to update news, blogs, insider trades, calender tables.
│   ├── update_screeners.py: Code to update All Signal Screener and ticker info tables.
│   └── update_tickerData.py: Code to update ticker quotes and all ticker related data(news, insider trades, ratings, fundamentals).
```

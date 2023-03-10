select COUNT(*) from dim_news;
-- LIST DUPLICATE VALUES IN A TABLE DIM_NEWS
select * from dim_news where ticker = 'TSLA';
select * from dim_blogs LIMIT 10;
select * from dim_inside_trades LIMIT 10;
select count(*) from dim_ratings LIMIT 10;
select COUNT(*) from dim_quotes LIMIT 10;
select * from fact_tickers LIMIT 10;
select * from dim_quarterly_earnings LIMIT 10;
select * from all_signal_screener LIMIT 10;
select current_accounts_payable, accumulated_depreciation_amortization_ppe from dim_balance_sheets LIMIT 100;

-- Count distinct values in a column
select count(distinct "Ticker") from all_signal_screener;
SELECT COUNT(DISTINCT "Ticker") from ticker_info;
SELECT DISTINCT "Ticker" from ticker_info;

SELECT * FROM dim_cash_flows WHERE ticker = 'TSLA';


SELECT nspname || '.' || relname AS "table",
    pg_size_pretty(pg_total_relation_size(C.oid)) AS "total_size"
FROM pg_class C
LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
WHERE nspname NOT IN ('pg_catalog', 'information_schema')
    AND C.relkind <> 'i'
    AND nspname !~ '^pg_toast'
ORDER BY pg_total_relation_size(C.oid) DESC;

select
  table_name,
  pg_size_pretty(pg_relation_size(quote_ident(table_name))),
  pg_relation_size(quote_ident(table_name))
from information_schema.tables
where table_schema = 'public'
order by 1 desc;

select COUNT(*) from dim_news;


SELECT COUNT(*) FROM inside_trades;
-- select apple quote for today date
select * from dim_quotes WHERE ticker = 'TSLA' AND quote_date >= '2023-03-07';
select * from dim_quotes WHERE ticker = 'TSLA' & quote_date = ;
select price, date, ticker from dim_fundamentals WHERE ticker = 'TSLA' AND date > '2023-02-17';

SELECT * FROM dim_fundamentals WHERE ticker = 'TSLA';
 
SELECT * FROM all_signal_screener WHERE "Ticker" = 'TSLA' LIMIT 10;

SELECT * FROM calendar;
-- News: 5067, Apple Quotes: 10645; 4477

SELECT COUNT(*) FROM inside_trades;
SELECT COUNT(*) FROM news;
SELECT COUNT(*) FROM blogs;
select COUNT(*) from calendar;


ALTER TABLE fact_tickers
ADD COLUMN company VARCHAR(255);

select * from fact_tickers;
DELETE FROM fact_tickers WHERE ticker = 'CM';

SELECT COUNT(*) FROM dim_balance_sheets WHERE ticker = 'CRM';
SELECT COUNT(*) FROM dim_cash_flows WHERE ticker = 'CRM';
SELECT COUNT(*) FROM dim_income_statements WHERE ticker = 'CRM';
SELECT COUNT(*) FROM dim_quarterly_earnings WHERE ticker = 'CRM';
SELECT COUNT(*) FROM dim_annual_earnings WHERE ticker = 'CRM';


SELECT * FROM dim_balance_sheets WHERE ticker = 'CRM';
SELECT * FROM dim_cash_flows WHERE ticker = 'CRM';
SELECT * FROM dim_income_statements WHERE ticker = 'CRM';
SELECT * FROM dim_quarterly_earnings WHERE ticker = 'CRM';
SELECT * FROM dim_annual_earnings WHERE ticker = 'CRM';

SELECT * FROM dim_news WHERE ticker = 'TSLA';

DELETE FROM dim_balance_sheets WHERE ticker = 'TSLA';
DELETE FROM dim_cash_flows WHERE ticker = 'TSLA';
DELETE FROM dim_income_statements WHERE ticker = 'TSLA';
DELETE FROM dim_quarterly_earnings WHERE ticker = 'TSLA';
DELETE FROM dim_annual_earnings WHERE ticker = 'TSLA';

SELECT * FROM dim_inside_trades WHERE ticker = 'TSLA';
SELECT * FROM dim_news WHERE ticker = 'AAPL' AND news_date >= '2023-03-03';

SELECT * FROM dim_quarterly_earnings WHERE ticker = 'TSLA';
SELECT * FROM dim_ratings;
SELECT * FROM dim_cash_flows WHERE ticker = 'CM';
SELECT * FROM dim_quotes WHERE ticker = 'TSLA' AND quote_date >= '2023-03-03';
SELECT COUNT(*) FROM news;
SELECT * FROM inside_trades WHERE trading_date >= '2023-03-07' AND ticker = 'CBAN';


DROP TABLE dim_inside_trades;
CREATE TABLE IF NOT EXISTS dim_inside_trades (
    inside_trade_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_tickers(ticker),
    traded_by VARCHAR(100),
    relationship VARCHAR(100),
    trading_date VARCHAR(25),
    transaction_type VARCHAR(55),
    share_price FLOAT,
    no_of_shares INT,
    transaction_value FLOAT,
    shares_total INT,
    sec_form_4 VARCHAR(255),
    sec_form_4_link VARCHAR(255),
    insider_id INT
);

DELETE FROM dim_news
WHERE (news_date,news_title) IN (
   SELECT news_date,news_title
   FROM dim_news
   GROUP BY news_date,news_title
   HAVING COUNT(*) > 1
);

-- 'traded_by', 'trading_date', 'transaction_type', 'share_price', 'no_of_shares', 'ticker', 'shares_total'
DELETE FROM dim_inside_trades
WHERE (traded_by, trading_date, transaction_type, share_price, no_of_shares ,ticker, shares_total, ticker, shares_total) IN (
   SELECT traded_by, trading_date, transaction_type, share_price, no_of_shares ,ticker, shares_total, ticker, shares_total
   FROM dim_inside_trades
   GROUP BY traded_by, trading_date, transaction_type, share_price, no_of_shares ,ticker, shares_total, ticker, shares_total
   HAVING COUNT(*) > 1
);

DELETE FROM dim_ratings
WHERE (ticker,rating_date, rating_status, rating_agency, previous_rating, previous_price) IN (
   SELECT ticker,rating_date, rating_status, rating_agency, previous_rating, previous_price
   FROM dim_ratings
   GROUP BY ticker,rating_date, rating_status, rating_agency, previous_rating, previous_price
   HAVING COUNT(*) > 1
);


SELECT COUNT(*) FROM dim_news;
SELECT COUNT(*) FROM dim_inside_trades;
SELECT COUNT(*) FROM dim_ratings;

SELECT ticker,traded_by, transaction_type, share_price, no_of_shares FROM inside_trades;

SELECT * FROM dim_fundamentals WHERE ticker = 'TSLA';
SELECT COUNT(*) FROM dim_fundamentals;


DELETE FROM dim_fundamentals
WHERE (ticker,market_cap, prev_close) IN (
   SELECT ticker,market_cap, prev_close
   FROM dim_fundamentals
   GROUP BY ticker,market_cap, prev_close
   HAVING COUNT(*) > 1
);

select count(*) from news;
select count(*) from inside_trades;
select count(*) from blogs;
select count(*) from calendar;

select * from news where news_date >= '2023-03-01' and news_date <= '2023-03-03';

SELECT trading_dates FROM market_calender;

DELETE FROM dim_news where news_date >= '2023-03-09';

select * from dim_quotes where ticker = 'TSLA' and quote_date >= '2023-03-08';

DELETE FROM  dim_quotes where  quote_date >= '2023-03-09';

select * from ticker_info LIMIT 10;
select COUNT(*) from all_signal_screener LIMIT 10;

select * from dim_fundamentals; where ticker = 'TSLA' and date >= '2023-03-08';

select * from all_signal_screener where "ticker" = 'TSLA' and "date" >= '2023-03-09';

select count(*) from ticker_info;
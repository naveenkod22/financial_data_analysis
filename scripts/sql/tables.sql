select * from dim_news LIMIT 10;
select * from dim_blogs LIMIT 10;
select * from dim_inside_trades LIMIT 10;
select * from dim_ratings LIMIT 10;
select * from dim_quotes LIMIT 10;
select * from fact_tickers LIMIT 10;
select * from dim_quarterly_earnings LIMIT 10;
select * from all_signal_screener LIMIT 10;
select current_accounts_payable, accumulated_depreciation_amortization_ppe from dim_balance_sheets LIMIT 100;

-- Count distinct values in a column
select count(distinct "Ticker") from all_signal_screener;
SELECT COUNT(DISTINCT "Ticker") from ticker_info;
SELECT DISTINCT "Ticker" from ticker_info;

SELECT * FROM dim_cash_flows WHERE ticker = 'AAPL';


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

select * from information_schema.tables;

select * from news;
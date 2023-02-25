select * from dim_news LIMIT 10;
select * from dim_blogs LIMIT 10;
select * from dim_inside_trades LIMIT 10;
select * from dim_ratings LIMIT 10;
select * from dim_quotes LIMIT 10;
select * from fact_tickers LIMIT 10;
select * from dim_quarterly_earnings LIMIT 10;
select * from all_signal_screener LIMIT 10;
select * from dim_balance_sheets LIMIT 10;

-- Count distinct values in a column
select count(distinct "Ticker") from all_signal_screener;
SELECT COUNT(DISTINCT "Ticker") from ticker_info;
SELECT DISTINCT "Ticker" from ticker_info;

DROP TABLE dim_balance_sheets;
CREATE TABLE IF NOT EXISTS dim_balance_sheets (
    balance_sheet_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_tickers(ticker),
    fiscal_date_ending DATE,
    reported_currency VARCHAR(8),
    total_assets FLOAT,
    total_current_assets FLOAT,
    cash_and_cash_equivalents_at_carrying_value FLOAT,
    cash_and_short_term_investments FLOAT,
    inventory FLOAT,
    current_net_receivables FLOAT,
    total_non_current_assets FLOAT,
    property_plant_equipment FLOAT,
    accumulated_depreciation_amortization_ppe FLOAT,
    intangible_assets_excluding_goodwill FLOAT,
    goodwill FLOAT,
    investments FLOAT,
    long_term_investments FLOAT,
    short_term_investments FLOAT,
    other_current_assets FLOAT,
    other_non_current_assets FLOAT,
    total_liabilities FLOAT,
    total_current_liabilities FLOAT,
    current_accounts_payable FLOAT,
    deferred_revenue FLOAT,
    current_debt FLOAT,
    short_term_debt FLOAT,
    total_non_current_liabilities FLOAT,
    capital_leases_obligations FLOAT,
    long_term_debt FLOAT,
    current_long_term_debt FLOAT,
    long_term_debt_non_current FLOAT,
    short_long_term_debt_total FLOAT,
    other_current_liabilities FLOAT,
    other_non_current_liabilities FLOAT,
    total_shareholder_equity FLOAT,
    treasury_stock FLOAT,
    retained_earnings FLOAT,
    common_stock FLOAT,
    common_stock_shares_outstanding FLOAT,
    report_type VARCHAR(8)
);
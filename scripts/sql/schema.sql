DROP TABLE IF EXISTS fact_tickers CASCADE;

CREATE TABLE IF NOT EXISTS fact_tickers (
  ticker VARCHAR(8) PRIMARY KEY,
  sector VARCHAR(255),
  industry VARCHAR(255),
  country VARCHAR(100),
  index VARCHAR(100),
  description TEXT
);

DROP TABLE dim_fundamentals;
CREATE TABLE IF NOT EXISTS dim_fundamentals (
    fundamental_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_tickers(ticker),
    date DATE, 
    p_e FLOAT, 
    eps_ttm FLOAT, 
    insider_own_percentage FLOAT, 
    shs_outstand BIGINT, 
    perf_week_percentage FLOAT, 
    market_cap BIGINT, 
    forward_p_e FLOAT, 
    eps_next_y FLOAT, 
    insider_trans_percentage FLOAT, 
    shs_float BIGINT, 
    perf_month_percentage FLOAT, 
    income BIGINT, 
    peg FLOAT, 
    eps_next_q FLOAT, 
    inst_own_percentage FLOAT, 
    perf_quarter_percentage FLOAT, 
    sales FLOAT, 
    p_s FLOAT, 
    eps_this_y_percentage FLOAT, 
    inst_trans_percentage FLOAT, 
    short_interest FLOAT, 
    perf_half_y_percentage FLOAT, 
    book_sh FLOAT, 
    p_b FLOAT, 
    eps_next_y_percentage_percentage FLOAT, 
    roa_percentage FLOAT, 
    target_price FLOAT, 
    perf_year_percentage FLOAT, 
    cash_sh FLOAT, 
    p_c FLOAT, 
    eps_next_5y_percentage FLOAT, 
    roe_percentage FLOAT, 
    range_from_52w FLOAT, 
    range_to_52w FLOAT, 
    perf_ytd_percentage FLOAT, 
    dividend FLOAT, 
    p_fcf FLOAT, 
    eps_past_5y_percentage FLOAT, 
    roi_percentage FLOAT, 
    high_52w_percentage FLOAT, 
    beta FLOAT, 
    dividend_percentage FLOAT, 
    quick_ratio FLOAT, 
    sales_past_5y_percentage FLOAT, 
    gross_margin_percentage FLOAT, 
    low_52w_percentage FLOAT, 
    atr FLOAT, 
    employees FLOAT, 
    current_ratio FLOAT, 
    sales_q_q_percentage FLOAT, 
    oper_margin_percentage FLOAT, 
    rsi_14 FLOAT, 
    volatility_w_percentage FLOAT, 
    volatility_m_percentage FLOAT, 
    optionable BOOLEAN,
    debt_eq FLOAT, 
    eps_q_q_percentage FLOAT, 
    profit_margin_percentage FLOAT, 
    rel_volume FLOAT, 
    prev_close FLOAT, 
    shortable BOOLEAN, 
    lt_debt_eq FLOAT, 
    payout_percentage FLOAT, 
    avg_volume FLOAT, 
    price FLOAT, 
    recom FLOAT, 
    sma20_percentage FLOAT, 
    sma50_percentage FLOAT, 
    sma200_percentage FLOAT, 
    volume FLOAT, 
    change_percentage FLOAT,
    short_float_percentage FLOAT, 
    short_float_ratio FLOAT,
    earnings_date DATE, 
    earnings_timing VARCHAR(25)
);

DROP TABLE IF EXISTS dim_news;
CREATE TABLE IF NOT EXISTS dim_news (
    news_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_tickers(ticker),
    news_date TIMESTAMP,
    news_title VARCHAR(555),
    news_link VARCHAR(255),
    news_source VARCHAR(255)
);

DROP TABLE IF EXISTS dim_quotes;
CREATE TABLE IF NOT EXISTS dim_quotes (
    quote_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_tickers(ticker),
    quote_date TIMESTAMP,
    open_price FLOAT,
    close_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    volume BIGINT,
    adj_close_price FLOAT,
    dividend FLOAT,
    splits FLOAT
);

DROP TABLE dim_inside_trades;
CREATE TABLE IF NOT EXISTS dim_inside_trades (
    inside_trade_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_tickers(ticker),
    traded_by VARCHAR(100),
    relationship VARCHAR(100),
    trading_date VARCHAR(50),
    transaction_type VARCHAR(55),
    share_price FLOAT,
    no_of_shares INT,
    transaction_value FLOAT,
    shares_total INT,
    sec_form_4 VARCHAR(255),
    sec_form_4_link VARCHAR(255),
    insider_id INT
);

DROP TABLE dim_ratings;
CREATE TABLE IF NOT EXISTS dim_ratings (
    rating_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_tickers(ticker),
    rating_date DATE,
    rating_status VARCHAR(55),
    rating_agency VARCHAR(55),
    previous_rating VARCHAR(55),
    current_rating VARCHAR(55),
    previous_price FLOAT,
    current_price FLOAT
);

DROP TABLE dim_balance_sheets;
CREATE TABLE IF NOT EXISTS dim_balance_sheets (
    balance_sheet_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_tickers(ticker),
    fiscal_date_ending DATE, 
    reported_currency VARCHAR(8), 
    total_assets BIGINT, 
    total_current_assets BIGINT, 
    cash_and_cash_equivalents_at_carrying_value BIGINT, 
    cash_and_short_term_investments BIGINT, 
    inventory BIGINT, 
    current_net_receivables BIGINT, 
    total_non_current_assets BIGINT, 
    property_plant_equipment BIGINT, 
    accumulated_depreciation_amortization_ppe BIGINT, 
    intangible_assets BIGINT, 
    intangible_assets_excluding_goodwill BIGINT, 
    goodwill BIGINT, 
    investments BIGINT, 
    long_term_investments BIGINT, 
    short_term_investments BIGINT, 
    other_current_assets BIGINT, 
    other_non_current_assets BIGINT, 
    total_liabilities BIGINT, 
    total_current_liabilities BIGINT, 
    current_accounts_payable BIGINT, 
    deferred_revenue BIGINT, 
    current_debt BIGINT, 
    short_term_debt BIGINT, 
    total_non_current_liabilities BIGINT, 
    capital_lease_obligations BIGINT, 
    long_term_debt BIGINT, 
    current_long_term_debt BIGINT, 
    long_term_debt_noncurrent BIGINT, 
    short_long_term_debt_total BIGINT, 
    other_current_liabilities BIGINT, 
    other_non_current_liabilities BIGINT, 
    total_shareholder_equity BIGINT, 
    treasury_stock BIGINT, 
    retained_earnings BIGINT, 
    common_stock BIGINT, 
    common_stock_shares_outstanding BIGINT, 
    report_type VARCHAR(25)
);

DROP TABLE dim_income_statements;
CREATE TABLE IF NOT EXISTS dim_income_statements (
    income_statement_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_tickers(ticker),
    fiscal_date_ending DATE,
    reported_currency VARCHAR(8),
    gross_profit BIGINT, 
    total_revenue BIGINT, 
    cost_of_revenue BIGINT, 
    costof_goods_and_services_sold BIGINT, 
    operating_income BIGINT, 
    selling_general_and_administrative BIGINT, 
    research_and_development BIGINT, 
    operating_expenses BIGINT, 
    investment_income_net BIGINT, 
    net_interest_income BIGINT, 
    interest_income BIGINT, 
    interest_expense BIGINT, 
    non_interest_income BIGINT, 
    other_non_operating_income BIGINT, 
    depreciation BIGINT, 
    depreciation_and_amortization BIGINT, 
    income_before_tax BIGINT, 
    income_tax_expense BIGINT, 
    interest_and_debt_expense BIGINT, 
    net_income_from_continuing_operations BIGINT, 
    comprehensive_income_net_of_tax BIGINT, 
    ebit BIGINT, 
    ebitda BIGINT, 
    net_income BIGINT, 
    report_type VARCHAR(25) 
);

DROP TABLE dim_cash_flows;
CREATE TABLE IF NOT EXISTS dim_cash_flows (
    cash_flow_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_tickers(ticker),
    fiscal_date_ending DATE,
    reported_currency VARCHAR(8),
    operating_cashflow BIGINT, 
    payments_for_operating_activities BIGINT, 
    proceeds_from_operating_activities BIGINT, 
    change_in_operating_liabilities BIGINT, 
    change_in_operating_assets BIGINT, 
    depreciation_depletion_and_amortization BIGINT, 
    capital_expenditures BIGINT, 
    change_in_receivables BIGINT, 
    change_in_inventory BIGINT, 
    profit_loss BIGINT, 
    cashflow_from_investment BIGINT, 
    cashflow_from_financing BIGINT, 
    proceeds_from_repayments_of_short_term_debt BIGINT, 
    payments_for_repurchase_of_common_stock BIGINT, 
    payments_for_repurchase_of_equity BIGINT, 
    payments_for_repurchase_of_preferred_stock BIGINT, 
    dividend_payout BIGINT, 
    dividend_payout_common_stock BIGINT, 
    dividend_payout_preferred_stock BIGINT, 
    proceeds_from_issuance_of_common_stock BIGINT, 
    proceeds_from_issuance_of_long_term_debt_and_capital_securities_net BIGINT, 
    proceeds_from_issuance_of_preferred_stock BIGINT, 
    proceeds_from_repurchase_of_equity BIGINT, 
    proceeds_from_sale_of_treasury_stock BIGINT, 
    change_in_cash_and_cash_equivalents BIGINT, 
    change_in_exchange_rate BIGINT, 
    net_income BIGINT,
    report_type VARCHAR(25) 
);

DROP TABLE dim_annual_earnings;
CREATE TABLE IF NOT EXISTS dim_annual_earnings (
    annual_earnings_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_tickers(ticker),
    fiscal_date_ending DATE,
    reported_eps FLOAT
);

DROP TABLE dim_quarterly_earnings;
CREATE TABLE IF NOT EXISTS dim_quarterly_earnings (
    quarterly_earnings_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_tickers(ticker),
    fiscal_date_ending DATE,
    reported_date DATE,
    reported_eps FLOAT,
    estimated_eps FLOAT,
    surprise FLOAT,
    surprise_percentage FLOAT
);


DROP TABLE inside_trades;
CREATE TABLE IF NOT EXISTS inside_trades (
    inside_trade_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8),
    traded_by VARCHAR(100),
    relationship VARCHAR(100),
    trading_date DATE,
    transaction_type VARCHAR(55),
    share_price FLOAT,
    no_of_shares INT,
    transaction_value FLOAT,
    total_shares INT,
    sec_form_4 VARCHAR(255),
    sec_form_4_link VARCHAR(255)
);

DROP TABLE news;
CREATE TABLE IF NOT EXISTS news (
    news_id SERIAL PRIMARY KEY,
    news_date TIMESTAMP,
    news_title VARCHAR(555),
    news_source VARCHAR(255),
    news_link VARCHAR(255)
);

DROP TABLE blogs;
CREATE TABLE IF NOT EXISTS blogs (
    blogs_id SERIAL PRIMARY KEY,
    blogs_date TIMESTAMP,
    blogs_title VARCHAR(555),
    blogs_source VARCHAR(255),
    blogs_link VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS calendar (
    calendar_id SERIAL PRIMARY KEY,
    news_date TIMESTAMP,
    release_title VARCHAR(255),
    impact INT,
    release_for VARCHAR(25),
    actual VARCHAR(25),
    expected VARCHAR(25),
    previous VARCHAR(25)
);

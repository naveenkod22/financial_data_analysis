CREATE TABLE IF NOT EXISTS fact_ticker (
  ticker VARCHAR(8) PRIMARY KEY,
  sector VARCHAR(55),
  industry TEXT,
  country TEXT,
  index TEXT,
  ticker_description TEXT
);

CREATE TABLE IF NOT EXISTS dim_fundamentals (
    fundamental_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_ticker(ticker),
    updated_date DATE,
    p_e FLOAT,
    eps_ttmc FLOAT,
    insider_own FLOAT,
    shs_outstanding BIGINT,
    perfomance_week FLOAT,
    market_cap BIGINT,
    forward_p_e FLOAT,
    eps_next_y FLOAT,
    insider_trans FLOAT,
    shs_float BIGINT,
    perfomance_month FLOAT,
    income BIGINT,
    peg FLOAT,
    eps_next_q FLOAT,
    inst_own FLOAT,
    short_float FLOAT,
    short_float_ratio FLOAT,
    perfomance_quarter FLOAT,
    sales BIGINT,
    p_s FLOAT,
    eps_this_y FLOAT,
    inst_trans FLOAT,
    short_interst BIGINT,
    performance_half_y FLOAT,
    book_sh FLOAT,
    p_b FLOAT,
    eps_next_y_percentage FLOAT,
    roa FLOAT,
    target_price FLOAT,
    performance_year FLOAT,
    cash_sh FLOAT,
    p_c FLOAT,
    eps_next_5y FLOAT,
    roe FLOAT,
    range_from_52w FLOAT,
    range_to_52w FLOAT,
    performance_ytd FLOAT,
    dividend FLOAT,
    p_fcp FLOAT,
    eps_past_5y FLOAT,
    roi FLOAT,
    high_52w FLOAT,
    beta FLOAT,
    dividend_percentage FLOAT,
    quick_ratio FLOAT,
    sales_past_5y FLOAT,
    gross_margin FLOAT,
    low_52w FLOAT,
    atr FLOAT,
    employees BIGINT,
    current_ratio FLOAT,
    sales_qq FLOAT,
    operational_margin FLOAT,
    rsi_14 FLOAT,
    volatility_w FLOAT,
    volatility_m FLOAT,
    optionable BOOLEAN,
    debt_equity FLOAT,
    eps_qq FLOAT,
    profit_margin FLOAT,
    rel_volume FLOAT,
    previous_close FLOAT,
    shortable BOOLEAN,
    lt_debt_equity FLOAT,
    earnings_date DATE,
    earnings_time VARCHAR(5),
    payout FLOAT,
    avg_volume BIGINT,
    price FLOAT,
    recom FLOAT,
    sma_20 FLOAT,
    sma_50 FLOAT,
    sma_200 FLOAT,
    volume BIGINT,
    change FLOAT
);

CREATE TABLE IF NOT EXISTS dim_news (
    news_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_ticker(ticker),
    news_date TIMESTAMP,
    news_title VARCHAR(555),
    news_link VARCHAR(255),
    news_source VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_quotes (
    quote_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_ticker(ticker),
    quote_date TIMESTAMP,
    open_price FLOAT,
    close_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    volume INT,
    adj_close_price FLOAT,
    dividend FLOAT,
    splits FLOAT
);

CREATE TABLE IF NOT EXISTS dim_inside_trade (
    inside_trade_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_ticker(ticker),
    trading_owner VARCHAR(100),
    relationship VARCHAR(100),
    trading_date DATE,
    transaction_type VARCHAR(55),
    cost FLOAT,
    no_of_shares INT,
    transcation_value FLOAT,
    shares_total INT,
    sec_form_4 VARCHAR(255),
    sec_form_4_link VARCHAR(255),
    insider_id INT
);

CREATE TABLE IF NOT EXISTS dim_ratings (
    rating_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_ticker(ticker),
    rating_date DATE,
    rating_status VARCHAR(55),
    rating_agency VARCHAR(55),
    previous_rating VARCHAR(55),
    current_rating VARCHAR(55),
    previous_price FLOAT,
    current_price FLOAT
);

CREATE TABLE IF NOT EXISTS dim_balance_sheet (
    balance_sheet_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_ticker(ticker),
    fiscal_date_ending DATE,
    reported_currency VARCHAR(8),
    total_assets FLOAT,
    total_current_assets FLOAT,
    cash_and_cash_equivalents FLOAT,
    cash_short_term_investments FLOAT,
    inventory FLOAT,
    current_net_receivables FLOAT,
    total_non_current_assets FLOAT,
    property_plant_equipment FLOAT,
    accumulated_depreciation_amortization_ppe FLOAT,
    intangible_assets FLOAT,
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

CREATE TABLE IF NOT EXISTS dim_income_statement (
    income_statement_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_ticker(ticker),
    fiscal_date_ending DATE,
    reported_currency VARCHAR(8),
    gross_profit FLOAT,
    cost_of_revenue FLOAT,
    cost_of_goods_and_services_sold FLOAT,
    operating_income FLOAT,
    selling_general_and_administrative FLOAT,
    research_and_development FLOAT,
    operating_expenses FLOAT,
    investment_income_net FLOAT,
    net_interest_income FLOAT,
    interest_income FLOAT,
    interest_expense FLOAT,
    non_interest_income FLOAT,
    other_non_operating_income FLOAT,
    depreciation FLOAT,
    depreciation_and_amortization FLOAT,
    income_before_tax FLOAT,
    income_tax_expense FLOAT,
    interest_and_depreciation_expense FLOAT,
    net_income_from_continuing_ops FLOAT,
    comprehensive_income_net_of_tax FLOAT,
    ebit FLOAT,
    ebitda FLOAT,
    net_income FLOAT,
    report_type VARCHAR(8) 
);

CREATE TABLE IF NOT EXISTS dim_annual_earnings (
    cashflow_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_ticker(ticker),
    fiscal_date_ending DATE,
    reported_eps FLOAT
);

CREATE TABLE IF NOT EXISTS dim_quarterly_earnings (
    cashflow_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_ticker(ticker),
    fiscal_date_ending DATE,
    reported_date DATE,
    reported_eps FLOAT,
    estimated_eps FLOAT,
    surprise FLOAT,
    surprise_percentage FLOAT
);

CREATE TABLE IF NOT EXISTS inside_trades (
    inside_trade_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8),
    trading_owner VARCHAR(100),
    relationship VARCHAR(100),
    trading_date DATE,
    transaction_type VARCHAR(55),
    cost FLOAT,
    no_of_shares INT,
    transcation_value FLOAT,
    shares_total INT,
    sec_form_4 VARCHAR(255),
    sec_form_4_link VARCHAR(255),
    insider_id INT
);

CREATE TABLE IF NOT EXISTS news (
    news_id SERIAL PRIMARY KEY,
    news_date TIMESTAMP,
    news_title VARCHAR(555),
    news_source VARCHAR(255),
    news_link VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS blogs (
    news_id SERIAL PRIMARY KEY,
    news_date TIMESTAMP,
    news_title VARCHAR(555),
    news_source VARCHAR(255),
    news_link VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS calendar (
    news_id SERIAL PRIMARY KEY,
    news_date TIMESTAMP,
    release_title VARCHAR(255),
    impact INT,
    release_for VARCHAR(25),
    actual VARCHAR(25),
    expected VARCHAR(25),
    previous VARCHAR(25)
);
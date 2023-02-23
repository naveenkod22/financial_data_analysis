DROP TABLE dim_annual_earnings;
CREATE TABLE IF NOT EXISTS dim_annual_earnings (
    annual_earnings_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_ticker(ticker),
    fiscal_date_ending DATE,
    reported_eps FLOAT
);

DROP TABLE dim_quarterly_earnings;
CREATE TABLE IF NOT EXISTS dim_quarterly_earnings (
    quarterly_earnings_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8) REFERENCES fact_ticker(ticker),
    fiscal_date_ending DATE,
    reported_date DATE,
    reported_eps FLOAT,
    estimated_eps FLOAT,
    surprise FLOAT,
    surprise_percentage FLOAT
);
DROP TABLE inside_trade;
CREATE TABLE IF NOT EXISTS inside_trade (
    inside_trade_id SERIAL PRIMARY KEY,
    ticker VARCHAR(8),
    traded_by VARCHAR(100),
    relationship VARCHAR(100),
    trading_date DATE,
    transaction_type VARCHAR(55),
    share_price FLOAT,
    no_of_shares INT,
    transcation_value FLOAT,
    shares_total INT,
    sec_form_4 VARCHAR(255),
    sec_form_4_link VARCHAR(255),
    insider_id INT
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
    blog_title VARCHAR(555),
    blog_source VARCHAR(255),
    blog_link VARCHAR(255)
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

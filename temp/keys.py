dict = {   'Ticker': 'Ticker',
    'Company': 'Company',
    'Sector': 'Sector',
    'Industry': 'Industry',
    'Country': 'Country',
    'Market Cap': 'Market_Cap',
    'Volume': 'Volume',
    'Shs Outstand': 'Shs_Outstand',
    'Employees': 'Employees',
    'Avg Volume': 'Avg_Volume',
    'Change': 'Change',
    'P/E': 'P_E',
    'EPS (ttm)': 'EPS_ttm',
    'Forward P/E': 'Forward_PE',
    'EPS next Y': 'EPS_next_Y',
    'Target Price': 'Target_Price',
    'High 52W(%)': 'High_52W_percentage',
    'Range from 52W(%)': 'Range_from_52W_percentage',
    'Range to 52W(%)': 'Range_to_52W_percentage',
    'Low 52W(%)': 'Low_52W_percentage',
    'Prev Close': 'Prev_Close',
    'Recom': 'Recom',
    'SMA20(%)': 'SMA20_percentage',
    'SMA50(%)': 'SMA50_percentage',
    'SMA200(%)': 'SMA200_percentage',
    'Perf Week(%)': 'Perf_Week_percentage',
    'Perf Month(%)': 'Perf_Month_percentage',
    'Perf Quarter(%)': 'Perf_Quarter_percentage',
    'Perf Half Y(%)': 'Perf_Half_Y_percentage',
    'Perf Year(%)': 'Perf_Year_percentage',
    'Perf YTD(%)': 'Perf_YTD_percentage',
    'EPS this Y(%)': 'EPS_this_Y_percentage',
    'EPS next Y Percentage(%)': 'EPS_next_Y_percentage',
    'ROA(%)': 'ROA_percentage',
    'ROE(%)': 'ROE_percentage',
    'EPS next 5Y(%)': 'EPS_next_5Y_percentage',
    'EPS past 5Y(%)': 'EPS_past_5Y_percentage',
    'Sales past 5Y(%)': 'Sales_past_5Y_percentage',
    'Sales Q/Q(%)': 'Sales_Q_Q_percentage',
    'EPS Q/Q(%)': 'EPS_Q_Q_percentage',
    'ROI(%)': 'ROI_percentage',
    'RSI (14)': 'RSI_14',
    'Volatility W(%)': 'Volatility_W_percentage',
    'Volatility M(%)': 'Volatility_M_percentage',
    'Rel Volume': 'Rel_Volume',
    'Insider Own(%)': 'Insider_Own_percentage',
    'Insider Trans(%)': 'Insider_Trans_percentage',
    'Inst Own(%)': 'Inst_Own_percentage',
    'Shs Float': 'Shs_Float',
    'Income': 'Income',
    'PEG': 'PEG',
    'Dividend': 'Dividend',
    'beta': 'beta',
    'Dividend %(%)': 'Dividend_percentage',
    'EPS next Q': 'EPS_next_Q',
    'Sales': 'Sales',
    'P/S': 'P_S',
    'Book/sh': 'Book_sh',
    'P/B': 'P_B',
    'Cash/sh': 'Cash_sh',
    'P/FCF': 'P_FCP',
    'P/C': 'P_C',
    'Debt/Eq': 'Debt_Eq',
    'LT Debt/Eq': 'LT_Debt_Eq',
    'Quick Ratio': 'Quick_Ratio',
    'Current Ratio': 'Current_Ratio',
    'Gross Margin(%)': 'Gross_Margin_percentage',
    'Oper. Margin(%)': 'Operational_Margin_percentage',
    'Profit Margin(%)': 'Profit_Margin_percentage',
    'Payout(%)': 'Payout_percentage',
    'Debt/Eq': 'Debt_Eq',
    'LT Debt/Eq': 'LT_Debt_Eq',
    'Short Interest': 'Short_Interest',
    'Short Float(%)': 'Short_Float_Percentage_percentage',
    'Short Float Ratio': 'Short_Float_Ratio',
    'ATR': 'ATR',
    'Signals': 'Signals',
    'NoOf Signals': 'NoOf_Signals',
    'Earnings Timing': 'Earnings_Timing',
    'Earnings Date': 'Earnings_Date',
    'Optionable': 'Optionable',
    'Shortable': 'Shortable',
    'Index': 'Index',
    'Short Float Percentage(%)': 'Short_Float_percentage',
    
 }

lst = ['Ticker', 'Company', 'Sector', 'Industry', 'Country', 'Market Cap',
       'P/E', 'Price', 'Change', 'Volume', 'Signals', 'NoOf Signals', 'Index',
       'EPS (ttm)', 'Insider Own(%)', 'Shs Outstand', 'Perf Week(%)',
       'Forward P/E', 'EPS next Y', 'Insider Trans(%)', 'Shs Float',
       'Perf Month(%)', 'Income', 'PEG', 'EPS next Q', 'Inst Own(%)',
       'Perf Quarter(%)', 'Sales', 'P/S', 'EPS this Y(%)', 'Inst Trans(%)',
       'Short Interest', 'Perf Half Y(%)', 'Book/sh', 'P/B',
       'EPS next Y Percentage(%)', 'ROA(%)', 'Target Price', 'Perf Year(%)',
       'Cash/sh', 'P/C', 'EPS next 5Y(%)', 'ROE(%)', '52W Range From',
       '52W Range To', 'Perf YTD(%)', 'Dividend', 'P/FCF', 'EPS past 5Y(%)',
       'ROI(%)', '52W High(%)', 'Beta', 'Dividend %(%)', 'Quick Ratio',
       'Sales past 5Y(%)', 'Gross Margin(%)', '52W Low(%)', 'ATR', 'Employees',
       'Current Ratio', 'Sales Q/Q(%)', 'Oper. Margin(%)', 'RSI (14)',
       'Volatility W(%)', 'Volatility M(%)', 'Optionable', 'Debt/Eq',
       'EPS Q/Q(%)', 'Profit Margin(%)', 'Rel Volume', 'Prev Close',
       'Shortable', 'LT Debt/Eq', 'Payout(%)', 'Avg Volume', 'Recom',
       'SMA20(%)', 'SMA50(%)', 'SMA200(%)', 'Description', 'Date',
       'Short Float Percentage(%)', 'Short Float Ratio', 'Earnings_Date',
       'Earnings_Timing']

# dict keys to list
keys = list(dict.keys())
# unique values in list
unique = list(set(lst))
print(len(unique))
print(len(keys))
"""
0 Ticker Ticker
1 Company Company
2 Sector Sector
3 Industry Industry
4 Country Country
5 Market Cap Market_Cap
6 Volume Volume
7 Shs Outstand Shs_Outstand
8 Employees Employees
9 Avg Volume Avg_Volume
10 Change Change
11 P/E P_E
12 EPS (ttm) EPS_ttm
13 Forward P/E Forward_PE
14 EPS next Y EPS_next_Y
15 Target Price Target_Price
16 Prev Close Prev_Close
17 Recom Recom
18 SMA20(%) SMA20_percentage
19 SMA50(%) SMA50_percentage
20 SMA200(%) SMA200_percentage
21 Perf Week(%) Perf_Week_percentage
22 Perf Month(%) Perf_Month_percentage
23 Perf Quarter(%) Perf_Quarter_percentage
24 Perf Half Y(%) Perf_Half_Y_percentage
25 Perf Year(%) Perf_Year_percentage
26 Perf YTD(%) Perf_YTD_percentage
27 EPS this Y(%) EPS_this_Y_percentage
28 EPS next Y Percentage(%) EPS_next_Y_percentage
29 ROA(%) ROA_percentage
30 ROE(%) ROE_percentage
31 EPS next 5Y(%) EPS_next_5Y_percentage
32 EPS past 5Y(%) EPS_past_5Y_percentage
33 Sales past 5Y(%) Sales_past_5Y_percentage
34 Sales Q/Q(%) Sales_Q_Q_percentage
35 EPS Q/Q(%) EPS_Q_Q_percentage
36 ROI(%) ROI_percentage
37 RSI (14) RSI_14
38 Volatility W(%) Volatility_W_percentage
39 Volatility M(%) Volatility_M_percentage
40 Rel Volume Rel_Volume
41 Insider Own(%) Insider_Own_percentage
42 Insider Trans(%) Insider_Trans_percentage
43 Inst Own(%) Inst_Own_percentage
44 Shs Float Shs_Float
45 Income Income
46 PEG PEG
47 Dividend Dividend
48 Dividend %(%) Dividend_percentage
49 EPS next Q EPS_next_Q
50 Sales Sales
51 P/S P_S
52 Book/sh Book_sh
53 P/B P_B
54 Cash/sh Cash_sh
55 P/FCF P_FCP
56 P/C P_C
57 Debt/Eq Debt_Eq
58 LT Debt/Eq LT_Debt_Eq
59 Quick Ratio Quick_Ratio
60 Current Ratio Current_Ratio
61 Gross Margin(%) Gross_Margin_percentage
62 Oper. Margin(%) Operational_Margin_percentage
63 Profit Margin(%) Profit_Margin_percentage
64 Payout(%) Payout_percentage
65 Short Interest Short_Interest
66 Short Float Ratio Short_Float_Ratio
67 ATR ATR
68 Signals Signals
69 NoOf Signals NoOf_Signals
70 Optionable Optionable
71 Shortable Shortable
72 Index Index
73 Short Float Percentage(%) Short_Float_percentage

"""


Index(['Ticker', 'Company', 'Sector', 'Industry', 'Country', 'Market_Cap',
       'P_E', 'Price', 'Change', 'Volume', 'Signals', 'NoOf_Signals', 'Index',
       'EPS_ttm', 'Insider_Own_percentage', 'Shs_Outstand',
       'Perf_Week_percentage', 'Forward_PE', 'EPS_next_Y',
       'Insider_Trans_percentage', 'Shs_Float', 'Perf_Month_percentage',
       'Income', 'PEG', 'EPS_next_Q', 'Inst_Own_percentage',
       'Perf_Quarter_percentage', 'Sales', 'P_S', 'EPS_this_Y_percentage',
       'Inst Trans(%)', 'Short_Interest', 'Perf_Half_Y_percentage', 'Book_sh',
       'P_B', 'EPS_next_Y_percentage', 'ROA_percentage', 'Target_Price',
       'Perf_Year_percentage', 'Cash_sh', 'P_C', 'EPS_next_5Y_percentage',
       'ROE_percentage', '52W Range From', '52W Range To',
       'Perf_YTD_percentage', 'Dividend', 'P_FCP', 'EPS_past_5Y_percentage',
       'ROI_percentage', '52W High(%)', 'Beta', 'Dividend_percentage',
       'Quick_Ratio', 'Sales_past_5Y_percentage', 'Gross_Margin_percentage',
       '52W Low(%)', 'ATR', 'Employees', 'Current_Ratio',
       'Sales_Q_Q_percentage', 'Operational_Margin_percentage', 'RSI_14',
       'Volatility_W_percentage', 'Volatility_M_percentage', 'Optionable',
       'Debt_Eq', 'EPS_Q_Q_percentage', 'Profit_Margin_percentage',
       'Rel_Volume', 'Prev_Close', 'Shortable', 'LT_Debt_Eq',
       'Payout_percentage', 'Avg_Volume', 'Recom', 'SMA20_percentage',
       'SMA50_percentage', 'SMA200_percentage', 'Description', 'Date',
       'Short_Float_percentage', 'Short_Float_Ratio', 'Earnings_Date',
       'Earnings_Timing'],
      dtype='object')
{'Forward P/E', 'Debt/Eq', 'Inst Own(%)', 'Rel Volume', 'Sales past 5Y(%)', 'P/C', 'Dividend %(%)', 'P/FCF', 'Short Float Percentage(%)', 'Prev Close', 'Shs Outstand', 'Perf Half Y(%)', 'Perf Year(%)', 'ROI(%)', 'High 52W(%)', 'Earnings Date', 'Payout(%)', 'Short Interest', 'Short Float(%)', 'Current Ratio', 'Sales Q/Q(%)', 'Target Price', 'EPS Q/Q(%)', 'EPS past 5Y(%)', 'EPS (ttm)', 'Perf Month(%)', 'Gross Margin(%)', 'Perf Week(%)', 'Market Cap', 'Oper. Margin(%)', 'NoOf Signals', 'Range from 52W(%)', 'ROE(%)', 'Low 52W(%)', 'EPS next Y Percentage(%)', 'Insider Trans(%)', 'Volatility W(%)', 'SMA200(%)', 'Book/sh', 'EPS next 5Y(%)', 'EPS this Y(%)', 'P/S', 'Perf Quarter(%)', 'Short Float Ratio', 'Range to 52W(%)', 'SMA20(%)', 'beta', 'Insider Own(%)', 'Earnings Timing', 'EPS next Q', 'Avg Volume', 'Volatility M(%)', 'Profit Margin(%)', 'EPS next Y', 'Shs Float', 'ROA(%)', 'RSI (14)', 'P/B', 'Quick Ratio', 'Perf YTD(%)', 'LT Debt/Eq', 'SMA50(%)', 'P/E', 'Cash/sh'}
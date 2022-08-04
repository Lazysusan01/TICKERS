import pandas as pd
import yfinance as yf
import numpy as np

def yahoo_finance(Ticker_codes):
    yahoo_df = pd.DataFrame()

    for i in Ticker_codes:
        ticker = yf.Ticker(i).info
        temp_df = pd.DataFrame([ticker['shortName'],ticker['regularMarketPrice'], ticker['previousClose']]).transpose()
        yahoo_df = pd.concat([yahoo_df, temp_df])

    #combine baltic
    return yahoo_df

Ticker_codes = ['BZ=F', 'NG=F', 'GBPUSD=X', 'JPY=X', 'BMS.L']

yahoo_finance = yahoo_finance(Ticker_codes)
yahoo_finance.columns=['Name','Price','Previous Close']

print(yahoo_finance)

yahoo_finance.loc[yahoo_finance['Name'] == 'BRAEMAR SHIPPING SERVICES PLC O', 'Name'] = 'Braemar'

print(yahoo_finance)
import pandas as pd
import yfinance as yf
# %%
    ## Yahoo finance API
    #data to be pulled

    ##commodities
    #brent crude /bbl : CL=F
    #nat gas futures : NG=F
    #coal futures API2 : MTF=F (getting delisted)

    ##currencies
    #gbp/usd : GBPUSD=X
    #usd/yen : JPY=X

    #braemar share price : 'bms.l'

    #data needed: Current price, prev day close, name
def yahoo_finance(Ticker_codes):
    yahoo_df = pd.DataFrame()

    for i, row in Ticker_codes.iterrows():
        ticker = yf.Ticker(Ticker_codes['Tickers'][i]).info
        temp_df = pd.DataFrame([ticker['shortName'],ticker['regularMarketPrice'], ticker['previousClose']]).transpose()
        yahoo_df = pd.concat([yahoo_df, temp_df])
    # for i in Ticker_codes:
    #     ticker = yf.Ticker(i).info
    #     temp_df = pd.DataFrame([ticker['shortName'],ticker['regularMarketPrice'], ticker['previousClose']]).transpose()
    #     yahoo_df = pd.concat([yahoo_df, temp_df])

    #combine baltic
    return yahoo_df
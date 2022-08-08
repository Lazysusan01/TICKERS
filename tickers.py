# %%
from cProfile import run
from email import header
from wsgiref import headers
import requests 
import pandas as pd
import yfinance as yf
from io import StringIO
import pyodbc
import numpy as np
from datetime import datetime
import time, os
from os import sys

# %%
def run_tickers():
    #importing braemarscreen API : CSVCURVE
    csv_curve_url = 'https://api.braemarscreen.com/v2/markets/csvcurve'
    headers_bs  = {'Authorization': "Token c8f1c7d8bf8015c688b744bd386273486afd5e33"}

    csv_curve = requests.get(csv_curve_url, headers=headers_bs).content
    csv_pd = pd.read_csv(StringIO(csv_curve.decode('utf-8')))

    #adding 'string' and name columns for search (market + product + type e.g 'C5TCJUNE22MIDPOINT')

    csv_str = list()
    csv_name = list()
    for i in range(len(csv_pd)):
        string = csv_pd['market'][i] + csv_pd['product'][i]+ csv_pd['type'][i]
        full_name = csv_pd['market'][i] +" "+csv_pd['product'][i]
        csv_str.append(string.upper())
        csv_name.append(full_name.upper())

    csv_pd.insert(0,'string',csv_str)
    csv_pd.insert(0,'name',csv_name)
    csv_pd.head()
    # %%
    #Importing baltic exchange data
    baltic_url = "https://api.balticexchange.com/api/v1.1/feed/FDS4EK10KTGNKFH5LWTBAATLYEPYDZW6/data"
    headers = {'x-apikey' : '37DIH7JMY1OAQH463E4X60I4GGEPIIZFU4ZMRTI3BV53RHCAY85Y2VZCQX474I4Z'}

    baltic_exchange = requests.get(baltic_url, headers =headers)
    baltic_json = baltic_exchange.json()
    #print(baltic_json['data'])

    # %%
    #parsing out the nested json response
    def baltic_curr_month_prices(baltic_json):
        df = pd.DataFrame(baltic_json)
        df2 = pd.json_normalize(df['groupings'])
        df3 = pd.json_normalize(df2[0])
        df4 = pd.json_normalize(df3['groups'])
        df5 = pd.json_normalize(df4[0])
        df6 = pd.json_normalize(df5['projections'])
        df7 = pd.json_normalize(df6[0])
        return df7.iloc[:,0:3]

    def baltic_front_month_prices(baltic_json):
        df = pd.DataFrame(baltic_json)
        df2 = pd.json_normalize(df['groupings'])
        df3 = pd.json_normalize(df2[0])
        df4 = pd.json_normalize(df3['groups'])
        df5 = pd.json_normalize(df4[0])
        df6 = pd.json_normalize(df5['projections'])
        df7 = pd.json_normalize(df6[1])
        
        return df7.iloc[:,0:3]  

    def baltic_next_q_prices(baltic_json):
        df = pd.DataFrame(baltic_json)
        df2 = pd.json_normalize(df['groupings'])
        df3 = pd.json_normalize(df2[0])
        df4 = pd.json_normalize(df3['groups'])
        df5 = pd.json_normalize(df4[1])
        df6 = pd.json_normalize(df5['projections'])
        df7 = pd.json_normalize(df6[1])
        
        return df7.iloc[:,0:3]  

    baltic_front_month_prices = baltic_front_month_prices(baltic_json)
    baltic_curr_month_prices = baltic_curr_month_prices(baltic_json)
    baltic_next_q_prices = baltic_next_q_prices(baltic_json)

    baltic_prices = pd.concat([baltic_curr_month_prices,baltic_next_q_prices])
    baltic_prices = baltic_prices.reset_index(drop =True)
    print(baltic_prices)
    # %%
    #creating search string for braemarscreens API output : csvcurve 
    #removing spaces and converting to upper
    curr_month = baltic_curr_month_prices['period'][0].replace(" ","")
    front_month = baltic_front_month_prices['period'][0].replace(" ","")
    next_q = baltic_next_q_prices['period'][0].replace(" ","")

    c5tc__curr_month_search_str = str('cape' + curr_month + 'midpoint').upper()
    c5tc_next_q_search_str = str('cape' + next_q + 'midpoint').upper()
    p4tc_curr_month_search_str = str('pmax' + curr_month + 'midpoint').upper()
    p4tc_next_q_search_str = str('pmax' + next_q + 'midpoint').upper()
    #singapore fuel oil prices
    hsfo = 'BEX35SGSINMIDPOINT'
    vlsfo = 'BEX05SGSINMIDPOINT'

    search_terms = {'string':[c5tc__curr_month_search_str,p4tc_curr_month_search_str,c5tc_next_q_search_str,p4tc_next_q_search_str,hsfo,vlsfo]}
    #,hsfo,vlsfo]}
    search_terms = pd.DataFrame(search_terms)

    print(search_terms)

    # %%
    #searching for strings in braemarscreens output 
    braemar_screens_data = pd.merge(search_terms,csv_pd, on = 'string', how='left')
    combined_ffa_data = pd.concat([braemar_screens_data,baltic_prices],axis = 1)

    #(5,baltic_prices['value'],)
    columns = ['name','price','value']
    combined_ffa_data = combined_ffa_data.loc[:,columns]
    combined_ffa_data.columns=['Name','Price','Previous Close']
    print(combined_ffa_data)

    # %%
    cnxn_str = ("Driver={SQL Server};"
                "Server=S1RESDB01.CORP.BRAEMAR.COM;"
                "Database=Braemar2;")

    cnxn = pyodbc.connect(cnxn_str)

    cursor = cnxn.cursor()
    #cursor.execute("SELECT * FROM ,(SELECT TOP 2 * FROM PRICE WHERE PRICE_DEFINITION_ID = 2088, ORDER BY DATE DESC) VLSFO")

    sql_query = pd.read_sql("""SELECT * FROM (SELECT TOP 2 * FROM PRICE WHERE PRICE_DEFINITION_ID = 2088 ORDER BY DATE DESC) VLSFO
        UNION
        SELECT * FROM
        (SELECT TOP 2 * FROM PRICE WHERE PRICE_DEFINITION_ID = 2087
        ORDER BY DATE DESC) HSFO
        UNION
        SELECT * FROM
        (SELECT TOP 2 * FROM PRICE WHERE PRICE_DEFINITION_ID = 4251
        ORDER BY DATE DESC) LNG
        UNION
        SELECT * FROM
        (SELECT TOP 2 * FROM PRICE WHERE PRICE_DEFINITION_ID = 4094
        ORDER BY DATE DESC) Coal
        UNION
        --Iron Ore (62FE)
        SELECT * FROM
        (SELECT TOP 2 * FROM PRICE WHERE PRICE_DEFINITION_ID = 4092
        ORDER BY DATE DESC) Ore
    """,cnxn)

    conditions = [
        (sql_query['PRICE_DEFINITION_ID'] == 4094), #coal
        (sql_query['PRICE_DEFINITION_ID'] == 4092), #iron ore 62
        (sql_query['PRICE_DEFINITION_ID'] == 4251), #LNG
        (sql_query['PRICE_DEFINITION_ID'] == 2087), #hsfo
        (sql_query['PRICE_DEFINITION_ID'] == 2088) #vlsfo
    ]
    values = ['Coal', 'Iron Ore', 'LNG', 'HSFO','VSLFO']

    sql_query['Name'] = np.select(conditions,values)

    sql_query.drop(['ID','COMPUTED_DATE','FORECAST_DATE','REPORTED_DATE','INSERTED_DATE'],axis=1,inplace=True)
    sql_query.sort_values(['PRICE_DEFINITION_ID','DATE'],inplace=True,ascending = [True, False])

    sql_query_latest = sql_query.iloc[::2,:]

    sql_query_previous = sql_query.iloc[1::2,:]

    #print(sql_query_previous,sql_query_latest)

    sql_query_concat = pd.concat([sql_query_latest.reset_index(drop=True),sql_query_previous.reset_index(drop=True)],axis=1)

    hsfo_prev = sql_query_latest.iloc[0 ,0]
    vlsfo_prev = sql_query_latest.iloc[1,0]
    combined_ffa_data.at[4,'Previous Close'] = hsfo_prev
    combined_ffa_data.at[5,'Previous Close'] = vlsfo_prev

    fuel_oil_spread_price = combined_ffa_data.loc[5,'Price'] - combined_ffa_data.loc[4,'Price']
    fuel_oil_spread_prev_price = combined_ffa_data.loc[5,'Previous Close'] - combined_ffa_data.loc[4,'Previous Close']
    fuel_oil_spread_df = pd.DataFrame(['Fuel Oil Spread',fuel_oil_spread_price,fuel_oil_spread_prev_price]).transpose()
    fuel_oil_spread_df.columns = ['Name','Price','Previous Close']

    #combined_ffa_data_2 = pd.concat([combined_ffa_data.reset_index(drop=True),fuel_oil_spread_df.reset_index(drop=True)])
    combined_ffa_data = pd.concat([combined_ffa_data,fuel_oil_spread_df])
    print(combined_ffa_data)


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

    # running yahoo finance function using input from yahoo finance spreadsheet 
    Ticker_codes = pd.read_excel('TICKERS PYTHON SCRIPT\Yahoo excel sheet.xlsx',0)
    yahoo_finance = yahoo_finance(Ticker_codes)
    yahoo_finance.columns=['Name','Price','Previous Close']

    # %%

    all_prices = pd.concat([combined_ffa_data,yahoo_finance],0)
    
    # Replacing names using Yahoo Excel sheet.xlsx
    replace_array = pd.read_excel('TICKERS PYTHON SCRIPT\Yahoo excel sheet.xlsx',1)
    

    for i, row in replace_array.iterrows():
        all_prices.loc[all_prices['Name'] == replace_array['String'][i], 'Name'] = replace_array['Replacement'][i]
    print(yahoo_finance)

    #adding last updated time
    now = datetime.now() # current date and time
    date_time = now.strftime("%X, %x")
    curr_time = "Update time: "+ date_time
    curr_time =    [curr_time,0,0]
    update_time = pd.DataFrame(curr_time).transpose()
    update_time.columns=['Name','Price','Previous Close']
    all_prices = pd.concat([all_prices,update_time])

    #Insert ID column & set index 
    all_prices_id = list(range(1,len(all_prices)+1))
    all_prices.insert(0,'ID',all_prices_id)
    all_prices.set_index('ID',drop=True,inplace=True)
    
    # save all_prices to all_prices excel sheet
    all_prices.to_excel('all_prices.xlsx')
    print(all_prices)

try:
    while True:
        run_tickers()
        time.sleep(100)
except:
    try:
        while True:
            run_tickers()
            time.sleep(100)
    except Exception as e:
        print(e)
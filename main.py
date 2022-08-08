# from asyncio import base_tasks
# import yfinance as yf
# from io import StringIO
# import pyodbc
# import numpy as np


# from os import sys
# import requests
import pandas as pd 
from baltic_exchange import get_baltic
from braemar_screen import get_braemar_screen
from braemar2_sql_request import get_sql
from yahoo_request import yahoo_finance
from datetime import datetime
import time

def run_tickers():
    baltic_prices = get_baltic()
    #print(baltic_prices)
    braemar_screen_csv = get_braemar_screen()

    #Creating search strings for braemar screens data
    curr_month = baltic_prices['period'][0].replace(" ","")
    next_q = baltic_prices['period'][3].replace(" ","")

    c5tc__curr_month_search_str = str('cape' + curr_month + 'midpoint').upper()
    c5tc_next_q_search_str = str('cape' + next_q + 'midpoint').upper()
    p4tc_curr_month_search_str = str('pmax' + curr_month + 'midpoint').upper()
    p4tc_next_q_search_str = str('pmax' + next_q + 'midpoint').upper()
    hsfo = 'BEX35SGSINMIDPOINT'
    vlsfo = 'BEX05SGSINMIDPOINT'

    search_terms = {'string':[c5tc__curr_month_search_str,p4tc_curr_month_search_str,c5tc_next_q_search_str,p4tc_next_q_search_str,hsfo,vlsfo]}
    search_terms = pd.DataFrame(search_terms)

    #searching for strings in braemarscreens output 
    braemar_screens_data = pd.merge(search_terms,braemar_screen_csv, on = 'string', how='left')
    combined_ffa_data = pd.concat([braemar_screens_data,baltic_prices],axis = 1)
    columns = ['name','price','value']
    combined_ffa_data = combined_ffa_data.loc[:,columns]
    combined_ffa_data.columns=['Name','Price','Previous Close']

    combined_sql_ffa_data  = get_sql(combined_ffa_data)
    print(combined_sql_ffa_data)

    # running yahoo finance function using input from yahoo finance spreadsheet 
    Ticker_codes = pd.read_excel('IO\Input.xlsx',0)
    yahoo_import = yahoo_finance(Ticker_codes)
    yahoo_import.columns=['Name','Price','Previous Close']

    # %%
    all_prices = pd.concat([combined_sql_ffa_data,yahoo_import],0)

    # Replacing names using Yahoo Excel sheet.xlsx
    replace_array = pd.read_excel('IO\Input.xlsx',1)

    for i, row in replace_array.iterrows():
        all_prices.loc[all_prices['Name'] == replace_array['String'][i], 'Name'] = replace_array['Replacement'][i]

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
    all_prices.to_excel('IO\Output.xlsx')
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
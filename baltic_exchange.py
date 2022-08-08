import requests
import pandas as pd

def get_baltic():
    #Importing baltic exchange data
    baltic_url = "https://api.balticexchange.com/api/v1.1/feed/FDS4EK10KTGNKFH5LWTBAATLYEPYDZW6/data"
    headers = {'x-apikey' : '37DIH7JMY1OAQH463E4X60I4GGEPIIZFU4ZMRTI3BV53RHCAY85Y2VZCQX474I4Z'}

    baltic_exchange = requests.get(baltic_url, headers =headers)
    baltic_json = baltic_exchange.json()

    #parsing out the nested json response
    #baltic_curr_month_prices
    df = pd.DataFrame(baltic_json)
    df2 = pd.json_normalize(df['groupings'])
    df3 = pd.json_normalize(df2[0])
    df4 = pd.json_normalize(df3['groups'])
    df5 = pd.json_normalize(df4[0])
    df6 = pd.json_normalize(df5['projections'])
    df7 = pd.json_normalize(df6[0])
    baltic_curr_month_prices = df7.iloc[:,0:3]

    # baltic_front_month_prices
    df = pd.DataFrame(baltic_json)
    df2 = pd.json_normalize(df['groupings'])
    df3 = pd.json_normalize(df2[0])
    df4 = pd.json_normalize(df3['groups'])
    df5 = pd.json_normalize(df4[0])
    df6 = pd.json_normalize(df5['projections'])
    df7 = pd.json_normalize(df6[1])
    baltic_front_month_prices = df7.iloc[:,0:3]  

    #baltic_next_q_prices
    df = pd.DataFrame(baltic_json)
    df2 = pd.json_normalize(df['groupings'])
    df3 = pd.json_normalize(df2[0])
    df4 = pd.json_normalize(df3['groups'])
    df5 = pd.json_normalize(df4[1])
    df6 = pd.json_normalize(df5['projections'])
    df7 = pd.json_normalize(df6[1])
    
    baltic_next_q_prices  = df7.iloc[:,0:3]  

    baltic_prices = pd.concat([baltic_curr_month_prices,baltic_next_q_prices])
    baltic_prices = baltic_prices.reset_index(drop =True)
    #print(baltic_prices)
    return baltic_prices
import requests
import pandas as pd
from io import StringIO

def get_braemar_screen():
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
    return csv_pd

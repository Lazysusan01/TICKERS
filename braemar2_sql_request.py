import pandas as pd
import numpy as np
import pyodbc

def get_sql(combined_ffa_data):
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
    combined_ffa_data = pd.concat([combined_ffa_data,fuel_oil_spread_df])

    return combined_ffa_data
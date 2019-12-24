# -*- coding: utf-8 -*-
"""
Created on Mon Dec 23 17:11:03 2019

@author: IHE378
"""

import entsoe
import pandas as pd
from entsoe.mappings import BIDDING_ZONES

#Nicolas's crendentials
TOKEN = "8d4721aa-8bfd-49d9-9156-a790a04b837e" 

#D10 Proxies
PROXY = {"http":"http://10.42.32.29:8080",
         "https":"https://10.42.32.29:8080"}

#Connect to client
connect = entsoe.EntsoePandasClient(api_key=TOKEN, proxies = PROXY, retry_count=20, retry_delay=30)

#Prepare arguments
year = 2019
domain = list(BIDDING_ZONES)
domain = ["BE","NL"]
start = pd.Timestamp("%i/01/01 00:00" % year)
end = pd.Timestamp("%i/01/01 00:00" % (year + 1))

#Start query
df_list = []   
for country in domain:
    try:
        df_prices = connect.query_day_ahead_prices(country_code=country,
                                                   start=start,
                                                   end=end)
        print("Query SUCCEEDED for country %s from %s to %s" %(country,
                                                            start.strftime("%Y/%m/%d"),
                                                            end.strftime("%Y/%m/%d")))
    except:
        print("Query FAILED for country %s from %s to %s" %(country,
                                                            start.strftime("%Y/%m/%d"),
                                                            end.strftime("%Y/%m/%d")))
        df_prices = None

    df_list.append(df_prices)
df = pd.concat(df_list, 1, keys = domain, names = "country")
df.to_csv('DA_prices_query.csv')
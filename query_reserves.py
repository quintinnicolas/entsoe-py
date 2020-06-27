# -*- coding: utf-8 -*-
"""
Created on Tue Dec 24 10:51:19 2019

@author: IHE378
"""

import entsoe
import pandas as pd
from entsoe.mappings import BIDDING_ZONES, MARKETAGREEMENTTYPE
from itertools import product

#Nicolas's crendentials
TOKEN = "8d4721aa-8bfd-49d9-9156-a790a04b837e" 

#D10 Proxies
PROXY = {"http":"http://10.42.32.29:8080",
         "https":"https://10.42.32.29:8080"}
PROXY = {}

#Connect to client
connect = entsoe.EntsoePandasClient(api_key=TOKEN, proxies = PROXY, retry_count=20, retry_delay=30)

#Prepare arguments
YEARS = range(2019, 2020)
#TENDERS = ["A%02d" % i for i in range (1, 5)] + ["A06", "A13"]
TENDERS = ["A02","A03","A06"]
FNAME = r"%i-%s-%s.csv"
ZONES = list(BIDDING_ZONES)
ZONES = ["BE","NL"]

for zone, tender, year in product(ZONES, TENDERS, YEARS):
    start = pd.Timestamp("%i/01/01 00:00" % year)
    end = pd.Timestamp("%i/01/01 00:00" % (year + 1))
    end = min(pd.Timestamp.today().ceil("D"), end)
    fname = FNAME % (year, zone, MARKETAGREEMENTTYPE[tender])
    try:
        df_capacity = connect.query_contracted_reserve_amount(zone, 
                                                              start = start, 
                                                              end = end,
                                                              type_marketagreement_type = tender)
        df_prices = connect.query_contracted_reserve_prices(zone, 
                                                            start = start, 
                                                            end = end,
                                                            type_marketagreement_type = tender)
        df_reserves = pd.concat([df_capacity,df_prices], 1, keys = ["capacity","price"], names = ["parameter","product"])
        df_reserves.to_csv(fname)
        print("Query SUCCEEDED for zone %s, for tender %s from %s to %s"
              %(zone,tender,start.strftime("%Y/%m/%d"),end.strftime("%Y/%m/%d")))
    except Exception as e:
        print("Query FAILED for zone %s, for tender %s from %s to %s due to:\n %s"
              %(zone,tender,start.strftime("%Y/%m/%d"),end.strftime("%Y/%m/%d"),e.args))
        continue
    
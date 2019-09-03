import pandas as pd
import entsoe
import numpy as np

#Nicolas's crendentials
TOKEN = "8d4721aa-8bfd-49d9-9156-a790a04b837e" 

#D10 Proxies
PROXY = {"http":"http://10.42.32.29:8080",
         "https":"https://10.42.32.29:8080"}
         
         
e = entsoe.EntsoePandasClient(api_key=TOKEN, proxies = PROXY, retry_count=20, retry_delay=30)

start_year = 2015
end_year = 2019

domains = ["BE","FR","ES","DE","PL","PT","CZ","GB","IT","CH","NL","HU","AT","SK"]
quarterhour = ["BE","DE","AT","NL","HU"]
halfhour = ["GB"]


df_dic = {}
for year in range(start_year, end_year):
    start = pd.Timestamp(year=year, month=1, day=1, tz='Europe/Brussels')
    end = pd.Timestamp(year=year+1, month=1, day=1, tz='Europe/Brussels')
    df_dic[year] = {}    
    for country in domains:
        print("Querying yearly load from %s to %s for country %s" %(start.strftime('%d-%m-%Y'),end.strftime('%d-%m-%Y'),country))
        s = e.query_load(country_code=country, start=start, end=end)
        if s is not None:
            df_dic[year][country] = s
    df_dic[year] = pd.concat(df_dic[year])
result = pd.concat(df_dic).reset_index()
result.columns = ["year","country","time","load"]
#result["year"] = [n.year for n in result.time]

result.load = result.load/1000/1000 #CONVERTING MW TO TW
result.loc[result.country.isin(quarterhour), "load"] = result.loc[result.country.isin(quarterhour),"load"]/4.
result.loc[result.country.isin(halfhour),"load"] = result.loc[result.country.isin(halfhour),"load"]/2.

result_pivot = pd.pivot_table(result,values = "load",index = ["country"],columns = ["year"], aggfunc = np.sum)


result.to_csv('load_list.csv')
result_pivot.to_csv('load_pivot.csv')
print("Script Terminated")
print("-----------------")

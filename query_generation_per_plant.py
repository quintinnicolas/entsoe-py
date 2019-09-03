import pandas as pd
import entsoe

#Nicolas's crendentials
TOKEN = "8d4721aa-8bfd-49d9-9156-a790a04b837e" 

#D10 Proxies
PROXY = {"http":"http://10.42.32.29:8080",
         "https":"https://10.42.32.29:8080"}
         
         
e = entsoe.EntsoePandasClient(api_key=TOKEN, proxies = PROXY, retry_count=20, retry_delay=30)

start = pd.Timestamp('20190814', tz='Europe/Brussels')
end = pd.Timestamp('20190815', tz='Europe/Brussels')

domains = ["BE"]

lst = list()
for country in domains:
    print("Querying installed generation data from %s to %s for country %s" %(start.strftime('%d-%m-%Y'),end.strftime('%d-%m-%Y'),country))
    s = e.query_generation_per_plant(country_code=country, start=start, end=end)
    if s is not None:
        lst.append(s)

result = pd.concat(lst)
result.to_csv('result.csv')
print("Script Terminated")

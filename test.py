import requests
from requests_ntlm import HttpNtlmAuth

import pendulum
import pandas as pd
import shutil
today = pendulum.today()
todaystr = today.format("YYYY-MM-DD")

def download_file(url,myauth):
    local_filename = url.split('/')[-1]
    with requests.get(url, auth=myauth, stream=True) as r:
        with open(local_filename, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    return local_filename


url = "https://www.ecdc.europa.eu/sites/default/files/documents/COVID-19-geographic-disbtribution-worldwide-" + todaystr + ".xlsx"
print(url)
myauth = HttpNtlmAuth(":", ":")

from dfply import *

r = download_file(url, myauth)
df = pd.read_excel(r)

df_deaths = df >> group_by(X.geoId) >> arrange(X.dateRep, ascending=True) >> mutate(death_cum= cumsum(X.deaths))
df_cases = df_deaths >> group_by(X.geoId) >> arrange(X.dateRep, ascending=True) >> mutate(cases_cum= cumsum(X.cases))

df_final = df_cases >>  mask(X.geoId == "FR")

print()

#gb.apply(lambda _df: _df.sort_values('deaths', ascending=False)).apply(lambda _df: 'deaths'.cumsum())


#df = df.loc[df['geoId'] == "FR"]

#df = df.loc[df['GeoId'] == "FR"]
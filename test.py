import requests
from requests_ntlm import HttpNtlmAuth

import pendulum
import pandas as pd
import shutil
today = pendulum.today()
todaystr = today.format("YYYY-MM-DD")


def download_file(url,myauth, dataFolder):
    local_filename = url.split('/')[-1]

    p = dataFolder / local_filename
    p.touch()

    with requests.get(url, auth=myauth, stream=True) as r:
        with p.open('wb') as f:
            shutil.copyfileobj(r.raw, f)

    return p


from dfply import *

from pathlib import Path

root =  Path.cwd() / 'covidapp'

Path.mkdir(root/'data',exist_ok=True)
dataFolder = root / 'data'


def remoteFileExists(d):
    url = "https://www.ecdc.europa.eu/sites/default/files/documents/COVID-19-geographic-disbtribution-worldwide-" + d + ".xlsx"
    myauth = HttpNtlmAuth(":", ":")
    r = requests.get(url, auth=myauth, stream=True)
    if r.status_code == 404:
      return False
    else:
      return True

if remoteFileExists(todaystr) == False:
    #Back In Time
    print("back in time")
    f = pendulum.yesterday()
    fstr = f.format("YYYY-MM-DD")
else:
    fstr = todaystr

file = dataFolder / Path("COVID-19-geographic-disbtribution-worldwide-" + fstr + ".xlsx")

if not file.exists():
    url = "https://www.ecdc.europa.eu/sites/default/files/documents/COVID-19-geographic-disbtribution-worldwide-" + fstr + ".xlsx"
    print(fstr)
    print("file not exist")
    myauth = HttpNtlmAuth(":", ":")
    r = download_file(url, myauth, dataFolder)
else:
    print("file exist")
    r = file

df = pd.read_excel(r)

df_deaths = df >> group_by(X.geoId) >> arrange(X.dateRep, ascending=True) >> mutate(death_cum= cumsum(X.deaths))
df_cases = df_deaths >> group_by(X.geoId) >> arrange(X.dateRep, ascending=True) >> mutate(cases_cum= cumsum(X.cases))

df_final = df_cases >>  mask(X.geoId == "FR")

print(df_final.to_json(orient='records', date_format='iso'))

#gb.apply(lambda _df: _df.sort_values('deaths', ascending=False)).apply(lambda _df: 'deaths'.cumsum())


#df = df.loc[df['geoId'] == "FR"]

#df = df.loc[df['GeoId'] == "FR"]
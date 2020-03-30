from flask import Flask
from flask_restful import Resource, Api

from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import time
import pendulum
import pandas as pd
import shutil
import requests
from requests_ntlm import HttpNtlmAuth
from dfply import *
from pathlib import Path

app = Flask(__name__)
api = Api(app)

def download_file(url,myauth, dataFolder):
    local_filename = url.split('/')[-1]
    p = dataFolder / local_filename
    p.touch()
    with requests.get(url, auth=myauth, stream=True) as r:
        with p.open('wb') as f:
            shutil.copyfileobj(r.raw, f)
    return p


class Ecdc(Resource):
    def get(self):
        today= pendulum.yesterday()
        todaystr=today.format("YYYY-MM-DD")

        root = Path.cwd()

        Path.mkdir(root / 'data', exist_ok=True)
        dataFolder = root / 'data'
        file = dataFolder / Path("COVID-19-geographic-disbtribution-worldwide-" + todaystr + ".xlsx")

        if not file.exists():
            url = "https://www.ecdc.europa.eu/sites/default/files/documents/COVID-19-geographic-disbtribution-worldwide-" + todaystr + ".xlsx"
            myauth = HttpNtlmAuth(":", ":")
            r = download_file(url, myauth, dataFolder)
            print("file not exist")
        else:
            print("file exist")
            r = file
        df = pd.read_excel(r)

        df_deaths = df >> group_by(X.geoId) >> arrange(X.dateRep, ascending=True) >> mutate(death_cum=cumsum(X.deaths))
        df_cases = df_deaths >> group_by(X.geoId) >> arrange(X.dateRep, ascending=True) >> mutate(cases_cum=cumsum(X.cases))

        df_final = df_cases >> mask(X.geoId == "FR")

        df_final_json = df_final.to_json(orient='records', date_format='iso')

        import json

        datajson = json.loads(df_final_json)

        return {'data':datajson}

api.add_resource(Ecdc, '/covid19/ecdc/')

def print_date_time():
    print(time.strftime("%A, %d. %B %Y %I:%M:%S %p"))

scheduler = BackgroundScheduler()
scheduler.add_job(func=print_date_time, trigger="interval", seconds=30)

scheduler.start()
atexit.register(lambda: scheduler.shutdown())

if __name__ == '__main__':
    app.run(use_reloader=False)


from flask import Flask
from flask_restful import Resource, Api

from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import time
import pendulum
import pandas as pd
import shutil

from dfply import *


app = Flask(__name__)

api = Api(app)

def download_file(url,myauth):
    local_filename = url.split('/')[-1]
    with requests.get(url, auth=myauth, stream=True) as r:
        with open(local_filename, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    return local_filename

import requests
from requests_ntlm import HttpNtlmAuth

class Ecdc(Resource):
    def get(self):
        today= pendulum.yesterday()
        todaystr=today.format("YYYY-MM-DD")

        url = "https://www.ecdc.europa.eu/sites/default/files/documents/COVID-19-geographic-disbtribution-worldwide-" + todaystr +".xlsx"
        print(url)
        myauth = HttpNtlmAuth(":", ":")

        r = download_file(url, myauth)
        df = pd.read_excel(r)

        df_deaths = df >> group_by(X.geoId) >> arrange(X.dateRep, ascending=True) >> mutate(death_cum=cumsum(X.deaths))
        df_cases = df_deaths >> group_by(X.geoId) >> arrange(X.dateRep, ascending=True) >> mutate(cases_cum=cumsum(X.cases))

        df_final = df_cases >> mask(X.geoId == "FR")

        return {"response": df_final.to_json(orient='records')}

api.add_resource(Ecdc, '/covid19/ecdc/')

def print_date_time():
    print(time.strftime("%A, %d. %B %Y %I:%M:%S %p"))

scheduler = BackgroundScheduler()
scheduler.add_job(func=print_date_time, trigger="interval", seconds=5)

scheduler.start()
atexit.register(lambda: scheduler.shutdown())

if __name__ == '__main__':
    app.run(use_reloader=False)


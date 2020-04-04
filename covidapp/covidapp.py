from flask import Flask
from flask_restful import Resource, Api

from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import time
import pendulum
import pandas as pd
import json
import shutil
import requests
from requests_ntlm import HttpNtlmAuth
from dfply import *
from pathlib import Path

from etalab_processing import consolidate_data

app = Flask(__name__)
api = Api(app)


def download_byte_file(url, filename, dataFolder, auth=None):
    p = dataFolder / filename
    p.touch()
    if auth != None:
        with requests.get(url, auth=auth, stream=True) as r:
            with p.open('wb') as f:
                shutil.copyfileobj(r.raw, f)
        return p
    else:
        with requests.get(url, stream=True) as r:
            with p.open('wb') as f:
                shutil.copyfileobj(r.raw, f)
        return p

def download_csv_file(url, filename, dataFolder):
    p = dataFolder / filename
    p.touch()
    with requests.get(url) as r:
        with p.open('wb') as f:
            f.write(r.content)
    return p

today = pendulum.today()
todaystr = today.format("YYYY-MM-DD")


def remoteFileExists(url):

    r = requests.get(url, auth=HttpNtlmAuth(":", ":"), stream=True)
    if r.status_code == 404:
        return False
    else:
        return True

def readyOrBackInTime(urlToTest):

    # Data for today is not ready or not available, back in time or take in data already dl
    if remoteFileExists(urlToTest) == False:
        # Back In Time
        f = pendulum.yesterday()
        return f.format("YYYY-MM-DD")
    else:
        return todaystr

class Etalab(Resource):

    def __init__(self, **kwargs):
        self.dataFolder = kwargs['dataFolder']
        root = Path.cwd()

        self.folder = Path(root / 'data' / self.dataFolder)
        self.folder.mkdir(exist_ok=True,parents=True)

        self.col_names = {
        "date" : "dateRep",
        "cas_confirmes":"cases_cum",
        "deces": "death_cum" ,
        "deces_ehpad" : "death_cum_ehpad",
        "reanimation":"ventilated" ,
        "hospitalises":"hospitalized",
        "gueris":"recover"
        }

    def get(self):

        url = "https://raw.githubusercontent.com/opencovid19-fr/data/master/dist/chiffres-cles.csv"

        fstr = readyOrBackInTime(url)

        file = self.folder / Path("chiffres-cles-"+ fstr +".csv")

        if not file.exists():
            local_filename = "chiffres-cles-"+ fstr +".csv"
            r = download_csv_file(url, local_filename, self.folder)
        else:
            r = file

        pd.set_option('display.max_columns', 20)
        pd.set_option('display.width', 500)

        # FILTER maille_code = FRA
        df = pd.read_csv(r)

        SOURCE_PRIORITIES = {
            1: 'ministere-sante',
            2: 'sante-publique-france',
            3: 'sante-publique-france-data',
            4: 'opencovid19-fr'
        }

        df_final = consolidate_data(df,SOURCE_PRIORITIES)
        #print(df_final.columns)
        df_final.rename(columns=self.col_names, inplace=True)
        df_final_json = df_final.to_json(orient='records', date_format='iso')

        datajson = json.loads(df_final_json)

        return {'data':datajson}


class Ecdc(Resource):

    def __init__(self, **kwargs):
        self.dataFolder = kwargs['dataFolder']
        root = Path.cwd()

        self.folder = Path(root / 'data' / self.dataFolder)
        self.folder.mkdir(exist_ok=True, parents=True)

        print("init with", self.folder)

    def get(self):

        url = "https://www.ecdc.europa.eu/sites/default/files/documents/COVID-19-geographic-disbtribution-worldwide-" + todaystr + ".xlsx"

        fstr = readyOrBackInTime(url)

        file = self.folder / Path("COVID-19-geographic-disbtribution-worldwide-" + fstr + ".xlsx")

        if not file.exists():
            url = "https://www.ecdc.europa.eu/sites/default/files/documents/COVID-19-geographic-disbtribution-worldwide-" + fstr + ".xlsx"
            myauth = HttpNtlmAuth(":", ":")
            local_filename = url.split('/')[-1]
            r = download_byte_file(url, local_filename, self.folder, myauth)
            print("file not exist")
        else:
            print("file exist")
            r = file

        df = pd.read_excel(r)

        df_deaths = df >> group_by(X.geoId) >> arrange(X.dateRep, ascending=True) >> mutate(death_cum=cumsum(X.deaths))
        df_cases = df_deaths >> group_by(X.geoId) >> arrange(X.dateRep, ascending=True) >> mutate(cases_cum=cumsum(X.cases))

        df_final = df_cases >> mask(X.geoId == "FR")

        df_final_json = df_final.to_json(orient='records', date_format='iso')

        datajson = json.loads(df_final_json)

        return {'data':datajson}

api.add_resource(Ecdc, '/covid19/ecdc/', resource_class_kwargs={'dataFolder': 'ecdc'} )
api.add_resource(Etalab, '/covid19/etalab/', resource_class_kwargs={'dataFolder': 'etalab'})

def print_date_time():
    print(time.strftime("%A, %d. %B %Y %I:%M:%S %p"))

scheduler = BackgroundScheduler()
scheduler.add_job(func=print_date_time, trigger="interval", seconds=30)

scheduler.start()
atexit.register(lambda: scheduler.shutdown())

if __name__ == '__main__':
    app.run(use_reloader=False)


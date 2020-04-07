from flask_restful import Resource
from covidapp.common.utils import *
from requests_ntlm import HttpNtlmAuth
from covidapp.resources.errors import *
import pandas as pd

class Ecdc(Resource):

    def __init__(self, **kwargs):
        self.dataFolder = kwargs['dataFolder']
        self.parser = kwargs['parser']

        self.dayly_col_names = {
            "date": "dateRep",
            "cases": "cases_daily",
            "deaths": "deaths_daily",
        }

        root = Path.cwd()

        self.folder = Path(root / 'data' / self.dataFolder)
        self.folder.mkdir(exist_ok=True, parents=True)

        print("init with", self.folder)

    def get(self):

        args = self.parser.parse_args()
        typeOfData = args['type']

        url = "https://www.ecdc.europa.eu/sites/default/files/documents/COVID-19-geographic-disbtribution-worldwide-" + getTodayStr() + ".xlsx"

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
        df_working = df >> mask(X.geoId == "FR")

        if (typeOfData == "cum"):
            df_deaths = df_working >> arrange(X.dateRep, ascending=True) >> mutate(deaths_cum=cumsum(X.deaths))
            df_cases = df_deaths >> arrange(X.dateRep, ascending=True) >> mutate(cases_cum=cumsum(X.cases))
            df_deaths_growth = df_cases >> arrange(X.dateRep, ascending=True) >> mutate (growth_deaths_cum=growth(X.deaths_cum))
            df_cases_growth = df_deaths_growth  >> arrange(X.dateRep, ascending=True) >> mutate(growth_cases_cum= growth(X.cases_cum))
            df_final = df_cases_growth >> select(X.dateRep, X.deaths_cum, X.cases_cum, X.growth_cases_cum, X.growth_deaths_cum)
        else:

            df_deaths_growth = df_working >> arrange(X.dateRep, ascending=True) >> mutate (growth_deaths_daily=growth(X.deaths))
            df_cases_growth = df_deaths_growth  >> arrange(X.dateRep, ascending=True) >> mutate(growth_cases_daily= growth(X.cases))
            df_final = df_cases_growth >> select(X.dateRep, X.deaths,X.cases,X.growth_cases_daily, X.growth_deaths_daily)
            df_final.rename(columns=self.dayly_col_names, inplace=True)


        df_final_json = df_final.to_json(orient='records', date_format='iso')

        datajson = json.loads(df_final_json)

        return {'data':datajson}


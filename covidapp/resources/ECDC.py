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
            "deaths": "deaths_all_daily",
        }

        root = Path.cwd()

        self.folder = Path(root / 'data' / self.dataFolder)
        self.folder.mkdir(exist_ok=True, parents=True)

        print("init with", self.folder)

    def get(self):

        # ECDC DATA ARE DAILY, we compute cumulated here.

        args = self.parser.parse_args()
        typeOfData = args['type']
        rolling = args['rolling']
        print("rolling = ", rolling)

        if (typeOfData == "daily") and args['rolling'] == None:
            raise NeedARollingWindowError

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
        pd.set_option('display.max_columns', 20)
        pd.set_option('display.width', 500)

        df_working.index = pd.to_datetime(df_working.dateRep)

        if (typeOfData == "cum"):

            df_deaths = df_working >> arrange(X.dateRep, ascending=True) >> mutate(deaths_all_cum=cumsum(X.deaths))
            df_cases = df_deaths >> arrange(X.dateRep, ascending=True) >> mutate(cases_cum=cumsum(X.cases))
            dict_dup_deaths_cum = compute_dict(df_cases,"deaths_all_cum")

            df_deaths_growth = df_cases >> arrange(X.dateRep, ascending=True) >> mutate (growth_deaths_cum=growth(X.deaths_all_cum))
            df_cases_growth = df_deaths_growth  >> arrange(X.dateRep, ascending=True) >> mutate(growth_cases_cum= growth(X.cases_cum))

            df_cases_growth = df_cases_growth.replace(np.inf, np.nan)

            minnm = min(df_cases_growth.growth_deaths_cum.min(), df_cases_growth.growth_cases_cum.min())
            maxnm = max(df_cases_growth.growth_deaths_cum.max(), df_cases_growth.growth_cases_cum.max())

            df_deaths_nmgrowth = df_cases_growth >> mutate(
                nm_growth_deaths_cum=normalized_growth(X.growth_deaths_cum, minnm, maxnm))
            df_cases_nmgrowth = df_deaths_nmgrowth >> mutate(
                nm_growth_cases_cum=normalized_growth(X.growth_cases_cum, minnm, maxnm))

            df_cases_nmgrowth[["Xdatei", "XDatef", "XDelta", "XValue"]] = df_cases_nmgrowth.apply(daysBeforeMultiply, result_type="expand", dict=dict_dup_deaths_cum, df=df_cases_nmgrowth,
                                                            deaths_all_cum=df_cases_nmgrowth['deaths_all_cum'], axis=1)

            df_final = df_cases_nmgrowth >> select(X.dateRep, X.deaths_all_cum, X.cases_cum, X.growth_deaths_cum,
                                                   X.growth_cases_cum, X.nm_growth_deaths_cum, X.nm_growth_cases_cum, X.Xdatei, X.XDatef, X.XDelta, X.XValue)
        else:

            df_deaths_growth = df_working >> arrange(X.dateRep, ascending=True) >> mutate (growth_deaths_daily=growth(X.deaths))
            df_cases_growth = df_deaths_growth  >> arrange(X.dateRep, ascending=True) >> mutate(growth_cases_daily= growth(X.cases))
            df_cases_growth = df_cases_growth.replace(np.inf, np.nan)

            minnm = min(df_cases_growth.growth_deaths_daily.min(),df_cases_growth.growth_cases_daily.min())
            maxnm = max(df_cases_growth.growth_deaths_daily.max(),df_cases_growth.growth_cases_daily.max())

            df_deaths_nmgrowth = df_cases_growth >> mutate(nm_growth_deaths_daily=normalized_growth(X.growth_deaths_daily, minnm,maxnm ))
            df_cases_nmgrowth = df_deaths_nmgrowth >> mutate(nm_growth_cases_daily=normalized_growth(X.growth_cases_daily, minnm,maxnm ))

            df_rolling_mean_deaths = df_cases_nmgrowth >> mutate(rolling_deaths_daily=rolling_mean(X.deaths, "{r}D".format(r=rolling), None))
            df_rolling_mean_cases = df_rolling_mean_deaths >> mutate(
                rolling_cases_daily=rolling_mean(X.cases, "{r}D".format(r=rolling), None))

            df_final = df_rolling_mean_cases >> select(X.dateRep,
                                                       X.deaths,
                                                       X.cases,
                                                       X.growth_deaths_daily,
                                                       X.growth_cases_daily,
                                                       X.nm_growth_deaths_daily,
                                                       X.nm_growth_cases_daily,
                                                       X.rolling_cases_daily,
                                                       X.rolling_deaths_daily)
            df_final.rename(columns=self.dayly_col_names, inplace=True)

        #df_final.to_csv('outxx.csv')
        df_final_json = df_final.to_json(orient='records', date_format='iso')

        datajson = json.loads(df_final_json)

        return {'data':datajson}


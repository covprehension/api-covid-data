from flask_restful import Resource
from covidapp.common.utils import *
from covidapp.resources.etalab_processing import *
from covidapp.resources.errors import *
import pandas as pd

class Etalab(Resource):

    def __init__(self, **kwargs):
        self.dataFolder = kwargs['dataFolder']
        self.parser = kwargs['parser']

        root = Path.cwd()

        self.folder = Path(root / 'data' / self.dataFolder)
        self.folder.mkdir(exist_ok=True,parents=True)

        self.col_names = {
        "date" : "dateRep",
        "cas_confirmes":"cases_cum_daily",
        "deces": "deaths_hospital_cum_daily" ,
        "deces_ehpad" : "deaths_ehpad_cum_daily",
        "reanimation":"ventilated_daily_state" ,
        "hospitalises":"hospitalized_daily_state",
        "gueris":"recover_cum_daily"
        }

    def get(self):

        # ECDC DATA ARE CUMULATED AND not consolidated, we compute daily here.

        args = self.parser.parse_args()
        typeOfData = args['type']
        rolling = args['rolling']

        if (typeOfData == "daily") and args['rolling'] == None:
            raise NeedARollingWindowError

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

        df_working = consolidate_data(df,SOURCE_PRIORITIES)
        df_working.index = pd.to_datetime(df_working.date)

        df_deaths_growth = df_working >> arrange(X.date, ascending=True) >> mutate(growth_deaths_cum=growth(X.deces))
        df_cases_growth = df_deaths_growth >> arrange(X.date, ascending=True) >> mutate(growth_cases_cum=growth(X.cas_confirmes))

        df_cases_growth = df_cases_growth.replace(np.inf, np.nan)

        minnm = min(df_cases_growth.growth_deaths_cum.min(), df_cases_growth.growth_cases_cum.min())
        maxnm = max(df_cases_growth.growth_deaths_cum.max(), df_cases_growth.growth_cases_cum.max())

        df_deaths_nmgrowth = df_cases_growth >> mutate(
            nm_growth_deaths_cum=normalized_growth(X.growth_deaths_cum, minnm, maxnm))
        df_cases_nmgrowth = df_deaths_nmgrowth >> mutate(
            nm_growth_cases_cum=normalized_growth(X.growth_cases_cum, minnm, maxnm))

        # Recompute cum data for some columns ...
        df_working_cum_deaths_all = df_cases_nmgrowth >> mutate(deaths_all_cum_daily= X.deces + X.deces_ehpad)

        # Recompute daily data for some columns ...

        df_working_daily_deaths_hospital = df_working_cum_deaths_all >> mutate(deaths_hospital_daily=rollbackcum(X.deces))
        df_working_daily_deaths_ehpad = df_working_daily_deaths_hospital >> mutate(deaths_ehpad_daily=rollbackcum(X.deces_ehpad))

        df_working_daily_deaths_all = df_working_daily_deaths_ehpad >> mutate(
            deaths_all_daily=X.deaths_hospital_daily + X.deaths_ehpad_daily)

        df_working_daily_cases = df_working_daily_deaths_all >> mutate(cases_daily=rollbackcum(X.cas_confirmes))
        df_working_ventilated = df_working_daily_cases >> mutate(ventilated_daily_diff=rollbackcum(X.reanimation))
        df_working_hospitalized = df_working_ventilated >> mutate(hospitalized_daily_diff=rollbackcum(X.hospitalises))
        df_working_recover = df_working_hospitalized >> mutate(recover_daily=rollbackcum(X.gueris))

        df_working_recover.rename(columns=self.col_names, inplace=True)

        if (typeOfData == "cum"):

            dict_dup_deaths_cum = compute_dict(df_working_recover, "deaths_all_cum_daily")

            df_working_recover[["Xdatei", "XDatef", "XDelta", "XValue"]] = df_working_recover.apply(daysBeforeMultiply, result_type="expand", dict=dict_dup_deaths_cum, df=df_working_recover,
                                                                deaths_all_cum_daily=df_working_recover['deaths_all_cum_daily'], axis=1)

            df_final = df_working_recover >> select(X.dateRep,
                                          X.cases_cum_daily,
                                          X.deaths_hospital_cum_daily,
                                          X.deaths_ehpad_cum_daily,
                                          X.deaths_all_cum_daily,
                                          X.ventilated_daily_state,
                                          X.hospitalized_daily_state,
                                          X.ventilated_daily_diff,
                                          X.hospitalized_daily_diff,
                                          X.recover_cum_daily,
                                          X.growth_deaths_cum,
                                          X.growth_cases_cum,
                                          X.nm_growth_deaths_cum,
                                          X.nm_growth_cases_cum,
                                          X.Xdatei,
                                          X.XDatef,
                                          X.XDelta,
                                          X.XValue )


        else:

            df_final_rolling_mean_deaths_hosp = df_working_recover >> mutate(
                rolling_deaths_hospital_daily=rolling_mean(X.deaths_hospital_cum_daily, "{r}D".format(r=rolling), None))

            df_final_rolling_mean_deaths_ehpad = df_final_rolling_mean_deaths_hosp >> mutate(
                rolling_deaths_ehpad_daily=rolling_mean(X.deaths_ehpad_cum_daily, "{r}D".format(r=rolling), None))

            df_final_rolling_mean_cases = df_final_rolling_mean_deaths_ehpad >> mutate(
                rolling_cases_daily=rolling_mean(X.cases_cum_daily, "{r}D".format(r=rolling), None))

            df_final_rolling_mean_deaths_all = df_final_rolling_mean_cases >> mutate(
                rolling_deaths_all_daily=rolling_mean(X.deaths_all_cum_daily, "{r}D".format(r=rolling), None))

            df_final = df_final_rolling_mean_deaths_all >> select(X.dateRep,
                                                             X.cases_daily,
                                                             X.deaths_hospital_daily,
                                                             X.deaths_ehpad_daily,
                                                             X.deaths_all_daily,
                                                             X.ventilated_daily_state,
                                                             X.hospitalized_daily_state,
                                                             X.ventilated_daily_diff,
                                                             X.hospitalized_daily_diff,
                                                             X.recover_daily,
                                                             X.rolling_deaths_hospital_daily,
                                                             X.rolling_deaths_ehpad_daily,
                                                             X.rolling_deaths_all_daily,
                                                             X.rolling_cases_daily)

        df_final_json = df_final.to_json(orient='records', date_format='iso')

        datajson = json.loads(df_final_json)

        return {'data':datajson}
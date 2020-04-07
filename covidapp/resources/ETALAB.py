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
        "cas_confirmes":"cases_cum",
        "deces": "deaths_cum" ,
        "deces_ehpad" : "deaths_ehpad_cum",
        "reanimation":"ventilated_cum" ,
        "hospitalises":"hospitalized_cum",
        "gueris":"recover_cum"
        }

    def get(self):

        args = self.parser.parse_args()
        typeOfData = args['type']

        if typeOfData != "cum":
            raise SimpleNotExistsError

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

        df_deaths_growth = df_working >> arrange(X.date, ascending=True) >> mutate(growth_deaths_cum=growth(X.deces))
        df_final = df_deaths_growth >> arrange(X.date, ascending=True) >> mutate(growth_cases_cum=growth(X.cas_confirmes))

        df_final.rename(columns=self.col_names, inplace=True)

        df_final_json = df_final.to_json(orient='records', date_format='iso')

        datajson = json.loads(df_final_json)

        return {'data':datajson}
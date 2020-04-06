from flask_restful import Resource
from covidapp.common.utils import *
from requests_ntlm import HttpNtlmAuth
from covidapp.resources.errors import *

class Ecdc(Resource):

    def __init__(self, **kwargs):
        self.dataFolder = kwargs['dataFolder']
        self.parser = kwargs['parser']
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

        if (typeOfData == "cum"):
            df_deaths = df >> group_by(X.geoId) >> arrange(X.dateRep, ascending=True) >> mutate(death_cum=cumsum(X.deaths))
            df_cases = df_deaths >> group_by(X.geoId) >> arrange(X.dateRep, ascending=True) >> mutate(cases_cum=cumsum(X.cases))
        else:
            df_cases = df

        df_final = df_cases >> mask(X.geoId == "FR")

        df_final_json = df_final.to_json(orient='records', date_format='iso')

        datajson = json.loads(df_final_json)

        return {'data':datajson}


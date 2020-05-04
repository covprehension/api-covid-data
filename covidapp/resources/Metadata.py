from flask_restful import Resource

class Metadata(Resource):
    def get(self):
        #https://flask.palletsprojects.com/en/1.1.x/quickstart/#apis-with-json
        return {
            'data':
                {"ecdc":{
                    "description" : "ECDC data",
                    "type" : [{
                        "name" : "cum",
                        "label" : "Données cumulées",
                        "options":[]
                              },{
                            "name": "daily",
                            "label": "Données journalières",
                            "options": [
                                {"label": "Moyenne mobile des décés (Hospitaliers et Ehpad) sur n jours",
                             "value": "rolling_deaths_daily"},
                            {"label": "Moyenne mobile des cas sur n jours",
                             "value": "rolling_cases_daily"}]
                        }

                    ]}
                ,"etalab": {
                    "description": "Etalab french data consolidated",
                    "type": [{
                        "name": "cum",
                        "label": "Données cumulées",
                        "options": []},{
                        "name": "daily",
                        "label": "Données journalières",
                        "options":[
                            {"label":"Moyenne mobile des décés Hospitalier sur n jours",
                             "value":"rolling_deaths_hospital_daily"},
                            {"label": "Moyenne mobile des décés Ehpad sur n jours",
                             "value": "rolling_deaths_ehpad_daily"},
                            {"label": "Moyenne mobile des décés totaux sur n jours",
                             "value": "rolling_deaths_all_daily"},
                            {"label": "Moyenne mobile des cas sur n jours",
                             "value": "rolling_cases_daily"}
                        ]
                    }
                ]}
            }}

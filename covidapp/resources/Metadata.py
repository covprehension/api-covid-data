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
                        "options":[
                            {"label":"Moyenne mobile des décés sur 3 jours",
                             "value":"rolling_deaths"}]
                    }]}
                ,"etalab": {
                    "description": "Etalab french data consolidated",
                    "type": [{
                        "name": "cum",
                        "label": "Données cumulées",
                        "options": []
                }]}
            }}

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
                        "options":["rolling"]
                    }]}
                ,"etalab": {
                    "description": "Etalab french data consolidated",
                    "type": [{
                        "name": "cum",
                        "label": "Données cumulées",
                        "options": []
                }]}
            }}

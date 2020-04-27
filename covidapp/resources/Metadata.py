from flask_restful import Resource

class Metadata(Resource):
    def get(self):
        #https://flask.palletsprojects.com/en/1.1.x/quickstart/#apis-with-json
        return {
            'data':
                {"ECDC":{
                    "description" : "ECDC data",
                    "type" : [{
                        "name" : "cum",
                        "options":[]
                              },{
                        "name": "daily",
                        "options":["rolling"]
                    }]}
                ,"ETALAB": {
                    "description": "Etalab french data consolidated",
                    "type": [{
                        "name": "cum",
                        "options": []
                }]}
            }}

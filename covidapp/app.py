from flask import Flask
from flask_restful import Resource, Api, reqparse
from covidapp.resources.errors import *

from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import time
from werkzeug.routing import ValidationError

from covidapp.resources.ECDC import Ecdc
from covidapp.resources.ETALAB import Etalab

def type_validate():
    def validate(s):
        if s in ["cum","simple"]:
            return s
        raise ValidationError("Only two choices are possible : cum or simple")
    return validate

parser = reqparse.RequestParser()
parser.add_argument('type',type=type_validate(), required=True)

app = Flask(__name__)
api = Api(app, errors=errors)

api.add_resource(Ecdc, '/covid19/ecdc', resource_class_kwargs={'dataFolder': 'ecdc', 'parser':parser} )
api.add_resource(Etalab, '/covid19/etalab', resource_class_kwargs={'dataFolder': 'etalab', 'parser':parser})

def print_date_time():
    print(time.strftime("%A, %d. %B %Y %I:%M:%S %p"))

# scheduler = BackgroundScheduler()
# scheduler.add_job(func=print_date_time, trigger="interval", seconds=30)
#
# scheduler.start()
# atexit.register(lambda: scheduler.shutdown())
from flask_restful import HTTPException

class CumNotExistsError(HTTPException):
    pass

class SimpleNotExistsError(HTTPException):
    pass

errors = {
    "CumNotExistsError": {
        "message": "Cumulated data don't exist for this source",
        "status": 400
    },
    "SimpleNotExistsError": {
        "message": "Day by Day data don't exist for this source",
        "status": 400
    }
}
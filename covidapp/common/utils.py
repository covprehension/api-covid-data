import pendulum
import json
import shutil
import requests
from dfply import *
from pathlib import Path
from requests_ntlm import HttpNtlmAuth

def getTodayStr():
    today = pendulum.today()
    return today.format("YYYY-MM-DD")

def download_byte_file(url, filename, dataFolder, auth=None):
    p = dataFolder / filename
    p.touch()
    if auth != None:
        with requests.get(url, auth=auth, stream=True) as r:
            with p.open('wb') as f:
                shutil.copyfileobj(r.raw, f)
        return p
    else:
        with requests.get(url, stream=True) as r:
            with p.open('wb') as f:
                shutil.copyfileobj(r.raw, f)
        return p

def download_csv_file(url, filename, dataFolder):
    p = dataFolder / filename
    p.touch()
    with requests.get(url) as r:
        with p.open('wb') as f:
            f.write(r.content)
    return p

def remoteFileExists(url):

    r = requests.get(url, auth=HttpNtlmAuth(":", ":"), stream=True)
    if r.status_code == 404:
        return False
    else:
        return True

def readyOrBackInTime(urlToTest):

    today = pendulum.today()
    todaystr = today.format("YYYY-MM-DD")

    # Data for today is not ready or not available, back in time or take in data already dl
    if remoteFileExists(urlToTest) == False:
        # Back In Time
        f = pendulum.yesterday()
        return f.format("YYYY-MM-DD")
    else:
        return todaystr








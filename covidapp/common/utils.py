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

# functions for dfply

@make_symbolic
def growth(series):
    return series.pct_change()

@make_symbolic
def rollbackcum(series):
    return series.diff()


@make_symbolic
def normalized_growth(series,min,max):
    # reverse function nm to range min max
    # for index, value in series.iteritems():
    #     print("index = " , index, " value =", value)
    #     if np.isfinite(value) or np.isnan(value):
    #         pass
    #     else:
    #         values = (min + ((value-min)(max - min) / (max - min)))
    #         series.at[index] = values
    return ((series-min) / (max - min))

@make_symbolic
def rolling_mean(series,w,w_type=None):
    return series.rolling(window=w, win_type=w_type).mean()

@make_symbolic
def to_datetime(series):
    return pd.to_datetime(series, format="%Y-%m-%d")

def compute_dict(df, name):
    df_map_value = df.drop_duplicates([name])
    dict_value = df_map_value[name].to_dict()
    return {v: k for k, v in dict_value.items()}

def daysBeforeMultiply(x, **kwargs):

    # get numerical index of row
    numericIndex = kwargs["df"].index.get_loc(x.name)
    dict_inverted = kwargs["dict"]

    # Skip the first line, returning Nan
    if numericIndex == 0 or np.isnan(x['deaths_all_cum_daily']):
        return x.name, np.NaN, np.NaN, np.NaN

    # If value_cum is the same than the previous row (nothing changed),
    # we need some tweaking (compute using the datebefore) to return same data
    ilocvalue = kwargs["df"].iloc[[numericIndex - 1]]["deaths_all_cum_daily"][0]
    if x['deaths_all_cum_daily'] == ilocvalue:
        name = dict_inverted[x['deaths_all_cum_daily']]
    else:
        name = x.name

    # Series to compare with actual row
    series =  kwargs["deaths_all_cum_daily"]
    # Cut this series by taking in account only the days before actual date
    cutedSeries = series[series.index < name]
    rowValueToCompare = float(x['deaths_all_cum_daily'])

    # User query to filter rows
    # https://stackoverflow.com/questions/40171498/is-there-a-query-method-or-similar-for-pandas-series-pandas-series-query
    result = cutedSeries.to_frame().query('deaths_all_cum_daily > 0').query(f'({rowValueToCompare} / deaths_all_cum_daily) >= 2.0')

    # If empty return Nan
    if result.empty:
        return x.name, np.NaN, np.NaN, np.NaN

    # Get the last result
    oneResult = result.tail(1).iloc[:, 0]

    # Compute values to return
    value = (rowValueToCompare/oneResult.values[0])
    idx = oneResult.index[0]
    # Delta between the actual row day, and the >=2 day
    delta = name - idx

    # return columns
    return name , idx, delta.days, value


from dfply import *
from covidapp.common.utils import *
from git import Repo,exc
from pathlib import Path

def init_etalab_repo():
    root = Path.cwd()

    folder = Path(root / "repo" / "etalab")

    cloned_repo = None

    try:
        cloned_repo = Repo.clone_from("https://github.com/opencovid19-fr/data.git", folder,branch='master')
        cloned_repo.remote().fetch()
    except exc.CommandError as CE:
        print("[Warning] Command Error:{0}".format(CE))
        try :
            cloned_repo = Repo(folder)
        except exc.CommandError as CE:
            print("[Warning] Command Error:{0}".format(CE))

    return cloned_repo

def update_etalab_repo(repo):

    try:
        git = repo.git
        git.pull()
    except exc.GitError as CE:
        print("[Warning] Command Error:{0}".format(CE))
        pass

def consolidate_data(df, priorities):

    ar = list(priorities.values())

    def getPriority(id): return priorities[id]

    df_fra = df >> mask(X.maille_code == "FRA")
    group_of_data = df_fra >> mask(X.source_type.isin(ar)) >> mutate(dt = to_datetime(X.date))
    groupedData = group_of_data[group_of_data['dt'] > '2020-02-29'].groupby('source_type')

    gDataStart = groupedData.get_group(getPriority(1))

    import copy

    def getValidDataFor(groupedData, date, column, acc, priority = 1 ) :
        # get corresponding group, starting from 1
        def gData(p) :
            try:
                fp = getPriority(p)
                return groupedData.get_group(fp) >> mask(X.dt == date)
            except KeyError:
                return pd.DataFrame()

        if priority > 4:
            #Value not Found in other priorities, back to original data
            acc.update({column : gData(1).get(column).values[0]})
            return copy.deepcopy(acc)

        pdata = gData(priority)

        if pdata.empty:
            # continue to test on the next priority sources if this key not exist
            return getValidDataFor(groupedData, date, column, acc, priority + 1)
        else:
            # key exist but value is Nan, try with next priority sources
            if pd.isna(pdata.get(column).values[0]) :
                 return getValidDataFor(groupedData, date, column, acc, priority + 1 )
            else:
                if priority > 1 :
                    print("At date ", date ," column ", column ," : ", gData(1).get(column).values[0]," replaced by ", gData(priority).get(column).values[0] , " at priority" , priority )
                    acc.update({column : gData(priority)[column].values[0]})
                    return copy.deepcopy(acc)
                else :
                    acc.update({column : gData(1).get(column).values[0]})
                    return copy.deepcopy(acc)

    def iterate_on_row(gdata, row):

        columns = ["cas_confirmes", "deces", "deces_ehpad", "reanimation", "hospitalises", "gueris"]
        acc = {}
        acc.update({"date" : row["dt"]})

        for c in columns:
            getValidDataFor(gdata, row["dt"], c, acc, 1)
        return acc

    rows_list = []

    for index, row in gDataStart.iterrows():
        rows_list.append(iterate_on_row(groupedData,row))

    final_df = pd.DataFrame(rows_list)
    return final_df

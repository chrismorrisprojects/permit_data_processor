import pandas as pd
from dotenv import dotenv_values

config = dotenv_values(".env")
DATA_DIR = config['DATA_DIR']
TEXAS_UIC_DIR = 'texas/drillingMasterDatabaseWithTrailers/'
naDate = '1800-01-01'
dateFmt = "%Y%m%d"




def fmtColDate(df, colList, errAct, naFill, fmtStr):
    for col in colList:
        df[col] = pd.to_datetime(df[col], format=fmtStr, errors=errAct).fillna(naFill)
    return df

def fmtColNumeric(df, colList, errAct, naFill):
    for col in colList:
        df[col] = pd.to_datetime(df[col], errors=errAct).fillna(naFill)
    return df

if __name__ == "__main__":
    file = open(f'{DATA_DIR}{TEXAS_UIC_DIR}daf802.txt', 'r')
    x = 0
    while x < 5:
        print(file.readline())
        x = x + 1
    #daMainDF = pd.read_fwf(file, colspecs=colSpecs, names=colNames)
    #daMainDF = fmtColDate(daMainDF, dateCols, "coerce", naDate, dateFmt)
    #daMainDF = fmtColNumeric(daMainDF, numCols, "coerce", 0)
    #daMainDF.to_pickle('daMainDF.pkl')


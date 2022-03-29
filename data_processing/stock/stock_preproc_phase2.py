import numpy as np
import pandas as pd
import os
from io import StringIO

def max_repNans(a):
    mask = np.concatenate(([False],np.isnan(a),[False]))
    if ~mask.any():
        return 0
    else:
        idx = np.nonzero(mask[1:] != mask[:-1])[0]
        return (idx[1::2] - idx[::2]).max()

basedir = "wna/1min"
files = os.listdir(basedir)


# Get the indices where we have at least one non-nan value for a stock
anyvals = np.array([])
dfs = []

for file in files:
    df = pd.read_csv(basedir + "/" + file).drop(columns=["Unnamed: 0"])
    dfs.append(df)
    
    this_anyvals = np.where(df.notnull().sum(axis=1) > 0)[0]
    
    if len(anyvals) == 0:
        anyvals = this_anyvals
    else:
        anyvals = np.union1d(anyvals,this_anyvals)

df = pd.concat(dfs, axis=1)

# Filter out novals (nights and holidays)
df = df.iloc[anyvals,:]

df.reset_index(drop=True, inplace=True)


okcols = []

# Filter out columns with too many consecutive nans
for col in df.columns:
    maxnans = max_repNans(df[col])
    if maxnans < 5000:
        okcols.append(col)
df = df[okcols]

# Get and save arrival times for each stock
with open("1min/arrival_times.txt", "w") as w:
    for col in df.columns:
        arr_times = np.where(df[col].notnull())[0]
        w.write(col + "," + ",".join(arr_times.astype(str).tolist()) + "\n")

# Fill nans using interpolation
interp = df.interpolate()

# Make and store logreturn df
logdf = np.log(interp)
logreturn = logdf.diff().fillna(0)

logreturn.to_csv("1min/stocks_1min_logreturn.csv", index=False)
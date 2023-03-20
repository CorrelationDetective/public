#!/usr/bin/python3

import pandas as pd
import numpy as np
import sys
import os
import re

def main(basename: str, dirname_raw:str, outdir_interpolated, outdir_logreturn):
    base = pd.read_csv(basename)

    # Enddate of base
    startdate = np.datetime64("2020-02-16T09:00")

    for name in base.columns:
        fname = re.sub(".1", "", name)
        path = f"{dirname_raw}/{fname}.csv"
        if os.path.exists(path):
            df = pd.read_csv(path)
            print(name)
        else:
            print(f"No arrivals for {name}")
            continue
        
        # To datetime
        df["datetime"] = df.date + " " + df.time
        df.datetime = pd.to_datetime(df.datetime, format="%m/%d/%Y %H:%M")
        df = df.drop_duplicates(subset=["datetime"])
        df = df.drop(["date", 'time'], axis=1) 

        # Get epoch column 
        df["epoch"] = ((df.datetime - startdate).dt.total_seconds() / 60).astype(int)

        # Get arrivals after startdate
        df = df[df.epoch > 0]

        # Sort-out columns
        df = df.drop("datetime", axis=1)[["epoch", "price"]]
        df.columns = ["t", "value"]

        # Save interpolated
        try:
            os.mkdir(f"{outdir_interpolated}/arrivals_0102")
        except:
            pass

        df.to_csv(f"{outdir_interpolated}/arrivals_0102/{name}.csv", index=False)

        # Get logreturns
        logdf = df.copy()

        # Compute log-differences and replace in df
        logdf['logdiff'] = round(np.log(logdf.value).diff(), 6)
        logdf = logdf.dropna()

        # Fill outliers
        logdf = logdf[(logdf.logdiff < 1e2) & (logdf.logdiff > -1e2)]

        # Sort-out columns
        logdf = logdf.drop("value", axis=1)
        logdf.columns = ["t", "value"]

        # Save logreturn
        try:
            os.mkdir(f"{outdir_logreturn}/arrivals_0102")
        except:
            pass

        logdf.to_csv(f"{outdir_logreturn}/arrivals_0102/{name}.csv", index=False)
    


if __name__ == '__main__':

    basename = "stock/2020_min/interpolated/stocks_2020_0102_10Min_col_interpolated.csv"
    dirname_raw = "stock/2020_min/concat"
    outdir_interpolated = "stock/2020_min/interpolated"
    outdir_logreturn = "stock/2020_min/logreturn"
    main(basename, dirname_raw, outdir_interpolated, outdir_logreturn)
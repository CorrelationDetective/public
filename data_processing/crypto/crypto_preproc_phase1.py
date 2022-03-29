import os
import numpy as np
import pandas as pd
import json
from json import JSONDecodeError as jde

def resampledf(df, resamp_interval):
    # Resample but do not fill
    starttime = np.datetime64('2021-04-14 01:00:00')
    endtime = np.datetime64('2021-07-12 01:00:00')

    # Filter for only april data
    df = df.loc[starttime:endtime]
    
    if len(df) == 0:
        return None

    add_df = pd.DataFrame(data=[[starttime, df.iloc[0].price], [endtime, df.iloc[-1].price]], columns=["timestamp", "price"])
    add_df.timestamp = add_df.timestamp.dt.ceil(resamp_interval)
    add_df = add_df.set_index("timestamp")

    df = df.append(add_df).sort_index()
    
    # Filter out duplicates which cause crash in resampling method
    df = df[~df.index.duplicated()]
    
#     Resample without filling
    df = df.resample(resamp_interval).asfreq()

    return df.price.to_numpy()

path = "data/crypto/raw_hourly"

files = os.listdir(path)
# files.extend(os.listdir(f"{path}/raw_koen"))

nanrows = []
ids = []

resamp_interval = "3h"

for i in range(len(files)):
    file = files[i]
    print(round(i / len(files) * 100, 2))
    with open(f"{path}/{file}") as f:
        try:
            test = json.loads(f.read())
            df = pd.DataFrame(data=test["prices"], columns=["timestamp", "price"])

            # convert types
            df.timestamp = pd.to_datetime(df.timestamp, unit="ms").dt.ceil(resamp_interval)
            df = df.set_index("timestamp")
            df.price = df.price.astype("float64")
            
            if len(df > 0):
                # Resample but do not fill
                row = resampledf(df, resamp_interval)

                if row is None:
                    continue
                
#                 Append coin name to start
                ids.append([file])
                
                # Append row to collection and delete
                nanrows.append(row)
                del(df)
        
        except:
            print(f"error processing file {file}")
            continue
        
# Save collection
wna = np.vstack(nanrows)
np.savetxt(f"data/crypto/crypto_withna_{resamp_interval}.csv", wna, delimiter=",")


import csv
with open(f"data/crypto/crypto_withna_ids_{resamp_interval}.csv", "w") as f:
    write = csv.writer(f, lineterminator='\n')
    write.writerows(ids)
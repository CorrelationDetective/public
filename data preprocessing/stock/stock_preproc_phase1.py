import numpy as np
import pandas as pd
import os
from io import StringIO
from multiprocessing import Pool

def resampledf(df, resamp_interval):
    df = df.set_index("date_time")["closing"]

    # Slice to fixed period
    starttime = np.datetime64('2020-04-01 10:00:00')
    endtime = np.datetime64('2020-05-12 15:00:00')

    df = df.loc[starttime:endtime]

    if len(df) == 0:
        return None, None
    
#     Artificially add new beginning and end
    add_df = pd.DataFrame(data=[[starttime, df.iloc[0]], [endtime, df.iloc[-1]]], columns=["timestamp", "closing"])
    add_df.timestamp = add_df.timestamp.dt.ceil(resamp_interval)
    add_df = add_df.set_index("timestamp").closing
    
    df = df.append(add_df).sort_index()
    
    # Filter out duplicates which cause crash in resampling method
    df = df[~df.index.duplicated()]

    #     Resample without filling
    df = df.resample(resamp_interval).asfreq()
    
    return df.to_numpy()

def save_batch(bid, headers, nanrows):
    wna = np.vstack(nanrows).T
    np.savetxt(f"wna/2020_withna_{resamp_interval}_batch{bid}.csv", wna, header=",".join(headers), delimiter=",")



def process_file(file):
    resamp_interval = "1Min"
    path20 = "2020AprilMay/combined"

    try:
        df = pd.read_csv(path20 + "/" + file, sep=",", 
                        names=["date","time","opening","highest","lowest","closing","transactions"],
                        parse_dates=[["date", "time"]]
                        )
        df = df[["date_time", "closing"]]

        nanrow = resampledf(df, resamp_interval)

        if nanrow is None:
            print("empty df")
            return None

        # this_novals = np.where(np.isnan(nanrow))

        # if len(novals) == 0:
        #     novals = this_novals
        # else:
        #     novals = np.intersect1d(novals, this_novals)

    #           Store header
        header = file.rstrip(".txt").lstrip("202004_")

        # Store wna
        print(f"SUCCES - stock {header}")

        sr = pd.Series(nanrow, name=header)

        return sr

    except Exception as e:
        print(f"error processing file {file}")
        print(e)
        return None
        
# # Write out indices where we had no values
# with open(f"novals_{resamp_interval}.txt", "w") as w:
#     w.write(",".join(novals.astype(str).tolist()))

# if len(nanrows) > 0:
#     save_batch(s // 100 + 1, headers, nanrows)
#     nanrows = []
#     headers = []

if __name__ == "__main__":
    resamp_interval = "1Min"
    path20 = "2020AprilMay/combined"
    files = os.listdir(path20)

    file_chunks = np.array_split(files, 10)
    for bid in range(len(file_chunks)):
        chunk = file_chunks[bid]

        print(f"starting on chunk {bid}")

        with Pool() as pool:
            series = pool.map(process_file, chunk)

            # Filter nans
            series = [ser for ser in series if ser is not None]

            df = pd.concat(series, axis=1)

            df.to_csv(f"wna/2020_withna_{resamp_interval}_batch{bid}.csv")
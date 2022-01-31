import os
import numpy as np
import pandas as pd
import json
from json import JSONDecodeError as jde

def max_repeatedNans(a):
    mask = np.concatenate(([False],np.isnan(a),[False]))
    if ~mask.any():
        return 0
    else:
        idx = np.nonzero(mask[1:] != mask[:-1])[0]
        return (idx[1::2] - idx[::2]).max()

resamp_interval="h"

wna = np.genfromtxt(f"withna/crypto_withna_{resamp_interval}.csv", delimiter=",")

with open(f"withna/crypto_withna_ids_{resamp_interval}.csv") as f:
    lines = f.readlines()
    ids = np.array([line.rstrip(".txt\n") for line in lines])

maxnans = []
for i in range(wna.shape[0]):
    maxnans.append(max_repeatedNans(wna[i,:]))
    
maxnans = np.array(maxnans).astype(int)

wna_nanfilt = wna[maxnans < 40, :]
filt_ids = ids[maxnans < 40]

df = pd.DataFrame(wna_nanfilt.T, columns=filt_ids)

with open(f"logreturn/arrival_times_{resamp_interval}.txt", "w") as w:
    for col in df.columns.tolist():
        val_idx = np.where(df[col].notnull())[0].astype("str").tolist()
        w.write(col + "," + ",".join(val_idx) + "\n")

interp = df.interpolate()
interp_arr = interp.to_numpy()

logreturn = np.diff(np.log(interp_arr))

# Fill hidden nans 
logreturn = np.nan_to_num(logreturn)

# Transpose matrix and add names as header
logt = logreturn.T

# Shuffle columns
np.random.seed(1)
perm = np.random.permutation(logt.shape[1])

logt_shuf = logt[:,perm]
ids_shuf = filt_ids[perm]

np.savetxt(f"logreturn/crypto_logreturn_{resamp_interval}.csv", logt_shuf, 
header=",".join(ids_shuf), delimiter=",")
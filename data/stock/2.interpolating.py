#!/usr/bin/python3

import sys
import pandas as pd
import glob
import re
import numpy as np
from typing import List

def max_repNans(a):
    mask = np.concatenate(([False],np.isnan(a),[False]))
    if ~mask.any():
        return 0
    else:
        idx = np.nonzero(mask[1:] != mask[:-1])[0]
        return (idx[1::2] - idx[::2]).max()

def main(dirname: str, outname:str, maxnans: int, maxsize: int):
	dfs = []
	anyvals = np.array([])
	totlen = 0

	for file in glob.glob(dirname + "/*.csv"):
	# for file in [dirname]:
		df = pd.read_csv(file)
		dfs.append(df)
		totlen = len(df)
	    
		# Monitor for which mins we have at least one value
		local_anyvals = np.where(df.notnull().sum(axis=1) > 0)[0]
		anyvals = np.union1d(anyvals,local_anyvals)

	print("number of mins with vals:", len(anyvals))
	print("total length in mins:", totlen)

	df = pd.concat(dfs,axis=1)

	# Filter out novals (nights and holidays)
	df = df.iloc[anyvals,:]

	# Check if df length does not exceed max length
	if len(df) > maxsize:
		print("df bigger than maxsize, slicing")
		# Slice the df
		df = df.iloc[-maxsize:,:]

	okcols = []

	# Filter out stocks with too many consecutive nans in the first 1000 minutes
	cols = df.columns
	for i in range(df.shape[1]):
		nans = max_repNans(df.iloc[:1000,i])
		if nans < maxnans:
			okcols.append(cols[i])
	df = df[okcols]

	print(f"Number of ok stocks: {len(okcols)}")

	# Interpolate data
	interp = df.interpolate()

	# Backfill df 
	interp = interp.backfill()

	# Transpose data and save (better for subset reading)
	interp.to_csv(f"{outname}_interpolated.csv", index=False)


if __name__ == '__main__':

	dirname = "stock/2020_min/withna/10min_batches"
	maxnans = 500
	outname = "stock/2020_min/interpolated/stocks_2020_0102_10Min_col"
	maxsize = 2000000000

	print("input params:")
	print(f"Dirname: {dirname}")
	print(f"Maxnans: {maxnans}")
	print(f"outname: {outname}")

	main(dirname, outname, maxnans, maxsize)
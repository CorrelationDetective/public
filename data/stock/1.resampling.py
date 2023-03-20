#!/usr/bin/python3

import sys
import pandas as pd
import glob
import re
import numpy as np
from typing import List

def main(dirname: str, startdate, enddate, outname):
	series = []

	batchid = 0
	i = 1
	for file in glob.glob(dirname + "/*.csv"):
		try:
			sr = process_file(file, startdate, enddate)
		except Exception as e:
			print(e)

		if sr is not None:
			series.append(sr)
			i+=1

		if i%500 == 0 and len(series) > 0:
			save_batch(batchid, outname, series)
			batchid += 1
			series = []

	# Save last batch
	if len(series) > 0:
		save_batch(batchid, outname, series)


def save_batch(batchid: int, outname: str, series: List[pd.Series]):
	print(f"Saving batch {batchid}")
	filename = f"{outname}_batch{batchid}.csv"

	# concat all series
	df = pd.concat(series, axis=1)

	df.to_csv(filename, index=False)


def process_file(filename: str, startdate: np.datetime64, enddate: np.datetime64):
	# stock_types = ["Asien", "Europa", "Nordamerika"]
	resample_interval="1Min"

	header = filename.split("/")[-1]
	header = re.sub(".csv", "", header)
	print(header)

	#     Filter out non stocks
	# stock_type = header.split("-")[0].split(".")[-1]
	# if stock_type not in stock_types:
	# 	print(f"Not a stock: {header}")
	# 	return

	try:
	#         Prepare dataframe
	    df = pd.read_csv(filename, header=0, names=['date', 'time', 'price']).dropna()
	except:
		print(f"Error in reading df: {header}")
		return

	df["datetime"] = df.date + " " + df.time
	df.datetime = pd.to_datetime(df.datetime, format="%m/%d/%Y %H:%M")
	df = df.drop_duplicates(subset=["datetime"])
	df = df.set_index(df.datetime).drop(["date", 'time', 'datetime'], axis=1) 

	if len(df) == 0:
		return

	#         Resample dataframe
	df = df.price
	df = df.loc[startdate:enddate]

	if len(df) == 0:
		return

	#     Artificially add new beginning and end
	adddf = pd.Series(data=[df.iloc[0], df.iloc[-1]], index=[startdate,enddate], name="price")
	df = pd.concat([df,adddf]).sort_index()

	# Filter out duplicates which cause crash in resampling method
	df = df[~df.index.duplicated()]

	#     Resample without filling
	df = df.resample(resample_interval).mean()

	sr = pd.Series(df, name=header)
	return sr


if __name__ == '__main__':

	# dirname = sys.argv[1]
	# startdate = sys.argv[2]
	# enddate = sys.argv[3]
	# outname = sys.argv[4]

	dirname = "stock/2020_min/concat"
	startdate = np.datetime64("2020-01-30T09:00")
	enddate = np.datetime64("2020-03-30T18:00")
	size = None
	outname = "stock/2020_min/withna/1min_batches/20200203_1Min_meanresamp_withna"

	print("input params:")
	print(f"Dirname: {dirname}")
	print(f"startdate: {startdate}")
	print(f"enddate: {enddate}")
	print(f"outname: {outname}")

	if size is not None:
		c_startdate = enddate - np.timedelta64(size, 'm')
		if c_startdate > startdate:
			print(f"Correcting startdate to {c_startdate}")
			startdate = c_startdate

	main(dirname, startdate, enddate, outname)
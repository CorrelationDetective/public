#!/usr/bin/python3

import pandas as pd
import numpy as np
import sys
import re

def main(filename: str):
	df = pd.read_csv(filename)

	logdf = df.copy()

	# Compute log-differences and replace in df
	logdiffs = np.log(df).diff().dropna().round(4)

	# Fill outliers
	logdiffs[(logdiffs < -1e2) | (logdiffs > 1e2)] = 0

	# Filter out low variances
	logdiffs = logdiffs.loc[:,logdiffs.var() > 1e-8]

	print("Saving")
	outname = re.sub("_interpolated", "", filename)
	outname = re.sub(".csv", "", outname)
	outname += "_logreturn"

	logdiffs.to_csv(f"{outname}.csv", index=False)


if __name__ == '__main__':

	filename = "stock/2020_min/interpolated/stocks_2020_0102_10Min_col_interpolated.csv"

	print("input params:")
	print(f"filename: {filename}")

	main(filename)
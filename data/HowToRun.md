This document provides instructions on how to run the provided pre-processing scripts on the raw data described in the paper.

Requirements:
- Java 8 or higher
- Python 3 or higher with pandas and numpy installed

Stock:
1. Download the raw price data from Yahoo Finance
2. Run 1.resampling.py with the input parameters of your liking (i.e. start and enddate of period you want to cover)
3. Run 2.interpolating.py to interpolate missing values
4. Run 3.interpolating.py to perform log-return normalization on the data
5. If you want to use the dataset for streaming experiments, run 4.create_arrivals.py to create arrival files

Crypto:
1. Get the coin ids for the coins you want to include from the CoinGecko API (paper used the ones in `coinids.txt`)
2. Download hourly data using CoinGecko API
3. Preprocess using the `*.py` files in the stock directory

Weather:
1. Run Main.java

FMRI:
1. Download the pre-processed data at https://openneuro.org/datasets/ds002837/versions/2.0.0. 
We used file *sub-1_task-500daysofsummer_bold_blur_censor*, which  includes the recommended pre-processing for voxel-based analytics.
2. Change the paths in `fmri_data_read.ipynb` to the right input/output
3. Run `fmri_data_read.ipynb` varying the down_res variable to the mean pooling kernel sizes described in the paper (standard is 4)

Deep:
1. Download the pre-processed data at https://sites.skoltech.ru/compvision/noimi/. 
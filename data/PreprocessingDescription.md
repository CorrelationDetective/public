This document provides a description of the preprocessing steps used for all datasets in the VLDBJ paper.

**Stocks**
Daily closing prices of 28678 stocks, covering a period from January 2, 2016 to December 31, 2020 leading to 1309 observations (excluding public holidays). All prices were normalized with log-return normalization, as is standard in finance.
For streaming, a more fine-grained dataset was extracted from the original data which included minute closing prices of 2039 stocks (filtering out low-variance stocks), covering the whole of 2020. 

**fMRI**
Functional MRI data of a participant watching a movie, prepared with the recommended steps for voxel-based analytics. 
We used file *sub-1_task-500daysofsummer_bold_blur_censor*, which already includes the recommended pre-processing for voxel-based analytics.
The data was further preprocessed by mean-pooling with kernels of 2x2x2, 3x3x3, 4x4x4, 6x6x6 and 8x8x8 voxels, each representing the mean activity level at a cube of voxels in the scan.
Constant-value time series were removed. This led to a total of 237, 509, 1440, 3152, and 9700 time series respectively, all of equal length (5470 observations), covering a period of ~1.5 hours. 
Link: [https://openneuro.org/datasets/ds002837/versions/2.0.0]

**SLP & TMP** 
Segment of the ISD weather dataset. 
We focused on two attributes contained in this dataset that were measured at regular intervals: (a) sea level pressure (SLP), and, (b) atmospheric temperatures (TMP). 
The experiments on static data were run on datasets containing the daily average values between January 1, 2016 and December 31, 2020, after removing the sensors that had faulty readings for five consecutive days, and applying forward-filling missing value imputation. 
The end dataset included 3222 time-series with sea-level pressure data, and 2200 time-series with temperature data, each with 2927 readings per time-series.
The streaming experiments were run on hourly measurements collected throughout year 2000, by sensors with variable update frequencies. Preprocessing was identical to the static weather datasets. This resulted in a total of 1898 available time-series of SLP data, and 2927 time-series of TMP data.
Link: [https://www.ncei.noaa.gov/access/search/dataset-search]

**SLP-small**
Sea Level Pressure data, as used (incl. preprocessing) in the case study of Agrawal et. al., 2020.
Kindly provided by the authors of Agrawal, 2020.
The dataset contains 171 time series, each with 108 observations.

**Crypto** 
3-hour closing prices of 7075 crypto-currencies, each with 713 observations, covering the period from April 14, 2021 to July 13, 2021. 
The data was retrieved via the CoinGecko API. Pre-processing was identical to the Stocks dataset. 
For the streaming experiments, a more fine-grained dataset was extracted from the original data which included minute closing prices of 3937 time-series (filtering out low-variance, and recently launched coins), covering the same period as the static version of this dataset.

**Deep** 
This dataset includes a billion vectors of length 96, obtained by extracting the embeddings from the final layers of a convolutional neural network. 
Link: [https://sites.skoltech.ru/compvision/noimi/]
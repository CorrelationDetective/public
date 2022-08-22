This document provides instructions on how to run the provided pre-processing scripts on the raw data described in the paper.

Requirements:
- Java 8 or higher
- Python 3 or higher with pandas and numpy installed


Stock:
1. Download the raw price data from the github link referenced in the paper (Yahoo Finance)
2. Change the input path in stock_preproc_phase1.py to path with your price data
3. Change output paths in stock_preproc_phase1.py to your liking
4. Run stock_preproc_phase1.py 
5. Change output paths in stock_preproc_phase2.py to your liking
6. Run stock_preproc_phase2.py 

Crypto:
1. Get the coin ids for the coins you want to include from the CoinGecko API (paper used all available)
2. Download hourly data using CoinGecko API
3. Change path in crypto_preproc_phase1.py to path with your price data
4. Change output paths in crypto_preproc_phase1.py to your liking
5. Run crypto_preproc_phase1.py
6. Change output paths in crypto_preproc_phase2.py to your liking
7. Run crypto_preproc_phase2.py

Weather:
1. Run Main.java

FMRI:
1. Download the pre-processed data at https://openneuro.org/datasets/ds002837/versions/2.0.0. We used file sub-1_task-500daysofsummer_bold_blur_censor, which  includes the recommended pre-processing for voxel-based analytics.
2. Change the paths in the jupyter notebook to the right input/output
3. Run the jupyter notebook varying the down_res variable to the mean pooling kernel sizes described in the paper (standard is 4)


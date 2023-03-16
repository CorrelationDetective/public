The jar in this folder provide all necessary functionality to reproduce the experiments of CD on static data.
It can be run as follows:

`java -cp CDStream.jar core/Main <data_path> <arrival_path> <output_path> <algorithm> <measure> <query_type> <p_l> <p_r> <tau> <delta> <top-k> <epochs>`

### Notes:

1. The base data should be in CSV format, with a header row indicating the name of each time series. There should be no index column. E.g.:

```
MSFT,AAPL,ASML,...
200,3000,500,...
202,2950,550,...
...
```

2. The `arrival_path` parameter should be a directory of CSV files with the name of the file referring to the time-series (i.e., column header in the base data) for which it contains the arrival times. The file should contain two columns; (t,value) with `t` containing the timepoints of the arrivals and `value` containing the values of those arrivals. Following the above example, the directory should contain the files `MSFT.csv`, `AAPL.csv`, `ASML.csv`,..., which look like the following:

```
t,value
16,-0.001426
90,0.002841
164,0.000476
238,0.002378
312,-0.001902
387,-0.000952
461,-0.005262
535,0.004293
...
```

3. The `output_path` parameter indicates the name of the output directory the algorithm should write its results and run reports.
4. The `algorithm` parameter can take the values _'CD'_, _'CDSTREAM'_, and _'CDHYBRID'_. 
5. The `measure` parameter can take the values _'euclidean_similarity'_ and _'pearson_correlation'_.
6. The `query_type` parameter can take the values _'threshold'_ and _'topk'_.
7. `p_l` and `p_r` indicate the correlation pattern (i.e., maximum cardinality of the considered vector combinations). For the multipole and total correlation metric, `p_r` is added to `p_l`.
8. `tau` indicates the correlation threshold, which is only used when the query type is set to _'threshold'_.
9. `delta` indicates the minimum jump, which is only used when the query type is set to _'threshold'_.
10. To run with the irreducibility constraint, set `delta = -1`.
11. `topk` indicates the maximum number of (top) results, which is only used when the query type is set to _'topk'_.
12. `epochs` indicates the number of epochs we want to run for (i.e., simulate).

Example query; Let's perform a PC(1,2) threshold query with CDStream with `tau = 0.95` for 100 epochs:

```Bash
java -cp CDStream.jar core/Main /path/to/data.csv /path/to/arrivaldir outputdir cdstream pearson_correlation threshold 1 2 0.95 0 0 100
```
The jar in this folder provide all necessary functionality to reproduce the experiments of CD on static data.
It can be run as follows:

`java -cp CorrelationDetective.jar core/Main <algorithm> <input_path> <output_path> <measure> <query_type> <tau> <delta> <top-k> <p_l> <p_r>`

### Notes:

1. The data should be in CSV format, with a header row indicating the name of each time series. There should be no index column. E.g.:

```
MSFT,AAPL,ASML,...
200,3000,500,...
202,2950,550,...
...
```

2. The `algorithm` parameter can take the values _'CD'_, _'OPT'_, and _'UNOPT'_.
3. The `measure` parameter can take the values _'euclidean_similarity'_, _'pearson_correlation'_, _'multipole'_, and _'total_correlation'_.
3. The `query_type` parameter can take the values _'threshold'_ and _'topk'_.
4. `tau` indicates the correlation threshold, which is only used when the query type is set to _'threshold'_.
5. `delta` indicates the minimum jump, which is only used when the query type is set to _'threshold'_.
6. To run with the irreducibility constraint, set `delta = -1`.
7. `topk` indicates the maximum number of (top) results, which is only used when the query type is set to _'topk'_.
8. `p_l` and `p_r` indicate the correlation pattern (i.e., maximum cardinality of the considered vector combinations). For the multipole and total correlation metric, `p_r` is added to `p_l`.

Example query; Let's perform a PC(1,2) threshold query with CD with `tau = 0.95`:

```Bash
java -cp CorrelationDetective.jar core/Main cd /path/to/data.csv outputdir pearson_correlation threshold 0.95 0 1 2
```
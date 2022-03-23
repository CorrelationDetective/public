The three jars in this folder provide all necessary functionality to reproduce the experiments of CD on static data:

|Jar|What it does|
|:--|:--|
|paper-experiments-one-run.jar runs| CD with or without minimum jump constraint|
|paper-experiments-one-run-irreducibility.jar| runs CD with irreducibility constraint|
|paper-experiments-one-run-with-print.jar| runs CD with or without minimum jump constraints, and additionally prints the result set. This was used for postprocessing to compare to CoMEtExtended|


Any of the jars can be run as follows:

`java -cp <name of jar> SimpleTest <data path> <measure> <tau> <delta> <top-k> <l_max> <r_max>`

### Notes:

1. The data should be in CSV format, with a header row indicating the name of each time series. There should be no index column. E.g.:

|**MSFT,AAPL,ASML,**...|
|:--|
|200,3000,500,...|
|202,2950,550,...|
|...|

2. The `measure` parameter can take the values _'multipearson'_ or _'multipole'_
3. `tau` indicates the starting correlation threshold, which is increased if at least top-k results are found
4. To run without constraint, set `delta = -5` and use paper-experiments-one-run.jar. For the irreducibility jar, delta is ignored and CD applies the irreducibility constraint
5. For the multipole metric, `r_max` is ignored, and `l_max` specifies the cardinality. for multipearson, both are used. 
6. If you use the multipole metric, all results up to cardinality `l_max` are found. For multipearson, it finds the pairwise results and the results of the exact cardinality `(l_max, r_max)` only. To find all results up to `(l-Max, r_max)`, you need to seperately run the smaller correlation patterns.

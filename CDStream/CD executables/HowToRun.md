The jar in this folder provide all necessary functionality to reproduce the experiments of CDHybrid

The jar can be run as follows:

`java -cp <name of jar> main <algorithm> <data_path> <arrival_times_path> <n> <l_max> <r_max> <tau> <delta> <batch_size>`


### Notes:

 1. The data should be in CSV format, with a header row indicating the name of each time series. There should be no index column. E.g.:

```
MSFT,AAPL,ASML,...
200,3000,500,...
202,2950,550,...
...
```

2. The arrival tives should be in rows of comma-separated integers, starting with the name of the vector it refers to. E.g.:

```
MSFT,1,6,7,8,...
AAPL,2,5,7,8,...
ASML,1,2,3,4,...
...
```

3. The `algorithm` parameter can take the following values:
	- streaming (to run CDStream base)
	- oneshot (to run CD)

4. `n` indicates the amount of vectors that should be used from the data
5. `tau` indicates the correlation threshold
6. To run with irreducibility, set `delta = 0`. To run with no constraint, set delta to something negative (e.g. -5)

The parameters of the experiment in the paper were:

|parameter | value | 
|:--|:--|
|`algorithm`| several runs with each of the available|
|`data`| [stocks, fmri]|
|`n`| [200,500,1000]|
|`l_max`| [1 2 3]|
|`r_max`| [2 3]|
|`tau`| [0.6, 0.7, 0.8]|
|`delta`| [-5, 0, 0.05, 0.10, 0.15]|
|`batch_size`| [50, 100, 200, 400, 800, 1000, 2000, 4000, 8000, 16000] |

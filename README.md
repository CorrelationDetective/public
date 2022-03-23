# Correlation Detective: Multivariate Correlations Discovery in Static and Streaming Data

To run CorrelationDetective , you need to have Java 8 (or higher) installed ([link](https://www.oracle.com/java/technologies/javase/javase8-archive-downloads.html)). 
Then you can run each of the algorithm variants by following the instructions in the HowToRun.md files in each of the respective folders (e.g., CDStream/CD executables/HowToRun.md).

For example, running the base CD algorithm with default parameters can be done by simply running the following command in the terminal;

```Bash
java -cp paper-experiments-one-run.jar SimpleTest path/to/data multipearson 0.9 0.05 10000 1 2
```

You can also run the algorithm with different query parameters by changing the arguments in the above command. 
Check the respective HowToRun.md files for descriptions on each of the parameters.

package core;

import _aux.Pair;
import _aux.lists.FastArrayList;
import _aux.lists.FastLinkedList;
import algorithms.AlgorithmEnum;
import algorithms.streaming.BaselineStream;
import data_reading.DataReader;
import data_reading.StreamDataReader;
import lombok.NonNull;
import queries.QueryTypeEnum;
import similarities.MultivariateSimilarityFunction;
import similarities.NormalizationFunction;
import similarities.functions.*;
import similarities.SimEnum;
import streaming.*;
import algorithms.streaming.SDOneShot;
import algorithms.streaming.SDHybrid;
import algorithms.streaming.sdstream.SDStream;
import algorithms.streaming.StreamingAlgorithm;

import java.io.File;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.*;

public class Main {
    public static void main(String[] args) {
        String codeVersion = "streaming";

        Level logLevel = Level.INFO;
        boolean saveStats = true;
        boolean saveResults = true;
        String experimentId = "";
        boolean parallel = true;
        boolean random = true;
        int seed = 0;
        String aggPattern = "avg";
        boolean allowSideOverlap = false;
        boolean empiricalBounding = true;
        double shrinkFactor = 1;
        int topKBufferRatio = 2;
        double maxApproxSizeRatio = .5;
        int kMeans = 10;
        boolean irreducibility = false;

        AlgorithmEnum algorithm;
        SimEnum simMetricName;
        QueryTypeEnum queryType;
        String dataPath;
        String arrivalPath;
        String outputPath;
        double tau;
        double minJump;
        int topK;
        int maxPLeft;
        int maxPRight;

//        Streaming
        int slidingWindowSize = 998;
        int timePerEpoch = 1;
        int basicWindowSize = 200;
        BatchModelEnum batchModel = BatchModelEnum.TIMEBASED;
        int arrivalBatchSize = 0;
        String basicWindowAggMethodName = "sum";
        int epochs;

//        Hybrid
        int warmupEpochs = 40;
        int boostEpochs = 0;
        int boostFactor = 10;

//        Read parameters from args
        if (args.length>0){
            try {
                int i = 0;
                dataPath = args[i];
                i++;
                arrivalPath = args[i];
                i++;
                outputPath = args[i];
                i++;

                //            Query type
                algorithm = AlgorithmEnum.valueOf(args[i].toUpperCase());
                i++;
                simMetricName = SimEnum.valueOf(args[i].toUpperCase());
                i++;
                queryType = QueryTypeEnum.valueOf(args[i].toUpperCase());
                i++;
                maxPLeft = Integer.parseInt(args[i]);
                i++;
                maxPRight = Integer.parseInt(args[i]);
                i++;

                //            Query params
                tau = Double.parseDouble(args[i]);
                i++;
                minJump = Double.parseDouble(args[i]);
                i++;
                topK = Integer.parseInt(args[i]);
                i++;

                //            Streaming params
                epochs = Integer.parseInt(args[i]);
                i++;
            } catch (Exception e){
                throw new IllegalArgumentException("Invalid arguments provided");
            }
        } else {
           throw new IllegalArgumentException("No arguments provided");
        }

        if (minJump < 0) {
            minJump = 0;
            irreducibility = true;
        }

        //        Compute maxT (also considering the arrival wave)
        int maxT = (epochs - boostEpochs) * timePerEpoch + (boostEpochs * timePerEpoch * boostFactor);

//        Initiate basicWindowAggMethod
        AggregationMethod basicWindowAggMethod = AggregationMethod.infer(basicWindowAggMethodName);

        //        Get similarity function from enum
        MultivariateSimilarityFunction simMetric;
        switch (simMetricName){
            case PEARSON_CORRELATION: default: simMetric = new PearsonCorrelation(); break;
            case EUCLIDEAN_SIMILARITY: simMetric = new EuclideanSimilarity(); break;
        }
        String resultDir = String.format("%s/results/streaming/%s/%d%d", outputPath, simMetric, maxPLeft, maxPRight,
                queryType);
        new File(resultDir).mkdirs();
        String resultPath = String.format("%s/%s_tau%.2f.csv", resultDir, algorithm, tau);

//        read data
        Pair<TimeSeries[], ArrivalQueue> dataPair = getData(dataPath, arrivalPath, maxT, true,
                slidingWindowSize, basicWindowAggMethod, simMetric.normFunction);

        TimeSeries[] timeSeries = dataPair._1;
        ArrivalQueue arrivalQueue = dataPair._2;

//        update parameters if we got less data
        int n = timeSeries.length;

        slidingWindowSize = timeSeries[0].getSlidingWindowSize();

        Parameters par = new Parameters(
                logLevel,
                codeVersion,
                saveStats,
                experimentId,
                saveResults,
                resultPath,
                algorithm,
                parallel,
                random,
                seed,
                queryType,
                simMetric,
                aggPattern,
                tau,
                minJump,
                irreducibility,
                topK,
                topKBufferRatio,
                maxPLeft,
                maxPRight,
                allowSideOverlap,
                outputPath,
                n,
                empiricalBounding,
                kMeans,
                shrinkFactor,
                maxApproxSizeRatio,
                slidingWindowSize,
                timePerEpoch,
                epochs,
                batchModel,
                arrivalBatchSize,
                basicWindowSize,
                basicWindowAggMethod,
                timeSeries,
                arrivalQueue,
                boostEpochs,
                boostFactor,
                warmupEpochs
        );
        par.init();

        run(par);
    }

    private static void run(@NonNull Parameters par) {
        par.LOGGER.info(String.format("----------- new run starting; querying %s with %s, n=%d, epochs=%d ---------------------",
                par.simMetric, par.algorithm, par.n, par.epochs));
        par.LOGGER.info("Starting time " + LocalDateTime.now());

        StreamingAlgorithm algorithm;
        switch (par.algorithm){
            case CD: default: algorithm = new SDOneShot(par); break;
            case CDSTREAM: algorithm = new SDStream(par); break;
            case CDHYBRID: algorithm = new SDHybrid(par); break;
            case BASELINE: algorithm = new BaselineStream(par); break;
        }

        algorithm.simulate();
    }

    public static Pair<TimeSeries[], ArrivalQueue> getData(String dataPath,
                                                           String arrivalPath,
                                                           int maxT,
                                                           boolean timeBased,
                                                           int slidingWindowSize,
                                                           AggregationMethod basicWindowAggMethod,
                                                           NormalizationFunction normFunction){
        Pair<String[], double[][]> dataPair = DataReader.readColumnMajorCSV(dataPath);
        HashMap<String, FastLinkedList<Double[]>> arrivals = StreamDataReader.getArrivalPackages(arrivalPath, maxT, timeBased);

        String[] headers = dataPair._1;
        double[][] data = dataPair._2;

        return initializeTimeSeries(data, headers, arrivals, slidingWindowSize, basicWindowAggMethod, normFunction);
    }

    public static Pair<TimeSeries[], ArrivalQueue> initializeTimeSeries(double[][] baseData,
                                                                        String[] headers,
                                                                        HashMap<String, FastLinkedList<Double[]>> arrivals,
                                                                        int slidingWindowSize,
                                                                        AggregationMethod basicWindowAggMethod,
                                                                        NormalizationFunction normFunction) {
        int n = baseData.length;
        int m = baseData[0].length;

//        Update sliding window size if necessary
        if (slidingWindowSize > m){
            System.out.println(String.format("Sliding window size %d is larger than the time series length %d. Setting sliding window size to %d", slidingWindowSize, m, m));
            slidingWindowSize = m;
        }

        FastArrayList<TimeSeries> timeSeries = new FastArrayList<>(n);
        ArrivalQueue arrivalQueue = new ArrivalQueue();

        int id = 0;

//        Initialize time series
        for (int i = 0; i < n; i++) {
//            Clean header
            String name = headers[i].replace(" ", "_").replace("#", "");

            double[] localBaseData = baseData[i];

//            Get arrival times if available
            if (arrivals != null) {
                FastLinkedList<Double[]> rawArrivals = arrivals.get(name);
                if (rawArrivals == null) {
//                    System.out.println(String.format("No arrival times found for %s", name));
                    continue;
                }
//                Initialize arrivals and add to arrival queue
                for (Double[] rawArrival : rawArrivals) {
                    double t = rawArrival[0];
                    double v = rawArrival[1];
                    arrivalQueue.add(new Arrival(t, v, id));
                }
            } else {
//                Add artificial arrivals to queue
                for (int j = 0; j < slidingWindowSize; j++){
                    arrivalQueue.add(new Arrival(j, localBaseData[slidingWindowSize + j], id));
                }

                // Make sure base data is sliced
                localBaseData = Arrays.copyOfRange(localBaseData, 0, slidingWindowSize);
            }

//            Create and add timeseries
            timeSeries.add(new TimeSeries(id, name, localBaseData, slidingWindowSize, basicWindowAggMethod, normFunction));

            id++;
        }
        TimeSeries[] timeSeriesArray = timeSeries.toArray(new TimeSeries[0]);

        return new Pair<>(timeSeriesArray, arrivalQueue);
    }




}
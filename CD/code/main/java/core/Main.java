package core;

import _aux.Pair;
import _aux.lists.FastArrayList;
import algorithms.Algorithm;
import algorithms.AlgorithmEnum;
import algorithms.baselines.SimpleBaseline;
import algorithms.baselines.SmartBaseline;
import algorithms.performance.SimilarityDetective;
import data_reading.DataReader;
import lombok.NonNull;
import queries.QueryTypeEnum;
import queries.ResultSet;
import queries.ResultTuple;
import similarities.MultivariateSimilarityFunction;
import similarities.functions.*;
import similarities.SimEnum;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) throws Exception {
        String codeVersion = "main";

        Level logLevel = Level.FINE;
        boolean saveStats = true;
        boolean saveResults = true;
        String experimentId = "1";
        boolean parallel = true;
        boolean random = true;
        int seed = 0;
        String aggPattern = "avg";
        boolean allowSideOverlap = false;
        int n = (int) 1e7;
        int m = (int) 1e7;
        int partition = 0;
        boolean empiricalBounding = true;
        double shrinkFactor = 1;
        double maxApproxSizeRatio = 0.5;
        int kMeans = 10;
        boolean geoCentroid = false;
        boolean irreducibility = false;

        AlgorithmEnum algorithm;
        SimEnum simMetricName;
        QueryTypeEnum queryType;
        int maxPLeft;
        int maxPRight;
        String inputPath;
        String outputPath;
        double tau;
        double minJump;
        int topK;

//        Read parameters from args
        if (args.length>0){
            int i=0;
            algorithm = AlgorithmEnum.valueOf(args[i].toUpperCase()); i++;
            inputPath = args[i]; i++;
            outputPath = args[i]; i++;
            simMetricName = SimEnum.valueOf(args[i].toUpperCase()); i++;
            queryType = QueryTypeEnum.valueOf(args[i].toUpperCase()); i++;
            tau = Double.parseDouble(args[i]); i++;
            minJump = Double.parseDouble(args[i]); i++;
            maxPLeft = Integer.parseInt(args[i]); i++;
            maxPRight = Integer.parseInt(args[i]); i++;
            topK = Integer.parseInt(args[i]); i++;
        } else {
            throw new Exception("Not enough arguments passed!");
        }

        if (minJump < 0){
            irreducibility = true;
            minJump = 0;
        }

//        Initiate logger
        Logger LOGGER = getLogger(logLevel);

        //        Get similarity function from enum
        MultivariateSimilarityFunction simMetric;
        switch (simMetricName){
            case PEARSON_CORRELATION: simMetric = new PearsonCorrelation(); break;
            case SPEARMAN_CORRELATION: simMetric = new SpearmanCorrelation(); break;
            case MULTIPOLE: simMetric = new Multipole(); break;
            case EUCLIDEAN_SIMILARITY: simMetric = new EuclideanSimilarity(); break;
            case MANHATTAN_SIMILARITY: simMetric = new MinkowskiSimilarity(1); break;
            case CHEBYSHEV_SIMILARITY: simMetric = new ChebyshevSimilarity(); break;
            case TOTAL_CORRELATION: simMetric = new TotalCorrelation(); break;
            default: throw new Exception("Unsupported similarity measure: " + simMetricName);
        }

//        Simmetric dependent parameters
        double startEpsilon = simMetric.simToDist(0.81*simMetric.MAX_SIMILARITY);
        double epsilonMultipolier = 0.8;

        String resultDir = String.format("%s/results/%s/%d%d", outputPath, simMetric, maxPLeft, maxPRight);
        new File(resultDir).mkdirs();

        String resultPath = String.format("%s/%s_tau%.2f.csv", resultDir, queryType, tau);

//        read data
        Pair<String[], double[][]> dataPair = getData(inputPath, LOGGER);
        String[] headers = dataPair.x;
        double[][] data = dataPair.y;

//        update parameters if we got less data
        n = data.length;
        m = data[0].length;

//        preprocess (if necessary)
        data = simMetric.preprocess(data);

        Parameters par = new Parameters(
                LOGGER,
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
                maxPLeft,
                maxPRight,
                allowSideOverlap,
                outputPath,
                headers,
                data,
                n,
                m,
                partition,
                empiricalBounding,
                kMeans,
                geoCentroid,
                startEpsilon,
                epsilonMultipolier,
                shrinkFactor,
                maxApproxSizeRatio
        );
        par.init();

        run(par);
    }

    private static void run(@NonNull Parameters par) {
        par.LOGGER.info(String.format("----------- new run starting; querying %s with %s ---------------------",
                par.simMetric, par.algorithm));
        par.LOGGER.info("Starting time " + LocalDateTime.now());

        Algorithm algorithm;
        switch (par.algorithm){
            case CD: default: algorithm = new SimilarityDetective(par); break;
            case UNOPT: algorithm = new SimpleBaseline(par); break;
            case OPT: algorithm = new SmartBaseline(par); break;
        }
        ResultSet resultSet = algorithm.run();
        par.statBag.nResults = resultSet.size();
        algorithm.printStats(par.statBag);

        par.LOGGER.info(String.format("Ending time " + LocalDateTime.now()));
        par.LOGGER.info("Number of reported results: " + resultSet.size());

        FastArrayList<ResultTuple> resultList = resultSet.toResultTuples(resultSet.getResults(), par.headers);

        par.LOGGER.info("Avg size of reported results: " + resultList.stream().mapToInt(ResultTuple::size).average().orElse(0));

//        Save stats
        if (par.saveStats){
            par.statBag.saveStats(par);
        }

//        Save results
        if (par.saveResults){
//            Save history if topK, otherwise just final resultset
//            if (par.queryType == QueryTypeEnum.TOPK){
//                resultList = resultSet.toResultTuples(resultSet.getResultHistory(), par.headers);
//            }
//                Add dummy result to indicate end of history
            resultList.add(new ResultTuple(new FastArrayList<>(0), new FastArrayList<>(0),
                    new FastArrayList<>(List.of("end")), new FastArrayList<>(List.of("end")), 0d, (long) (par.statBag.totalDuration * 1000000000L)));

            saveResults(resultList, par);
        }

    }

    public static Pair<String[], double[][]> getData(String inputPath, Logger LOGGER) {
        Pair<String[], double[][]> dataPair = DataReader.readColumnMajorCSV(inputPath, LOGGER);
        return dataPair;
    }



    public static void saveResults(FastArrayList<ResultTuple> results, Parameters parameters){
        try {
//            Make root dirs if necessary
            String rootdirname = Pattern.compile("\\/[A-z_0-9.]+.csv").matcher(parameters.resultPath).replaceAll("");
            new File(rootdirname).mkdirs();

            File file = new File(parameters.resultPath);

            FileWriter fw = new FileWriter(file, false);

//            Write header
            fw.write("lhs,rhs,headers1,headers2,sim,timestamp\n");

            String[] headers = parameters.headers;

//            Write results
            for (ResultTuple result: results){
                result.sortSides();

                fw.write(String.format("%s,%s,%s,%s,%.4f,%d%n",
                        result.LHS.stream().map(Object::toString).collect(Collectors.joining("-")),
                        result.RHS.stream().map(Object::toString).collect(Collectors.joining("-")),
                        String.join("-", result.lHeaders),
                        String.join("-", result.rHeaders),
                        result.similarity,
                        result.timestamp
                ));
            }

            fw.close();

        }catch (IOException e){
            e.printStackTrace();
        }
    }

    public static Logger getLogger(Level logLevel){
        Logger mainLogger = Logger.getLogger("com.logicbig");
        mainLogger.setUseParentHandlers(false);

        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter() {
            private static final String format = "[%1$tF %1$tT] [%2$-7s] %3$s %n";
            @Override
            public synchronized String format(LogRecord lr) {
                return String.format(format,
                        new Date(lr.getMillis()),
                        lr.getLevel().getLocalizedName(),
                        lr.getMessage()
                );
            }
        });
        handler.setLevel(logLevel);
        mainLogger.addHandler(handler);
        mainLogger.setLevel(logLevel);

        return mainLogger;
    }
}
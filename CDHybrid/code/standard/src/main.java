import enums.AlgorithmEnum;
import org.apache.commons.lang3.ArrayUtils;
import streaming.*;
import _aux.*;
import bounding.*;
import data.*;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.security.spec.AlgorithmParameterSpec;
import java.util.*;
import java.util.stream.IntStream;

public class main {

    public static void main(String[] args) throws Exception {
        HashMap<String, Object> parameters = new HashMap<>();
        parameters.put("codeVersion", "hybrid");

        System.out.println("running process " + ManagementFactory.getRuntimeMXBean().getName());
        System.out.println("cores = " + Runtime.getRuntime().availableProcessors());

        long loadStart = System.currentTimeMillis();
        String dataPath = "";
        String arrivalTimesPath = "";

        if (args.length > 0){
            parameters.put("algorithm", AlgorithmEnum.valueOf(args[0].toUpperCase()));
            dataPath = args[1];
            arrivalTimesPath = args[2];
            parameters.put("n", Integer.parseInt(args[3]));
            parameters.put("pLeft", Integer.parseInt(args[4]));
            parameters.put("pRight", Integer.parseInt(args[5]));
            parameters.put("tau", Double.parseDouble(args[6]));
            parameters.put("minJump", Double.parseDouble(args[7]));

            parameters.put("parallel", true);
            parameters.put("epochs", 360);
            parameters.put("w", 2000);
            parameters.put("boostFactor", 10);
            parameters.put("boostPeriod", 50);
            parameters.put("warmupPeriod", 30);
            parameters.put("run", 1);

        } else {
            parameters.put("algorithm", AlgorithmEnum.HYBRID);
            dataPath = "data/stock/1min/stocks_1min_logreturn.csv";
            arrivalTimesPath = "data/stock/1min/arrival_times.txt";
            parameters.put("n", 500);
            parameters.put("w", 2000);
            parameters.put("epochs", 360);
            parameters.put("parallel", true);
            parameters.put("pLeft", 1);
            parameters.put("pRight", 2);
            parameters.put("tau", 0.8);
            parameters.put("minJump", 0.05);

            parameters.put("boostPeriod", 50);
            parameters.put("boostFactor", 10);
            parameters.put("warmupPeriod", 30);

            parameters.put("run", 1);
        }

        int boostFactor = (int) parameters.get("boostFactor");
        int epochs = (int) parameters.get("epochs");

        int w = (int) parameters.get("w");
        int m = w * 3;
        parameters.put("m", m);

        int n = (int) parameters.get("n");
        int weatherResampleRate = 1;

        Pair<String[], double[][]> nameDataPair = StreamDataReader.readColumnMajorCSV(dataPath, n, m);
        parameters.put("tsNames", nameDataPair.x);
        parameters.put("dataMatrix", nameDataPair.y);

        parameters.put("arrivalMap", StreamDataReader.getArrivalTimes(arrivalTimesPath, w));

//        ----------------------------- ONESHOT SETTINGS ------------------------------------------

        System.err.println("Loading duration " + (System.currentTimeMillis()-loadStart)/1000);


        parameters.put("clusteringRetries", 10);
        parameters.put("startEpsilon", 10);
        parameters.put("epsilonMultiplier", 0.825);
        parameters.put("defaultDesiredClusters", 10);
        parameters.put("maxLevels", 10);
        parameters.put("breakFirstKLevelsToMoreClusters", 0);
        parameters.put("useKMeans", true);

        ArrayList<Double> durations = new ArrayList<>();

        runTest(parameters);
    }

    public static ArrayList<Double> runTest(HashMap<String, Object> parameters){
        int n = (int) parameters.get("n");

        AlgorithmEnum algorithmType = (AlgorithmEnum) parameters.get("algorithm");
        AlgorithmEnum runningAlgorithm = algorithmType.equals(AlgorithmEnum.ONESHOT) ? AlgorithmEnum.ONESHOT: AlgorithmEnum.STREAMING;
        parameters.put("runningAlgorithm", runningAlgorithm);

        int w = (int) parameters.get("w");
        int epochs = (int) parameters.get("epochs");
        int boostFactor = (int) parameters.get("boostFactor");

        System.out.println("Running test " + algorithmType +
                "_n" + n + "_w_" + w + "_epochs_" + epochs + "_boostFactor_" + boostFactor);

        ArrayList<Double> durations = new ArrayList<>();

        TimeSeries[] timeSeries = new TimeSeries[n];
        double[][] dataMatrix = (double[][]) parameters.get("dataMatrix");
        String[] tsNames = (String[]) parameters.get("tsNames");
        HashMap<String, int[]> arrivalMap = (HashMap<String, int[]>) parameters.get("arrivalMap");

//        Initialize timeseries objects
        for (int i = 0; i < n; i++) {
            String name = tsNames[i].replace("# ", "");
            int[] arrivalTimes = arrivalMap.get(name);

            if (arrivalTimes == null){
                throw new InputMismatchException("No arrival times found for time-series " + name);
            }

            arrivalTimes = Arrays.stream(arrivalTimes).filter(t -> t > w).map(t -> t-w).toArray();

            timeSeries[i] = new TimeSeries(i, name, dataMatrix[i], arrivalTimes, w, n);
        }

        parameters.put("timeSeries", timeSeries);

        parameters.put("seed", 1);
        boolean streaming = !(parameters.get("algorithm")).equals("oneshot");
        parameters.put("streaming", streaming);

        Long oneshotStart = System.currentTimeMillis();


//        --------------------------- RUN ONESHOT INITIALIZATION ---------------------------------
        HierarchicalBounding HB = new HierarchicalBounding(parameters);
        Set<int[]> resultSet = HB.recursiveBounding(parameters);
        double oneshotDuration = StreamLib.getDuration(oneshotStart, System.currentTimeMillis());

        System.out.println("Oneshot duration: " + oneshotDuration);
        System.out.println("Total DCCs: " + HB.dccCount);
        System.out.println("Total Results: " + resultSet.size());



        parameters.put("HB", HB);
        parameters.put("saveStats", false);

//        --------------------------- START STREAMING ---------------------------------
        System.out.println("Started streaming algorithm. Time: " + System.currentTimeMillis());
        StreamingDetective SD = new StreamingDetective(parameters);
        durations = SD.simulate();
        System.out.println("Average duration: " + durations.stream().mapToDouble(x -> x).average());


        return durations;
    }
}
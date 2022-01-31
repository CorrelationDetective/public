import streaming.*;
import _aux.*;
import bounding.*;
import data.*;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.stream.IntStream;

public class main {

    public static void main(String[] args) throws Exception {
        HashMap<String, Object> parameters = new HashMap<>();
        parameters.put("codeVersion", "streaming_v4_2");

        System.out.println("running process " + ManagementFactory.getRuntimeMXBean().getName());
        System.out.println("cores = " + Runtime.getRuntime().availableProcessors());

        long loadStart = System.currentTimeMillis();
        String dataPath = "";
        String arrivalTimesPath = "";

        if (args.length > 0){
            parameters.put("algorithm", args[0]);
            dataPath = args[1];
            arrivalTimesPath = args[2];
            parameters.put("n", Integer.parseInt(args[3]));
            parameters.put("maxT", Integer.parseInt(args[4]));
            parameters.put("pLeft", Integer.parseInt(args[5]));
            parameters.put("pRight", Integer.parseInt(args[6]));
            parameters.put("tau", Double.parseDouble(args[7]));
            parameters.put("minJump", Double.parseDouble(args[8]));

            parameters.put("run", 1);
            parameters.put("parallel", true);
            parameters.put("batchModel", "arrivalBased");
            parameters.put("batchSize", 50);
            parameters.put("epochSize", 1);
            parameters.put("w", 2000);

        } else {
            parameters.put("algorithm", "streaming");
            dataPath = "data/stock/1min/stocks_1min_logreturn.csv";
            arrivalTimesPath = "data/stock/1min/arrival_times.txt";
            parameters.put("n", 200);
            parameters.put("w", 2000);
            parameters.put("maxT", 100);
            parameters.put("parallel", true);
            parameters.put("pLeft", 1);
            parameters.put("pRight", 2);
            parameters.put("tau", 0.8);
            parameters.put("minJump", 0.05);

            parameters.put("batchModel", "arrivalBased");
            parameters.put("batchSize", 50);
            parameters.put("epochSize", 1);
            parameters.put("run", 1);
        }

        int maxT = (int) parameters.get("maxT");

        String batchModel = (String) parameters.get("batchModel");
        int epochSize = (int) parameters.get("epochSize");

        if (batchModel.equals("timeBased")){
            parameters.put("maxT", epochSize*maxT);
        }

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

        String algorithmType = (String) parameters.get("algorithm");

        int w = (int) parameters.get("w");
        int maxT = (int) parameters.get("maxT");
        int epochSize = (int) parameters.get("epochSize");

        System.out.println("Running test " + algorithmType +
                "_n" + n + "_w_" + w + "_maxT_" + maxT + "_epochSize_" + epochSize);

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
        Set<DCC> resultSet = HB.recursiveBounding(parameters);
        double oneshotDuration = StreamLib.getDuration(oneshotStart, System.currentTimeMillis());

        System.out.println("Oneshot duration: " + oneshotDuration);
        System.out.println("Total DCCs: " + HB.dccCount);
        System.out.println("ResultSet size: " + HB.resultSet.stream().mapToInt(DCC::getSize).sum());

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
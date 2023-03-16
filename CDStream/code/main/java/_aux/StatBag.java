package _aux;

import _aux.lists.FastLinkedList;
import algorithms.streaming.StreamingAlgorithm;
import core.Parameters;
import lombok.Getter;
import org.apache.commons.lang3.time.StopWatch;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class StatBag {
    public StopWatch stopWatch = new StopWatch();
    public double totalDuration;
    public long nResults;
    public FastLinkedList<Stage> stageDurations = new FastLinkedList<>();

//    Determines if other stats are actually measures
    private boolean experiment;

//    CC stats
    @Getter private AtomicLong nLookups = new AtomicLong(0);
    @Getter private AtomicLong nCCs = new AtomicLong(0);
    @Getter private AtomicLong totalCCSize = new AtomicLong(0);
    @Getter private AtomicLong nPosDCCs = new AtomicLong(0);
    @Getter private AtomicLong nNegDCCs = new AtomicLong(0);

//    Time stats
    @Getter private AtomicLong nSecCCs = new AtomicLong(0);
    @Getter private AtomicLong nParallelCCs = new AtomicLong(0);
    @Getter private AtomicLong DFSTime = new AtomicLong(0);

//    Streaming stats
    @Getter private StreamingAlgorithm[] activatedAlgorithms;
    @Getter private double[] durations;
    @Getter private int[] resultCounts;
    @Getter private int[] arrivalCounts;
    @Getter private FastLinkedList<Double> simulatedTimes = new FastLinkedList<>();
    @Getter private int[] violationCounts;
    @Getter private AtomicLong nIndexedDCCs = new AtomicLong(0);
    @Getter private long[] dccCounts;
    @Getter private AtomicLong totalIndexedDCCsSize = new AtomicLong(0);
    @Getter private long[] dccSizes;

//    Misc
    @Getter private Map<String, Object> miscStats = new HashMap<>();


    public StatBag(boolean experiment, int epochs) {
        this.experiment = experiment;

        activatedAlgorithms = new StreamingAlgorithm[epochs];
        durations = new double[epochs];
        resultCounts = new int[epochs];
        arrivalCounts = new int[epochs];

        dccCounts = new long[epochs];
        dccSizes = new long[epochs];
        violationCounts = new int[epochs];
    }

//    ---------------------------------------- METHODS ---------------------------------------------

//    Only computed if experiment is true
    public void incrementStat(AtomicLong stat){
        if (!experiment) stat.incrementAndGet();
    }

//    Only computed if experiment is true
    public void addToStat(AtomicLong stat, Supplier<Integer> value){
        if (!experiment) stat.addAndGet(value.get());
    }

    public void saveStats(Parameters parameters) {
        saveRunSummary(parameters);
        saveRunReport(parameters);
    }

    private void saveRunReport(Parameters parameters){
        String outputPath = parameters.outputPath + "/reports";

        try {
//            Make the output directory if necessary
            new File(outputPath).mkdirs();

            String fileName = String.format("%s/%s_report_%s.csv", outputPath, parameters.algorithm, parameters.dateTime.replace("-", "_"));
            File file = new File(fileName);

            parameters.LOGGER.info("saving report to " + fileName);

            FileWriter writer = new FileWriter(fileName, false);

//            Write header
            writer.write("epoch,simulatedTime,activeAlgorithm,duration,nResults,nArrivals,nViolations,nIndexedDCCs,avgIndexedDCCSize\n");

//            Write data
            for (int i = 0; i < durations.length; i++) {
                writer.write(String.format("%d,%f,%s,%f,%d,%d,%d,%d,%f\n", i+1,
                        simulatedTimes.poll(),
                        activatedAlgorithms[i].getClass().getSimpleName(),
                        durations[i],
                        resultCounts[i],
                        arrivalCounts[i],
                        violationCounts[i],
                        dccCounts[i],
                        (double) dccSizes[i] / dccCounts[i]));
            }
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void saveRunSummary(Parameters parameters){
        String outputPath = parameters.outputPath;

        try {
//            Make the output directory if necessary
            new File(outputPath).mkdirs();

            String fileName = String.format("%s/runs_%s.csv", outputPath, parameters.algorithm);
            File file = new File(fileName);

            boolean exists = file.exists();

            List<String> excludedColumns = Arrays.asList(
                    "LOGGER", "data", "statBag", "saveResults",
                    "resultPath", "headers", "outputPath", "stageDurations",
                    "Wl", "Wr", "otherStats", "stopWatch", "pairwiseDistances", "randomGenerator", "miscStats", "logLevel",
                    "durations", "resultCounts", "arrivalCounts", "simulatedTimes", "violationCounts", "falseViolationCounts",
                    "experiment", "dccSizes", "dccCounts", "resultWriter", "maxT", "nResults", "totalIndexedDCCsSize", "nIndexedDCCs",
                    "lbIndex", "ubIndex", "arrivalQueue", "time", "basicWindowAggMethod", "timeSeries", "warmupStop", "forkJoinPool"
            );

            FileWriter resultWriter = new FileWriter(fileName, true);

            parameters.LOGGER.info("saving stats to " + fileName);

//            Parameter fields
            List<Field> paramFields = Arrays.stream(parameters.getClass().getDeclaredFields()) // get all attributes of parameter class
                    .filter(field -> !excludedColumns.contains(field.getName()))
                    .collect(Collectors.toList());

//            Also get statbag fields
            List<Field> statBagFields = Arrays.stream(this.getClass().getDeclaredFields()) // get all attributes of statbag class
                    .filter(field -> !excludedColumns.contains(field.getName()))
                    .collect(Collectors.toList());


//            Create k-v store for all fields
            HashMap<String, Object> fieldMap = new HashMap<>();
            paramFields.forEach(field -> {
                try {
                    fieldMap.put(field.getName(), field.get(parameters));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            });
            statBagFields.forEach(field -> {
                try {
                    fieldMap.put(field.getName(), field.get(this));
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
            });
            fieldMap.putAll(miscStats);

//            Add stagedurations as nested list
            String sdString = this.stageDurations.stream().map(st -> String.valueOf(st.getDuration())).collect(Collectors.joining("-"));
            fieldMap.put("stageDurations", sdString);

            List<String> keys = new ArrayList<>();
            List<Object> values = new ArrayList<>();

//            Get keys and values in sorted order of keys
            fieldMap.keySet().stream().sorted(String.CASE_INSENSITIVE_ORDER::compare).forEach(key -> {
                keys.add(key);
                values.add(fieldMap.get(key));
            });

//            Create header
            if (!exists) {
                String header = String.join(",", keys);
                resultWriter.write(header + "\n");
            }

//            Create row
            String row = values.stream().filter(Objects::nonNull).map(Object::toString).collect(Collectors.joining(","));
            resultWriter.write(row + "\n");
            resultWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

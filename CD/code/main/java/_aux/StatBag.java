package _aux;

import _aux.lists.FastArrayList;
import _aux.lists.FastLinkedList;
import core.Parameters;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.time.StopWatch;

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class StatBag {
    public StopWatch stopWatch = new StopWatch();
    public double totalDuration;
    public long nResults;
    public FastLinkedList<Stage> stageDurations = new FastLinkedList<Stage>();

//    Determines if other stats are actually measures
    @NonNull private boolean experiment;

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

//    Only computed if experiment is true
    public void incrementStat(AtomicLong stat){
        if (!experiment) stat.incrementAndGet();
    }

//    Only computed if experiment is true
    public void addStat(AtomicLong stat, Supplier<Integer> value){
        if (!experiment) stat.addAndGet(value.get());
    }

    public void saveStats(Parameters parameters){

        String outputPath = parameters.outputPath;

        try {
            new File(outputPath).mkdirs();

            String fileName = String.format("%s/runs_%s.csv", outputPath, parameters.algorithm);
            File file = new File(fileName);

            boolean exists = file.exists();

            List<String> excludedColumns = Arrays.asList(
                    "LOGGER", "data", "statBag", "saveResults",
                    "resultPath", "headers", "outputPath", "stageDurations",
                    "Wl", "Wr", "otherStats", "stopWatch", "pairwiseDistances", "randomGenerator"
            );

            FileWriter resultWriter = new FileWriter(fileName, true);

            parameters.LOGGER.info("saving to " + fileName);

//            Parameter fields
            List<Field> paramFields = Arrays.stream(parameters.getClass().getDeclaredFields()) // get all attributes of parameter class
                    .filter(field -> !excludedColumns.contains(field.getName()))
                    .collect(Collectors.toList());

//            Also get statbag fields
            List<Field> statBagFields = Arrays.stream(this.getClass().getDeclaredFields()) // get all attributes of statbag class
                    .filter(field -> !excludedColumns.contains(field.getName()))
                    .collect(Collectors.toList());


//            Create k-v store for all fields
            ConcurrentHashMap<String, Object> fieldMap = new ConcurrentHashMap<>();
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

//            Add stagedurations as nested list
            String sdString = this.stageDurations.stream().map(st -> String.valueOf(st.getDuration())).collect(Collectors.joining("-"));
            fieldMap.put("stageDurations", sdString);

//            Create header
            if (!exists) {
                String header = fieldMap.keySet().stream().collect(Collectors.joining(","));
                resultWriter.write(header + "\n");
            }

//            Create row
            String row = fieldMap.values().stream().map(Object::toString).collect(Collectors.joining(","));
            resultWriter.write(row + "\n");
            resultWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

package bounding;

import clustering.Cluster;
import streaming.DCC;

import java.io.PrintWriter;
import java.io.Writer;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static _aux.lib.getStream;

public class ProgressiveApproximation {

//    public ArrayList<ClusterCombination> additionalPositives = new ArrayList<>(1000);
    double corrThreshold;
    boolean useEmpiricalBounds;
    double minJump;
    double maxApproximationSize;
    public CorrelationBounding CB;
    public List<String> additionalPositiveResultSet;
    List<String> header;
    private HierarchicalBounding HB;

    public ProgressiveApproximation(double corrThreshold, boolean useEmpriricalBounds, double minJump, double maxApproximationSize,
                                    List<String> header, CorrelationBounding CB, HierarchicalBounding HB){
        this.corrThreshold = corrThreshold;
        this.useEmpiricalBounds = useEmpriricalBounds;
        this.minJump = minJump;
        this.maxApproximationSize = maxApproximationSize;
        this.header = header;
        this.CB = CB;
        this.HB = HB;
    }

    public List<DCC> ApproximateProgressively(List<DCC> approximatedCCs, int numberOfPriorityBuckets, List<DCC> positiveDCCs,  boolean parallel, int topK){
        // first group the approximated cluster combinations by their critical shrinkfactor
        Stream<DCC> clusterCombinationStream = getStream(approximatedCCs, parallel);


        Map<Integer, List<DCC>> priorityBuckets = clusterCombinationStream.unordered()
                .collect(Collectors.groupingBy(cc -> getPriorityBucket(cc.boundTuple.criticalShrinkFactor, numberOfPriorityBuckets)));

        List<Integer> sortedKeys = new ArrayList<>(priorityBuckets.keySet());
        Collections.sort(sortedKeys);

        // now itereate over the buckets with approximated DCCs, start with highest priority ie lowest critical shrinkfactor

        for(int key = 1; key<=numberOfPriorityBuckets; key++){
            List<DCC> priorityDCCs = priorityBuckets.remove(key);


            if(priorityDCCs == null) continue; //continue to the next bucket if this one does not contain any DCCs to process

            Stream<DCC> priorityDCCStream = getStream(priorityDCCs, parallel);


            int finalKey = key;
//            Map<Integer, List<ClusterCombination>> processedDCCs = priorityDCCStream.unordered() // all priority buckets that come from here should have lower priority ie higher id then the key currently being processed
//                    .flatMap(cc -> (HierarchicalBounding.corrBounding(cc, corrThreshold, useEmpiricalBounds, parallel, minJump, Math.min((double) finalKey/numberOfPriorityBuckets, 1.0), maxApproximationSize)).stream())
//                    .collect(Collectors.groupingBy(cc -> getPriorityBucket(cc.getCriticalShrinkFactor(), numberOfPriorityBuckets)));

            List<DCC> processedDCCs = priorityDCCStream.unordered() // all priority buckets that come from here should have lower priority ie higher id then the key currently being processed
            .flatMap(cc -> (HB.corrBounding(cc.LHS, cc.RHS, 1)).stream())
            .collect(Collectors.toList());

//            List<DCC> partialAdditionalPositives = new ArrayList<>(100);

//            getStream(processedDCCs.keySet(), parallel).unordered().forEach(newKey -> { //
//                if(newKey <= -1){
//                    partialAdditionalPositives.addAll(processedDCCs.get(newKey));
//                }else if(newKey < numberOfPriorityBuckets+1){
//                    if(newKey < finalKey){
//                        System.err.println("ProgressiveAprroximation: we are missing something here");
//                    }
//                    List<DCC> bucket = priorityBuckets.get(newKey);
//                    if(bucket==null){
//                        priorityBuckets.put(newKey, processedDCCs.get(newKey));
//                    }else{
//                        bucket.addAll(processedDCCs.get(newKey));
//                    }
//                }
//            });

            Map<Boolean, List<DCC>> unpackedDCCs = HB.unpackAndCheckMinJump(processedDCCs);

            positiveDCCs.addAll(unpackedDCCs.get(true));

            if(positiveDCCs.size() > topK){
                positiveDCCs = getStream(positiveDCCs, parallel)
                        .sorted((cc1, cc2) -> Double.compare(cc2.boundTuple.lower, cc1.boundTuple.lower))
                        .limit(topK)
                        .collect(Collectors.toList());
                corrThreshold = positiveDCCs.get(positiveDCCs.size()-1).boundTuple.lower;
            }

            String topKString = "";

//            positiveResultSet = PostProcessResults.removeDuplicatesAndToString(positiveDCCs, header, CB, parallel);

            if(topKString.length()>0){
                System.out.println(topKString);
            }

        }
        return positiveDCCs;
    }


    int getPriorityBucket(double criticalShrinkFactor, int nBuckets){
        return (int) Math.ceil((Math.min(criticalShrinkFactor, 1) * nBuckets));
    }
}

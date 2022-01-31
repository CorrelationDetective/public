package bounding;

import _aux.PostProcessResults;
import clustering.Cluster;
import clustering.ClusterCombination;

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

    public ProgressiveApproximation(double corrThreshold, boolean useEmpriricalBounds, double minJump, double maxApproximationSize, List<String> header, CorrelationBounding CB){
        this.corrThreshold = corrThreshold;
        this.useEmpiricalBounds = useEmpriricalBounds;
        this.minJump = minJump;
        this.maxApproximationSize = maxApproximationSize;
        this.header = header;
        this.CB = CB;
    }

    public List<ClusterCombination> ApproximateProgressively(List<ClusterCombination> approximatedCCs, int numberOfPriorityBuckets, List<ClusterCombination> positiveDCCs, boolean parallel, int topK, long startMS){

        // first group the approximated cluster combinations by their critical shrinkfactor

        Stream<ClusterCombination> clusterCombinationStream = getStream(approximatedCCs, parallel);


        Map<Integer, List<ClusterCombination>> priorityBuckets = clusterCombinationStream.unordered()
                .collect(Collectors.groupingBy(cc -> getPriorityBucket(cc.getCriticalShrinkFactor(), numberOfPriorityBuckets)));

        List<Integer> sortedKeys = new ArrayList<>(priorityBuckets.keySet());
        Collections.sort(sortedKeys);

        // now itereate over the buckets with approximated DCCs, start with highest priority ie lowest critical shrinkfactor



        for(int key = 1; key<=numberOfPriorityBuckets; key++){


            List<ClusterCombination> priorityDCCs = priorityBuckets.remove(key);


            if(priorityDCCs == null) continue; //continue to the next bucket if this one does not contain any DCCs to process

            Stream<ClusterCombination> priorityDCCStream = getStream(priorityDCCs, parallel);


            int finalKey = key;
//            Map<Integer, List<ClusterCombination>> processedDCCs = priorityDCCStream.unordered() // all priority buckets that come from here should have lower priority ie higher id then the key currently being processed
//                    .flatMap(cc -> (HierarchicalBounding.corrBounding(cc, corrThreshold, useEmpiricalBounds, parallel, minJump, Math.min((double) finalKey/numberOfPriorityBuckets, 1.0), maxApproximationSize)).stream())
//                    .collect(Collectors.groupingBy(cc -> getPriorityBucket(cc.getCriticalShrinkFactor(), numberOfPriorityBuckets)));
//
            Map<Integer, List<ClusterCombination>> processedDCCs = priorityDCCStream.unordered() // all priority buckets that come from here should have lower priority ie higher id then the key currently being processed
            .flatMap(cc -> (HierarchicalBounding.corrBounding(cc, corrThreshold, useEmpiricalBounds, parallel, minJump, 1, maxApproximationSize)).stream())
            .collect(Collectors.groupingBy(cc -> getPriorityBucket(cc.getCriticalShrinkFactor(), numberOfPriorityBuckets)));

            List<ClusterCombination> partialAdditionalPositives = new ArrayList<>(100);

            getStream(processedDCCs.keySet(), parallel).unordered().forEach(newKey -> { //
                if(newKey <= -1){
                    partialAdditionalPositives.addAll(processedDCCs.get(newKey));
                }else if(newKey < numberOfPriorityBuckets+1){
                    if(newKey < finalKey){
                        System.err.println("ProgressiveAprroximation: we are missing something here");
                    }
                    List<ClusterCombination> bucket = priorityBuckets.get(newKey);
                    if(bucket==null){
                        priorityBuckets.put(newKey, processedDCCs.get(newKey));
                    }else{
                        bucket.addAll(processedDCCs.get(newKey));
                    }
                }
            });

            positiveDCCs.addAll(PostProcessResults.unpackAndCheckMinJump(partialAdditionalPositives, CB, minJump, parallel));

            String topKString = "";

            if(positiveDCCs.size() > topK){
                positiveDCCs = getStream(positiveDCCs, parallel)
                        .sorted((cc1, cc2) -> Double.compare(cc2.getLB(), cc1.getLB()))
                        .limit(topK)
                        .collect(Collectors.toList());
                corrThreshold = positiveDCCs.get(positiveDCCs.size()-1).getLB();
                topKString = topKString + "(Tau updated to " + corrThreshold +")";
            }

//            positiveResultSet = PostProcessResults.removeDuplicatesAndToString(positiveDCCs, header, CB, parallel);

            System.out.println("found " + positiveDCCs.size() +" positives at time " + LocalTime.now() +". Runtime so far: " + (System.currentTimeMillis()-startMS)/1000 + " seconds. Bucket " + key + "/" + numberOfPriorityBuckets + " contained " + priorityDCCs.size() +" CCs that were processed.");
            if(topKString.length()>0){
                System.out.println(topKString);
            }

        }

        return positiveDCCs;



    }


    int getPriorityBucket(double criticalShrinkFactor, int nBuckets){
        return (int) Math.ceil((Math.min(criticalShrinkFactor, 1) * nBuckets));
    }

    int getNumDCCsOfBucketList(List<List<ClusterCombination>> bucketList){
        int out = 0;
        for(List<ClusterCombination> bucket : bucketList){
            out += bucket.size();
        }
        return out;
    }

    public void testApproximationQuality(List<ClusterCombination> approximatedCCs, int numberOfPriorityBuckets, int p, boolean parallel){

        // first group the approximated cluster combinations by their critical shrinkfactor

        Stream<ClusterCombination> clusterCombinationStream = getStream(approximatedCCs, parallel);


        Map<Integer, List<ClusterCombination>> priorityBuckets = clusterCombinationStream.unordered()
                .collect(Collectors.groupingBy(cc -> getPriorityBucket(cc.getCriticalShrinkFactor(), numberOfPriorityBuckets)));

        List<Integer> sortedKeys = new ArrayList<>(priorityBuckets.keySet());
        Collections.sort(sortedKeys);

        // now itereate over the buckets with approximated DCCs, start with highest priority ie lowest critical shrinkfactor

        List<List<double[]>> data = new ArrayList<>();

        for(int key : sortedKeys){
            List<ClusterCombination> bucket = priorityBuckets.get(key);

            List<double[]> corrsAndThresholds = getStream(bucket, parallel)
                    .flatMap(cc -> cc.getSingletonClusters().stream())
                    .map(cc ->{
                        CB.calcBound(cc, true);
                        double corr = cc.getLB();
                        double thresh = Math.max(cc.getMaxSubsetCorr(CB) + minJump, corrThreshold);

                        double[] out = new double[2];
                        out[0] = corr; out[1] = thresh;
                        return out;
                    })
                    .collect(Collectors.toList());

            data.add(corrsAndThresholds);

        }

        try{
            PrintWriter writer = new PrintWriter("approximation_test" + p + ".txt");

            for(int i =0; i< data.size(); i++){
                writer.write("bucket " + i + "\n");
                List<double[]> b = data.get(i);
                for(double[] l : b){
                    writer.write(l[0] + ", " + l[1] + "\n");
                }
            }

            writer.close();
        } catch (Exception e){
            e.printStackTrace();
        }




    }

}

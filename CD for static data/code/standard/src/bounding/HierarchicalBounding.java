package bounding;

import _aux.PostProcessResults;
import _aux.ResultsTuple;
//import _aux.lib;
import clustering.*;

//import java.lang.reflect.Array;
//import java.time.LocalDateTime;
//import java.time.LocalTime;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import _aux.lib.*;

import static _aux.lib.getStream;
import static java.lang.Math.E;
import static java.lang.Math.min;


public class HierarchicalBounding {

    public static CorrelationBounding CB;
    public int defaultDesiredClusters;

    public double[][] data;
    public int n_dim;
    public int n_vec;
    public ArrayList<ArrayList<Cluster>> clustersPerLevel;
    public ArrayList<Cluster> allclusters;

    public double n_comparisons;
    public double n_2bound_comparisons;
    public double n_3bound_comparisons;
    public long n_2corr_lookups;

    //    public long[] comp_saved_per_level; // 3-bounds
    public double n_positive;
    public ArrayList<int[]> positives;
    public ArrayList<ResultsTuple> resultsTuples;

//    public HashMap<Integer, Integer> combinedSizesFreq = new HashMap<>();
//    public HashMap<Integer, Integer> combinedLevelsFreq = new HashMap<>();
//    public HashMap<Integer, Integer> combinedLevelsFreqComparisons = new HashMap<>();
    public double unavoidableComparisons = 0;


    // only for 3-bounds:
    public double n_decided;
    public double n_undecided;

    public int maxLevels;

    int globalClustID = 0;

    boolean useKMeans;
    int breakFirstKLevelsToMoreClusters;
    int clusteringRetries;
    public ArrayList<ClusterCombination> unCorrelated;
    public List<ClusterCombination> positiveDCCs = new ArrayList<>(1000);
    public List<String> positiveResultSet;
    List<String> header;
    ProgressiveApproximation PA;
    boolean parallel;


    public HierarchicalBounding(double[][] data, int maxLevels, int defaultDesiredClusters, boolean useKMeans, int breakFirstKLevelsToMoreClusters, int clusteringRetries, boolean parallel, List<String> header) {
        this.data = data;
        this.n_dim = data[0].length;
        this.n_vec = data.length;
        this.clustersPerLevel = new ArrayList<>();
        for (int i = 0; i <= maxLevels + 1; i++) {
            this.clustersPerLevel.add(new ArrayList<>());
        }
        this.useKMeans=useKMeans;
        this.breakFirstKLevelsToMoreClusters=breakFirstKLevelsToMoreClusters;
        this.clusteringRetries=clusteringRetries;

        this.allclusters = new ArrayList<>(20000);
//        this.n_2bound_comparisons = 0;
//        this.n_3bound_comparisons = 0;
//        this.n_2corr_lookups = 0;
        this.maxLevels = maxLevels;
        this.n_decided = 0;
        this.n_undecided = 0;
        this.n_positive = 0;
        this.positives = new ArrayList<>();
        this.resultsTuples = new ArrayList<>();
        this.defaultDesiredClusters = defaultDesiredClusters;
        this.unCorrelated = new ArrayList<>();
        this.header=header;
        this.parallel = parallel;



    }

    public void recursiveBounding(double startEpsilon, double corrThreshold, double epsilonMultiplier, boolean useEmpiricalBounds,
                                  double minJump, double shrinkFactor, double maxApproximationSize, int numBuckets, int topK,
                                  int maxPLeft, int maxPRight,  long startMS) {
        this.CB = new CorrelationBounding(data, useEmpiricalBounds, parallel);
        // get clusters:
        long startClustering = System.currentTimeMillis();
        RecursiveClustering RC = new RecursiveClustering(data, CB, maxLevels, defaultDesiredClusters, useKMeans, breakFirstKLevelsToMoreClusters, clusteringRetries);
        RC.fitRecursiveClusters(startEpsilon, epsilonMultiplier);
        long stopClustering = System.currentTimeMillis();
        System.out.println("Clustering takes " + (stopClustering-startClustering)/1000);
        this.allclusters = RC.allClusters;
        this.clustersPerLevel = RC.clustersPerLevel;
        this.globalClustID = RC.globalClustID;
        List<Cluster> topLevelClusters = this.clustersPerLevel.get(0);

        CB.initializeBoundsCache(globalClustID, useEmpiricalBounds);

        PA = new ProgressiveApproximation(corrThreshold, useEmpiricalBounds, minJump, maxApproximationSize, header, CB);

        // recursive bounding:


        Map<Boolean, List<ClusterCombination>> DCCs;

        //first do 2-correlations -> so quick and can help in case of topK to increase corrThreshold
        ArrayList<Cluster> rootLeft = new ArrayList(); rootLeft.add(this.clustersPerLevel.get(0).get(0));
        ArrayList<Cluster> rootRight = new ArrayList(); rootRight.add(this.clustersPerLevel.get(0).get(0));
        ClusterCombination PairwiseRootCandidate = new MultiPearsonClusterCombination(rootLeft, rootRight);

        List<ClusterCombination> pairwiseDCCs = corrBounding(PairwiseRootCandidate, corrThreshold, useEmpiricalBounds, parallel, minJump, 1, maxApproximationSize);
        List<ClusterCombination> pairwisePostives = getStream(pairwiseDCCs, parallel).collect(Collectors.partitioningBy(ClusterCombination::isPositive)).get(true);
        this.positiveDCCs.addAll(PostProcessResults.unpackAndCheckMinJump(pairwisePostives, CB, minJump, parallel));
        if(positiveDCCs.size() > topK){
            positiveDCCs = getStream(positiveDCCs, parallel)
                    .sorted((cc1, cc2) -> Double.compare(cc2.getLB(), cc1.getLB()))
                    .limit(topK)
                    .collect(Collectors.toList());
            PA.corrThreshold = positiveDCCs.get(positiveDCCs.size()-1).getLB();
            corrThreshold = PA.corrThreshold;

        }
        System.out.println("correlation threshold updated to " + corrThreshold + " after pairwise correlations have been processed. Number of positives so far: " + positiveDCCs.size());

//         now start with multivariate correlations //////////////////////////

        for(int print = 0; print<topLevelClusters.size(); print++){ // progress bar
            System.out.print(".");
        }
        System.out.println();


        double finalCorrThreshold = corrThreshold;
        rootLeft = new ArrayList<>();
        rootRight = new ArrayList<>();
        Cluster rootCluster = clustersPerLevel.get(0).get(0);
        for(int i = 0; i<maxPLeft; i++){
            rootLeft.add(rootCluster);
        }
        for(int i = 0; i<maxPRight; i++){
            rootRight.add(rootCluster);
        }

        ClusterCombination rootCandidate = new MultiPearsonClusterCombination(rootLeft, rootRight);
        List<ClusterCombination> rootCandidateList = new ArrayList<>(); rootCandidateList.add(rootCandidate);

        DCCs = getStream(rootCandidateList, parallel).unordered()
                .flatMap(cc -> getStream(corrBounding(cc, finalCorrThreshold, useEmpiricalBounds, parallel, minJump, shrinkFactor, maxApproximationSize), parallel))
                .filter(dcc -> dcc.getCriticalShrinkFactor() <= 1)
                .collect(Collectors.partitioningBy(ClusterCombination::isPositive));
//        DCCs = stream.unordered()
//                .mapToObj(i1 -> {
//                    ArrayList<ClusterCombination> partial = new ArrayList<>(100);
//                    Cluster c1 = topLevelClusters.get(i1);
//                    for(int i2=0; i2<topLevelClusters.size(); i2++) {
//                        Cluster c2 = topLevelClusters.get(i2);
//                        for (int i3 = i2; i3 < topLevelClusters.size(); i3++) {
//                            Cluster c3 = topLevelClusters.get(i3);
//                            for(int i4 = i3; i4<topLevelClusters.size(); i4++) {
//                                Cluster c4 = topLevelClusters.get(i4);
//                                ArrayList<Cluster> LHS = new ArrayList<>();
//                                ArrayList<Cluster> RHS = new ArrayList<>();
//                                LHS.add(c1);
//                                RHS.add(c2);
//                                RHS.add(c3);
//                                RHS.add(c4);
//                                ClusterCombination CC = new MultiPearsonClusterCombination(LHS, RHS); // root CC
//
////                                    ArrayList<Cluster> clusters = new ArrayList<>();
////                                    clusters.add(c1); clusters.add(c2); clusters.add(c3); clusters.add(c4);
////
////                                    MultipoleClusterCombination CC = new MultipoleClusterCombination(clusters);
//
//                                List<ClusterCombination> partialDCCs = corrBounding(CC, finalCorrThreshold, useEmpiricalBounds, parallel, minJump,shrinkFactor,maxApproximationSize);
//                                for(ClusterCombination dcc : partialDCCs){
//                                    if(dcc.getCriticalShrinkFactor() <= 1){
//                                        partial.add(dcc);
//                                    }
//                                }
//
//                            }
//
//                        }
//                    }
//                    System.out.print(".");
//                    return partial;
//                })
//                .flatMap(Collection::stream)
//                .collect(Collectors.partitioningBy(ClusterCombination::isPositive));
        System.out.println();
        System.out.println("initial phase with shrink UB finished at time " + LocalTime.now() +". Runtime so far: " + (System.currentTimeMillis() - startMS)/1000 + " seconds.");


        this.positiveDCCs.addAll(PostProcessResults.unpackAndCheckMinJump(DCCs.get(true), CB, minJump, parallel));
        if(positiveDCCs.size() > topK){
            positiveDCCs = getStream(positiveDCCs, parallel)
                    .sorted((cc1, cc2) -> Double.compare(cc2.getLB(), cc1.getLB()))
                    .limit(topK)
                    .collect(Collectors.toList());
            PA.corrThreshold = positiveDCCs.get(positiveDCCs.size()-1).getLB();
            corrThreshold = PA.corrThreshold;
            System.out.println("tau updated to " + corrThreshold);
        }


        System.out.println("positives so far: " + this.positiveDCCs.size());


        List<ClusterCombination> approximatedCCs = DCCs.get(false);
        this.positiveDCCs = PA.ApproximateProgressively(approximatedCCs, numBuckets, positiveDCCs, parallel, topK, startMS);
        this.positiveDCCs = PostProcessResults.unpackAndCheckMinJump(this.positiveDCCs, CB, minJump, parallel);
        this.positiveResultSet = PostProcessResults.removeDuplicatesAndToString(this.positiveDCCs, header, CB, parallel);


        System.out.println("number of positives: " + this.positiveResultSet.size() + " at time: " + LocalTime.now() +". Runtime: " + (System.currentTimeMillis() - startMS)/1000 +" seconds");


    }



    public static List<ClusterCombination> corrBounding(ClusterCombination CC, double corrThreshold, boolean useEmpiricalBounds, boolean parallel, double minJump, double shrinkFactor, double maxApproximationSize){
        // now calculate the 3-corr bounds for all combinations
        ArrayList<ClusterCombination> out = new ArrayList<>();



        if (true) {


            CB.calcBound(CC, useEmpiricalBounds);

            double LB = CC.getLB();
            double shrunkUB = CC.getShrunkUB(shrinkFactor, maxApproximationSize);

            double jumpBasedThreshold = CC.getMaxLowerBoundSubset() + minJump;
            double newThreshold;
            if(CC.getClusters().size() >2){
                newThreshold = Math.max(jumpBasedThreshold, corrThreshold);
            }else{
                newThreshold = corrThreshold;
            }

//                newThreshold = corrThreshold;

            // check if we need higher resolution clustering:
            if ((LB < newThreshold) && (shrunkUB >= newThreshold)) { // we need more precise clusters to be conclusive, it is possible that we are highly correlated
                CC.setDecisive(false);

                ArrayList<ClusterCombination> newCandidates = CC.getSplittedCC();

                return getStream(newCandidates, parallel).unordered()
                        .flatMap(newCC -> corrBounding(newCC, corrThreshold, useEmpiricalBounds, parallel, minJump, shrinkFactor, maxApproximationSize).stream())
                        .collect(Collectors.toList());
                ////  https://tinyurl.com/4tj9cz2u  ////



            } else {


                CC.setDecisive(true);

//                this.n_decided += CC.getNComparisons();

                if(shrunkUB < newThreshold) { // negative DCC
                    CC.setPositive(false);
//                    out.add(CC);

//                    if(CC.isMultipoleCandidate() && newThreshold <= 1){
//                        out.add(CC);
//                    }

                    double criticalShrinkFactor;
                    if(CC.getSlack() > 0 && newThreshold <= 1){ //
                        criticalShrinkFactor = (newThreshold - CC.getCenterOfBounds())/CC.getSlack();
                        if(criticalShrinkFactor < shrinkFactor){
                            System.err.println("HB: this should not happen!");
                        }
                    }else{
                        criticalShrinkFactor = 10; // any number >1 will do; point is that we do not want to investigate further since this CC can never become positive
                    }
                    CC.setCriticalShrinkFactor(criticalShrinkFactor);
                    if(criticalShrinkFactor <= 1 && newThreshold <= 1){
                        out.add(CC);
                    }
                }else if(LB >=newThreshold){ //positive DCC
//                        this.n_positive += CC.getNComparisons();
                    CC.setPositive(true);
                    CC.setCriticalShrinkFactor(-10);
                    out.add(CC);
                }




                // some code used in the past for statistics, don't bother for now, but might be useful lateron

//                System.out.println("decision for clusters!");
                // we can draw a conclusion using these bounds
//                System.err.println("Combined levels " + combinedLevels);
//                combinedLevelsFreq.merge(combinedLevels, 1, Integer::sum);
//                double n_comb = countNComparisons(LHS, RHS);
////                unavoidableComparisons++;
//////                combinedSizesFreq.merge((int) n_comb, 1, Integer::sum);
////
//                this.n_decided += n_comb;
////
//                if (lowerBoundP >= corrThreshold) { // check results and add positive
//                    this.n_positive += n_comb;
////                    for(int ID1 : c1.listOfContents){
////                        for(int ID2: c2.listOfContents){
////                            for(int ID3:c3.listOfContents){
////                                double corr_1_2 = lib.pearsonWithAlreadyNormedVectors(data[ID1], data[ID2]);
////                                double corr_1_3 = lib.pearsonWithAlreadyNormedVectors(data[ID1], data[ID3]);
////                                double corr_2_3 = lib.pearsonWithAlreadyNormedVectors(data[ID2], data[ID3]);
////                                double triplet_corr = lib.pearsonWithAlreadyNormedVectors(data[ID1], lib.znorm(lib.add(data[ID2], data[ID3])));
////                                if(triplet_corr >= Math.max(Math.max(corr_1_2, corr_1_3), corr_2_3) + minJump){
////                                    this.positives.add(new int[]{ID1, ID2, ID3});
////
////                                    this.resultsTuples.add(new ResultsTuple(new int[]{ID1, ID2, ID3},
////                                            lowerBound3, triplet_corr,
////                                            "Decided "+ n_comb + " in one go, clusters " +
////                                                    c1.getClusterId() + "(" +
////                                                    c1.listOfContents.size() + ") - " +
////                                                    c2.getClusterId() + "(" +
////                                                    c2.listOfContents.size() + ") - " +
////                                                    c3.getClusterId() + "(" +
////                                                    c3.listOfContents.size() + ")"));
////                                }
////                            }
////                        }
////                    }
//                }
            }
        } else {  // we do not need to do this comparison because we will get it at some other point during the calculation, when cID2 and cID3 are switched
            //let's still add these comparisons to the #considered for easier verification
//            this.n_decided += CC.getNComparisons();
        }

        return out;
    }





// ---------------------------------------------------------------------------------------------------------------------
// ---------------------------------------------------------------------------------------------------------------------

// ---------------------------------------------------  archive:  ------------------------------------------------------

// ---------------------------------------------------------------------------------------------------------------------
// ---------------------------------------------------------------------------------------------------------------------












//    public void corrBoundingMultiPearson(ArrayList<Cluster> LHS, ArrayList<Cluster> RHS, double corrThreshold, boolean useEmpiricalBounds, double minJump) {
//        // now calculate the 3-corr bounds for all combinations
//
//        boolean doCalculation = true;
//
//
//        for(int i=1; i<LHS.size(); i++){
//            if(!doCalculation){
//                break;
//            }
//            Cluster c1 = LHS.get(i-1);
//            Cluster c2 = LHS.get(i);
//            if(c1.getClusterId() > c2.getClusterId()){ // skip the calculation as we will encounter this case flipped some other time
//                doCalculation=false;
//            }
//        }
//
//        for(int i=1; i<RHS.size(); i++){
//            if(!doCalculation){
//                break;
//            }
//            Cluster c1 = RHS.get(i-1);
//            Cluster c2 = RHS.get(i);
//            if(c1.getClusterId() > c2.getClusterId()){ // skip the calculation as we will encounter this case flipped some other time
//                doCalculation=false;
//            }
//        }
//
//
//        if (doCalculation) {
////            int combinedLevels = c1.getLevel() + c2.getLevel() + c3.getLevel();
////            combinedLevelsFreqComparisons.merge(combinedLevels, 1, Integer::sum);
//            double lowerBoundP = -1;
//            double upperBoundP = +1;
//            double newThreshold = 0;
//            //    if (true || combinedLevels>2 || !c1.hasChildren() || !c2.hasChildren() || !c3.hasChildren())
//            {  // always execute this -- do not skip the first level
//                double[] boundsP = CB.getBound(LHS, RHS, useEmpiricalBounds);
//                lowerBoundP = boundsP[0];
//                upperBoundP = boundsP[1];
////                double jumpBasedThreshold = boundsP[2];
////                if (jumpBasedThreshold < 1) {
////                    this.n_3bound_comparisons++;
////                }
//
////            System.out.println("theoretical 3-bounds: " +lowerBound3 + " "+ upperBound3 + " empirical: " + empiricalLowerBound3 + " " + empiricalUpperBound3);
//
//                // check if we need higher resolution clustering:
////                newThreshold = Math.max(jumpBasedThreshold, corrThreshold);
//                newThreshold = corrThreshold;
//            }
//
//            if ((lowerBoundP < newThreshold && upperBoundP >= newThreshold)) { // we need more precise clusters to be conclusive, it is possible that we are highly correlated
//                int cToBreak = getClusterToBreakMultiPearson(LHS, RHS);
//
//                if(cToBreak < LHS.size()){ // break down on the lhs
//                    ArrayList<Cluster> newLHS = new ArrayList<Cluster>(LHS);
//                    Cluster drillDownCLuster = LHS.get(cToBreak);
//                    newLHS.remove(cToBreak);
//                    for(Cluster sc: drillDownCLuster.getSubClusters()){
//                        newLHS.add(cToBreak, sc);
//                        corrBoundingMultiPearson(newLHS, RHS, corrThreshold, useEmpiricalBounds, minJump);
//                        newLHS.remove(cToBreak);
//                    }
//
//                }else{
//                    cToBreak = cToBreak-LHS.size();
//                    ArrayList<Cluster> newRHS = new ArrayList<Cluster>(RHS);
//                    Cluster drillDownCLuster = RHS.get(cToBreak);
//                    newRHS.remove(cToBreak);
//                    for(Cluster sc: drillDownCLuster.getSubClusters()){
//                        newRHS.add(cToBreak, sc);
//                        corrBoundingMultiPearson(LHS, newRHS, corrThreshold, useEmpiricalBounds, minJump);
//                        newRHS.remove(cToBreak);
//                    }
//                }
//
//
//
//            } else { // we can draw a conclusion using these bounds
////                System.err.println("Combined levels " + combinedLevels);
////                combinedLevelsFreq.merge(combinedLevels, 1, Integer::sum);
////                double n_comb = countNComparisons(LHS, RHS);
//////                unavoidableComparisons++;
////////                combinedSizesFreq.merge((int) n_comb, 1, Integer::sum);
//////
////                this.n_decided += n_comb;
//////
////                if (lowerBoundP >= corrThreshold) { // check results and add positive
////                    this.n_positive += n_comb;
//////                    for(int ID1 : c1.listOfContents){
//////                        for(int ID2: c2.listOfContents){
//////                            for(int ID3:c3.listOfContents){
//////                                double corr_1_2 = lib.pearsonWithAlreadyNormedVectors(data[ID1], data[ID2]);
//////                                double corr_1_3 = lib.pearsonWithAlreadyNormedVectors(data[ID1], data[ID3]);
//////                                double corr_2_3 = lib.pearsonWithAlreadyNormedVectors(data[ID2], data[ID3]);
//////                                double triplet_corr = lib.pearsonWithAlreadyNormedVectors(data[ID1], lib.znorm(lib.add(data[ID2], data[ID3])));
//////                                if(triplet_corr >= Math.max(Math.max(corr_1_2, corr_1_3), corr_2_3) + minJump){
//////                                    this.positives.add(new int[]{ID1, ID2, ID3});
//////
//////                                    this.resultsTuples.add(new ResultsTuple(new int[]{ID1, ID2, ID3},
//////                                            lowerBound3, triplet_corr,
//////                                            "Decided "+ n_comb + " in one go, clusters " +
//////                                                    c1.getClusterId() + "(" +
//////                                                    c1.listOfContents.size() + ") - " +
//////                                                    c2.getClusterId() + "(" +
//////                                                    c2.listOfContents.size() + ") - " +
//////                                                    c3.getClusterId() + "(" +
//////                                                    c3.listOfContents.size() + ")"));
//////                                }
//////                            }
//////                        }
//////                    }
////                }
//            }
//        }  // we do not need to do this comparison because we will get it at some other point during the calculation, when cID2 and cID3 are switched
//    }



//    public int getClusterToBreakMultipoles(ArrayList<Cluster> clusters) {
//        return getClusterToBreakChooseLarger(clusters);
//    }
//
//
//    public int getClusterToBreakChooseLarger(ArrayList<Cluster> clusters) { // best
//        // break cluster with max diameter that is still breakable:
//
//
//        double maxRadius = -1;
//        int idToBreak=0;
//        for(int i=0; i< clusters.size(); i++){
//            Cluster c = clusters.get(i);
//            double maxDist = c.getMaxDist();
//            if(maxDist >= maxRadius){ // >= because we need to break the last cluster first in case of identical clusters
//                idToBreak = i;
//                maxRadius = maxDist;
//            }
//        }
//        return idToBreak;
//    }
//
//    public int getClusterToBreakMultiPearson(ArrayList<Cluster> LHS, ArrayList<Cluster> RHS) {
//        return getClusterToBreakChooseLarger(LHS, RHS);
//    }
//
//
//    public int getClusterToBreakChooseLarger(ArrayList<Cluster> LHS, ArrayList<Cluster> RHS) { // best
//        // break cluster with max diameter that is still breakable:
//
//        ArrayList<Cluster> allClusters = new ArrayList<>();
//        allClusters.addAll(LHS); allClusters.addAll(RHS);
//        double maxRadius = -1;
//        int idToBreak=0;
//        for(int i=0; i< allClusters.size(); i++){
//            Cluster c = allClusters.get(i);
//            double maxDist = c.getMaxDist();
//            if(maxDist >= maxRadius){ // >= because we need to break the last cluster first in case of identical clusters
//                idToBreak = i;
//                maxRadius = maxDist;
//            }
//        }
//        return idToBreak;
//    }


//    public double countNComparisons(ArrayList<Cluster> LHS, ArrayList<Cluster> RHS) {
//
//
//        Cluster c1 = LHS.get(0);
//        Cluster c2 = RHS.get(0);
//        Cluster c3 = RHS.get(1);
//        ArrayList<Integer> pIDs1 = c1.listOfContents;
//        ArrayList<Integer> pIDs2 = c2.listOfContents;
//        ArrayList<Integer> pIDs3 = c3.listOfContents;
//        if (c2.getClusterId() == c3.getClusterId()) {// c2==c3
//            return 0.5 * pIDs1.size() * pIDs2.size() * (pIDs3.size() - 1);
//        } else { // c2!=c3, but it might be a subcluster
//            int n_overlap = 0;
//            for (int id2 : pIDs2) {
//                for (int id3 : pIDs3) {
//
//                    if (id2 == id3) {
//                        n_overlap++;
//                    }
//                }
//            }
//
//
//            int n_unique = pIDs2.size() - n_overlap;
//            if (pIDs3.size() > pIDs2.size() && n_overlap > 0) {
//                System.out.println("we made a mistake");
//            }
//
//            double result = 0.5 * pIDs1.size() * n_overlap * (n_overlap - 1) + pIDs1.size() * n_unique * pIDs3.size();
//            if (result <= 0) {
//                System.err.println("debug: less than 1 comparison");
//            }
//
//
//            return result;
//        }
//    }


//    public int getClusterToBreakChooseLargerSize(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) { // best
//        // break cluster with max diameter that is still breakable:
//        if (c1.listOfContents.size() > c2.listOfContents.size() && c1.listOfContents.size() > c3.listOfContents.size()) { // c1 has the largest diam
//            return 1;
//        } else if (c2.listOfContents.size() > c3.listOfContents.size()) { // c2 is the largest (note that because first if is not true, c2.maxDist >= c1.maxDist)
//            return 2;
//        } else { // c3.maxDist >= c1.maxDist && c3.maxDist >= c2.maxDist
//            return 3;
//        }
//    }
//
//    public int getClusterToBreakChooseLargerTimesSize(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) { // bad
//        // break cluster with max diameter that is still breakable:
//        if (c1.getMaxDist() * c1.listOfContents.size() > c2.getMaxDist() * c2.listOfContents.size() && c1.getMaxDist() * c1.listOfContents.size() > c3.getMaxDist() * c3.listOfContents.size()) { // c1 has the largest diam
//            return 1;
//        } else if (c2.getMaxDist() * c2.listOfContents.size() > c3.getMaxDist() * c3.listOfContents.size()) { // c2 is the largest (note that because first if is not true, c2.maxDist >= c1.maxDist)
//            return 2;
//        } else { // c3.maxDist >= c1.maxDist && c3.maxDist >= c2.maxDist
//            return 3;
//        }
//    }
//
//    public int getClusterToBreakChooseA(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) { // not very bad, but worse than larger
//        if (c1.getMaxDist() > 0)
//            return 1;
//        else {
//            if (c2.getMaxDist() > c3.getMaxDist()) { // c2 is the largest (note that because first if is not true, c2.maxDist >= c1.maxDist)
//                return 2;
//            } else { // c3.maxDist >= c1.maxDist && c3.maxDist >= c2.maxDist
//                return 3;
//            }
//        }
//    }
//
//    public int getClusterToBreakChooseLargerFromBC(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) { // very bad
//        if (c2.getMaxDist() == 0 && c3.getMaxDist() == 0)
//            return 1;
//        else {
//            if (c2.getMaxDist() > c3.getMaxDist()) { // c2 is the largest (note that because first if is not true, c2.maxDist >= c1.maxDist)
//                return 2;
//            } else { // c3.maxDist >= c1.maxDist && c3.maxDist >= c2.maxDist
//                return 3;
//            }
//        }
//    }
//
//    public int getClusterToBreakChooseLargerAvg(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) {
//        // break cluster with max getAvgDist that is still breakable:
//        if (c1.getAvgDist() > c2.getAvgDist() && c1.getAvgDist() > c3.getAvgDist()) { // c1 has the largest diam
//            return 1;
//        } else if (c2.getAvgDist() > c3.getAvgDist()) { // c2 is the largest (note that because first if is not true, c2.maxDist >= c1.maxDist)
//            return 2;
//        } else { // c3.maxDist >= c1.maxDist && c3.maxDist >= c2.maxDist
//            return 3;
//        }
//    }
//
//
//    public int getClusterToBreakBasedOnTheoreticalBounds(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) {
//        return -1;
//    }
//
//    public int getClusterToBreakChooseSmaller(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) {
//        int[] sizes = new int[]{c1.listOfContents.size(), c2.listOfContents.size(), c3.listOfContents.size()};
//        double[] dists = new double[]{c1.getMaxDist(), c2.getMaxDist(), c3.getMaxDist()};
//        ArrayList<Integer> cands = new ArrayList<>(3);
//        if (sizes[0] > 1) cands.add(0);
//        if (sizes[1] > 1) cands.add(1);
//        if (sizes[2] > 1) cands.add(2);
//        int bestClusterToBreak = -1;
//        double bestDistance = Double.MAX_VALUE;
//        for (int i : cands) {
//            if (dists[i] < bestDistance) {
//                bestClusterToBreak = i;
//                bestDistance = dists[i];
//            }
//        }
//        return bestClusterToBreak + 1;
//    }
//
//
//    public int getClusterToBreakChooseLargerDifferenceWithChildrenSum(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) { // best
//        // break cluster with max diameter that is still breakable:
//        double maxDistChild1 = 0, maxDistChild2 = 0, maxDistChild3 = 0;
//        if (c1.hasChildren()) {
//            for (Cluster child : c1.getSubClusters()) {
//                maxDistChild1 += child.getMaxDist();
//            }
//            maxDistChild1 = c1.getMaxDist() - maxDistChild1 / c1.getSubClusters().size();
//        } else
//            maxDistChild1 = -Double.MAX_VALUE;
//        if (c2.hasChildren()) {
//            for (Cluster child : c2.getSubClusters()) {
//                maxDistChild2 += child.getMaxDist();
//            }
//            maxDistChild2 = c2.getMaxDist() - maxDistChild2 / c2.getSubClusters().size();
//        } else
//            maxDistChild2 = -Double.MAX_VALUE;
//        if (c3.hasChildren()) {
//            for (Cluster child : c3.getSubClusters()) {
//                maxDistChild3 += child.getMaxDist();
//            }
//            maxDistChild3 = c3.getMaxDist() - maxDistChild3 / c3.getSubClusters().size();
//        } else
//            maxDistChild3 = -Double.MAX_VALUE;
//
//        if (maxDistChild1 >= maxDistChild2 && maxDistChild1 >= maxDistChild3 && c1.hasChildren()) { // c1 has the largest diam
//            if (!c1.hasChildren())
//                System.err.println("Problemo");
//            return 1;
//        } else if (maxDistChild2 > maxDistChild3 && c2.hasChildren()) { // c2 is the largest (note that because first if is not true, c2.maxDist >= c1.maxDist)
//            if (!c2.hasChildren())
//                System.err.println("Problemo");
//            return 2;
//        } else { // c3.maxDist >= c1.maxDist && c3.maxDist >= c2.maxDist
//            if (!c3.hasChildren())
//                System.err.println("Problemo");
//            return 3;
//        }
//    }
//
//    public int getClusterToBreakChooseLargerRatioWithChildrenMax(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) { // best
//        // break cluster with max diameter that is still breakable:
//        double maxDistChild1 = 0, maxDistChild2 = 0, maxDistChild3 = 0;
//        if (c1.hasChildren()) {
//            for (Cluster child : c1.getSubClusters()) {
//                maxDistChild1 = Math.max(maxDistChild1, c1.getMaxDist() / (1 + child.getMaxDist()));
//            }
//        } else
//            maxDistChild1 = -Double.MAX_VALUE;
//        if (c2.hasChildren()) {
//            for (Cluster child : c2.getSubClusters()) {
//                maxDistChild2 = Math.max(maxDistChild2, c2.getMaxDist() / (1 + child.getMaxDist()));
//            }
//        } else
//            maxDistChild2 = -Double.MAX_VALUE;
//        if (c3.hasChildren()) {
//            for (Cluster child : c3.getSubClusters()) {
//                maxDistChild3 = Math.max(maxDistChild3, c3.getMaxDist() / (1 + child.getMaxDist()));
//            }
//        } else
//            maxDistChild3 = -Double.MAX_VALUE;
//
//        if (maxDistChild1 >= maxDistChild2 && maxDistChild1 >= maxDistChild3 && c1.hasChildren()) { // c1 has the largest diam
//            if (!c1.hasChildren())
//                System.err.println("Problemo");
//            return 1;
//        } else if (maxDistChild2 > maxDistChild3 && c2.hasChildren()) { // c2 is the largest (note that because first if is not true, c2.maxDist >= c1.maxDist)
//            if (!c2.hasChildren())
//                System.err.println("Problemo");
//            return 2;
//        } else { // c3.maxDist >= c1.maxDist && c3.maxDist >= c2.maxDist
//            if (!c3.hasChildren())
//                System.err.println("Problemo");
//            return 3;
//        }
//    }
//
//    public int getClusterToBreakChooseLargerRatioWithChildrenMin(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) { // best
//        // break cluster with max diameter that is still breakable:
//        double maxDistChild1 = 0, maxDistChild2 = 0, maxDistChild3 = 0;
//        if (c1.hasChildren()) {
//            for (Cluster child : c1.getSubClusters()) {
//                maxDistChild1 = Math.min(maxDistChild1, c1.getMaxDist() / (1 + child.getMaxDist()));
//            }
//        } else
//            maxDistChild1 = -Double.MAX_VALUE;
//        if (c2.hasChildren()) {
//            for (Cluster child : c2.getSubClusters()) {
//                maxDistChild2 = Math.min(maxDistChild2, c2.getMaxDist() / (1 + child.getMaxDist()));
//            }
//        } else
//            maxDistChild2 = -Double.MAX_VALUE;
//        if (c3.hasChildren()) {
//            for (Cluster child : c3.getSubClusters()) {
//                maxDistChild3 = Math.min(maxDistChild3, c3.getMaxDist() / (1 + child.getMaxDist()));
//            }
//        } else
//            maxDistChild3 = -Double.MAX_VALUE;
//
//        if (maxDistChild1 >= maxDistChild2 && maxDistChild1 >= maxDistChild3 && c1.hasChildren()) { // c1 has the largest diam
//            if (!c1.hasChildren())
//                System.err.println("Problemo");
//            return 1;
//        } else if (maxDistChild2 > maxDistChild3 && c2.hasChildren()) { // c2 is the largest (note that because first if is not true, c2.maxDist >= c1.maxDist)
//            if (!c2.hasChildren())
//                System.err.println("Problemo");
//            return 2;
//        } else { // c3.maxDist >= c1.maxDist && c3.maxDist >= c2.maxDist
//            if (!c3.hasChildren())
//                System.err.println("Problemo");
//            return 3;
//        }
//    }
//
//    public int getClusterToBreakChooseLargerDifferenceWithChildrenMin(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) { // best
//        // break cluster with max diameter that is still breakable:
//        double maxDistChild1 = 0, maxDistChild2 = 0, maxDistChild3 = 0;
//        if (c1.hasChildren()) {
//            for (Cluster child : c1.getSubClusters()) {
//                maxDistChild1 = Math.min(maxDistChild1, child.getMaxDist());
//            }
//            maxDistChild1 = c1.getMaxDist() - maxDistChild1;
//        } else
//            maxDistChild1 = -Double.MAX_VALUE;
//        if (c2.hasChildren()) {
//            for (Cluster child : c2.getSubClusters()) {
//                maxDistChild2 = Math.min(maxDistChild2, child.getMaxDist());
//            }
//            maxDistChild2 = c2.getMaxDist() - maxDistChild2;
//        } else
//            maxDistChild2 = -Double.MAX_VALUE;
//        if (c3.hasChildren()) {
//            for (Cluster child : c3.getSubClusters()) {
//                maxDistChild3 = Math.min(maxDistChild3, child.getMaxDist());
//            }
//            maxDistChild3 = c3.getMaxDist() - maxDistChild3;
//        } else
//            maxDistChild3 = -Double.MAX_VALUE;
//
//        if (maxDistChild1 >= maxDistChild2 && maxDistChild1 >= maxDistChild3 && c1.hasChildren()) { // c1 has the largest diam
//            if (!c1.hasChildren())
//                System.err.println("Problemo");
//            return 1;
//        } else if (maxDistChild2 > maxDistChild3 && c2.hasChildren()) { // c2 is the largest (note that because first if is not true, c2.maxDist >= c1.maxDist)
//            if (!c2.hasChildren())
//                System.err.println("Problemo");
//            return 2;
//        } else { // c3.maxDist >= c1.maxDist && c3.maxDist >= c2.maxDist
//            if (!c3.hasChildren())
//                System.err.println("Problemo");
//            return 3;
//        }
//    }
}

package bounding;

import _aux.*;
import clustering.Cluster;
import clustering.RecursiveClustering;
import com.google.common.collect.Ordering;
import streaming.*;

import java.io.FileWriter;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.*;

import static _aux.lib.getStream;


public class HierarchicalBounding {

    public FileWriter ccWriter;
    public int defaultDesiredClusters = 10;

    public int n_dim;
    public int n_vec;

    public double n_comparisons;
    public double n_2bound_comparisons;
    public double n_3bound_comparisons;
    public long n_2corr_lookups;

    //    public long[] comp_saved_per_level; // 3-bounds
    public double n_positive;
    public ArrayList<int[]> positives;
    public double n_negative;
    public ArrayList<int[]> negatives;

    public ArrayList<ResultsTuple> positiveTuples;
    public ArrayList<ResultsTuple> negativeTuples;
    public ArrayList<Boolean> inPositive;

    // only for 3-bounds:
    public double n_decided;
    public double n_undecided;

    public int maxLevels;

    public TimeSeries[] timeSeries;
    public CorrelationBounding CB;
    public RecursiveClustering RC;
    public ProgressiveApproximation PA;

    boolean useKMeans;
    int breakFirstKLevelsToMoreClusters;
    int clusteringRetries;
    public double maxApproximationSize;

    public double tau;
    public boolean useEmpiricalBounds;
    public double minJump;
    public boolean streaming;
    public double groupingTime;
    public int dccCount = 0;
    public int totalDCCSize;
    public int epoch;
    public final int pLeft;
    public final int pRight;
    public final int k;
    public final int topKbufferSize;
    public final boolean parallel;

    public List<DCC> positiveDCCs = new ArrayList<>();
    public ConcurrentLinkedQueue<DCC> negativeDCCs = new ConcurrentLinkedQueue<>();
    public ConcurrentLinkedQueue<DCC> topkBuffered = new ConcurrentLinkedQueue<>();

    //    Grouping and pruning attributes
    //                               negDCC
    public ArrayList<Map<Key, List<DCC>>> ubGrouping;
    public ArrayList<Map<Key, List<DCC>>> lbGrouping;


    public HierarchicalBounding(HashMap<String, Object> parameters){
        this.timeSeries = (TimeSeries[]) parameters.get("timeSeries");
        this.n_dim = timeSeries[0].w;
        this.n_vec = timeSeries.length;
        this.pLeft = (int) parameters.get("pLeft");
        this.pRight = (int) parameters.get("pRight");
        this.k = (int) parameters.get("k");
        this.topKbufferSize = Math.max(((int) parameters.get("topKbufferSize")) * k, 200);
        this.parallel = (boolean) parameters.get("parallel");
        this.breakFirstKLevelsToMoreClusters=(int) parameters.get("breakFirstKLevelsToMoreClusters");
        this.clusteringRetries=(int) parameters.get("clusteringRetries");
        this.maxApproximationSize = (double) parameters.get("maxApproximationSize");

        this.n_2bound_comparisons = 0;
        this.n_3bound_comparisons = 0;
        this.n_2corr_lookups = 0;
        this.maxLevels = (int) parameters.get("maxLevels");
        this.n_decided = 0;
        this.n_undecided = 0;
        this.n_positive = 0;
        this.positives = new ArrayList<>();
        this.n_negative = 0;
        this.negatives = new ArrayList<>();
        this.positiveTuples = new ArrayList<>();
        this.negativeTuples = new ArrayList<>();

        this.defaultDesiredClusters = (int) parameters.get("defaultDesiredClusters");

        this.inPositive = new ArrayList<>(Collections.nCopies(n_vec, false));

        this.groupingTime = 0d;

        this.totalDCCSize = 0;

        this.useKMeans = (boolean) parameters.get("useKMeans");

        this.streaming = (boolean) parameters.get("streaming");


        if (streaming){
            int n = timeSeries.length;
            int arraySize = (n*(n-1)) / 2;
            ubGrouping = new ArrayList<>(arraySize);
            lbGrouping = new ArrayList<>(arraySize);

//            Initialize groupings
            for (int i = 0; i< timeSeries.length; i++){
                for (int j = i+1; j< timeSeries.length; j++){
                    ubGrouping.add(new HashMap<>());
                    lbGrouping.add(new HashMap<>());
                }
            }
        }

        Integer t = (Integer) parameters.get("t");
        this.epoch = t == null ? 0: t;
    }


    public List<DCC> recursiveBounding(HashMap<String, Object> parameters) {
        this.positiveDCCs = new ArrayList<>();
        this.negativeDCCs = new ConcurrentLinkedQueue<>();
        this.topkBuffered = new ConcurrentLinkedQueue<>();
        this.dccCount = 0;
        this.tau = (double) parameters.get("tau");
        this.minJump = (double) parameters.get("minJump");
        this.streaming = (boolean) parameters.get("streaming");
        this.CB = new CorrelationBounding(timeSeries, this);

        PA = new ProgressiveApproximation(tau, useEmpiricalBounds, minJump, Math.sqrt(200 * (1- (-5))), new ArrayList<>(), CB, this);

        this.RC = new RecursiveClustering(timeSeries, maxLevels, defaultDesiredClusters, useKMeans, breakFirstKLevelsToMoreClusters, clusteringRetries,
                (int) parameters.get("seed"));
        RC.setCB(CB);
        CB.RC = RC;

        RC.fitRecursiveClusters((int) parameters.get("startEpsilon"),
                (double) parameters.get("epsilonMultiplier"), RC.clusteringRetries, this.epoch);

        CB.initializeBoundsCache(RC.globalClustID);


        // class containing all relevant tools for bounding cluster correlations:
        Cluster root = RC.clustersPerLevel.get(0).get(0);

//        //first do 2-correlations -> so quick and can help in case of topK to increase corrThreshold
        ArrayList<Cluster> rootLeft = new ArrayList<>(Collections.singletonList(root));
        ArrayList<Cluster> rootRight = new ArrayList<>(Collections.singletonList(root));
//
////        Fill topKbuffered and negativeDCCs
        corrBounding(rootLeft, rootRight, 1);
//
        List<DCC> positiveSingletons = unpackAndCheckMinJump(new ArrayList<>(topkBuffered)).get(true);
//
//        //        Sort on LB (ascending)
        positiveSingletons.sort(DCC::compareTo);
//
////        Get top K
        ArrayList<DCC> topKSingletons = new ArrayList<>(positiveSingletons.subList(Math.max(positiveSingletons.size() - k, 0), positiveSingletons.size()));
//
////        Update threshold
        this.tau = topKSingletons.get(0).boundTuple.lower;
//
////        Reset pos and neg dccs
        topkBuffered = new ConcurrentLinkedQueue<>();
        negativeDCCs = new ConcurrentLinkedQueue<>();
        dccCount = 0;

        System.out.println("correlation threshold updated to " + tau + " after pairwise correlations have been processed.");


//        Now get all DCCs for p
        ArrayList<Cluster> rootLHS = new ArrayList<>();
        for (int i=0; i<pLeft; i++){
            rootLHS.add(root);
        }

        ArrayList<Cluster> rootRHS = new ArrayList<>();
        for (int i=0; i<pRight; i++){
            rootRHS.add(root);
        }


//        Get top k results by breaking down positiveDCCs to singletons and setting threshold
        if (streaming){
            corrBounding(rootLHS, rootRHS, 1);

            initializeTopK();

            lib.getStream(negativeDCCs, parallel)
                    .forEach(this::groupDCC);
        } else {
            corrBounding(rootLHS, rootRHS, 0);

//            Unpack
            topkBuffered = new ConcurrentLinkedQueue<>(unpackAndCheckMinJump(new ArrayList<>(topkBuffered)).get(true));

//        Update tau again
            if (topkBuffered.size() > k){
//                Sort topKBuffer ascending
//                Get top k
                this.topkBuffered = new ConcurrentLinkedQueue<>(topkBuffered.stream()
                        .sorted((dccA, dccB) -> dccB.compareTo(dccA))
                        .limit(k)
                        .collect(Collectors.toList()));

                this.tau = new ArrayList<>(topkBuffered).get(topkBuffered.size() - 1).boundTuple.lower;
                System.out.println("correlation threshold updated to " + tau);
            }

//        Get hidden positives with progressive approximation
            this.positiveDCCs = PA.ApproximateProgressively(new ArrayList<>(negativeDCCs), 50,
                    new ArrayList<>(topkBuffered), parallel, k);
        }

        return this.positiveDCCs;
    }

    public Integer getClusterToBreakChooseLarger(List<Cluster> LHS, List<Cluster> RHS) { // best
        Integer cToBreak = null;
        Double maxDiameter = 0d;

        int i = 0;
        for (Cluster c: LHS){
            if (c.diameter >= maxDiameter){
                cToBreak = i;
                maxDiameter = c.diameter;
            }
            i++;
        }

        for (Cluster c: RHS){
            if (c.diameter >= maxDiameter){
                cToBreak = i;
                maxDiameter = c.diameter;
            }
            i++;
        }

        return cToBreak;
    }


    public List<DCC> corrBounding(ArrayList<Cluster> LHS, ArrayList<Cluster> RHS, double shrinkFactor) {
        ArrayList<DCC> out = new ArrayList<>();

        List<Integer> lhsIds = LHS.stream().map(Cluster::getClusterId).collect(Collectors.toList());
        List<Integer> rhsIds = RHS.stream().map(Cluster::getClusterId).collect(Collectors.toList());


        if (Ordering.natural().isOrdered(lhsIds) && Ordering.natural().isOrdered(rhsIds)) { // cID2<cID3 actually calculate. if this does not hold we will get the calculation at some other point with cid2 and cid3 switched
            CorrBoundTuple boundsTuple = CB.calcBound(LHS, RHS, shrinkFactor, true);

//            UCC
            if (boundsTuple.state == 0 || RHS.get(0).getParent() == RHS.get(0)) { // we need more precise clusters to be conclusive, it is possible that we are higher correlated
                return breakUCC(LHS, RHS, shrinkFactor);
            }
            else {
                dccCount++;

//                Positive DCC
                if (boundsTuple.state == 1) { // check results and add positive
                    DCC dcc = new DCC(LHS, RHS, boundsTuple);
                    topkBuffered.add(dcc);
                    out.add(dcc);
                }
//                Negative DCC
                else if (boundsTuple.state == -1) {
                    DCC dcc = new DCC(LHS, RHS, boundsTuple);

                    if (epoch > 0 && streaming){
                        groupDCC(dcc);
                        negativeDCCs.add(dcc);
                    } else {
                        negativeDCCs.add(dcc);

                        if (dcc.boundTuple.criticalShrinkFactor <= 1)
                            out.add(dcc);
                    }
                }
            }
        }
        return out;
    }

    public List<DCC> breakUCC(ArrayList<Cluster> LHS, ArrayList<Cluster> RHS, double shrinkFactor){
        int cToBreak = getClusterToBreakChooseLarger(LHS, RHS);

        boolean onLHS = cToBreak <= LHS.size() - 1;

        ArrayList<Cluster> allC = new ArrayList<>(pLeft + pRight);
        allC.addAll(LHS); allC.addAll(RHS);

        Cluster breakC = allC.get(cToBreak);

        if (!breakC.hasChildren()){
            System.out.println("BreakUCC - trying to break singleton cluster");
            return new ArrayList<>();
        }

        return getStream(breakC.getSubClusters(), parallel).unordered()
                .flatMap(sc -> {
                    ArrayList<Cluster> newLHS = (ArrayList<Cluster>) LHS.clone();
                    ArrayList<Cluster> newRHS = (ArrayList<Cluster>) RHS.clone();
                    if (onLHS){
                        newLHS.set(cToBreak, sc);
                    } else {
                        newRHS.set(cToBreak - LHS.size(), sc);
                    }
                    return corrBounding(newLHS, newRHS, shrinkFactor).stream();
                }).collect(Collectors.toList());
    }

    public Map<Boolean, List<DCC>> unpackAndCheckMinJump(List<DCC> dccList){
        return dccList.stream()
                .filter(dcc -> dcc != null && dcc.boundTuple.state != 0)
                .flatMap(dcc -> dcc.unpackToSingletons(timeSeries, CB).stream())
                .filter(dcc -> { // remove 2-correlations that have the same LHs and RHS
                    return !(dcc.LHS.size() + dcc.RHS.size() == 2 &&
                            dcc.LHS.get(0).listOfContents.get(0).equals(dcc.RHS.get(0).listOfContents.get(0)));
                })
                .collect(Collectors.partitioningBy(DCC::isPositive));
    }


//    ========================================== STREAMING METHODS =================================
    public void initializeTopK(){
        Map<Boolean, List<DCC>> partitionedSingletons = unpackAndCheckMinJump(new ArrayList<>(topkBuffered));

//        Put hidden negatives in negativeDCCs and filter out
        List<DCC> hiddenNegatives = partitionedSingletons.get(false);
        negativeDCCs.addAll(hiddenNegatives);

        List<DCC> singletons = partitionedSingletons.get(true);

//        Sort on LB (ascending)
        singletons.sort(DCC::compareTo);

//        Get top K
        ArrayList<DCC> topKSingletons = new ArrayList<>(singletons.subList(Math.max(singletons.size() - k, 0), singletons.size()));
        positiveDCCs = topKSingletons;

        int effectiveK = positiveDCCs.size();

//        Merge singletonDCCs back to parent
//        List<DCC> topKDCCs = topKSingletons.stream().map(dcc -> dcc.parent).collect(Collectors.toList());

//        Get singletonDCCs which fell out of topK
        ArrayList<DCC> fallOutDCCs = new ArrayList<>(singletons.subList(0, singletons.size() - effectiveK));

//        Add topK fallOuts to Buffer
        List<DCC> fallOutSubset = new ArrayList<>(fallOutDCCs.subList(Math.max(fallOutDCCs.size() - (topKbufferSize - k), 0), fallOutDCCs.size()));
        List<DCC> notInSubset = new ArrayList<>(fallOutDCCs.subList(0, Math.max(fallOutDCCs.size() - (topKbufferSize - k), 0)));

        this.topkBuffered = new ConcurrentLinkedQueue<>(fallOutSubset);
        this.topkBuffered.addAll(positiveDCCs);

//        Update tau
        if (fallOutSubset.size() > 0){
            this.tau = fallOutSubset.get(0).boundTuple.lower;
        } else {
            this.tau = positiveDCCs.get(0).boundTuple.lower;
        }

//        Change state for notInSubset and add to negativeDCCs
        notInSubset.forEach(dcc -> {
                dcc.boundTuple.state = -1;
        });
        negativeDCCs.addAll(notInSubset);
    }


    public void reFillTopkBuffered(){
        System.out.println("refilling topK buffer!");

        //            Sort negativeDCCs on LB (ascending)
        List<DCC> negDCCList = new ArrayList<>(negativeDCCs);
        negDCCList.sort(DCC::compareTo);

        int nrFill = topKbufferSize - topkBuffered.size();

//            Take dccs to fill
        List<DCC> fillDCCs = negDCCList.subList(negDCCList.size() - nrFill, negDCCList.size());

//            Change state for fills
        fillDCCs.forEach(dcc -> dcc.boundTuple.state = 1);
        negativeDCCs.removeAll(fillDCCs);

//            Add in front
        List<DCC> top2kList = new ArrayList<>(topkBuffered);
        top2kList.addAll(0, fillDCCs);
        topkBuffered = new ConcurrentLinkedQueue<>(top2kList);
    }



    public void groupDCC(DCC dcc){
        if (dcc == null){
            return;
        }

        if (dcc.boundTuple.state != -1){
            System.out.println("Should not group this DCC! State = " + dcc.boundTuple.state);
            return;
        }

        dcc.boundTuple.setDomBounds();

//        Group LHS within
        if (dcc.LHS.size() > 1){
            groupDomBoundElement(
                    dcc.boundTuple.dom_withinLHS_BDP1,
                    dcc.boundTuple.dom_withinLHS_BDP2,
                    dcc, true, false); // lower = null -> determine based on dcc context
        }

//        Group RHS within
        if (dcc.RHS.size() > 1) {
            groupDomBoundElement(
                    dcc.boundTuple.dom_withinRHS_BDP1,
                    dcc.boundTuple.dom_withinRHS_BDP2,
                    dcc, false, false); // lower = null -> determine based on dcc context
        }
//        Group between
        groupDomBoundElement(
                dcc.boundTuple.dom_between_BDP1,
                dcc.boundTuple.dom_between_BDP2,
                dcc, null, false); // lower = null -> determine based on dcc context

        dcc.groupedState = dcc.boundTuple.state;
    }

    public void groupDomBoundElement(int[] BDC1s, int[] BDC2s, DCC dcc, Boolean LHS, boolean forMJ){
        ArrayList<Cluster> clusterSide1 = LHS == null || LHS ? dcc.LHS: dcc.RHS;
        ArrayList<Cluster> clusterSide2 = LHS == null || !LHS ? dcc.RHS: dcc.LHS;

        int boundIdx = 0;
        for (int cid1 = 0; cid1<clusterSide1.size(); cid1++){
            for (int cid2 = LHS == null ? 0: cid1 +1; cid2<clusterSide2.size(); cid2++){
//                Because clusterSides are sorted we always know that BDC1 is on the left side
                int BDP1 = BDC1s[boundIdx];
                int BDP2 = BDC2s[boundIdx];

                Key BDP = BDP1 < BDP2 ? new Key(BDP1, BDP2): new Key(BDP2, BDP1);

                Cluster c1 = clusterSide1.get(cid1);
                Cluster c2 = clusterSide2.get(cid2);

                Cluster cs1 = c1.getClusterId() < c2.getClusterId()? c1: c2;
                Cluster cs2 = c1.getClusterId() < c2.getClusterId()? c2: c1;

//              Group dcc for both sides of the BDC
                for (int id1: cs1.listOfContents){
                    for (int id2: cs2.listOfContents){
//                        Pairwise correlations of one never change anyway
                        if (id1 != id2){
                            groupDCConBDC(id1, id2, BDP, dcc, LHS == null);
                        }
                    }
                }

                boundIdx++;
            }
        }
    }

    public void groupDCConBDC(int id1, int id2, Key BDP, DCC dcc, Boolean nom){
        boolean pos = dcc.boundTuple.state == 1;
        int fractionCase = dcc.boundTuple.fractionCase;

//        Determine if bound is ub or lb
        boolean lower = (!pos && !nom && fractionCase != 2);

//        Upper bound of one -> will never be exceeded
        if (!lower && BDP.x == BDP.y){return;}

//        Update grouping
        ArrayList<Map<Key, List<DCC>>> grouping = lower ? lbGrouping: ubGrouping;

        Key idxKey = id1 < id2 ? new Key(id1, id2): new Key(id2, id1);
        int groupingIndex = StreamLib.getGroupIndex(idxKey.x, idxKey.y, timeSeries.length);

//        Get first level group
        Map<Key, List<DCC>> node = grouping.get(groupingIndex);

        if (node == null){
            throw new IllegalStateException("Something went wrong in hashmap initialization");
        }

//        Get second level group
        synchronized (node){
            List<DCC> dccList = node.computeIfAbsent(BDP, k1 -> new ArrayList<>());

//        Add DCC to second level group
            dccList.add(dcc);
        }

    }


    public int[][] addClusterConnectivity(Cluster c, int[][] conMatrix){
        for (int i=0; i<c.listOfContents.size(); i++){
            for (int j=i+1; j<c.listOfContents.size(); j++){
                int id1 = c.listOfContents.get(i);
                int id2 = c.listOfContents.get(j);
                conMatrix[id1][id2] ++;
                conMatrix[id2][id1] ++;
            }
        }

        if (c.hasChildren()){
            for (Cluster sc: c.getSubClusters()){
                conMatrix = addClusterConnectivity(sc, conMatrix);
            }
        }
        return conMatrix;
    }

    public double countNComparisons(Cluster c1, Cluster c2, Cluster c3) {
        ArrayList<Integer> pIDs1 = c1.listOfContents;
        ArrayList<Integer> pIDs2 = c2.listOfContents;
        ArrayList<Integer> pIDs3 = c3.listOfContents;
        if (c2.getClusterId() == c3.getClusterId()) {// c2==c3
            return 0.5 * pIDs1.size() * pIDs2.size() * (pIDs3.size() - 1);
        } else { // c2!=c3, but it might be a subcluster
            int n_overlap = 0;
            for (int id2 : pIDs2) {
                for (int id3 : pIDs3) {

                    if (id2 == id3) {
                        n_overlap++;
                    }
                }
            }


            int n_unique = pIDs2.size() - n_overlap;
            if (pIDs3.size() > pIDs2.size() && n_overlap > 0) {
                System.out.println("we made a mistake");
            }

            double result = 0.5 * pIDs1.size() * n_overlap * (n_overlap - 1) + pIDs1.size() * n_unique * pIDs3.size();
            if (result <= 0) {
                System.err.println("debug: less than 1 comparison");
            }


            return result;
        }
    }


// ---------------------------------------------------------------------------------------------------------------------
// ---------------------------------------------------------------------------------------------------------------------

// ---------------------------------------------------  archive:  ------------------------------------------------------

    public int getClusterToBreakChooseLargerSize(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) { // best
        // break cluster with max diameter that is still breakable:
        if (c1.listOfContents.size() > c2.listOfContents.size() && c1.listOfContents.size() > c3.listOfContents.size()) { // c1 has the largest diam
            return 1;
        } else if (c2.listOfContents.size() > c3.listOfContents.size()) { // c2 is the largest (note that because first if is not true, c2.maxDist >= c1.maxDist)
            return 2;
        } else { // c3.maxDist >= c1.maxDist && c3.maxDist >= c2.maxDist
            return 3;
        }
    }

    public int getClusterToBreakChooseLargerTimesSize(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) { // bad
        // break cluster with max diameter that is still breakable:
        if (c1.getMaxDist() * c1.listOfContents.size() > c2.getMaxDist() * c2.listOfContents.size() && c1.getMaxDist() * c1.listOfContents.size() > c3.getMaxDist() * c3.listOfContents.size()) { // c1 has the largest diam
            return 1;
        } else if (c2.getMaxDist() * c2.listOfContents.size() > c3.getMaxDist() * c3.listOfContents.size()) { // c2 is the largest (note that because first if is not true, c2.maxDist >= c1.maxDist)
            return 2;
        } else { // c3.maxDist >= c1.maxDist && c3.maxDist >= c2.maxDist
            return 3;
        }
    }

    public int getClusterToBreakChooseA(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) { // not very bad, but worse than larger
        if (c1.getMaxDist() > 0)
            return 1;
        else {
            if (c2.getMaxDist() > c3.getMaxDist()) { // c2 is the largest (note that because first if is not true, c2.maxDist >= c1.maxDist)
                return 2;
            } else { // c3.maxDist >= c1.maxDist && c3.maxDist >= c2.maxDist
                return 3;
            }
        }
    }

    public int getClusterToBreakChooseLargerFromBC(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) { // very bad
        if (c2.getMaxDist() == 0 && c3.getMaxDist() == 0)
            return 1;
        else {
            if (c2.getMaxDist() > c3.getMaxDist()) { // c2 is the largest (note that because first if is not true, c2.maxDist >= c1.maxDist)
                return 2;
            } else { // c3.maxDist >= c1.maxDist && c3.maxDist >= c2.maxDist
                return 3;
            }
        }
    }

    public int getClusterToBreakChooseLargerAvg(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) {
        // break cluster with max getAvgDist that is still breakable:
        if (c1.getAvgDist() > c2.getAvgDist() && c1.getAvgDist() > c3.getAvgDist()) { // c1 has the largest diam
            return 1;
        } else if (c2.getAvgDist() > c3.getAvgDist()) { // c2 is the largest (note that because first if is not true, c2.maxDist >= c1.maxDist)
            return 2;
        } else { // c3.maxDist >= c1.maxDist && c3.maxDist >= c2.maxDist
            return 3;
        }
    }


    public int getClusterToBreakBasedOnTheoreticalBounds(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) {
        return -1;
    }

    public int getClusterToBreakChooseSmaller(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) {
        int[] sizes = new int[]{c1.listOfContents.size(), c2.listOfContents.size(), c3.listOfContents.size()};
        double[] dists = new double[]{c1.getMaxDist(), c2.getMaxDist(), c3.getMaxDist()};
        ArrayList<Integer> cands = new ArrayList<>(3);
        if (sizes[0] > 1) cands.add(0);
        if (sizes[1] > 1) cands.add(1);
        if (sizes[2] > 1) cands.add(2);
        int bestClusterToBreak = -1;
        double bestDistance = Double.MAX_VALUE;
        for (int i : cands) {
            if (dists[i] < bestDistance) {
                bestClusterToBreak = i;
                bestDistance = dists[i];
            }
        }
        return bestClusterToBreak + 1;
    }


    public int getClusterToBreakChooseLargerDifferenceWithChildrenSum(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) { // best
        // break cluster with max diameter that is still breakable:
        double maxDistChild1 = 0, maxDistChild2 = 0, maxDistChild3 = 0;
        if (c1.hasChildren()) {
            for (Cluster child : c1.getSubClusters()) {
                maxDistChild1 += child.getMaxDist();
            }
            maxDistChild1 = c1.getMaxDist() - maxDistChild1 / c1.getSubClusters().size();
        } else
            maxDistChild1 = -Double.MAX_VALUE;
        if (c2.hasChildren()) {
            for (Cluster child : c2.getSubClusters()) {
                maxDistChild2 += child.getMaxDist();
            }
            maxDistChild2 = c2.getMaxDist() - maxDistChild2 / c2.getSubClusters().size();
        } else
            maxDistChild2 = -Double.MAX_VALUE;
        if (c3.hasChildren()) {
            for (Cluster child : c3.getSubClusters()) {
                maxDistChild3 += child.getMaxDist();
            }
            maxDistChild3 = c3.getMaxDist() - maxDistChild3 / c3.getSubClusters().size();
        } else
            maxDistChild3 = -Double.MAX_VALUE;

        if (maxDistChild1 >= maxDistChild2 && maxDistChild1 >= maxDistChild3 && c1.hasChildren()) { // c1 has the largest diam
            if (!c1.hasChildren())
                System.err.println("Problemo");
            return 1;
        } else if (maxDistChild2 > maxDistChild3 && c2.hasChildren()) { // c2 is the largest (note that because first if is not true, c2.maxDist >= c1.maxDist)
            if (!c2.hasChildren())
                System.err.println("Problemo");
            return 2;
        } else { // c3.maxDist >= c1.maxDist && c3.maxDist >= c2.maxDist
            if (!c3.hasChildren())
                System.err.println("Problemo");
            return 3;
        }
    }

    public int getClusterToBreakChooseLargerRatioWithChildrenMax(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) { // best
        // break cluster with max diameter that is still breakable:
        double maxDistChild1 = 0, maxDistChild2 = 0, maxDistChild3 = 0;
        if (c1.hasChildren()) {
            for (Cluster child : c1.getSubClusters()) {
                maxDistChild1 = Math.max(maxDistChild1, c1.getMaxDist() / (1 + child.getMaxDist()));
            }
        } else
            maxDistChild1 = -Double.MAX_VALUE;
        if (c2.hasChildren()) {
            for (Cluster child : c2.getSubClusters()) {
                maxDistChild2 = Math.max(maxDistChild2, c2.getMaxDist() / (1 + child.getMaxDist()));
            }
        } else
            maxDistChild2 = -Double.MAX_VALUE;
        if (c3.hasChildren()) {
            for (Cluster child : c3.getSubClusters()) {
                maxDistChild3 = Math.max(maxDistChild3, c3.getMaxDist() / (1 + child.getMaxDist()));
            }
        } else
            maxDistChild3 = -Double.MAX_VALUE;

        if (maxDistChild1 >= maxDistChild2 && maxDistChild1 >= maxDistChild3 && c1.hasChildren()) { // c1 has the largest diam
            if (!c1.hasChildren())
                System.err.println("Problemo");
            return 1;
        } else if (maxDistChild2 > maxDistChild3 && c2.hasChildren()) { // c2 is the largest (note that because first if is not true, c2.maxDist >= c1.maxDist)
            if (!c2.hasChildren())
                System.err.println("Problemo");
            return 2;
        } else { // c3.maxDist >= c1.maxDist && c3.maxDist >= c2.maxDist
            if (!c3.hasChildren())
                System.err.println("Problemo");
            return 3;
        }
    }

    public int getClusterToBreakChooseLargerRatioWithChildrenMin(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) { // best
        // break cluster with max diameter that is still breakable:
        double maxDistChild1 = 0, maxDistChild2 = 0, maxDistChild3 = 0;
        if (c1.hasChildren()) {
            for (Cluster child : c1.getSubClusters()) {
                maxDistChild1 = Math.min(maxDistChild1, c1.getMaxDist() / (1 + child.getMaxDist()));
            }
        } else
            maxDistChild1 = -Double.MAX_VALUE;
        if (c2.hasChildren()) {
            for (Cluster child : c2.getSubClusters()) {
                maxDistChild2 = Math.min(maxDistChild2, c2.getMaxDist() / (1 + child.getMaxDist()));
            }
        } else
            maxDistChild2 = -Double.MAX_VALUE;
        if (c3.hasChildren()) {
            for (Cluster child : c3.getSubClusters()) {
                maxDistChild3 = Math.min(maxDistChild3, c3.getMaxDist() / (1 + child.getMaxDist()));
            }
        } else
            maxDistChild3 = -Double.MAX_VALUE;

        if (maxDistChild1 >= maxDistChild2 && maxDistChild1 >= maxDistChild3 && c1.hasChildren()) { // c1 has the largest diam
            if (!c1.hasChildren())
                System.err.println("Problemo");
            return 1;
        } else if (maxDistChild2 > maxDistChild3 && c2.hasChildren()) { // c2 is the largest (note that because first if is not true, c2.maxDist >= c1.maxDist)
            if (!c2.hasChildren())
                System.err.println("Problemo");
            return 2;
        } else { // c3.maxDist >= c1.maxDist && c3.maxDist >= c2.maxDist
            if (!c3.hasChildren())
                System.err.println("Problemo");
            return 3;
        }
    }

    public int getClusterToBreakChooseLargerDifferenceWithChildrenMin(Cluster c1, Cluster c2, Cluster c3, double corrThreshold, boolean useEmpiricalBounds, double minJump) { // best
        // break cluster with max diameter that is still breakable:
        double maxDistChild1 = 0, maxDistChild2 = 0, maxDistChild3 = 0;
        if (c1.hasChildren()) {
            for (Cluster child : c1.getSubClusters()) {
                maxDistChild1 = Math.min(maxDistChild1, child.getMaxDist());
            }
            maxDistChild1 = c1.getMaxDist() - maxDistChild1;
        } else
            maxDistChild1 = -Double.MAX_VALUE;
        if (c2.hasChildren()) {
            for (Cluster child : c2.getSubClusters()) {
                maxDistChild2 = Math.min(maxDistChild2, child.getMaxDist());
            }
            maxDistChild2 = c2.getMaxDist() - maxDistChild2;
        } else
            maxDistChild2 = -Double.MAX_VALUE;
        if (c3.hasChildren()) {
            for (Cluster child : c3.getSubClusters()) {
                maxDistChild3 = Math.min(maxDistChild3, child.getMaxDist());
            }
            maxDistChild3 = c3.getMaxDist() - maxDistChild3;
        } else
            maxDistChild3 = -Double.MAX_VALUE;

        if (maxDistChild1 >= maxDistChild2 && maxDistChild1 >= maxDistChild3 && c1.hasChildren()) { // c1 has the largest diam
            if (!c1.hasChildren())
                System.err.println("Problemo");
            return 1;
        } else if (maxDistChild2 > maxDistChild3 && c2.hasChildren()) { // c2 is the largest (note that because first if is not true, c2.maxDist >= c1.maxDist)
            if (!c2.hasChildren())
                System.err.println("Problemo");
            return 2;
        } else { // c3.maxDist >= c1.maxDist && c3.maxDist >= c2.maxDist
            if (!c3.hasChildren())
                System.err.println("Problemo");
            return 3;
        }
    }
}

package bounding;

import _aux.*;
import clustering.Cluster;
import clustering.RecursiveClustering;
import com.google.common.collect.Ordering;
import enums.AlgorithmEnum;
import streaming.*;

import java.io.FileWriter;
import java.util.*;
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

    // only for 3-bounds:
    public double n_decided;
    public double n_undecided;

    public int maxLevels;

    public TimeSeries[] timeSeries;
    public CorrelationBounding CB;
    public RecursiveClustering RC;

    boolean useKMeans;
    int breakFirstKLevelsToMoreClusters;
    int clusteringRetries;
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
    public final boolean parallel;

    public Set<int[]> resultSet;
    public List<DCC> positiveDCCs = new ArrayList<>();

    //    Grouping and pruning attributes
    //                               posDCC    negDCC     posMinJump
    public ArrayList<Map<Key, Tuple3<List<DCC>, List<DCC>, List<DCC>>>> ubGrouping;
    public ArrayList<Map<Key, Tuple3<List<DCC>, List<DCC>, List<DCC>>>> lbGrouping;

    public HierarchicalBounding(HashMap<String, Object> parameters){
        this.timeSeries = (TimeSeries[]) parameters.get("timeSeries");
        this.n_dim = timeSeries[0].w;
        this.n_vec = timeSeries.length;
        this.pLeft = (int) parameters.get("pLeft");
        this.pRight = (int) parameters.get("pRight");
        this.parallel = (boolean) parameters.get("parallel");
        this.resultSet = new HashSet<>();
        this.breakFirstKLevelsToMoreClusters=(int) parameters.get("breakFirstKLevelsToMoreClusters");
        this.clusteringRetries=(int) parameters.get("clusteringRetries");

        this.n_2bound_comparisons = 0;
        this.n_3bound_comparisons = 0;
        this.n_2corr_lookups = 0;
        this.maxLevels = (int) parameters.get("maxLevels");
        this.n_decided = 0;
        this.n_undecided = 0;

        this.defaultDesiredClusters = (int) parameters.get("defaultDesiredClusters");

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

    public Set<int[]> recursiveBounding(HashMap<String, Object> parameters) {
        this.dccCount = 0;
        this.tau = (double) parameters.get("tau");
        this.minJump = (double) parameters.get("minJump");
        this.streaming = parameters.get("runningAlgorithm").equals(AlgorithmEnum.STREAMING);
        this.CB = new CorrelationBounding(timeSeries, this);

        this.RC = new RecursiveClustering(timeSeries, maxLevels, defaultDesiredClusters, useKMeans, breakFirstKLevelsToMoreClusters, clusteringRetries,
                (int) parameters.get("seed"));
        RC.setCB(CB);
        CB.RC = RC;

        RC.fitRecursiveClusters((int) parameters.get("startEpsilon"),
                (double) parameters.get("epsilonMultiplier"), RC.clusteringRetries, this.epoch);

        CB.initializeBoundsCache(RC.globalClustID);


        // class containing all relevant tools for bounding cluster correlations:
        Cluster root = RC.clustersPerLevel.get(0).get(0);

        ArrayList<Cluster> rootLHS = new ArrayList<>();
        for (int i=0; i<pLeft; i++){
            rootLHS.add(root);
        }

        ArrayList<Cluster> rootRHS = new ArrayList<>();
        for (int i=0; i<pRight; i++){
            rootRHS.add(root);
        }

        corrBounding(rootLHS, rootRHS);

//        Unpack DCCs in positiveDCCs to stocks, check minjump etc.
        processResultSet();

        return this.resultSet;
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


    private String toCCString(Cluster c){
        if (c.getParent() != null){
            return "P"+c.getParent().oldId + "C"+c.getParent().getSubClusters().indexOf(c);
        } else{
            return "Root";
        }
    }


    public List<DCC> corrBounding(ArrayList<Cluster> LHS, ArrayList<Cluster> RHS) {
        List<DCC> out = new ArrayList<>();

        List<Integer> lhsIds = LHS.stream().map(Cluster::getClusterId).collect(Collectors.toList());
        List<Integer> rhsIds = RHS.stream().map(Cluster::getClusterId).collect(Collectors.toList());


        if (Ordering.natural().isOrdered(lhsIds) && Ordering.natural().isOrdered(rhsIds)) { // cID2<cID3 actually calculate. if this does not hold we will get the calculation at some other point with cid2 and cid3 switched
            CorrBoundTuple boundsTuple = CB.calcBound(LHS, RHS);

            double jumpBasedThreshold = boundsTuple.max2CorrLowerBound + minJump;
            double newThreshold = Math.max(jumpBasedThreshold, tau);


//            UCC
            if (boundsTuple.state == 0 || RHS.get(0).getParent() == RHS.get(0)) { // we need more precise clusters to be conclusive, it is possible that we are higher correlated
                return breakUCC(LHS, RHS);
            }
            else {
                dccCount++;

//                Check if no duplicates between sides
//                Positive DCC
                    if (boundsTuple.state == 1) { // check results and add positive

                        DCC dcc = new DCC(LHS, RHS, boundsTuple);
                        out.add(dcc);

                        this.positiveDCCs.add(dcc);

                        if (streaming){
                            groupDCC(dcc);
                        }
                    }
//                Negative DCC
                    else if (boundsTuple.state == -1  && newThreshold <= 1 && streaming) {
                        DCC dcc = new DCC(LHS, RHS, boundsTuple);

                        groupDCC(dcc);
                    }
//                }
            }
        }
        return out;
    }

    public List<DCC> breakUCC(ArrayList<Cluster> LHS, ArrayList<Cluster> RHS){
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
                    return corrBounding(newLHS, newRHS).stream();
                }).collect(Collectors.toList());
    }

    public void groupDCC(DCC dcc){
        if (dcc.boundTuple.state == 0){
            throw new IllegalStateException("Not a DCC!");
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

//        Group DCCs also for minJump constraints
        if (minJump >= 0){
            groupForMinjump(dcc);
        }
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
                            groupDCConBDC(id1, id2, BDP, dcc, LHS == null, forMJ);
                        }
                    }
                }

                boundIdx++;
            }
        }
    }

    public void groupDCConBDC(int id1, int id2, Key BDP, DCC dcc, Boolean nom, boolean forMJ){
        boolean pos = dcc.boundTuple.state == 1;
        int fractionCase = dcc.boundTuple.fractionCase;

//        Determine if bound is ub or lb
        boolean lower = ((pos && !forMJ && !(!nom && fractionCase == 0)) || (!pos && !nom && fractionCase != 2));

//        Upper bound of one -> will never be exceeded
        if (!lower && BDP.x == BDP.y){return;}

//        Update grouping
        ArrayList<Map<Key, Tuple3<List<DCC>, List<DCC>, List<DCC>>>> grouping = lower ? lbGrouping: ubGrouping;

        Key idxKey = id1 < id2 ? new Key(id1, id2): new Key(id2, id1);
        int groupingIndex = StreamLib.getGroupIndex(idxKey.x, idxKey.y, timeSeries.length);

//        Get first level group
        Map<Key, Tuple3<List<DCC>, List<DCC>, List<DCC>>> node = grouping.get(groupingIndex);

        if (node == null){
            throw new IllegalStateException("Something went wrong in hashmap initialization");
        }

//        Get second level group
        Tuple3<List<DCC>, List<DCC>, List<DCC>> partitionedDccList = getSecondLevelGroup(BDP, node);

//        Initialize minJump grouped cell if necessary
        if (forMJ && partitionedDccList.z == null){partitionedDccList.z = new ArrayList<>();}

//        Add DCC to second level group
        List<DCC> dccList = forMJ ? partitionedDccList.z : pos ? partitionedDccList.x: partitionedDccList.y;
        addDCCtoList(dcc, dccList);
    }

//    When multithreading, sometimes have to wait until array can be accessed
    public void addDCCtoList(DCC toAdd, List<DCC> list){
        synchronized (list){
            list.add(toAdd);
        }
    }

    //    When multithreading, sometimes have to wait until grouping can be accessed
    public Tuple3<List<DCC>, List<DCC>, List<DCC>> getSecondLevelGroup(Key key, Map<Key, Tuple3<List<DCC>, List<DCC>, List<DCC>>> node){
        Tuple3<List<DCC>, List<DCC>, List<DCC>> partitionedDccList;
        synchronized (node){
            partitionedDccList = node.get(key);
            if (partitionedDccList == null) {
                partitionedDccList = new Tuple3<>(new ArrayList<>(), new ArrayList<>(), null);
                node.put(key, partitionedDccList);
            }
        };

        return partitionedDccList;
    }

    public void groupForMinjump(DCC dcc){
        if (dcc.isPositive()){
            switch (dcc.boundTuple.fractionCase) {
                case 0: { // dcc was grouped on LB on between cluster bounds
                    groupDomBoundElement(
                            dcc.boundTuple.between_BDP1_UB,
                            dcc.boundTuple.between_BDP2_UB,
                            dcc, null, true);
                } break;
                case -1:
                case 1:
                case 2: { // dcc was grouped on LB on within cluster bounds
                    groupDomBoundElement(
                            dcc.boundTuple.withinLHS_BDP1_UB,
                            dcc.boundTuple.withinLHS_BDP2_UB,
                            dcc, true, true);

                    groupDomBoundElement(
                            dcc.boundTuple.withinRHS_BDP1_UB,
                            dcc.boundTuple.withinRHS_BDP2_UB,
                            dcc, false, true);
                }break;
            }
        } else if (dcc.boundTuple.lower >= tau && dcc.boundTuple.state == -1){
            switch (dcc.boundTuple.fractionCase) {
                case -1:
                case 0:
                case 1: { // dcc was grouped on UB on between cluster bounds
                    groupDomBoundElement(
                            dcc.boundTuple.between_BDP1_LB,
                            dcc.boundTuple.between_BDP2_LB,
                            dcc, null, true);
                } break;
                case 2: { // dcc was grouped on UB on all cluster bounds
                    groupDomBoundElement(
                            dcc.boundTuple.between_BDP1_LB,
                            dcc.boundTuple.between_BDP2_LB,
                            dcc, null, true);
                    groupDomBoundElement(
                            dcc.boundTuple.withinLHS_BDP1_LB,
                            dcc.boundTuple.withinLHS_BDP2_LB,
                            dcc, true, true);
                    groupDomBoundElement(
                            dcc.boundTuple.withinRHS_BDP1_LB,
                            dcc.boundTuple.withinRHS_BDP2_LB,
                            dcc, false, true);
                }break;
            }
        }
    }

    public Set<Double[]> processResultSet(){
        this.resultSet = new HashSet<>();

        Set<Double[]> resultsToReturn = new HashSet<>();
        lib.getStream(positiveDCCs, false).filter(Objects::nonNull).forEach(dcc -> {
            try {
                Cluster c1 = dcc.LHS.get(0);
                Cluster c2 = dcc.RHS.get(0);
                Cluster c3 = dcc.RHS.get(1);

                for (int a: c1.listOfContents){
                    for (int b: c2.listOfContents){
                        for (int c: c3.listOfContents){
                            if (!(a == b || a == c)) {
                                double corr_1_2 = timeSeries[a].pairwiseCorrelations[b];
                                double corr_1_3 = timeSeries[a].pairwiseCorrelations[c];
                                double corr_2_3 = timeSeries[b].pairwiseCorrelations[c];

                                double corr_est = (corr_1_2 + corr_1_3) / Math.sqrt(2 + 2*corr_2_3);
                                if (corr_est >= 0.95 & corr_est >= Math.max(corr_1_2, Math.max(corr_1_3, corr_2_3)) + minJump) {
                                    resultSet.add(new int[]{a, b, c});
                                    resultsToReturn.add(new Double[]{(double) epoch, (double) a, (double) b, (double) c, corr_est});
                                }
                            }
                        }
                    }
                }
            } catch (NullPointerException ignored){}
        });
        return resultsToReturn;
    }

    public int[][] getClusterFootprint(){
        int[][] conMatrix = new int[timeSeries.length][timeSeries.length];

        for (Cluster c: RC.clustersPerLevel.get(1)){
            conMatrix = addClusterConnectivity(c, conMatrix);
        }

        return conMatrix;
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

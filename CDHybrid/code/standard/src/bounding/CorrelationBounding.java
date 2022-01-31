package bounding;

import clustering.Cluster;
import _aux.Pair;
import clustering.RecursiveClustering;
import _aux.Tuple3;
import streaming.CorrBoundTuple;
import streaming.TimeSeries;
import streaming.StreamLib;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class CorrelationBounding {

    public int n_comparisons;
    public ConcurrentHashMap<String, Pair<Tuple3<Integer, Integer, double[]>, Tuple3<Integer, Integer, double[]>>> bound2Lookup;
    public int num_clusters;
    public TimeSeries[] timeSeries;
    public int n_2bound_comparisons;
    public long n_2corr_lookups;
    public RecursiveClustering RC;
    public HierarchicalBounding HB;
    public int n_vec; public int n_dim;


    public CorrelationBounding(TimeSeries[] timeSeries, HierarchicalBounding HB){
        this.timeSeries = timeSeries;
        this.n_vec = timeSeries.length;
        this.n_dim = timeSeries[0].w;
        this.HB = HB;

        this.n_comparisons=0;
        this.n_2bound_comparisons = 0;
        this.n_2corr_lookups=0;

        Stream<TimeSeries> stockStream = HB.parallel ? Arrays.stream(timeSeries).parallel() : Arrays.stream(timeSeries);

        stockStream.forEach(stock -> {
            stock.oldCorrelations = stock.pairwiseCorrelations.clone();
            stock.computePairwiseCorrelations(timeSeries, false);
        });
    }

    public void initializeBoundsCache(int num_clusters){
        this.num_clusters = num_clusters;
        this.bound2Lookup = new ConcurrentHashMap<>(num_clusters*num_clusters, 0.4f);
    }

    public CorrBoundTuple calcBound(List<Cluster> LHS, List<Cluster> RHS) { // it returns an array of lower, higher,
        double max2CorrLowerBound = -1;

        double nominator_lower = 0;
        double nominator_upper = 0;

        //numerator: (nominator -- dyslexia strikes?!)
        ArrayList<Pair<Tuple3<Integer, Integer, double[]>, Tuple3<Integer, Integer, double[]>>> betweenClusterBounds =
                new ArrayList<>(LHS.size() * RHS.size());
        for (Cluster c1 : LHS) {
            for (Cluster c2 : RHS) {
                Pair<Tuple3<Integer, Integer, double[]>, Tuple3<Integer, Integer, double[]>> rawBounds = get2BoundOrCompute(c1, c2);
                betweenClusterBounds.add(rawBounds);

                double[] bounds = new double[]{StreamLib.getBoundFromBoundTuple(rawBounds.x), StreamLib.getBoundFromBoundTuple(rawBounds.y)};
                nominator_lower += bounds[0];
                nominator_upper += bounds[1];
                max2CorrLowerBound = Math.max(bounds[0], max2CorrLowerBound);
            }
        }

        //denominator: first sqrt
        double denominator_lower_left = LHS.size();
        double denominator_upper_left = LHS.size();

        ArrayList<Pair<Tuple3<Integer, Integer, double[]>, Tuple3<Integer, Integer, double[]>>> withinLHSBounds =
                new ArrayList<>(LHS.size() * LHS.size());
        for (int i = 0; i < LHS.size(); i++) {
            Cluster c1 = LHS.get(i);
            for (int j = i + 1; j < LHS.size(); j++) {
                Cluster c2 = LHS.get(j);
                Pair<Tuple3<Integer, Integer, double[]>, Tuple3<Integer, Integer, double[]>> rawBounds = get2BoundOrCompute(c1, c2);
                withinLHSBounds.add(rawBounds);

                double[] bounds = new double[]{StreamLib.getBoundFromBoundTuple(rawBounds.x), StreamLib.getBoundFromBoundTuple(rawBounds.y)};
                denominator_lower_left += 2 * bounds[0];
                denominator_upper_left += 2 * bounds[1];
                max2CorrLowerBound = Math.max(bounds[0], max2CorrLowerBound);
            }
        }

        //denominator: second sqrt
        double denominator_lower_right = RHS.size();
        double denominator_upper_right = RHS.size();

        ArrayList<Pair<Tuple3<Integer, Integer, double[]>, Tuple3<Integer, Integer, double[]>>> withinRHSBounds =
                new ArrayList<>(LHS.size() * LHS.size());
        for (int i = 0; i < RHS.size(); i++) {
            Cluster c1 = RHS.get(i);
            for (int j = i + 1; j < RHS.size(); j++) {
                Cluster c2 = RHS.get(j);
                Pair<Tuple3<Integer, Integer, double[]>, Tuple3<Integer, Integer, double[]>> rawBounds = get2BoundOrCompute(c1, c2);
                withinRHSBounds.add(rawBounds);

                double[] bounds = new double[]{StreamLib.getBoundFromBoundTuple(rawBounds.x), StreamLib.getBoundFromBoundTuple(rawBounds.y)};
                denominator_lower_right += 2 * bounds[0];
                denominator_upper_right += 2 * bounds[1];
                max2CorrLowerBound = Math.max(bounds[0], max2CorrLowerBound);
            }
        }

        //denominator: whole. note that if bounds are too loose we could get a non-positive value, while this is not possible due to Pos. Def. of variance.
        double denominator_lower = Math.sqrt(Math.max(denominator_lower_left, 1e-7) * Math.max(denominator_lower_right, 1e-7)); // in case we have big clusters, we can get the scenario where the bounds are negative -- not possible!
        double denominator_upper = Math.sqrt(Math.max(denominator_upper_left, 1e-7) * Math.max(denominator_upper_right, 1e-7));

        CorrBoundTuple boundTuple = new CorrBoundTuple(withinLHSBounds, withinRHSBounds, betweenClusterBounds, max2CorrLowerBound);

        //case distinction for final bound
        if (nominator_lower >= 0) {
            boundTuple.fractionCase = 0;
            boundTuple.lower = nominator_lower / denominator_upper;
            boundTuple.upper = nominator_upper / denominator_lower;
        } else if (nominator_lower < 0 && nominator_upper >= 0) {
            boundTuple.fractionCase = 1;
            boundTuple.lower = nominator_lower / denominator_lower;
            boundTuple.upper = nominator_upper / denominator_lower;
        } else if (nominator_upper < 0) {
            boundTuple.fractionCase = 2;
            boundTuple.lower = nominator_lower / denominator_lower;
            boundTuple.upper = nominator_upper / denominator_upper;
        } else {
            boundTuple.fractionCase = -1;
            boundTuple.lower = -1000;
            boundTuple.upper = 1000;
            System.out.println("debug: " + nominator_lower + nominator_upper + denominator_lower + denominator_upper);
        }

        boundTuple.lower = Math.min(1, Math.max(-1, boundTuple.lower));
        boundTuple.upper = Math.min(1, Math.max(-1, boundTuple.upper));

        boundTuple.setState(HB.tau, HB.minJump);

        return boundTuple;
    }

    public Pair<Tuple3<Integer, Integer, double[]>, Tuple3<Integer, Integer, double[]>> get2BoundOrCompute(Cluster c1, Cluster c2) {
        int id1 = c1.getClusterId();
        int id2 = c2.getClusterId();

        String comparisonSignature = getBound2Id(id1, id2);

        Pair<Tuple3<Integer, Integer, double[]>, Tuple3<Integer, Integer, double[]>> empiricalBounds = this.bound2Lookup.get(comparisonSignature);
        if (empiricalBounds == null) { // missing in the bounds hash table
            Integer lowerX = c1.listOfContents.get(0);
            Integer lowerY = c2.listOfContents.get(0);
            double lowerCorr;

            Integer upperX = c1.listOfContents.get(0);
            Integer upperY = c2.listOfContents.get(0);
            double upperCorr;

            if (!(c1 == c2 && c1.listOfContents.size() == 1)) {
                lowerCorr = 1d;
                upperCorr = -1d;
                double corr;

                outerloop:
                for (int pID1 : c1.listOfContents) {
                    for (int pID2 : c2.listOfContents) {

                        this.n_2corr_lookups++;

                        corr = timeSeries[pID1].pairwiseCorrelations[pID2];
                        // corr<0.95 remove this when done testing; can we prune some more with this??

                        if (corr <= lowerCorr) {
                            lowerX = pID1;
                            lowerY = pID2;
                            lowerCorr = corr;
                        }

                        if (corr >= upperCorr) {
                            upperX = pID1;
                            upperY = pID2;
                            upperCorr = corr;
                        }
                        if (lowerCorr == -1 && upperCorr == 1) {
                            break outerloop;
                        }
                    }
                }
            }

            Tuple3<Integer, Integer, double[]> lower = new Tuple3<>(lowerX, lowerY, timeSeries[lowerX].pairwiseCorrelations);
            Tuple3<Integer, Integer, double[]> upper = new Tuple3<>(upperX, upperY, timeSeries[upperX].pairwiseCorrelations);


            Pair<Tuple3<Integer, Integer, double[]>, Tuple3<Integer, Integer, double[]>> result =
                    new Pair<>(lower, upper);

//                Order integers in bounds
            result = id1 > id2 ? result.getMirror(timeSeries, result): result;

            bound2Lookup.put(comparisonSignature, result);

            empiricalBounds = result;
        }
        return empiricalBounds;
    }

    private String getBound2Id(Integer id1, Integer id2) {
        if (id1 < id2) {
            return id1 + "-" + id2;
        } else {
            return id2 + "-" + id1;
        }
    }






}

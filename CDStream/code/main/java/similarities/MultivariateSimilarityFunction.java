package similarities;

import _aux.Pair;
import _aux.StatBag;
import _aux.lib;
import _aux.lists.FastArrayList;
import bounding.ClusterBounds;
import bounding.ClusterCombination;
import bounding.ClusterPair;
import clustering.Cluster;
import lombok.Getter;
import lombok.Setter;
import streaming.TimeSeries;
import streaming.index.ExtremaPair;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.IntStream;

public abstract class MultivariateSimilarityFunction {
    @Getter @Setter protected StatBag statBag;
    @Setter public int totalClusters;

//    Angle between time series
    @Getter public DistanceFunction distFunc = lib::znormalizedAngle;
    @Getter public NormalizationFunction normFunction = lib::znorm;

    public double MAX_SIMILARITY = 1d;
    public double MIN_SIMILARITY = -1d;
    public double SIMRANGE = MAX_SIMILARITY - MIN_SIMILARITY;
    @Getter public double maxApproximationSize = simToDist((MAX_SIMILARITY - MIN_SIMILARITY) / 2);

    public ConcurrentHashMap<Integer, ClusterPair> pairwiseClusterCache = new ConcurrentHashMap<>(100000, .4f);

    //    WlSqSum for each subset of Wl (i.e. [0], [0,1], [0,1,2], ...)
    private Map<Integer, Double> WlSqSum = new HashMap<>(4);
    private Map<Integer, Double> WrSqSum = new HashMap<>(4);

//    ----------------------- METHODS --------------------------------
    public String toString(){
        return this.getClass().getSimpleName();
    }

    public abstract boolean hasEmpiricalBounds();
    public abstract boolean isTwoSided();
    public boolean canStream(){return false;}
    public abstract double sim(TimeSeries x, TimeSeries y);

    public abstract double simToDist(double sim, int windowSize);
    public abstract double simToDist(double sim);
    public abstract double distToSim(double dist, int windowSize);
    public abstract double distToSim(double dist);

    public abstract ClusterBounds empiricalSimilarityBounds(FastArrayList<Cluster> LHS, FastArrayList<Cluster> RHS, double[] Wl, double[] Wr, double[][] pairwiseDistances);
    public double[][] computePairwiseDistances(TimeSeries[] data, boolean parallel) {
        int n = data.length;

//        Initialize running dots
        lib.getStream(IntStream.range(0, n).boxed(), parallel).forEach(i -> {
            data[i].initializeRunningDots(data);
        });

        double[][] pairwiseDistances = new double[n][n];
        lib.getStream(IntStream.range(0, n).boxed(), parallel).forEach(i -> {
            lib.getStream(IntStream.range(i+1, n).boxed(), parallel).forEach(j -> {
                double dist = distFunc.dist(data[i], data[j]);
                pairwiseDistances[i][j] = dist;
                pairwiseDistances[j][i] = dist;
            });
        });
        return pairwiseDistances;
    }

    protected ClusterPair empiricalDistanceBounds(Cluster C1, Cluster C2, double[][] pairwiseDistances){
        int cpHash = this.getClusterPairHash(C1, C2);

        ClusterPair cachedCP = pairwiseClusterCache.get(cpHash);

        if (cachedCP != null && cachedCP.c1.equals(C1) && cachedCP.c2.equals(C2)){
            return cachedCP;
        } else {
            double lb = Double.MAX_VALUE;
            double ub = -Double.MAX_VALUE;
            int[] lbids = new int[]{-1, -1};
            int[] ubids = new int[]{-1, -1};

            for (Integer i : C1.pointsIdx) {
                for (Integer j: C2.pointsIdx) {
                    double dist = i==j ? 0: pairwiseDistances[i][j];
                    if (dist < lb) {
                        lb = dist;
                        lbids[0] = i;
                        lbids[1] = j;
                    }
                    if (dist > ub) {
                        ub = dist;
                        ubids[0] = i;
                        ubids[1] = j;
                    }
                }
            }
            statBag.addToStat(statBag.getNLookups(), () -> C1.size() * C2.size());

//            Complete clusterPair
            double[] bounds = new double[]{lb, ub};

            ExtremaPair lbExtrema = new ExtremaPair(lbids[0], lbids[1]);
            ExtremaPair ubExtrema = new ExtremaPair(ubids[0], ubids[1]);
            ClusterPair cp = new ClusterPair(C1, C2, bounds, lbExtrema, ubExtrema);

            pairwiseClusterCache.put(cpHash, cp);
            return cp;
        }
    }

    public int getClusterPairHash(Cluster C1, Cluster C2){
        int c1id = Math.min(C1.id, C2.id);
        int c2id = Math.max(C1.id, C2.id);
        return c1id * totalClusters + c2id;
    }

    public void bound(ClusterCombination CC, double[] Wl, double[] Wr, double[][] pairwiseDistances){
        ClusterBounds bounds;
        bounds = empiricalSimilarityBounds(CC.getLHS(), CC.getRHS(), Wl, Wr, pairwiseDistances);
        CC.checkAndSetBounds(bounds);
    }

    public double correctBound(double bound){
        return Math.min(Math.max(bound, MIN_SIMILARITY), MAX_SIMILARITY);
    }

    public void clearCache(){
        pairwiseClusterCache.clear();
    }

//    TODO COULD PUSH DOWN TO ADDITIONAL ABSTRACTION
    public Pair<Double,Double> getWeightSquaredSums(double[] Wl, double[] Wr) {
        if (!WlSqSum.containsKey(Wl.length)) {
            double runSumSq = 0;
            for (int i = 0; i < Wl.length; i++) {
                runSumSq += Wl[i] * Wl[i];
            }
            WlSqSum.put(Wl.length, runSumSq);
        }

        if (!WrSqSum.containsKey(Wr.length)) {
            double runSumSq = 0;
            for (int i = 0; i < Wr.length; i++) {
                runSumSq += Wr[i] * Wr[i];
            }
            WrSqSum.put(Wr.length, runSumSq);
        }

        return new Pair<>(WlSqSum.get(Wl.length), WrSqSum.get(Wr.length));
    }
}


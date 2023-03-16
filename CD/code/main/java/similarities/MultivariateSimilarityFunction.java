package similarities;

import _aux.Pair;
import _aux.StatBag;
import _aux.lib;
import _aux.lists.FastArrayList;
import bounding.ClusterBounds;
import bounding.ClusterCombination;
import clustering.Cluster;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

public abstract class MultivariateSimilarityFunction {
    @Getter @Setter private StatBag statBag;
    @Setter public int totalClusters;
    @Getter public DistanceFunction distFunc = lib::euclidean;
    public double MAX_SIMILARITY = 1d;
    public double MIN_SIMILARITY = -1d;
    public double SIMRANGE = MAX_SIMILARITY - MIN_SIMILARITY;


    public ConcurrentHashMap<Long, double[]> pairwiseClusterCache = new ConcurrentHashMap<>(100000, .4f);

    //    WlSqSum for each subset of Wl (i.e. [0], [0,1], [0,1,2], ...)
    private Map<Integer, Double> WlSqSum = new HashMap<>(4);
    private Map<Integer, Double> WrSqSum = new HashMap<>(4);

//    ----------------------- METHODS --------------------------------
    public String toString(){
        return this.getClass().getSimpleName();
    }

    public abstract boolean hasEmpiricalBounds();
    public abstract boolean isTwoSided();
    public abstract double sim(double[] x, double[] y);

    public double[][] preprocess(double[][] data){
        for (int i = 0; i < data.length; i++) {
            data[i] = preprocess(data[i]);
        }
        return data;
    };
    public abstract double[] preprocess(double[] vector);

    public abstract double simToDist(double sim);
    public abstract double distToSim(double dist);

    public double getMaxApproximationSize(double ratio){
        return simToDist(MIN_SIMILARITY + ratio*SIMRANGE);
    }

    public void clearCache(){
        pairwiseClusterCache = new ConcurrentHashMap<>(100000, .4f);
    }

    public double[][] computePairwiseDistances(double[][] data, boolean parallel) {
        int n = data.length;
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

    public double[] theoreticalDistanceBounds(Cluster C1, Cluster C2, double[][] pairwiseDistances){
        long ccID = hashPairwiseCluster(C1.id, C2.id);

        double[] cachedBounds = pairwiseClusterCache.get(ccID);
        if (cachedBounds == null) {
//            Only compute if centroids are geometric, otherwise get from cache
            double centroidDistance = C1.geoCentroid ? this.distFunc.dist(C1.getCentroid(), C2.getCentroid()): pairwiseDistances[C1.centroidIdx][C2.centroidIdx];
            double lbDist = Math.max(0,centroidDistance - C1.getRadius() - C2.getRadius());
            double ubDist = Math.max(0,centroidDistance + C1.getRadius() + C2.getRadius());
            double[] bounds = new double[]{lbDist, ubDist};
            pairwiseClusterCache.put(ccID, bounds);
            return bounds;
        } else {
            return cachedBounds;
        }
    }
    public double[] empiricalDistanceBounds(Cluster C1, Cluster C2, double[][] pairwiseDistances){
        long ccID = hashPairwiseCluster(C1.id, C2.id);

        double[] cachedBounds = pairwiseClusterCache.get(ccID);
        if (cachedBounds == null) {
            double lb = Double.MAX_VALUE;
            double ub = -Double.MAX_VALUE;
            for (Integer i : C1.pointsIdx) {
                for (Integer j: C2.pointsIdx) {
                    double dist = pairwiseDistances[i][j];
                    lb = Math.min(lb, dist);
                    ub = Math.max(ub, dist);
                }
            }
            statBag.addStat(statBag.getNLookups(), () -> C1.size() * C2.size());

            double[] bounds = new double[]{lb, ub};
            pairwiseClusterCache.put(ccID, bounds);
            return bounds;
        } else {
            return cachedBounds;
        }
    }

    public abstract ClusterBounds theoreticalSimilarityBounds(FastArrayList<Cluster> LHS, FastArrayList<Cluster> RHS, double[] Wl, double[] Wr, double[][] pairwiseDistances);
    public abstract ClusterBounds empiricalSimilarityBounds(FastArrayList<Cluster> LHS, FastArrayList<Cluster> RHS, double[] Wl, double[] Wr, double[][] pairwiseDistances);

    public void bound(ClusterCombination CC, boolean empiricalBounding, double[] Wl, double[] Wr, double[][] pairwiseDistances, long boundingTime){
        ClusterBounds bounds;
        if (empiricalBounding) {
            bounds = empiricalSimilarityBounds(CC.getLHS(), CC.getRHS(), Wl, Wr, pairwiseDistances);
        } else {
            bounds = theoreticalSimilarityBounds(CC.getLHS(), CC.getRHS(), Wl, Wr, pairwiseDistances);
        }
        CC.checkAndSetBounds(bounds);
        CC.setBoundingTimestamp(boundingTime);
    }

    public long hashPairwiseCluster(int id1, int id2) {
        if (id1 < id2) {
            return (long) id1 * this.totalClusters + id2;
        } else {
            return (long) id2 * this.totalClusters + id1;
        }
    }

    public double correctBound(double bound){
        return Math.min(Math.max(bound, MIN_SIMILARITY), MAX_SIMILARITY);
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


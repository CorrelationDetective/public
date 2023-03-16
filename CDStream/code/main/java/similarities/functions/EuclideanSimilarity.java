package similarities.functions;

import _aux.Pair;
import _aux.lib;
import _aux.lists.FastArrayList;
import _aux.lists.FastLinkedList;
import bounding.ClusterBounds;
import bounding.ClusterPair;
import clustering.Cluster;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import similarities.DistanceFunction;
import similarities.MultivariateSimilarityFunction;
import streaming.TimeSeries;
import streaming.index.ExtremaPair;
import streaming.index.IndexInstruction;


import java.util.stream.IntStream;

public class EuclideanSimilarity extends PearsonCorrelation {
    public EuclideanSimilarity() {
        distFunc = lib::angleFull;
        MAX_SIMILARITY = 1;
        MIN_SIMILARITY = 0;
        SIMRANGE = MAX_SIMILARITY - MIN_SIMILARITY;
    }

    @Override public boolean hasEmpiricalBounds() {return true;}
    @Override public boolean isTwoSided() {return true;}
    @Override public boolean canStream() {return true;}

    @Override public double sim(TimeSeries ts1, TimeSeries ts2) {
        int w = ts1.getSlidingWindowSize();
        double d2 = 2*(w - ts1.runningDots[ts2.id]);
        return 1 / (1 + Math.sqrt(d2));
    }

    @Override public double simToDist(double sim, int windowSize) {
        if (sim == 0) return Double.MAX_VALUE;

        double d = 1 / (sim) - 1;
        double d2 = d*d;

//        d2 = 2*w - 2*dot
        double dot = windowSize - (d2 / 2);
        return Math.acos(dot / windowSize);
    }

    @Override public double distToSim(double dist, int windowSize) {
//        dist is angle
        double dot = Math.cos(dist) * windowSize;
        double d2 = 2*windowSize - 2*dot;
        return 1 / (1 + Math.sqrt(d2));
    }

    private double angleToDot(double angle, int windowSize) {
        return Math.cos(angle) * windowSize;
    }

    @Override public ClusterBounds empiricalSimilarityBounds(FastArrayList<Cluster> LHS, FastArrayList<Cluster> RHS,
                                                             double[] Wl, double[] Wr, double[][] pairwiseDistances){
        double betweenLowerDot = 0;
        double betweenUpperDot = 0;

        double withinLowerDot = 0;
        double withinUpperDot = 0;

        double maxLowerBoundSubset = 0;

        int w = LHS.get(0).getCentroid().getSlidingWindowSize();

//        For arraylist initialization
        int lSize = LHS.size();
        int rSize = RHS.size();
        int nPairwise = lSize * rSize + (lSize * (lSize - 1) / 2) + (rSize * (rSize - 1) / 2);

        //        Initialize structures that will hold the Cluster pairs of this CC
        FastArrayList<IndexInstruction> lbIndexInstructions = new FastArrayList<>(nPairwise);
        FastArrayList<IndexInstruction> ubIndexInstructions = new FastArrayList<>(nPairwise);

//        Get all pairwise between cluster distances
        for (int i = 0; i < lSize; i++) {
            for (int j = 0; j < rSize; j++) {
                ClusterPair cp = empiricalDistanceBounds(LHS.get(i), RHS.get(j), pairwiseDistances);
                double[] angleBounds = cp.getBounds();
                double lowerDot = angleToDot(angleBounds[1], w);
                double upperDot = angleToDot(angleBounds[0], w);

                //                Add to lower and upper
                betweenLowerDot += 2 * Wl[i] * Wr[j] * lowerDot;
                betweenUpperDot += 2 * Wl[i] * Wr[j] * upperDot;

                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, distToSim(angleBounds[1], w));
/**
 *              Add to index instructions
 *              If point dist grows ->
 *              dots shrink ->
 *              cluster dists grows ->
 *              sim becomes smaller ->
 *              bound lbSim on ubDists, ubSim on lbDists
 */
                lbIndexInstructions.add(new IndexInstruction(cp, true));
                ubIndexInstructions.add(new IndexInstruction(cp, false));
            }
        }


//        Get all pairwise within cluster (side) distances LHS
        for (int i = 0; i < lSize; i++) {
            for (int j = i+1; j < lSize; j++) {
                ClusterPair cp = empiricalDistanceBounds(LHS.get(i), LHS.get(j), pairwiseDistances);
                double[] angleBounds = cp.getBounds();
                double lowerDot = angleToDot(angleBounds[1], w);
                double upperDot = angleToDot(angleBounds[0], w);

//                Add to lower and upper
                withinLowerDot += 2 * Wl[i] * Wl[j] * lowerDot;
                withinUpperDot += 2 * Wl[i] * Wl[j] * upperDot;

                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, distToSim(angleBounds[1], w));

/**
 *              Add to index instructions
 *              If point dist grows ->
 *              dots shrink ->
 *              cluster dists shrinks ->
 *              sim becomes grows ->
 *              bound lbSim on lbDists, ubSim on ubDists
 */
                lbIndexInstructions.add(new IndexInstruction(cp, false));
                ubIndexInstructions.add(new IndexInstruction(cp, true));

            }
        }

        //        Get all pairwise within cluster (side) distances RHS
        for (int i = 0; i < rSize; i++) {
            for (int j = i+1; j < rSize; j++) {
                ClusterPair cp = empiricalDistanceBounds(RHS.get(i), RHS.get(j), pairwiseDistances);
                double[] angleBounds = cp.getBounds();
                double lowerDot = angleToDot(angleBounds[1], w);
                double upperDot = angleToDot(angleBounds[0], w);

//                Add to lower and upper
                withinLowerDot += 2 * Wr[i] * Wr[j] * lowerDot;
                withinUpperDot += 2 * Wr[i] * Wr[j] * upperDot;

                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, distToSim(angleBounds[1], w));

/**
 *              Add to index instructions
 *              If point dist grows ->
 *              dots shrink ->
 *              cluster dists shrinks ->
 *              sim becomes grows ->
 *              bound lbSim on lbDists, ubSim on ubDists
 */
                lbIndexInstructions.add(new IndexInstruction(cp, false));
                ubIndexInstructions.add(new IndexInstruction(cp, true));
            }
        }

        Pair<Double,Double> ws = getWeightSquaredSums(Wl, Wr);

        double normSum = w*(ws._1 + ws._2);

        double lowerDist = Math.sqrt(Math.max(0,normSum - betweenUpperDot + withinLowerDot));
        double upperDist = Math.sqrt(Math.max(0,normSum - betweenLowerDot + withinUpperDot));

//        Compute bounds
        double lower = 1 / (1 + upperDist);
        double upper = 1 / (1 + lowerDist);

        return new ClusterBounds(correctBound(lower), correctBound(upper), maxLowerBoundSubset, lbIndexInstructions, ubIndexInstructions);
    }
}

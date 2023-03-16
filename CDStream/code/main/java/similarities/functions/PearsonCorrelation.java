package similarities.functions;

import _aux.Pair;
import _aux.lib;
import _aux.lists.FastArrayList;
import _aux.lists.FastLinkedList;
import bounding.ClusterBounds;
import bounding.ClusterCombination;
import bounding.ClusterPair;
import clustering.Cluster;
import similarities.MultivariateSimilarityFunction;
import streaming.TimeSeries;
import streaming.index.DccIndex;
import streaming.index.ExtremaPair;
import streaming.index.IndexInstruction;

import java.util.LinkedList;

public class PearsonCorrelation extends MultivariateSimilarityFunction {
    public PearsonCorrelation() {
        distFunc = lib::angleFull;
    }

    @Override public boolean hasEmpiricalBounds() {return true;}
    @Override public boolean isTwoSided() {return true;}
    @Override public boolean canStream() {return true;}

    @Override public double sim(TimeSeries ts1, TimeSeries ts2){
        int w = ts1.getSlidingWindowSize();
        return lib.bound(ts1.runningDots[ts2.id] / w, -1,1);
    }
    @Override public double simToDist(double sim) {
        return Math.acos(sim);
    }
    @Override public double simToDist(double sim, int windowSize) {
        return Math.acos(sim);
    }
    @Override public double distToSim(double dist) {return Math.cos(dist);}
    @Override public double distToSim(double dist, int windowSize) {return Math.cos(dist);}

    public ClusterBounds getBounds(FastArrayList<Cluster> LHS, FastArrayList<Cluster> RHS, double[][] pairwiseDistances, double[] Wl, double[] Wr){
        double lower;
        double upper;
        double maxLowerBoundSubset = -1;

        Pair<Double, Double> weightSquares = getWeightSquaredSums(Wl, Wr);

//        Initialize structures that will hold the Cluster pairs of this CC
        int lSize = LHS.size();
        int rSize = RHS.size();
        FastArrayList<ClusterPair> nominatorClusterPairs = new FastArrayList<>(lSize * rSize);
        FastArrayList<ClusterPair> denominatorClusterPairs = new FastArrayList<>(lSize * (lSize-1)/2 + rSize * (rSize-1)/2);

        double nominator_lower = 0;
        double nominator_upper = 0;

        //nominator
        for (int i = 0; i < lSize; i++) {
            for (int j = 0; j < rSize; j++) {
                ClusterPair cp = empiricalDistanceBounds(LHS.get(i), RHS.get(j), pairwiseDistances);

//                Add to cluster pairs
                nominatorClusterPairs.add(cp);

//                Update nominator bounds
                double[] angleBounds = cp.getBounds();
                double[] simBounds = new double[]{this.distToSim(Math.min(Math.PI, angleBounds[1])),
                        this.distToSim(angleBounds[0])};
                nominator_lower += Wl[i] * Wr[j] * simBounds[0];
                nominator_upper += Wl[i] * Wr[j] * simBounds[1];
                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, simBounds[0]);
            }
        }

        //denominator: first sqrt
        double denominator_lower_left = weightSquares._1;
        double denominator_upper_left = weightSquares._1;

        for(int i=0; i< lSize; i++){
            for(int j=i+1; j< lSize; j++){
                ClusterPair cp = empiricalDistanceBounds(LHS.get(i), LHS.get(j), pairwiseDistances);

//                Add to cluster pairs
                denominatorClusterPairs.add(cp);

//                Update nominator bounds
                double[] angleBounds = cp.getBounds();
                double[] simBounds = new double[]{this.distToSim(Math.min(Math.PI, angleBounds[1])),
                        this.distToSim(angleBounds[0])};
                denominator_lower_left += 2 * Wl[i] * Wl[j] * simBounds[0];
                denominator_upper_left += 2 * Wl[i] * Wl[j] * simBounds[1];
                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, simBounds[0]);
            }
        }

        //denominator: second sqrt
        double denominator_lower_right = weightSquares._2;
        double denominator_upper_right = weightSquares._2;

        for(int i=0; i< rSize; i++){
            for(int j=i+1; j< rSize; j++){
                ClusterPair cp = empiricalDistanceBounds(RHS.get(i), RHS.get(j), pairwiseDistances);

//                Add to cluster pairs
                denominatorClusterPairs.add(cp);

//                Update nominator bounds
                double[] angleBounds = cp.getBounds();
                double[] simBounds = new double[]{this.distToSim(Math.min(Math.PI, angleBounds[1])),
                        this.distToSim(angleBounds[0])};
                denominator_lower_right += 2 * Wr[i] * Wr[j] * simBounds[0];
                denominator_upper_right += 2 * Wr[i] * Wr[j] * simBounds[1];
                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, simBounds[0]);
            }
        }

        //denominator: whole. note that if bounds are too loose we could get a non-positive value, while this is not possible due to Pos. Def. of variance.
        double denominator_lower = Math.sqrt(Math.max(denominator_lower_left, 1e-7)*Math.max(denominator_lower_right, 1e-7));
        double denominator_upper = Math.sqrt(Math.max(denominator_upper_left, 1e-7)*Math.max(denominator_upper_right, 1e-7));

//        Initialize structures that will hold the indexInstructions of this CC
        FastArrayList<IndexInstruction> lbIndexInstructions = new FastArrayList<>(nominatorClusterPairs.size() + denominatorClusterPairs.size());
        FastArrayList<IndexInstruction> ubIndexInstructions = new FastArrayList<>(nominatorClusterPairs.size() + denominatorClusterPairs.size());

        //nominator index instructions (mind that we switch here because we group based on distance, not similarity)
        for (ClusterPair cp : nominatorClusterPairs) {
            lbIndexInstructions.add(new IndexInstruction(cp, true));
            ubIndexInstructions.add(new IndexInstruction(cp, false));
        }

        //case distinction for final bound
        if (nominator_lower >= 0) {
            lower = nominator_lower / denominator_upper;
            upper = nominator_upper / denominator_lower;

            //denumerator index instructions (mind that we switch here because we group based on distance, not similarity)
            for (ClusterPair cp : denominatorClusterPairs){
                lbIndexInstructions.add(new IndexInstruction(cp, false));
                ubIndexInstructions.add(new IndexInstruction(cp, true));
            }
        } else if (nominator_lower < 0 && nominator_upper >= 0) {
            lower = nominator_lower / denominator_lower;
            upper = nominator_upper / denominator_lower;

            //denumerator index instructions (mind that we switch here because we group based on distance, not similarity)
            for (ClusterPair cp : denominatorClusterPairs) {
                lbIndexInstructions.add(new IndexInstruction(cp, true));
                ubIndexInstructions.add(new IndexInstruction(cp, true));
            }
        } else if (nominator_upper < 0) {
            lower = nominator_lower / denominator_lower;
            upper = nominator_upper / denominator_upper;

            //denumerator index instructions (mind that we switch here because we group based on distance, not similarity)
            for (ClusterPair cp : denominatorClusterPairs) {
                lbIndexInstructions.add(new IndexInstruction(cp, true));
                ubIndexInstructions.add(new IndexInstruction(cp, false));
            }
        } else {
            lower = -1000;
            upper = 1000;
        }

        return new ClusterBounds(correctBound(lower), correctBound(upper), maxLowerBoundSubset, lbIndexInstructions, ubIndexInstructions);
    }


//    Empirical bounds
    @Override public ClusterBounds empiricalSimilarityBounds(FastArrayList<Cluster> LHS, FastArrayList<Cluster> RHS, double[] Wl, double[] Wr, double[][] pairwiseDistances) {
        return getBounds(LHS, RHS, pairwiseDistances, Wl, Wr);
    }
}

package similarities.functions;

import _aux.lib;
import _aux.lists.FastArrayList;
import _aux.lists.FastLinkedList;
import bounding.ClusterBounds;
import clustering.Cluster;
import similarities.MultivariateSimilarityFunction;

import java.util.*;
import java.util.stream.IntStream;

public class TotalCorrelation extends MultivariateSimilarityFunction {

    private static final int bins = 10;
    private double[][] pairwiseEntropies;

    public TotalCorrelation() {
        distFunc = lib::euclidean;
//        distFunc = (a,b) -> 1 / (mutualInformation(a,b) + 1e-10);
        MAX_SIMILARITY = 3*lib.log2(bins);
        MIN_SIMILARITY = 0;
        SIMRANGE = MAX_SIMILARITY - MIN_SIMILARITY;
    }

    @Override
    public double getMaxApproximationSize(double ratio){
        return 6;
    }


    public static double[] discretize(double[] in){
        double[] out = new double[in.length];
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;

//        Find min and max (edges of bin linspace)
        for (double v : in) {
            min = Math.min(min, v);
            max = Math.max(max, v);
        }

//        Create linspace between min and max and map values to it
        double xrange = (max - min);
        for (int i = 0; i < in.length; i++) {
            out[i] = Math.floor((in[i] - min) / xrange * bins);
            if (out[i] == bins) out[i] = bins - 1;
        }
        return out;
    }

    public static double entropy(double[] in){
        double[] hist = new double[bins];
        for (double v : in) {
            hist[(int) v]++;
        }
        double out = 0;
        for (double v : hist) {
            if (v > 0) {
                double p = v / in.length;
                out += p * Math.log(p);
            }
        }
        return -out;
    }

    public static double jointEntropy(double[] a, double[] b){
        double[] hist = new double[bins*bins];
        for (int i = 0; i < a.length; i++) {
            hist[(int) (a[i] * bins + b[i])]++;
        }

        double out = 0;
        for (int i=0; i<hist.length; i++) {
            if (hist[i] > 0) {
                double p = hist[i] / a.length;
                out += p * Math.log(p);
            }
        }
        return -out;
    }

    public static double mutualInformation(double[] a, double[] b){
        return entropy(a) + entropy(b) - jointEntropy(a,b);
    }

//    Compute joint entropy over full set of points -- M being a row-wise matrix of points
//    H(A,B,C) = H(A) + H(B|A) + H(C|A,B) = H(A) + H(A,B) - H(A) + H(A,B,C) - H(A,B)
    public static double jointEntropy(double[][] M){
        Map<Integer, Integer> hist = new HashMap<>((int) Math.pow(bins, M.length));
        int n = M.length;
        int m = M[0].length;

//        Compute histogram
        for (int j = 0; j < m; j++) {
//            TODO IMPROVE
            int key = 1;
            for (int i = 0; i < n; i++) {
                key = bins * key + (int) M[i][j];
            }
            hist.put(key, hist.getOrDefault(key, 0) + 1);
        }

//        Compute entropy
        double out = 0;
        for (int v : hist.values()) {
            double p = v / (double) m;
            out += p * Math.log(p);
        }

        return -out;
    }

    @Override public boolean hasEmpiricalBounds() {return true;}
    @Override public boolean isTwoSided() {return false;}
    @Override public double[] preprocess(double[] vector) {
        return discretize(vector);
    }

    @Override public double sim(double[] x, double[] y) {
        return entropy(x) + entropy(y) - jointEntropy(x, y);
    }

    @Override public double simToDist(double sim) {
        return 100;
    }
    @Override public double distToSim(double dist) {

        return 1;
    }



    public static double totalCorrelation(FastArrayList<Cluster> clusters, double[][] pairwiseEntropies){
        double TC = 0;
//            Compute sum of entropies
        double[][] M = new double[clusters.size()][clusters.get(0).getCentroid().length];
        for (int i = 0; i < clusters.size(); i++) {
            Cluster c = clusters.get(i);
            double[] x = c.getCentroid();
            if (pairwiseEntropies == null){
                TC += entropy(x);
            } else {
                TC += pairwiseEntropies[c.centroidIdx][c.centroidIdx];
            }
            M[i] = x;
        }

//            Compute joint entropy
        double HC = jointEntropy(M);
        TC -= HC;
        return TC;
    }


    //    Pairwise distances in this case are the conditional entropies of the two variables
    @Override public double[][] computePairwiseDistances(double[][] data, boolean parallel) {
        int n = data.length;
        pairwiseEntropies = new double[n][n];

//        First get all the single entropies
        for (int i = 0; i < n; i++) {
            pairwiseEntropies[i][i] = entropy(data[i]);
        }

//        Then compute the conditional entropies
        lib.getStream(IntStream.range(0, n).boxed(), parallel).forEach(i -> {
            lib.getStream(IntStream.range(i+1, n).boxed(), parallel).forEach(j -> {
                pairwiseEntropies[i][j] = pairwiseEntropies[j][i] = jointEntropy(data[i], data[j]);
            });
        });

//        Lastly compute pairwiseDistances like any other metric (with euclidean distance in this case)
        return super.computePairwiseDistances(data, parallel);
    }

    @Override
    public double[] empiricalDistanceBounds(Cluster C1, Cluster C2, double[][] pairwiseEntropies){
        double[] c1EntropyBounds = C1.getEntropyBounds(pairwiseEntropies);
        double[] c2EntropyBounds = C2.getEntropyBounds(pairwiseEntropies);

        long ccID = hashPairwiseCluster(C1.id, C2.id);

        if (pairwiseClusterCache.containsKey(ccID)) {
            return pairwiseClusterCache.get(ccID);
        } else {
            double lb = Double.MAX_VALUE;
            double ub = -Double.MAX_VALUE;
            for (Integer i: C1.pointsIdx) {
                for (Integer j: C2.pointsIdx) {
                    double e = pairwiseEntropies[i][j];
                    lb = Math.min(lb, e);
                    ub = Math.max(ub, e);
                }
            }
            this.getStatBag().addStat(this.getStatBag().getNLookups(), () -> C1.size() * C2.size());

//            Joint entropies are always bigger than the largest single entropy (i.e. J(a,b) >= max(H(a), H(b))
            lb = Math.max(lb, Math.max(c1EntropyBounds[0], c2EntropyBounds[0]));
            ub = Math.max(ub, lb);

            double[] bounds = new double[]{lb, ub};
            pairwiseClusterCache.put(ccID, bounds);
            return bounds;
        }
    }

    @Override
    public ClusterBounds empiricalSimilarityBounds(FastArrayList<Cluster> LHS, FastArrayList<Cluster> RHS, double[] Wl, double[] Wr, double[][] pairwiseDistances) {
//        Mind that we do not have a RHS
//        LB = sum(lb_entropy(LHS)) - sum_ordered_i_to_p-1(ub_cEntropy(LHS[i], LHS[i+1]))
//        UB = sum(ub_entropy(LHS)) - max(lb_entropy(LHS))

//        Exact computation if only singleton clusters
        if (LHS.stream().noneMatch(c -> c.size() > 1)){
            double TC = totalCorrelation(LHS, pairwiseEntropies);
            return new ClusterBounds(TC,TC,0);
        }

        double lb = 0;
        double ub = 0;
//        TODO IMPLEMENT
        double maxLowerBoundSubset = 0;

        int p = LHS.size();

//        Compute/get joint entropy bounds
        double[][] jointEntropyUBs = new double[p][p];
        double maxJointEntropyLB = 0;
        for (int i = 0; i < p; i++) {
            for (int j = i; j < p; j++) {
                double[] entropyBounds;
                if (i==j){ // individual entropy
                    entropyBounds = LHS.get(i).getEntropyBounds(pairwiseEntropies);
                    lb += entropyBounds[0];
                    ub += entropyBounds[1];
                } else { // joint entropy
                    entropyBounds = empiricalDistanceBounds(LHS.get(i), LHS.get(j), pairwiseEntropies);
                    maxJointEntropyLB = Math.max(maxJointEntropyLB, entropyBounds[0]);
                }
                jointEntropyUBs[i][j] = jointEntropyUBs[j][i] = entropyBounds[1];
            }
        }

//        compute LB
        lb -= getJointEntropyUB(jointEntropyUBs);
        ub -= maxJointEntropyLB;

        return new ClusterBounds(lb, ub, maxLowerBoundSubset);
    }

    public double getJointEntropyUB(double[][] jointEntropyUBs){
//        Joint entropy UB = sum_i_to_p(lb_entropy(LHS[i]) - min_permutation sum_i_to_p-1(ub_cEntropy(LHS[i], LHS[i+1]))

        int p = jointEntropyUBs.length;

//        First compute conditional entropies H(I|J) = H(I,J) - H(J), and write to flat array
        double[] conditionalEntropiesFlat = new double[p*p];
        double[][] conditionalEntropies = new double[p][p];
        for (int i = 0; i < p; i++) {
            for (int j = 0; j < p; j++) {
                double ce = i==j ? Double.MAX_VALUE : Math.max(0,jointEntropyUBs[i][j] - jointEntropyUBs[j][j]);
                conditionalEntropies[i][j] = ce;
                conditionalEntropiesFlat[i*p + j] = ce;
            }
        }

//        Argsort the conditional entropies (ascending)
        int[] argSortedConditionalEntropiesFlat = IntStream.range(0, conditionalEntropiesFlat.length).boxed()
                .sorted(Comparator.comparingDouble(i -> conditionalEntropiesFlat[i])).mapToInt(ele -> ele).toArray();

        double jointEntropyUB = 0;

//        Iterate over sorted cEntropies and create ordering
        boolean[] used = new boolean[p];
        int nUsed = 0;
        for (int i = 0; i < argSortedConditionalEntropiesFlat.length; i++) {
            if (nUsed == p) break;

            int idx = argSortedConditionalEntropiesFlat[i];
            int i1 = idx / p;
            int i2 = idx % p;
            if (used[i1] || used[i2]) {
                continue;
            }
            used[i1] = true;
            nUsed++;
            jointEntropyUB += i1 == i2 ? jointEntropyUBs[i1][i1]: conditionalEntropies[i1][i2];
        }

        return jointEntropyUB;
    }



    //    Theoretical bounds -- not implemented for this similarity measure
    @Override public ClusterBounds theoreticalSimilarityBounds(FastArrayList<Cluster> LHS, FastArrayList<Cluster> RHS,
                                                               double[] Wl, double[] Wr, double[][] pairwiseDistances){
        return empiricalSimilarityBounds(LHS, RHS, Wl, Wr, pairwiseDistances);
    }



}

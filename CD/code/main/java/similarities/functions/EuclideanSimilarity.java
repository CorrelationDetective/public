package similarities.functions;

import _aux.Pair;
import _aux.lib;
import _aux.lists.FastArrayList;
import bounding.ClusterBounds;
import clustering.Cluster;
import similarities.MultivariateSimilarityFunction;


import static similarities.functions.MinkowskiSimilarity.aggCentroid;
import static similarities.functions.MinkowskiSimilarity.aggRadius;

public class EuclideanSimilarity extends MultivariateSimilarityFunction {
    public EuclideanSimilarity() {
        distFunc = lib::angle;
        MAX_SIMILARITY = 1;
        MIN_SIMILARITY = 0;
        SIMRANGE = MAX_SIMILARITY - MIN_SIMILARITY;
    }

    @Override public boolean hasEmpiricalBounds() {return true;}
    @Override public boolean isTwoSided() {return true;}
    @Override public double[] preprocess(double[] vector) {
        return lib.l2norm(vector);
    }

    @Override public double sim(double[] x, double[] y) {
        return 1 / (1 + Math.sqrt(2 - 2*Math.cos(this.distFunc.dist(x, y))));
    }

    @Override public double simToDist(double sim) {
        if (sim == 0) return Double.MAX_VALUE;

        double d = 1 / (sim) - 1;

//        d2 to dot
        return Math.acos(1 - ((d*d) / 2));
    }

    @Override public double distToSim(double dist) {
        return 1 / (1 + Math.sqrt(2 - 2*Math.cos(dist)));
    }

    @Override public ClusterBounds theoreticalSimilarityBounds(FastArrayList<Cluster> LHS, FastArrayList<Cluster> RHS,
                                                               double[] Wl, double[] Wr, double[][] pairwiseDistances){
//        Get representation of aggregated clusters
        double[] CXc = aggCentroid(LHS, Wl);
        double CXr = aggRadius(LHS, Wl);

        double[] CYc = aggCentroid(RHS, Wr);
        double CYr = aggRadius(RHS, Wr);

        double centroidDistance = lib.euclidean(CXc, CYc);

        double lowerDist = Math.max(0,centroidDistance - CXr - CYr);
        double upperDist = Math.max(0,centroidDistance + CXr + CYr);

        double lowerSim = 1 / (1 + upperDist);
        double upperSim = 1 / (1 + lowerDist);

//        Now get maxLowerBoundSubset
        double maxLowerBoundSubset = this.MIN_SIMILARITY;
        for (int i = 0; i < LHS.size(); i++) {
            for (int j = 0; j < RHS.size(); j++) {
                double[] angleBounds = theoreticalDistanceBounds(LHS.get(i), RHS.get(j), pairwiseDistances);
                double simBound = this.distToSim(Math.min(Math.PI, angleBounds[1]));

                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, simBound);
            }
        }

        return new ClusterBounds(correctBound(lowerSim), correctBound(upperSim), maxLowerBoundSubset);
    }

    @Override public ClusterBounds empiricalSimilarityBounds(FastArrayList<Cluster> LHS, FastArrayList<Cluster> RHS, double[] Wl, double[] Wr, double[][] pairwiseDistances){
        double betweenLowerDot = 0;
        double betweenUpperDot = 0;

        double withinLowerDot = 0;
        double withinUpperDot = 0;

        double maxLowerBoundSubset = this.MIN_SIMILARITY;

//        Get all pairwise between cluster distances
        for (int i = 0; i < LHS.size(); i++) {
            for (int j = 0; j < RHS.size(); j++) {
                double[] bounds = empiricalDistanceBounds(LHS.get(i), RHS.get(j), pairwiseDistances);
                double dot0 = Math.cos(bounds[0]);
                double dot1 = Math.cos(bounds[1]);
                betweenLowerDot += 2 * Wl[i] * Wr[j] * Math.min(dot0,dot1);
                betweenUpperDot += 2 * Wl[i] * Wr[j] * Math.max(dot0,dot1);
                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, distToSim(bounds[1]));
            }
        }


//        Get all pairwise within cluster (side) distances LHS
        for (int i = 0; i < LHS.size(); i++) {
            for (int j = i+1; j < LHS.size(); j++) {
                double[] bounds = empiricalDistanceBounds(LHS.get(i), LHS.get(j), pairwiseDistances);
                double dot0 = Math.cos(bounds[0]);
                double dot1 = Math.cos(bounds[1]);
                withinLowerDot += 2 * Wl[i] * Wl[j] * Math.min(dot0,dot1);
                withinUpperDot += 2 * Wl[i] * Wl[j] * Math.max(dot0,dot1);
                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, distToSim(bounds[1]));
            }
        }

        //        Get all pairwise within cluster (side) distances RHS
        for (int i = 0; i < RHS.size(); i++) {
            for (int j = i+1; j < RHS.size(); j++) {
                double[] bounds = empiricalDistanceBounds(RHS.get(i), RHS.get(j), pairwiseDistances);
                double dot0 = Math.cos(bounds[0]);
                double dot1 = Math.cos(bounds[1]);
                withinLowerDot += 2 * Wr[i] * Wr[j] * Math.min(dot0,dot1);
                withinUpperDot += 2 * Wr[i] * Wr[j] * Math.max(dot0,dot1);
                maxLowerBoundSubset = Math.max(maxLowerBoundSubset, distToSim(bounds[1]));
            }
        }

        Pair<Double, Double> weightSquares = getWeightSquaredSums(Wl, Wr);
        double wSqSum = weightSquares.x + weightSquares.y;

//        Compute bounds
        double lowerD = Math.sqrt(Math.max(0,wSqSum - betweenUpperDot + withinLowerDot));
        double upperD = Math.sqrt(Math.max(0,wSqSum - betweenLowerDot + withinUpperDot));

        double lower = 1 / (1 + upperD);
        double upper = 1 / (1 + lowerD);

        return new ClusterBounds(correctBound(lower), correctBound(upper), maxLowerBoundSubset);
    }




}

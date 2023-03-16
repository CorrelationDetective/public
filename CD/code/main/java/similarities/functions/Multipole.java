package similarities.functions;

import Jama.Matrix;
import _aux.lib;
import _aux.lists.FastArrayList;
import bounding.ClusterBounds;
import clustering.Cluster;
import similarities.MultivariateSimilarityFunction;


public class Multipole extends MultivariateSimilarityFunction {
    public Multipole() {
        distFunc = lib::angle;
    }

    @Override public boolean hasEmpiricalBounds() {return true;}
    @Override public boolean isTwoSided() {return false;}
    @Override public double[] preprocess(double[] vector) {
        return lib.l2norm(vector);
    }

    @Override public double sim(double[] x, double[] y) {
        return Math.min(Math.max(lib.dot(x, y), -1),1);
    }

    @Override public double simToDist(double sim) {
        return Math.acos(sim);
    }
    @Override public double distToSim(double dist) {return Math.cos(dist);}

    @Override public ClusterBounds empiricalSimilarityBounds(FastArrayList<Cluster> LHS, FastArrayList<Cluster> RHS, double[] Wl, double[] Wr, double[][] pairwiseDistances) {
        return getBounds(LHS, RHS, pairwiseDistances, Wl, Wr, true);
    }

    //    Theoretical bounds
    @Override public ClusterBounds theoreticalSimilarityBounds(FastArrayList<Cluster> LHS, FastArrayList<Cluster> RHS, double[] Wl, double[] Wr, double[][] pairwiseDistances){
        return getBounds(LHS, RHS, pairwiseDistances, Wl, Wr, false);
    }

    public ClusterBounds getBounds(FastArrayList<Cluster> LHS, FastArrayList<Cluster> RHS, double[][] pairwiseDistances, double[] Wl, double[] Wr, boolean empirical){
        double lower;
        double upper;

        if (RHS.size() > 0){
            throw new IllegalArgumentException("RHS must be empty for one-sided bounds");
        }

        double[][] lowerBoundsArray = new double[LHS.size()][LHS.size()];
        double[][] upperBoundsArray = new double[LHS.size()][LHS.size()];
        double highestAbsLowerBound = 0;

        // create upper and lower bound matrices U and L as described in paper
        for(int i=0; i< LHS.size(); i++) {
            // we can fill the diagonal with 1's since we always pick one vector from each cluster
            lowerBoundsArray[i][i] = 1;
            upperBoundsArray[i][i] = 1;
            Cluster c1 = LHS.get(i);
            for (int j = i + 1; j < LHS.size(); j++) {
                Cluster c2 = LHS.get(j);
                double[] angleBounds = empirical ? empiricalDistanceBounds(c1, c2, pairwiseDistances) : theoreticalDistanceBounds(c1, c2, pairwiseDistances);
                double[] simBounds = new double[]{this.distToSim(Math.min(Math.PI, angleBounds[1])),
                        this.distToSim(angleBounds[0])};

                if (simBounds[0] > 0) {
                    highestAbsLowerBound = Math.max(highestAbsLowerBound, simBounds[0]); // smaller angle = higher similarity
                } else if (simBounds[1] < 0) {
                    highestAbsLowerBound = Math.max(highestAbsLowerBound, Math.abs(simBounds[1]));
                }

                lowerBoundsArray[i][j] = simBounds[0];
                lowerBoundsArray[j][i] = simBounds[0];

                upperBoundsArray[i][j] = simBounds[1];
                upperBoundsArray[j][i] = simBounds[1];
            }
        }

        // Calculate bounds on multipoles as described in Section: Application to Multipoles
        // Use jama for linear algebra. possible alternative: OjAlgo backend
        Matrix upperPairwise2 = new Matrix(upperBoundsArray);
        Matrix lowerPairwise2 = new Matrix(lowerBoundsArray);

        Matrix estimateMat2 = upperPairwise2.plus(lowerPairwise2).times(0.5);
        Matrix slackMat2 = upperPairwise2.minus(lowerPairwise2);

        double[] eigenVals2 = estimateMat2.eig().getRealEigenvalues();
        double slack2 = slackMat2.norm2();

        double smallestEig = 1;
        for(double e : eigenVals2){
            if(e < smallestEig){
                smallestEig = e;
            }
        }

        lower = 1 - (smallestEig + 0.5 * slack2);
        upper = 1 - (smallestEig - 0.5 * slack2);

        return new ClusterBounds(correctBound(lower), correctBound(upper), highestAbsLowerBound);
    }

}

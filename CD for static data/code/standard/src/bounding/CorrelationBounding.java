package bounding;

import Jama.Matrix;
import _aux.lib;
import clustering.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;


public class CorrelationBounding {

    public int n_comparisons;
    public double[][] pointCorrelations;
    public ConcurrentHashMap<Long, double[]> bounds2Global;
    public ConcurrentHashMap<Long, double[]> empiricalBounds2Global;// = new ConcurrentHashMap<>((int) (2153509/0.4), 0.4f);
    public int num_clusters;
    public int n_vec; public int n_dim;
    public double[][] data;
//    public int n_2bound_comparisons;
//    public long n_2corr_lookups;
    public double timeForLA;
    public double temp;
    boolean warningPrint = false;




//    Writer out;
//
//    {
//        try {
//            out = new FileWriter("loose_theoretical_bounds.txt");
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public PrintWriter writer = new PrintWriter(out);



    public CorrelationBounding(double[][] data, boolean useEmpiricalBounds, boolean parallel){
        this.data = data;
        this.n_vec = data.length;
        this.n_dim = data[0].length;

        this.n_comparisons=0;
//        this.n_2bound_comparisons = 0;
//        this.n_2corr_lookups=0;
        this.timeForLA = 0;
        this.temp = 0;



//            double highestPairwise = -1;
        this.pointCorrelations = new double[n_vec][n_vec];
        IntStream ints = IntStream.range(0, n_vec);
        if(parallel){
            ints = ints.parallel();
        }

        ints.forEach(i -> {
            this.pointCorrelations[i][i] = 1;
            for(int j=i+1; j<n_vec; j++){
                this.pointCorrelations[i][j] = lib.pearsonWithAlreadyNormedVectors(data[i], data[j]);
                this.pointCorrelations[j][i] = this.pointCorrelations[i][j];
//                    highestPairwise = Math.max(highestPairwise, this.pointCorrelations[i][j]);
//                    this.n_2bound_comparisons++;
            }
        });




//            System.out.println(highestPairwise);


    }

    public void initializeBoundsCache(int num_clusters, boolean useEmpiricalBounds){
        this.num_clusters = num_clusters;
        empiricalBounds2Global = new ConcurrentHashMap<>(num_clusters*num_clusters, 0.4f);
        if(useEmpiricalBounds){

        }else{
            bounds2Global = new ConcurrentHashMap<>(num_clusters*num_clusters, 0.4f);
        }

    }

    public void calcBound(ClusterCombination CC, boolean getEmpirical){
        if(CC.isMultiPearsonCandidate()){
            // pattern is multiPearson
            calcMultiPearsonBound((MultiPearsonClusterCombination) CC, getEmpirical);
        }else if(CC.isMultipoleCandidate()){
            // pattern is multipole
            calcMultipoleBound((MultipoleClusterCombination) CC, getEmpirical);
        }else if(CC.isCCACandidate()){
            calcCCABound((CCAClusterCombination) CC, getEmpirical);
        }

//        if(CC.canBeSplit()){
//            // now that we calculated the bound, let's get the slack between the bound and the actual max/min values
//            // not the fastest way but only for testing anyway. only multipoles
//            double min = 1;
//            double max = 0;
//            double corr;
//
//            ArrayList<ClusterCombination> singletons = CC.getSingletonClusters();
//            List<Double> corrs = new ArrayList<>();
//            for( ClusterCombination scc : singletons){
//                calcBound(scc, getEmpirical);
//                corr = scc.getLB();
//                if(Math.abs(corr - scc.getUB()) > 0.001){
//                    System.err.println("corrbounding: checking actual values on non-singleton clusters");
//                }
//                corrs.add(corr);
//
//            }
//            StringBuilder corrstring = new StringBuilder();
//            for(double c : corrs){
//                corrstring.append(c).append(", ");
//            }
//            writer.println(CC.getLB() + "\t" + CC.getUB() +"\t" + corrstring);
//        }

    }

    private void calcCCABound(CCAClusterCombination cc, boolean getEmpirical) {

        // TODO: the cca metric is not yet supported, the bounds are not correct!
//        if(!warningPrint){
//            System.err.println("------------------------------------------------------\n\n");
//            System.err.println("The CCA metric is not yet supported!");
//            System.err.println("------------------------------------------------------\n\n");
//            warningPrint=true;
//        }
//
//
//        //comment out the piece below to start experimenting
//        cc.setLB(-1.);
//        cc.setUB(-1.);
//        return;
        // -------------




//
        double[][] crossCorrLB = new double[cc.getLHS().size()][cc.getRHS().size()];
        double[][] crossCorrUB = new double[cc.getLHS().size()][cc.getRHS().size()];
        List<Cluster> LHS = cc.getLHS();
        List<Cluster> RHS = cc.getRHS();

        double[][] corrLHSLB = new double[LHS.size()][LHS.size()];
        double[][] corrLHSUB = new double[LHS.size()][LHS.size()];
        double[][] corrRHSLB = new double[RHS.size()][RHS.size()];
        double[][] corrRHSUB = new double[RHS.size()][RHS.size()];

        for(int i=0; i<LHS.size(); i++){
            Cluster c1 = LHS.get(i);
            for(int j=0; j< RHS.size(); j++){
                Cluster c2 = RHS.get(j);
                double[] pairwiseBounds = get2BoundOrCompute(c1, c2, getEmpirical);
                double highestAbsValue = Math.max(Math.abs(pairwiseBounds[0]), Math.abs(pairwiseBounds[1]));
                double lowestAbsValue = Math.min(Math.abs(pairwiseBounds[0]), Math.abs(pairwiseBounds[1]));
                crossCorrLB[i][j] = -highestAbsValue;
                crossCorrUB[i][j] = highestAbsValue;
                cc.checkAndSetMaxSubsetLowerBound(lowestAbsValue);
            }
        }

        for(int i=0; i<LHS.size(); i++){
            Cluster c1 = LHS.get(i);
            corrLHSLB[i][i] = 1; corrLHSUB[i][i] = 1;

            for(int j=i+1; j<LHS.size(); j++){
                Cluster c2 = LHS.get(j);
                double[] pairwiseBounds = get2BoundOrCompute(c1, c2, getEmpirical);
                double highestAbsValue = Math.max(Math.abs(pairwiseBounds[0]), Math.abs(pairwiseBounds[1]));
                double lowestAbsValue = Math.min(Math.abs(pairwiseBounds[0]), Math.abs(pairwiseBounds[1]));
                corrLHSLB[i][j] = - highestAbsValue; corrLHSLB[j][i] = corrLHSLB[i][j];
                corrLHSUB[i][j] = highestAbsValue; corrLHSUB[j][i] = corrLHSUB[i][j];
                cc.checkAndSetMaxSubsetLowerBound(lowestAbsValue);
            }
        }

        for(int i=0; i<RHS.size(); i++){
            Cluster c1 = RHS.get(i);
            corrRHSLB[i][i] = 1; corrRHSUB[i][i] = 1;

            for(int j=i+1; j<RHS.size(); j++){
                Cluster c2 = RHS.get(j);
                double[] pairwiseBounds = get2BoundOrCompute(c1, c2, getEmpirical);
                double highestAbsValue = Math.max(Math.abs(pairwiseBounds[0]), Math.abs(pairwiseBounds[1]));
                double lowestAbsValue = Math.min(Math.abs(pairwiseBounds[0]), Math.abs(pairwiseBounds[1]));
                corrRHSLB[i][j] = - highestAbsValue; corrRHSLB[j][i] = corrRHSLB[i][j];
                corrRHSUB[i][j] = highestAbsValue; corrRHSUB[j][i] = corrRHSUB[i][j];
                cc.checkAndSetMaxSubsetLowerBound(lowestAbsValue);
            }
        }

        Matrix LBCrossmat = new Matrix(crossCorrLB);
        Matrix UBCrossmat = new Matrix(crossCorrUB);
        Matrix LBLHSmat = new Matrix(corrLHSLB);
        Matrix UBLHSmat = new Matrix(corrLHSUB);
        Matrix LBRHSmat = new Matrix(corrRHSLB);
        Matrix UBRHSmat = new Matrix(corrRHSUB);

        Matrix prodLB;
        Matrix prodUB;

        try {
            prodLB = LBLHSmat.inverse().times(LBCrossmat).times(LBRHSmat.inverse()).times(LBCrossmat.transpose());
            prodUB = UBLHSmat.inverse().times(UBCrossmat).times(UBRHSmat.inverse()).times(UBCrossmat.transpose());
        } catch(Exception e){
            cc.checkAndSetLB(0); cc.checkAndSetUB(1);
            return;
        }

        double[] eigLower = prodLB.eig().getRealEigenvalues();
        double[] eigUpper = prodUB.eig().getRealEigenvalues();

        double lower = 0;
        double upper = 0;
        for(int i = 0; i < eigLower.length; i++){
            if(eigLower[i] > lower){
                lower = eigLower[i];
            }
            if(eigUpper[i] > upper){
                upper = eigUpper[i];
            }
        }

        cc.checkAndSetLB(Math.sqrt(lower));
        cc.checkAndSetUB(Math.sqrt(upper));

    }


    public double calc2CorrTheoryBound(boolean lowerBound, double centroidCorr, double maxDist1, double maxDist2, int n_dim) {
        // calculate one 2-corr bound according to the formula derived in the theory
        double angleEstimate = Math.acos(Math.max(Math.min(centroidCorr, 1), -1)); // min and max added for floating point error robustness
        double angleDeviation1 = Math.acos(1 - Math.pow(maxDist1, 2) / (2 * n_dim));
        double angleDeviation2 = Math.acos(1 - Math.pow(maxDist2, 2) / (2 * n_dim));
        double angleDeviation = angleDeviation1 + angleDeviation2;
        if (!lowerBound) {
            double upper = Math.cos(Math.max(angleEstimate - angleDeviation, 0));
            upper = Math.min(upper, 1);
            if(Double.isNaN(upper)){
                System.err.println("nan pairwise bound");
            }
            return upper;
        } else{
            double lower = Math.cos(Math.min(angleEstimate + angleDeviation, Math.PI));
            lower = Math.max(lower, -1);
            if(Double.isNaN(lower)){
                System.err.println("nan pairwise bound");
            }
            return lower;
        }
    }



    public void calcMultipoleBound(MultipoleClusterCombination CC, boolean getEmpirical) { // it returns an array of lower, higher
//        double[] result = new double[3];
        double lower;
        double upper;
        ArrayList<Cluster> clusters = CC.getClusters();

        double geoMean = CC.getRadiiGeometricMean();


        double[][] upperBoundsArray = new double[clusters.size()][clusters.size()];
        double[][] lowerBoundsArray = new double[clusters.size()][clusters.size()];

        double highestAbsLowerBound = 0;
//        double highestAbsUpperBound = 0;

//        int[] clustersizes = new int[CC.getClusters().size()];

        // create upper and lower bound matrices U and L as described in paper
        for(int i=0; i< clusters.size(); i++){
            // we can fill the diagonal with 1's since we always pick one vector from each cluster
            lowerBoundsArray[i][i] = 1;
            upperBoundsArray[i][i] = 1;
            Cluster c1 = clusters.get(i);
//            clustersizes[i] = c1.getNPoints();
            for(int j=i+1; j< clusters.size(); j++){
                Cluster c2 = clusters.get(j);
                double[] bounds = get2BoundOrCompute(c1, c2, getEmpirical);

                if(bounds[0] > 0){
                    highestAbsLowerBound = Math.max(highestAbsLowerBound, bounds[0]); // we know for sure there is a pairwise correlation of strength at least LB
                }else if(bounds[1] < 0){
                    highestAbsLowerBound = Math.max(highestAbsLowerBound, Math.abs(bounds[1])); // we know for sure there is a pairwise correlation of strength at least |UB|
                }

//                highestAbsUpperBound = Math.max(highestAbsUpperBound, Math.abs(bounds[1]));

                lowerBoundsArray[i][j] = bounds[0]; lowerBoundsArray[j][i] = bounds[0];

                upperBoundsArray[i][j] = bounds[1]; upperBoundsArray[j][i] = bounds[1];


            }
        }

        // calculate bounds on multipoles as described in Section: Application to Multipoles

        // use jama for linear algebra. possible alternative: OjAlgo backend

        Matrix upperPairwise2 = new Matrix(upperBoundsArray);
        Matrix lowerPairwise2 = new Matrix(lowerBoundsArray);

        Matrix estimateMat2 = upperPairwise2.plus(lowerPairwise2).times(0.5);
        Matrix slackMat2 = upperPairwise2.minus(lowerPairwise2);




        double[] eigenVals2 = estimateMat2.eig().getRealEigenvalues();
        double slack2 = slackMat2.norm2();
//        long stop = System.currentTimeMillis();
//        this.temp += (stop-start);
//        if(this.temp > Double.MAX_VALUE/10){
//            this.timeForLA += this.temp/1000;
//            this.temp=0;
//        }

        double smallestEig = 1;
        for(double e : eigenVals2){
            if(e < smallestEig){
                smallestEig = e;
            }
        }



        upper = 1 - (smallestEig - 0.5 * slack2);
        lower = 1 - (smallestEig + 0.5 * slack2);

        lower = Math.max(lower, CC.getLB());
        upper = Math.min(upper, CC.getUB());

        CC.checkAndSetLB(lower); CC.checkAndSetUB(upper);
        CC.setCenterOfBounds((lower+upper)/2);
        CC.setSlack(Math.max(upper - CC.getCenterOfBounds(), 0)); // max for floating point errors

        //check if any 2-correlation exceeds the highest LB so far, but only if we're not considering 2-correlations!
        if(CC.getClusters().size() > 2){
            CC.checkAndSetMaxSubsetLowerBound(highestAbsLowerBound);
        }



//        boolean nonTrivial = true;
//        for(Cluster c: clusters){
//            if(c.listOfContents.size()<10){
//                nonTrivial=false;
//                break;
//            }
//        }
//
//        if(upper<0.95 && nonTrivial){
//            System.out.println("we found a decisive upper bound");
//        }


//        result[0] = lower; result[1] = upper;
//
//
//
//        // minimum jump from 2-corr:
//
//        result[2] = highestLowerBound; //todo: minjump change this to highestLowerBound

//        if(upper < 0.95 && upper - lower > 0.05){
//            System.out.println("upper < 95");
//        }


//        System.out.print("number of combinations: " + CC.getNComparisons());
//
//        System.out.print(" bounds: " + lower + " " + upper + " " + highestLowerBound);
//        System.out.print(" decisive: " + ((upper<0.95) || (lower >= 0.95)));
//        System.out.println();



//        return result;
    }

    public void calcMultiPearsonBound(MultiPearsonClusterCombination CC, boolean getEmpirical) { // it returns an array of lower, higher,
        double[] result = new double[3];
        double lower;
        double upper;
        double max2CorrLowerBound = -1;

        double nominator_lower = 0;
        double nominator_upper = 0;

        double geoMean = CC.getRadiiGeometricMean();



        ArrayList<Cluster> LHS = CC.getLHS(); ArrayList<Cluster> RHS = CC.getRHS();

        //numerator: (nominator -- dyslexia strikes?!)
        for(Cluster c1 : LHS){
            for(Cluster c2: RHS){
                double[] bounds = get2BoundOrCompute(c1, c2, getEmpirical);
                nominator_lower += bounds[0];
                nominator_upper+= bounds[1];
                max2CorrLowerBound = Math.max(bounds[0], max2CorrLowerBound);
            }
        }


        //denominator: first sqrt
        double denominator_lower_left = LHS.size();
        double denominator_upper_left = LHS.size();

        for(int i=0; i< LHS.size(); i++){
            Cluster c1 = LHS.get(i);
            for(int j=i+1; j< LHS.size(); j++){
                Cluster c2 = LHS.get(j);
                double[] bounds = get2BoundOrCompute(c1, c2, getEmpirical);
                denominator_lower_left += 2*bounds[0];
                denominator_upper_left += 2*bounds[1];
                max2CorrLowerBound = Math.max(bounds[0], max2CorrLowerBound);
            }
        }


        //denominator: second sqrt
        double denominator_lower_right = RHS.size();
        double denominator_upper_right = RHS.size();

        for(int i=0; i< RHS.size(); i++){
            Cluster c1 = RHS.get(i);
            for(int j=i+1; j< RHS.size(); j++){
                Cluster c2 = RHS.get(j);
                double[] bounds = get2BoundOrCompute(c1, c2, getEmpirical);
                denominator_lower_right += 2*bounds[0];
                denominator_upper_right += 2*bounds[1];
                max2CorrLowerBound = Math.max(bounds[0], max2CorrLowerBound);
            }
        }




        //denominator: whole. note that if bounds are too loose we could get a non-positive value, while this is not possible due to Pos. Def. of variance.
        double denominator_lower = Math.sqrt(Math.max(denominator_lower_left, 1e-7)*Math.max(denominator_lower_right, 1e-7)); // in case we have big clusters, we can get the scenario where the bounds are negative -- not possible!
        double denominator_upper = Math.sqrt(Math.max(denominator_upper_left, 1e-7)*Math.max(denominator_upper_right, 1e-7));


        //case distinction for final bound
        if (nominator_lower >= 0) {
            lower = nominator_lower / denominator_upper;
            upper = nominator_upper / denominator_lower;
        } else if (nominator_lower < 0 && nominator_upper >= 0) {
            lower = nominator_lower / denominator_lower;
            upper = nominator_upper / denominator_lower;
        } else if (nominator_upper < 0) {
            lower = nominator_lower / denominator_lower;
            upper = nominator_upper / denominator_upper;
        } else {
            lower = -1000;
            upper = 1000;
            System.out.println("debug: " + nominator_lower + nominator_upper + denominator_lower + denominator_upper);
        }



        lower = Math.max(lower, CC.getLB());
        upper = Math.min(upper, CC.getUB());
        CC.setCenterOfBounds((lower+upper)/2);
        CC.setSlack(upper - CC.getCenterOfBounds());
        CC.checkAndSetMaxSubsetLowerBound(max2CorrLowerBound);
//        if (c1.listOfContents.size() == 1 && c2.listOfContents.size() == 1 && c3.listOfContents.size() == 1 && Math.abs(upper - lower) > 1E-10) {
//            System.err.println("debug: all clusters of size 1 but still a loose bound");
//            System.err.println("clusters: " + c1.getClusterId() +" " + c2.getClusterId() + " " + c3.getClusterId()+ ". Bounds: " + lower + " " + upper +". numer lower/upper, denom lower/upper: " + nominator_lower + " " + nominator_upper + " " + denominator_lower + " " + denominator_upper);
//        }

        // bugdetection:
        if(Double.isNaN(lower) || Double.isNaN(upper)){
            System.err.println("debug CorrelationBounding.java: NaN upper and/or lower bound on multiple correlation");
        }




        CC.checkAndSetLB(lower); CC.checkAndSetUB(upper);

    }


    public double[] get2BoundOrCompute(Cluster c1, Cluster c2, boolean getEmpirical) {
        double lower;
        double upper;
        double corr;
        int id1 = c1.getClusterId(); int id2 = c2.getClusterId();
        long comparisonSignature = getUniqueId(id1, id2);
        if (!getEmpirical) {
            double[] computedBounds = bounds2Global.get(comparisonSignature);
            if (computedBounds == null) { // the key is missing in the bounds hash table
                if (c1.getClusterId() == c2.getClusterId() && c1.listOfContents.size() == 1) {
                    lower = 1;
                    upper = 1;
                } else {
                    corr = this.pointCorrelations[c1.getCentroid()][c2.getCentroid()];
                    lower = this.calc2CorrTheoryBound(true, corr, c1.getMaxDist(), c2.getMaxDist(), n_dim);
                    if (!(id1 == id2)) { // clusters are not identical
                        upper = this.calc2CorrTheoryBound(false, corr, c1.getMaxDist(), c2.getMaxDist(), n_dim);
                    } else {
                        upper = 1;
                    }

                    if (c1.listOfContents.size() == 1 && c2.listOfContents.size() == 1 && Math.abs(lower - upper) > 1E-6) {
                        System.out.println("debug: both max dists are 0 and bounds are far apart");
                    }
                }


                double[] result = new double[]{lower, upper};
                bounds2Global.put(comparisonSignature, result);
//                this.n_2bound_comparisons++;
                return result;
            } else {
                if (c1.listOfContents.size() == 1 && c2.listOfContents.size() == 1 && Math.abs(computedBounds[1]-computedBounds[0]) > 1e-6 ) {
                    System.out.println("debug: cluster sizes 1 and bounds are far apart");
                }
                return computedBounds;
            }
        }else{
            double[] empiricalBounds = this.empiricalBounds2Global.get(comparisonSignature);

            if (empiricalBounds == null) { // missing in the bounds hash table
//                this.n_2corr_lookups++;
                if (c1 == c2 && c1.listOfContents.size() == 1) {
                    lower=1; upper=1;
                } else {
                    lower = 1;
                    upper = -1;

                    outerloop:
                    for(int pID1 : c1.listOfContents){
                        for(int pID2 : c2.listOfContents){

//                            this.n_2corr_lookups++;

                            corr = this.pointCorrelations[pID1][pID2];
                            // corr<0.95 remove this when done testing; can we prune some more with this??
                            lower = Math.min(corr, lower);
                            upper=Math.max(corr,upper);
                            if(lower==-1 && upper == 1){
                                break outerloop;
                            }


                        }
                    }

                }
//                Collections.sort(correlations);


                double[] result = new double[]{lower, upper};
//                double[] result = new double[correlations.size()];
//                for(int i=0; i<correlations.size(); i++){ // put the sorted correlations in the result array
//                    result[i] = correlations.get(i);
//                }

//                if(c1.listOfContents.size() == 1 && c2.listOfContents.size() == 1 && Math.abs(upper-lower) > 1e-10){
//                    System.err.println("debug: 2 clusters of size 1 and a loose bound");
//                }
                empiricalBounds2Global.put(comparisonSignature, result);


                return result;
            } else
            if(c1.listOfContents.size() == 1 && c2.listOfContents.size() == 1 && Math.abs(empiricalBounds[empiricalBounds.length-1]-empiricalBounds[0]) > 1e-10){
                System.err.println("debug: 2 clusters of size 1 and a loose bound");
            }
            return empiricalBounds;
        }
    }

    long getUniqueId(int id1, int id2) {
        if (id1 < id2) {
            return (long) id1 * num_clusters + id2;
        } else {
            return (long) id2 * num_clusters + id1;
        }
    }








}

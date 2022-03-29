package clustering;

import bounding.CorrelationBounding;

import java.util.ArrayList;

public abstract class ClusterCombination {

    boolean isMultipoleCandidate = false;
    boolean isMultiPearsonCandidate = false;
    boolean isDuplicate = false;
    boolean isDecisive = false;
    boolean isPositive = false;
    int nComparisons = -1;
    double LB = -Double.MAX_VALUE;
    double UB = Double.MAX_VALUE;
    double maxLowerBoundSubset = -1;
    double slack;
    double criticalShrinkFactor;
    double centerOfBounds;
    boolean isCCACandidate;

    public ClusterCombination() {
    }

    public void setPositive(boolean isPositive){
        this.isPositive = isPositive;
    }

    public boolean isPositive(){
        return this.isPositive;
    }

    public boolean isDuplicate(){
        // todo: fix this

        return this.isDuplicate;
    }

    public boolean isMultipoleCandidate(){
        return this.isMultipoleCandidate;
    }

    public boolean isMultiPearsonCandidate(){
        return this.isMultiPearsonCandidate;
    }

    public abstract ArrayList<ClusterCombination> getSplittedCC();

    public boolean canBeSplit(){
         for(Cluster c : this.getClusters()){
             if(c.getNPoints() > 1){
                 return true;
             }
         }
         return false;
    }

    public boolean isDecisive(){
        return this.isDecisive;
    }
    public void setDecisive(boolean bool){
        this.isDecisive = bool;
    }

    public abstract ArrayList<Cluster> getClusters();

    public int getNComparisons(){

        if(this.nComparisons <= 0){
            int out = 1;
            for(Cluster c: this.getClusters()){
                out *= c.getNPoints();
            }
            this.nComparisons = out;
            return out;
        }else{
            return this.nComparisons;
        }
    }

    public void checkAndSetMaxSubsetLowerBound(double LB){
        if(LB > this.maxLowerBoundSubset){
            this.maxLowerBoundSubset=LB;
        }
    }

    public double getMaxLowerBoundSubset(){
        return this.maxLowerBoundSubset;
    }

    public void checkAndSetLB(double LB){
        this.LB = Math.max(LB, this.LB);
    }

    public void checkAndSetUB(double UB){
        this.UB = Math.min(UB, this.UB);
    }

    public double getLB(){
        return this.LB;
    }

    public double getUB(){
        return this.UB;
    }

    public abstract double getMaxSubsetCorr(CorrelationBounding CB);

    public abstract ArrayList<ClusterCombination> getSingletonClusters();

    public abstract ArrayList<Cluster> getLHS();

    public abstract ArrayList<Cluster> getRHS();

    public double getRadiiGeometricMean(){
        double out = 1;
        for(Cluster c: this.getClusters()){
            out *= c.getMaxDist();
        }
        return Math.pow(out, 1.0/this.getClusters().size());
    }

    public double getNPointsGeometricMean(){
        double out = 1;
        for(Cluster c : this.getClusters()){
            out *= c.getNPoints();
        }
        return Math.pow(out, 1.0/this.getClusters().size());
    }

    public void setSlack(double slack){
        if(Double.isNaN(slack) || slack < 0){
            System.err.println("warning: this CC is being set with an incorrect slack!");
        }
        this.slack = slack;
    }

    public double getSlack() {
        return slack;
    }

    public void setCriticalShrinkFactor(double criticalShrinkFactor) {
        if(Double.isNaN(criticalShrinkFactor)){
            System.err.println("warning: this clustercombination has a NaN critical shrinkfactor!");
        }
        this.criticalShrinkFactor = criticalShrinkFactor;
    }

    public double getCriticalShrinkFactor(){
        return this.criticalShrinkFactor;
    }

    public void setCenterOfBounds(double center){
        if(Double.isNaN(center)){
            System.err.println("warning: this clustercombination has a NaN critical center of bounds!");
        }
        this.centerOfBounds = center;
    }

    public double getCenterOfBounds(){
        return this.centerOfBounds;
    }

    public boolean isCCACandidate(){
        return this.isCCACandidate;
    };

    public double getShrunkUB(double shrinkFactor, double maxApproximationSize){
        if(this.getRadiiGeometricMean() < maxApproximationSize){
            return centerOfBounds + slack * shrinkFactor;
        }else{
            return UB;
        }

    }
}



